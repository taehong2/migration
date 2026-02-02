import oracledb
import re
import sys
import os
oracledb.defaults.fetch_lobs = True

# 변수

#oracle_username = 'altact'

# 연결 객체 초기화
mysql_conn = oracle_conn = None

try:
    # 2. Oracle 연결
    try:
        oracledb.init_oracle_client(lib_dir=r"C:\instantclient_19_26")
        dsn = oracledb.makedsn("182.162.96.167", 1551, sid="mvno")
        oracle_conn = oracledb.connect(
            user='altact',
            password='Altact!21c',
            dsn=dsn
        )
        oracle_cursor = oracle_conn.cursor()
        print("Oracle 연결 성공")
    except oracledb.Error as e:
        print(f"[오류] Oracle 연결 실패: {e}")
        sys.exit(1)


     #ddl추출
    target_schema = "ALTACT" ##        지정필요
    DEFAULT_INDEX_TS = "ALTACT_IX"
    #target_table = "XPD_CUSTOM_HIST_DTL" 
    #cursor = oracle_conn.cursor()

    def get_ddl_text(cursor, obj_type, name, owner):
        cursor.execute("""
            SELECT DBMS_LOB.GETLENGTH(DBMS_METADATA.GET_DDL(:obj_type, :name, :owner))
            FROM dual
        """, {"obj_type": obj_type, "name": name, "owner": owner})
        row = cursor.fetchone()
        if not row or row[0] is None:
            return ""
        total = int(row[0])

        parts, pos, chunk = [], 1, 4000
        while pos <= total:
            cursor.execute("""
                SELECT DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DDL(:obj_type, :name, :owner), :chunk, :pos)
                FROM dual
            """, {"obj_type": obj_type, "name": name, "owner": owner,
                "chunk": chunk, "pos": pos})
            s = cursor.fetchone()[0]
            if not s:
                break
            parts.append(s)
            pos += chunk
        return "".join(parts)
    
    def format_constraint_using_index(ddl: str) -> str:
        if "USING INDEX" not in ddl:
            return ddl

        ddl = ddl.replace("USING INDEX", "\n    USING INDEX")

        keywords = ["NOLOGGING", "LOGGING", "PCTFREE", "STORAGE", "TABLESPACE"]
        for kw in keywords:
            ddl = re.sub(rf"\b{kw}\b", f"\n    {kw}", ddl, flags=re.IGNORECASE)

        return ddl
        
    # --- 혹시 섞여 들어온 테이블 레벨 제약을 문자열에서 제거 (보조 안전장치) ---
    def strip_table_constraints(table_ddl: str) -> str:
        ddl = re.sub(
            r'(\s*,\s*)?(CONSTRAINT\s+[^,()]+(?:\([^()]*\))?(?:\s+USING\s+INDEX[^,()]*)?|PRIMARY\s+KEY\s*\([^)]*\)(?:\s+USING\s+INDEX[^,()]*)?)',
            '', table_ddl, flags=re.IGNORECASE | re.DOTALL
        )
        ddl = re.sub(r',\s*\)', '\n)', ddl)  # 닫는 괄호 앞 마지막 콤마 정리
        return ddl
    
    def pick_index_ts(cur, table_name, fallback=DEFAULT_INDEX_TS):
        """
        테이블의 TABLESPACE를 조회하여 반환.
        - 테이블스페이스가 존재하면 그대로 반환
        - 존재하지 않으면 fallback 값 반환
        """
        cur.execute("""
            SELECT tablespace_name
            FROM user_tables
            WHERE table_name = :t
        """, {"t": table_name.upper()})
        
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return fallback
    
    def build_using_index_clause(cur, table_name, cons_name):
        """
        PK/UNIQUE 제약이 현재 참조하는 인덱스의 속성으로
        USING INDEX ( PCTFREE .. NOLOGGING .. STORAGE(..) TABLESPACE .. ) 절을 만들어 반환.
        값이 없는 항목은 생략.
        """
        # 제약이 참조하는 인덱스명
        cur.execute("""
            SELECT index_name
            FROM user_constraints
            WHERE table_name = :t AND constraint_name = :c
        """, {"t": table_name.upper(), "c": cons_name})
        row = cur.fetchone()
        idx_name = row[0] if row else None

        pctfree = None; logging = None; tbs = None
        initial = None; nxt = None; minext = None; maxext = None

        if idx_name:
            # 기본 속성
            cur.execute("""
                SELECT pct_free,
                    DECODE(logging, 'YES','LOGGING','NOLOGGING') AS logging,
                    tablespace_name
                FROM user_indexes
                WHERE index_name = :i
            """, {"i": idx_name})
            r = cur.fetchone()
            if r:
                pctfree, logging, tbs = r

            # 저장소(세그먼트) 속성
            cur.execute("""
                SELECT initial_extent, next_extent, min_extents, max_extents
                FROM user_segments
                WHERE segment_name = :i AND segment_type = 'INDEX'
            """, {"i": idx_name})
            r = cur.fetchone()
            if r:
                initial, nxt, minext, maxext = r

        # 절 조립
        parts = []
        if pctfree is not None:
            parts.append(f"PCTFREE {pctfree}")
        if logging:
            parts.append(logging)  # LOGGING 또는 NOLOGGING

        storage_parts = []
        if initial: storage_parts.append(f"INITIAL {initial}")
        if nxt:     storage_parts.append(f"NEXT {nxt}")
        if minext:  storage_parts.append(f"MINEXTENTS {minext}")
        if maxext:  storage_parts.append(f"MAXEXTENTS {maxext}")
        if storage_parts:
            parts.append("STORAGE(" + " ".join(storage_parts) + ")")

        if tbs:
            parts.append(f"TABLESPACE {tbs}")

        # 인덱스 이름은 명시하지 않고 옵션만 -> 오라클이 동일 옵션으로 인덱스 생성
        return "USING INDEX (" + " ".join(parts) + ")" if parts else ""

    def ensure_tablespace_in_index_ddl(idx_ddl: str, tablespace: str) -> str:
        """
        CREATE [UNIQUE] INDEX … 구문에 TABLESPACE 절이 **없을 때만** 추가.
        (이미 있으면 그대로 반환)
        """
        if not idx_ddl:
            return idx_ddl

        # 이미 TABLESPACE가 있으면 그대로
        if "TABLESPACE" in idx_ddl.upper():
            return idx_ddl

        # 세미콜론 위치
        ddl = idx_ddl.rstrip()
        semipos = ddl.rfind(";")
        if semipos == -1:
            # 세미콜론이 없다면 끝에 추가
            return ddl + f"\nTABLESPACE {tablespace};\n"

        # 세미콜론 앞에 TABLESPACE 삽입
        before = ddl[:semipos].rstrip()
        after = ddl[semipos:]  # 보통 ';'
        return f"{before}\nTABLESPACE {tablespace}{after}\n"

    def set_metadata_transforms(cur):
        try:
            cur.execute("""
            BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'DEFAULT');
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'EMIT_SCHEMA',TRUE);
            END;""")
        except Exception as e:
            print(f"[경고] DBMS_METADATA 세션 파라미터 설정 실패(무시): {e}")

    base_dir = r"C:\project\venv\ALTACT"
    os.makedirs(base_dir, exist_ok=True)
    tables_ddl_file      = os.path.join(base_dir, "tables_ddl2.sql")
    indexes_ddl_file     = os.path.join(base_dir, "indexes_ddl2.sql")
    constraints_ddl_file = os.path.join(base_dir, "constraints_ddl2.sql")

    try:
        oracle_cursor.execute("""
            SELECT table_name
            FROM all_tables
            WHERE owner = :owner
            ORDER BY table_name
        """, [target_schema.upper()])
        tables = [r[0] for r in oracle_cursor.fetchall()]
        print(f"[INFO] 테이블 {len(tables)}개 조회 완료.")
    except Exception as e:
        print(f"[오류] 테이블 목록 조회 실패: {e}")
        tables = []  # 이후 블록이 빈 목록으로 안전하게 스킵되도록


    # ---------------------------------------------------
    # 1) TABLES (CREATE TABLE만, 제약 없음) + COLUMN COMMENT
    # ---------------------------------------------------
    try:
        with open(tables_ddl_file, "w", encoding="utf-8") as ft:
            for table_name in tables:
                oracle_cursor.execute("""
                BEGIN
                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',        FALSE);
                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS',    FALSE);
                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',            FALSE);
                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES', TRUE);
                -- DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',     TRUE);
                END;""")
                
                # CREATE TABLE (제약 제외)
                ddl = get_ddl_text(oracle_cursor, 'TABLE', table_name.upper(), target_schema.upper())
                ddl = strip_table_constraints(ddl) 

                ft.write(f"-- =============================================\n")
                ft.write(f"-- TABLE: {target_schema}.{table_name}\n")
                ft.write(f"-- =============================================\n")
                ft.write(ddl + "\n\n")

                 # 2) TABLE COMMENT (있을 때만)
                oracle_cursor.execute("""
                    SELECT comments
                    FROM all_tab_comments
                    WHERE owner = :owner
                    AND table_name = :tname
                """, {"owner": target_schema.upper(), "tname": table_name.upper()})
                tbl_cmt_row = oracle_cursor.fetchone()
                if tbl_cmt_row:
                    tbl_comment = tbl_cmt_row[0]
                    if tbl_comment:  # NULL/빈값 제외
                        safe_tbl_cmt = tbl_comment.replace("'", "''")
                        ft.write(
                            f"COMMENT ON TABLE {target_schema}.{table_name} "
                            f"IS '{safe_tbl_cmt}';\n"
                        )

                # COLUMN COMMENTS (헤더 없이)
                oracle_cursor.execute("""
                    SELECT column_name, comments
                    FROM all_col_comments
                    WHERE owner = :owner
                    AND table_name = :tname
                    AND comments IS NOT NULL
                    ORDER BY column_name
                """, {"owner": target_schema.upper(), "tname": table_name.upper()})
                rows = oracle_cursor.fetchall()
                for col, comment in rows:
                    safe = (comment or "").replace("'", "''")
                    ft.write(
                        f"COMMENT ON COLUMN {target_schema}.{table_name}.{col} "
                        f"IS '{safe}';\n"
                    )
                if tbl_cmt_row or rows:
                    ft.write("\n")
        print(f"[완료] 테이블 DDL 파일 생성: {tables_ddl_file}")
    except Exception as e:
        print(f"[경고] 테이블 DDL 저장 중 오류: {e}")


    # ---------------------------------------------------
    # 2) INDEXES (NONUNIQUE만, PK/UNIQUE 제외)
    # ---------------------------------------------------
    # 2) INDEXES: NONUNIQUE + (PK/UNIQUE용 UNIQUE 인덱스 생성)
    try:
        with open(indexes_ddl_file, "w", encoding="utf-8") as fi:
            set_metadata_transforms(oracle_cursor)

            for table_name in tables:
                out = []  # 이 테이블에서 쓸 라인들 임시 보관

                # 1) NONUNIQUE (제약 인덱스 제외)
                oracle_cursor.execute("""
                    SELECT ui.index_name
                    FROM user_indexes ui
                    WHERE ui.table_owner = :owner
                    AND ui.table_name  = :tname
                    AND ui.generated   = 'N'
                    AND ui.uniqueness  = 'NONUNIQUE'
                    AND NOT EXISTS (
                            SELECT 1
                            FROM user_constraints uc
                            WHERE uc.owner           = ui.table_owner
                                AND uc.table_name      = ui.table_name
                                AND uc.index_name      = ui.index_name
                                AND uc.constraint_type IN ('P','U')
                        )
                    ORDER BY ui.index_name
                """, {"owner": target_schema.upper(), "tname": table_name.upper()})

                for (idx_name,) in oracle_cursor.fetchall():
                    raw = get_ddl_text(oracle_cursor, 'INDEX', idx_name, target_schema.upper())
                    ts  = pick_index_ts(oracle_cursor, table_name)
                    ddl = ensure_tablespace_in_index_ddl(raw, ts)
                    out.append(f"-- {target_schema}.{table_name} {idx_name}\n{ddl}")

                # 1-2) 사용자 정의 UNIQUE (제약 인덱스 제외)
                oracle_cursor.execute("""
                    SELECT ui.index_name
                    FROM user_indexes ui
                    WHERE ui.table_owner = :owner
                    AND ui.table_name  = :tname
                    AND ui.generated   = 'N'
                    AND ui.uniqueness  = 'UNIQUE'
                    AND NOT EXISTS (
                            SELECT 1
                            FROM user_constraints uc
                            WHERE uc.owner           = ui.table_owner
                                AND uc.table_name      = ui.table_name
                                AND uc.index_name      = ui.index_name
                                AND uc.constraint_type IN ('P','U')
                        )
                    ORDER BY ui.index_name
                """, {"owner": target_schema.upper(), "tname": table_name.upper()})

                for (idx_name,) in oracle_cursor.fetchall():
                    raw = get_ddl_text(oracle_cursor, 'INDEX', idx_name, target_schema.upper())
                    ts  = pick_index_ts(oracle_cursor, table_name)
                    ddl = ensure_tablespace_in_index_ddl(raw, ts)
                    out.append(f"-- {target_schema}.{table_name} {idx_name} (user-defined UNIQUE)\n{ddl}")

                # === 이 테이블에 기록할 게 있을 때만 쓰기 ===
                if out:
                    fi.write("\n".join(out))
                    fi.write("\n\n")  # 테이블 간 구분 1줄만

  

        print(f"[완료] 인덱스 DDL 파일 생성: {indexes_ddl_file}")

    except Exception as e:
        print(f"[경고] 인덱스 DDL 저장 중 오류: {e}")


    # ---------------------------------------------------
    # 3) CONSTRAINTS (PK/UNIQUE만)  ※ FK/CHECK 원하면 말해줘서 추가
    #    GET_DDL 사용하지 않고 직접 조립 → LOB/네트워크 이슈 방지
    # ---------------------------------------------------
    try:
        with open(constraints_ddl_file, "w", encoding="utf-8") as fc:
            for table_name in tables:
                # PK / UNIQUE 만
                oracle_cursor.execute("""
                    SELECT constraint_name, constraint_type
                    FROM user_constraints
                    WHERE table_name = :tname
                    AND constraint_type IN ('P','U')
                    ORDER BY constraint_name
                """, {"tname": table_name.upper()})
                rows = oracle_cursor.fetchall()

                for cname, ctype in rows:
                    # 컬럼 리스트 (순서 보장)
                    oracle_cursor.execute("""
                        SELECT column_name
                        FROM user_cons_columns
                        WHERE constraint_name = :cname
                        AND table_name      = :tname
                        ORDER BY position
                    """, {"cname": cname, "tname": table_name.upper()})
                    cols = [r[0] for r in oracle_cursor.fetchall()]
                    col_list = ", ".join(cols)

                    using_clause = build_using_index_clause(oracle_cursor, table_name, cname)

                    if ctype == 'P':
                        ddl = (f"ALTER TABLE {target_schema}.{table_name} "
                            f"ADD CONSTRAINT {cname} PRIMARY KEY ({col_list})")
                    else:
                        ddl = (f"ALTER TABLE {target_schema}.{table_name} "
                            f"ADD CONSTRAINT {cname} UNIQUE ({col_list})")

                    if using_clause:
                        ddl += " " + using_clause
                    ddl += ";"
                    
                    ddl = format_constraint_using_index(ddl)

                    fc.write(ddl + "\n")

                if rows:
                    fc.write("\n")

        print(f"[완료] 제약조건 DDL 파일 생성( PK/UNIQUE, USING INDEX 옵션 포함 ): {constraints_ddl_file}")

    except Exception as e:
        print(f"[경고] 제약조건 DDL 저장 중 오류: {e}")
    

finally:
    # 7. 연결 종료
    try:
        if mysql_conn:
            mysql_conn.close()
        if oracle_conn:
            oracle_conn.close()
        print("DB 연결 종료 완료")
    except Exception as e:
        print(f"[경고] 연결 종료 중 오류 발생: {e}")

    