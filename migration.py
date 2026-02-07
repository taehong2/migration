import mysql.connector 
import oracledb
import re
import sys
import unicodedata
oracledb.defaults.fetch_lobs = True

# 변수
oracle_username = 'altact'
table_name = 'ORDER_SETLE'


# 연결 객체 초기화
mysql_conn = oracle_conn = None

try:
    # 1. MySQL 연결
    try:
        mysql_conn = mysql.connector.connect(   #생성자를 통한 객체 정의
            host="182.162.96.167",  
            port=3306,   
            user="memintgr",
            password="devpass12intgr#",
            database='memintgr'
        
        )
        mysql_cursor = mysql_conn.cursor() # 객체를 커서로 초기화
        print("MySQL 연결 성공")
    except mysql.connector.Error as e:
        print(f"[오류] MySQL 연결 실패: {e}")
        sys.exit(1)

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

    def split_top_level(def_block: str) -> list[str]:
        items, buf = [], []
        depth = 0
        quote = None
        esc = False

        for ch in def_block:
            if quote:
                buf.append(ch)
                if esc:
                    esc = False
                elif ch == '\\':
                    esc = True
                elif ch == quote:
                    quote = None
                continue

            if ch in ("'", '"'):
                quote = ch
                buf.append(ch)
                continue

            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1

            if ch == ',' and depth == 0:
                item = ''.join(buf).strip()
                if item:
                    items.append(item)
                buf = []
            else:
                buf.append(ch)

        last = ''.join(buf).strip()
        if last:
            items.append(last)
        return items
    
    def clean_comment(comment):
            comment = str(comment)

            # 유니코드 제어 문자 및 보이지 않는 문자 제거
            blacklist = ['\u200b', '\ufeff', '\ufffd']
            comment = ''.join(
                ch for ch in comment
                if ch not in blacklist and unicodedata.category(ch)[0] != 'C'  # C: Other (제어 문자 포함)
            )

            # 따옴표 이스케이프
            comment = comment.replace("'", "''")
            # 줄바꿈, 탭, 여러 공백류를 단일 공백으로 정리
            comment = re.sub(r'\s+', ' ', comment)

            # 개행 및 캐리지리턴 제거
            return comment.strip()
    
    def _squeeze_spaces(s: str) -> str:
        return re.sub(r"\s+", " ", s).strip()
    
    def parse_default(rest: str):
        m = re.search(r"\bdefault\s+((?:'[^']*'|\"[^\"]*\"|[^\s,]+))", rest, re.IGNORECASE)
        if not m:
            return None, None, rest

        raw = m.group(1).strip()
        clean = raw.strip().lower()
        
        if clean.startswith("(") and clean.endswith(")"):
            clean = clean[1:-1].strip()

        rest_wo = (rest[:m.start()] + rest[m.end():]).strip()
        rest_wo = _squeeze_spaces(rest_wo)

        return raw, clean, rest_wo
    
    def map_default_to_oracle(default_raw: str, default_clean: str, oracle_type: str, allow_type_override: bool = False):
        warn = None
        default_sql = ""
        
        if default_clean in ("null",):
            return "", oracle_type, None
        
        fn_map = {
            "now()": "SYSTIMESTAMP",
            "current_timestamp": "SYSTIMESTAMP",
            "current_timestamp()": "SYSTIMESTAMP",
            "curdate()": "TRUNC(SYSDATE)",
            "sysdate()": "SYSDATE",
            "sysdate": "SYSDATE",
        }
            # uuid()/newid() 계열 처리
        # - Oracle SYS_GUID()는 RAW(16) 반환
        # - 컬럼이 문자열이면 RAWTOHEX(SYS_GUID())로 32자리 HEX 문자열로 맞추는 게 안전함
        if default_clean in ("uuid()", "uuid", "newid()", "newid", "sys_guid()", "sys_guid"):
            if "RAW" in oracle_type.upper():
                default_sql = "DEFAULT SYS_GUID()"
            else:
                # 문자열 컬럼에 안전한 기본값(32 hex)
                default_sql = "DEFAULT RAWTOHEX(SYS_GUID())"
                warn = f"DEFAULT {default_clean} → RAWTOHEX(SYS_GUID())로 치환함 (컬럼 타입={oracle_type})"
            return default_sql, oracle_type, warn

        if default_clean in fn_map:
            return f"DEFAULT {fn_map[default_clean]}", oracle_type, None

        # b'0'/b'1' 같은 비트 리터럴(자주 나옴)
        if default_clean in ("b'0'", "b'1'"):
            if "NUMBER" in oracle_type.upper():
                num = "0" if default_clean == "b'0'" else "1"
                return f"DEFAULT {num}", oracle_type, None
            warn = f"비트 리터럴 DEFAULT({default_raw})은 Oracle에서 직접 호환 어려움"
            return f"DEFAULT {default_raw}", oracle_type, warn

        # 숫자 문자열 '0', '0.00' 정규화(컬럼이 NUMBER면 따옴표 제거)
        if re.fullmatch(r"'[0-9]+(\.[0-9]+)?'", default_raw):
            if "NUMBER" in oracle_type.upper():
                num = default_raw.strip("'")
                return f"DEFAULT {num}", oracle_type, None
            # 문자 타입이면 그대로 유지
            return f"DEFAULT {default_raw}", oracle_type, None
        return f"DEFAULT {default_raw}", oracle_type, None
    
    def map_mysql_type_to_oracle(mysql_type: str) -> str:
        """
        MySQL 컬럼 타입 문자열을 Oracle 타입으로 매핑함.
        - 입력 예: int, bigint, varchar(50), char(1), text, decimal(12,2),
                numeric(10,0), date, datetime, timestamp(6), tinyint(1), float, double
        - 출력 예: NUMBER, VARCHAR2(50), CHAR(1), CLOB, NUMBER(12,2),
                DATE, TIMESTAMP(6), NUMBER(1), BINARY_FLOAT, BINARY_DOUBLE
        """
        if not mysql_type:
            return "VARCHAR2(255)"

        t = mysql_type.strip().lower()

        # unsigned 제거(Oracle엔 unsigned 없음 → 필요 시 체크 제약으로 별도 처리)
        t = re.sub(r"\bunsigned\b", "", t).strip()

        # zerofill 제거
        t = re.sub(r"\bzerofill\b", "", t).strip()

        # 타입명 + 괄호 파라미터 분리
        # ex) "decimal(12,2)" -> base="decimal", args="12,2"
        m = re.match(r"^([a-z]+)\s*(\((.*?)\))?$", t)
        if not m:
            return "VARCHAR2(255)"

        base = m.group(1)
        args = (m.group(3) or "").strip()

        # ---- 문자계열 ----
        if base in ("varchar", "nvarchar", "character varying"):
            size = args.split(",")[0].strip() if args else "255"
            return f"VARCHAR2({size})"

        if base in ("char", "nchar", "character"):
            size = args.split(",")[0].strip() if args else "1"
            return f"CHAR({size})"

        # text류 → CLOB
        if base in ("tinytext", "text", "mediumtext", "longtext"):
            return "CLOB"

        # ---- 숫자계열 ----
        if base in ("tinyint",):
            # MySQL tinyint(1)은 boolean 용도로 자주 씀
            if args.strip() == "1":
                return "NUMBER(1)"
            return "NUMBER(3)"

        if base in ("smallint",):
            return "NUMBER(5)"

        if base in ("mediumint",):
            return "NUMBER(7)"

        if base in ("int", "integer"):
            return "NUMBER(10)"

        if base in ("bigint",):
            return "NUMBER(19)"

        if base in ("decimal", "numeric"):
            if args and "," in args:
                p, s = [x.strip() for x in args.split(",", 1)]
                return f"NUMBER({p},{s})"
            if args:
                p = args.strip()
                return f"NUMBER({p})"
            return "NUMBER"

        # float/double 매핑(원하면 NUMBER로 통일해도 됨)
        if base in ("float",):
            return "BINARY_FLOAT"

        if base in ("double", "real", "double precision"):
            return "BINARY_DOUBLE"

        # ---- 날짜/시간 ----
        if base in ("date",):
            return "DATE"

        # MySQL datetime은 Oracle에서 DATE 또는 TIMESTAMP로 선택 가능
        # 운영/이관 관점에선 TIMESTAMP가 더 안전(초/소수점 보존)
        if base in ("datetime",):
            # datetime(6) 같은 케이스
            frac = args.split(",")[0].strip() if args else None
            if frac and frac.isdigit():
                return f"TIMESTAMP({frac})"
            return "TIMESTAMP(6)"

        if base in ("timestamp",):
            frac = args.split(",")[0].strip() if args else None
            if frac and frac.isdigit():
                return f"TIMESTAMP({frac})"
            return "TIMESTAMP(6)"

        if base in ("time",):
            # MySQL TIME은 날짜 없이 시간만 → Oracle은 DATE/TIMESTAMP로 직접 대응 애매
            # 보수적으로 VARCHAR2(8~15) 권장(또는 INTERVAL DAY TO SECOND 정책)
            return "VARCHAR2(15)"

        if base in ("year",):
            return "NUMBER(4)"

        # ---- 바이너리/LOB ----
        if base in ("blob", "tinyblob", "mediumblob", "longblob"):
            return "BLOB"

        if base in ("binary", "varbinary"):
            # 길이 있으면 RAW(n) 정도로 매핑 (최대 2000)
            if args and args.strip().isdigit():
                n = int(args.strip())
                n = min(n, 2000)
                return f"RAW({n})"
            return "BLOB"

        # ---- 기타 ----
        if base in ("json",):
            # Oracle 21c+면 JSON type도 가능하지만 버전 의존 → CLOB로 보수 매핑
            return "CLOB"

        if base in ("enum", "set"):
            # enum/set은 후보값 체크 제약이 필요함. 우선 VARCHAR2로 수용.
            size = "255"
            return f"VARCHAR2({size})"

        # 못 잡는 타입은 일단 varchar로
        return "VARCHAR2(255)"

    # 3. DDL 변환 함수
    def convert_mysql_to_oracle_ddl(mysql_ddl):
        # 0) CREATE TABLE (...) 괄호 블록(def_block) 추출 (괄호 매칭 + quote 처리)
        start = mysql_ddl.find('(')
        if start < 0:
            raise ValueError("DDL에서 '(' 를 찾지 못함")

        depth = 0
        end = None
        quote = None  # "'", '"', '`'
        esc = False

        for i in range(start, len(mysql_ddl)):
            ch = mysql_ddl[i]

            if quote:
                if esc:
                    esc = False
                    continue
                if ch == '\\':
                    esc = True
                    continue
                if ch == quote:
                    quote = None
                continue

            if ch in ("'", '"', '`'):
                quote = ch
                continue

            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    end = i
                    break

        if end is None:
            raise ValueError("DDL 괄호 매칭 실패")

        def_block = mysql_ddl[start + 1:end]  # 괄호 내부만

        # 1) 최상위 콤마 기준 분리
        items = split_top_level(def_block)

        oracle_lines = [f"CREATE TABLE {table_name} ("]
        primary_key = ""
        column_comments = {}
        unique_keys = []  
        indexes = []    # for Key
        
        for item in items:
            line = item.strip().rstrip(',')

            try:
                # 2) PRIMARY KEY 처리
                if line.upper().startswith('PRIMARY KEY'):
                    match = re.search(r'\((.*?)\)', line)
                    if match:
                        pk_cols = match.group(1).replace('`', '').strip()
                        pk_constraint_name = f"pk_{table_name.upper()}"
                        primary_key = f"  CONSTRAINT {pk_constraint_name} PRIMARY KEY ({pk_cols})"
                    else:
                        print(f"[WARNING] PRIMARY KEY 구문 파싱 실패: {line}")
                    continue
            except Exception as e:
                print(f"[ERROR] '{col}' PK 처리 중 오류 발생: {e}")

            try:  
                # 3) KEY / UNIQUE KEY 처리 (수집)
                m = re.match(r'(?i)^(unique\s+)?key\s+`([^`]+)`\s*\((.*)\)\s*$', line)
                if m:
                    is_unique = bool(m.group(1))
                    key_name = m.group(2)
                    cols_raw = m.group(3)

                    cols = []
                    for c in cols_raw.split(','):
                        c = c.strip()
                        c = c.replace('`', '').strip()

                        # col(10) 같은 prefix 길이(인덱스 prefix) 제거
                        c = re.sub(r'\(\s*\d+\s*\)$', '', c).strip()

                        cols.append(c)

                    if is_unique:
                        unique_keys.append((key_name, cols))
                    else:
                        indexes.append((key_name, cols))
                    continue
            except Exception as e:
                print(f"[ERROR] '{col}' KEY/UK 처리 중 오류 발생: {e}")

            # 4) 컬럼 파싱
            col_match = re.match(r'`(.+?)`\s+([^\s]+(?:\([^\)]*\))?)\s*(.*)', line)
            if not col_match:
                # ENGINE/CHARSET/COMMENT 같은 테이블 옵션이 섞이면 무시
                continue

            col_name, col_type, rest = col_match.groups()

            # 5) 타입 매핑(함수로 교체)
            oracle_type = map_mysql_type_to_oracle(col_type)

            # 6) DEFAULT 처리
            default_val = ''
            default_raw, default_clean, rest_wo_default = parse_default(rest)
            if default_raw:
                default_val, oracle_type, warn = map_default_to_oracle(default_raw, default_clean, oracle_type)
                if warn:
                    print(f"[WARN] {table_name}.{col_name} - {warn}")
            else:
                rest_wo_default = rest

            # 7) NOT NULL 처리( DEFAULT 제거된 rest 기준 )
            not_null = 'NOT NULL' if 'not null' in rest_wo_default.lower() else ''

            # 8) COMMENT 처리( DEFAULT 제거된 rest 기준 )
            comment_match = re.search(r"comment\s+'(.*?)'", rest_wo_default, re.IGNORECASE)
            if comment_match:
                column_comments[col_name.upper()] = comment_match.group(1)

            col_def = f"{col_name} {oracle_type} {default_val} {not_null}".strip()
            oracle_lines.append(col_def + ',')

        # 9) PK 붙이기 / 마지막 콤마 정리
        if primary_key:
            # 마지막 컬럼 라인이 콤마로 끝나야 constraint 추가 가능
            if oracle_lines and not oracle_lines[-1].endswith(','):
                oracle_lines[-1] += ','
            oracle_lines.append(primary_key)
        else:
            # PK 없으면 마지막 줄 콤마 제거
            if len(oracle_lines) > 1 and oracle_lines[-1].endswith(','):
                oracle_lines[-1] = oracle_lines[-1][:-1]

        oracle_lines.append(');')

        uk_sqls = []
        for uk_name, cols in unique_keys:
            cols_sql = ", ".join(cols)
                # 제약명은 30자 제한 걸릴 수 있음(필요하면 축약 함수 추가)
            uk_sqls.append(
                f"ALTER TABLE {table_name} ADD CONSTRAINT {uk_name.upper()} UNIQUE ({cols_sql})"
            )

        index_sqls = [] # for KEY
        for idx_name, cols in indexes:
            cols_sql = ", ".join(cols)
            index_sqls.append(
                f"CREATE INDEX {idx_name.upper()} ON {table_name} ({cols_sql})"
            )

        # 10) COMMENT SQL 생성
        comment_sqls = []
        for col, comment in column_comments.items():
            try:
                safe_comment = clean_comment(comment)
                comment_sql = (
                    f"COMMENT ON COLUMN {oracle_conn.username.upper()}."
                    f"{table_name}.{col} IS '{safe_comment}'"
                )
                comment_sqls.append(comment_sql)
            except Exception as e:
                print(f"[ERROR] '{col}' 컬럼 주석 처리 중 오류 발생: {e}")

        return '\n'.join(oracle_lines), comment_sqls, uk_sqls, index_sqls
        
   
    # 4. MySQL DDL 추출
    try:
        mysql_cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
        _, create_stmt = mysql_cursor.fetchone()
        oracle_ddl, comment_sqls, uk_sqls, index_sqls = convert_mysql_to_oracle_ddl(create_stmt)
        print("MySQL DDL 추출 성공")
    except Exception as e:
        print(f"[오류] MySQL 테이블 DDL 추출 실패: {e}")
        sys.exit(1)

    # 5. Oracle 테이블 생성
    try:
    # 테이블 존재 여부 확인 (대소문자 구분주의)
        oracle_cursor.execute(f"""
        SELECT COUNT(*) 
        FROM user_tables 
        WHERE table_name = '{table_name.upper()}'
        """)
        table_exists = oracle_cursor.fetchone()[0] > 0
        if table_exists:
            print(f"[오류] Oracle에 이미 테이블 '{table_name}' 이(가) 존재합니다. 시스템 종료합니다.")
            sys.exit(1)
    # 존재하지 않으면 생성
        print(f"Oracle에 테이블 '{table_name}' 를 새롭게 생성합니다.")
        print("-- 생성할 Oracle DDL --")
        print(oracle_ddl)
        oracle_cursor.execute(oracle_ddl.rstrip().rstrip(';'))      ####
        print(f"Oracle 테이블 {table_name} 생성 완료")
        
    except Exception as e:
        print(f"[오류] Oracle 테이블 생성 중 실패: {e}")
        sys.exit(1)

    
    print(f"[INFO] index_sqls={len(index_sqls)}, comment_sqls={len(comment_sqls)}")  
      
    # 6. 데이터 이관
    try:
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        mysql_cursor.execute("SELECT DATABASE() AS db")
        print("[DB]", mysql_cursor.fetchone())

        mysql_cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        print("[CNT]", (mysql_cursor.fetchone()))

        mysql_cursor.execute(f"SELECT * FROM `{table_name}`")
        rows = mysql_cursor.fetchall()
        print("[FETCHED ROWS]", len(rows))

        if rows:
            columns = rows[0].keys()
            col_str = ', '.join(columns)
            bind_vars = ', '.join([f':{i+1}' for i in range(len(columns))])
            insert_sql = f"INSERT INTO {table_name} ({col_str}) VALUES ({bind_vars})"
            data = [tuple(row[col] for col in columns) for row in rows]
            oracle_cursor.executemany(insert_sql, data)
            oracle_conn.commit()
            print(f"{len(rows)} rows 이관 완료.")
        else:
            print("이관할 데이터가 없습니다.")
    except Exception as e:
        print(f"[오류] 데이터 이관 실패: {e}")
        sys.exit(1)

    # index 구문 실행
    try:
        for ddl in index_sqls:
            print(f"[INFO] 인덱스 생성: {ddl}")
            oracle_cursor.execute(ddl)
    except Exception as e:
        print(f"[오류] Index 구문 실행 중 실패: {e}")
        sys.exit(1)

    # uk 구문 실행
    try:
        for ddl in uk_sqls:
            print(f"[INFO] UK 생성: {ddl}")
            oracle_cursor.execute(ddl)
    except Exception as e:
        print(f"[오류] UK 구문 실행 중 실패: {e}")
        
    # 6. COMMENT 구문 실행
    try:
        print("comment 구문 실행:")
        for comment_sql in comment_sqls:
            print(f"{comment_sql}")
            oracle_cursor.execute(comment_sql)
        print("모든 COMMENT 구문 실행 완료")
    except Exception as e:
        print(f"[오류] COMMENT 구문 실행 중 실패: {e}")
        sys.exit(1)    

except Exception as e:
    print(f"[예기치 못한 오류] {e}")
    sys.exit(1)

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

    