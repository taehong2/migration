import mysql.connector 
import oracledb
import re
import sys
import unicodedata
oracledb.defaults.fetch_lobs = True

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple

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
            password="",
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
            password='',
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
        # - 컬럼이 문자열이면 RAWTOHEX(SYS_GUID())로 32자리 HEX 문자열로 맞춤
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

        # b'0'/b'1' 같은 비트 리터럴
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

        # unsigned 제거(Oracle엔 unsigned 없으므로)
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
            # MySQL tinyint(1)은 boolean 용도로 사용
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
    
    # class 정의 
    @dataclass
    class ColumnSpec:
        name: str
        mysql_type: str
        nullable: bool = True
        default_raw: Optional[str] = None
        default_clean: Optional[str] = None
        comment: Optional[str] = None
        extra: str = ""  # auto_increment 등 원문 보존(필요시)
    
    @dataclass
    class KeySpec:
        name: str
        cols: List[str]
        unique: bool

    @dataclass
    class TableSpec:
        schema: Optional[str]
        name: str
        columns: List[ColumnSpec] = field(default_factory=list)
        pk_cols: List[str] = field(default_factory=list)
        keys: List[KeySpec] = field(default_factory=list)  # unique/non-unique
        table_comment: Optional[str] = None
    
    # Oracle 타입으로 변환된 결과를 담을 클래스 정의
    @dataclass
    class OracleColumnSpec:
        name: str
        ora_type: str
        nullable: bool = True
        default_sql: str = ""      # "DEFAULT ...", 없으면 ""
        comment: Optional[str] = None

    @dataclass
    class OracleTableSpec:
        schema: Optional[str]
        name: str
        columns: List[OracleColumnSpec] = field(default_factory=list)
        pk_cols: List[str] = field(default_factory=list)
        uk_list: List[Tuple[str, List[str]]] = field(default_factory=list)   # (uk_name, cols)
        ix_list: List[Tuple[str, List[str]]] = field(default_factory=list)   # (ix_name, cols)
        table_comment: Optional[str] = None



    def parse_mysql_create_table(mysql_ddl: str, table_name: str, schema: str | None = None) -> TableSpec:
        # 1) 괄호 블록 추출
        start = mysql_ddl.find('(')
        if start < 0:
            raise ValueError("DDL에서 '('를 찾지 못함")

        depth, end = 0, None
        quote, esc = None, False

        for i in range(start, len(mysql_ddl)):
            ch = mysql_ddl[i]
            if quote:
                if esc:
                    esc = False
                elif ch == '\\':
                    esc = True
                elif ch == quote:
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

        def_block = mysql_ddl[start + 1:end]
        items = split_top_level(def_block)

        spec = TableSpec(schema=schema, name=table_name)

        for item in items:
            line = item.strip().rstrip(',')

            # PK
            if line.upper().startswith('PRIMARY KEY'):
                m = re.search(r'\((.*?)\)', line)
                if m:
                    cols = [c.strip().replace('`','') for c in m.group(1).split(',')]
                    spec.pk_cols = cols
                continue

            # KEY / UNIQUE KEY
            m = re.match(r'(?i)^(unique\s+)?key\s+`([^`]+)`\s*\((.*)\)\s*$', line)
            if m:
                is_unique = bool(m.group(1))
                key_name = m.group(2)
                cols_raw = m.group(3)

                cols = []
                for c in cols_raw.split(','):
                    c = c.strip().replace('`','')
                    c = re.sub(r'\(\s*\d+\s*\)$', '', c).strip()  # prefix len 제거
                    cols.append(c)
                spec.keys.append(KeySpec(name=key_name, cols=cols, unique=is_unique))
                continue

            # 컬럼 정의
            cm = re.match(r'`(.+?)`\s+([^\s]+(?:\([^\)]*\))?)\s*(.*)', line)
            if not cm:
                continue

            col_name, col_type, rest = cm.groups()

            default_raw, default_clean, rest_wo_default = parse_default(rest)  # helper 사용
            nullable = not ('not null' in rest_wo_default.lower())

            comment = None
            mcom = re.search(r"comment\s+'(.*?)'", rest_wo_default, re.IGNORECASE)
            if mcom:
                comment = mcom.group(1)

            spec.columns.append(ColumnSpec(
                name=col_name,
                mysql_type=col_type,
                nullable=nullable,
                default_raw=default_raw,
                default_clean=default_clean,
                comment=comment,
                extra=rest_wo_default
            ))

        return spec
    
    def normalize_cols(cols: List[str]) -> List[str]:
        return [c.strip().upper() for c in cols]

    def transform_to_oracle(spec: TableSpec) -> OracleTableSpec:
        ora = OracleTableSpec(schema=spec.schema, name=spec.name)

        # 컬럼 변환
        for c in spec.columns:
            ora_type = map_mysql_type_to_oracle(c.mysql_type)  # 네 함수 사용

            default_sql = ""
            if c.default_raw:
                default_sql, ora_type, warn = map_default_to_oracle(c.default_raw, c.default_clean, ora_type)
                # warn은 로깅만

            ora.columns.append(OracleColumnSpec(
                name=c.name.upper(),
                ora_type=ora_type,
                nullable=c.nullable,
                default_sql=default_sql,
                comment=c.comment
            ))

        ora.pk_cols = [x.upper() for x in spec.pk_cols]

        # KEY/UK 변환
        pk_norm = normalize_cols(ora.pk_cols)

        for k in spec.keys:
            cols_norm = normalize_cols(k.cols)

            if k.unique:
                # PK와 동일 컬럼셋이면 UK 스킵(ORA-02261 방지)
                if pk_norm and cols_norm == pk_norm:
                    continue
                uk_name = f"UK_{spec.name}_{k.name}".upper()
                ora.uk_list.append((uk_name[:30], cols_norm))
            else:
                ix_name = f"IX_{spec.name}_{k.name}".upper()
                ora.ix_list.append((ix_name[:30], cols_norm))

        return ora
    
    # OracleTableSpec → SQL 묶음 생성
    @dataclass
    class OracleSQLBundle:
        create_table: str
        pk_sql: Optional[str]
        uk_sqls: List[str]
        ix_sqls: List[str]
        comment_sqls: List[str]

    def emit_oracle_sql(ora: OracleTableSpec, oracle_username: str) -> OracleSQLBundle:
        full_table = ora.name.upper()

        lines = [f"CREATE TABLE {full_table} ("]
        for c in ora.columns:
            nn = "" if c.nullable else "NOT NULL"
            lines.append(f"  {c.name} {c.ora_type} {c.default_sql} {nn}".strip() + ",")

        pk_sql = None
        if ora.pk_cols:
            pk_name = f"PK_{ora.name}".upper()[:30]
            pk_cols = ", ".join([c.upper() for c in ora.pk_cols])
            pk_sql = f"  CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols})"
            lines.append(pk_sql)
        else:
            # 마지막 콤마 제거
            if lines[-1].endswith(","):
                lines[-1] = lines[-1][:-1]

        lines.append(")")

        create_table = "\n".join(lines)

        uk_sqls = [
            f"ALTER TABLE {full_table} ADD CONSTRAINT {uk_name} UNIQUE ({', '.join(cols)})"
            for uk_name, cols in ora.uk_list
        ]

        ix_sqls = [
            f"CREATE INDEX {ix_name} ON {full_table} ({', '.join(cols)})"
            for ix_name, cols in ora.ix_list
        ]

        comment_sqls = []
        for c in ora.columns:
            if c.comment:
                safe = clean_comment(c.comment)          # 네 함수
                safe = safe.replace("'", "''")           # Oracle 문자열 안전 처리(필수)
                comment_sqls.append(
                    f"COMMENT ON COLUMN {oracle_username.upper()}.{full_table}.{c.name} IS '{safe}'"
                )

        return OracleSQLBundle(
            create_table=create_table,
            pk_sql=pk_sql,
            uk_sqls=uk_sqls,
            ix_sqls=ix_sqls,
            comment_sqls=comment_sqls
        )

    def execute_bundle(oracle_cursor, oracle_conn, bundle: OracleSQLBundle):
        oracle_cursor.execute(bundle.create_table.rstrip(';'))

        # 권장: 데이터 적재 후 제약/인덱스 생성(대량 적재 성능)
        # 실행 순서만 보여주고 실행하지는 않음
        for sql in bundle.uk_sqls:
            oracle_cursor.execute(sql.rstrip(';'))
        for sql in bundle.ix_sqls:
            oracle_cursor.execute(sql.rstrip(';'))
        for sql in bundle.comment_sqls:
            oracle_cursor.execute(sql.rstrip(';'))

        oracle_conn.commit()

    def transfer_data(mysql_conn, oracle_cursor, oracle_conn, src_schema: str, table_name: str):
        cur = mysql_conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM `{src_schema}`.`{table_name}`")
        rows = cur.fetchall()
        if not rows:
            print("이관할 데이터가 없습니다.")
            return 0

        cols = list(rows[0].keys())
        col_str = ", ".join(cols)
        bind = ", ".join([f":{i+1}" for i in range(len(cols))])
        ins = f"INSERT INTO {table_name.upper()} ({col_str}) VALUES ({bind})"

        data = [tuple(r[c] for c in cols) for r in rows]
        oracle_cursor.executemany(ins, data)
        oracle_conn.commit()
        return len(rows)
    
    # 1) MySQL DDL 추출
    try:
        mysql_meta = mysql_conn.cursor()
        mysql_meta.execute(f"SHOW CREATE TABLE `{table_name}`")
        _, create_stmt = mysql_meta.fetchone()
        print("DDL 추출 완료")
    except Exception as e:
        print(f"MySQL DDL 추출 중 오류 발생] {e}")
        sys.exit(1)
    try:
    # 2) parse
        spec = parse_mysql_create_table(create_stmt, table_name=table_name, schema=None)
        print("parse 완료")
    except Exception as e:
        print(f"[parse 중 오류 발생]{e}")

    # 3) transform
    try:
        ora_spec = transform_to_oracle(spec)
        print("Oracle DDL로 변환 완료")
    except Exception as e:
        print(f"[Oracle DDL 변환 중 오류 발생]{e}")
    
    # 4) emit
    try:
        bundle = emit_oracle_sql(ora_spec, oracle_username=oracle_conn.username)
        print("emit 완료")
    except Exception as e:
        print(f"[Oracle SQL emit 중 오류 발생]{e}")

    # 5) execute: create table
    try:
        oracle_cursor.execute(bundle.create_table.rstrip(';'))
        oracle_conn.commit()
        print("table 생성 완료")
    except Exception as e:
        print(f"[table 생성중 오류 발생]{e}")


    # 6) transfer data
    try:
        transfer_data(mysql_conn, oracle_cursor, oracle_conn, src_schema="memintgr", table_name=table_name)
        print("테이블 이관 완료")
    except Exception as e:
        print(f"[테이블 이관 중 오류 발생]{e}")

    # 7) execute: constraints/index/comments
    try:
        for sql in bundle.uk_sqls: oracle_cursor.execute(sql.rstrip(';'))  
        print("constraint 생성 완료")  
    except Exception as e:
        print(f"[constraint 생성중 오류 발생]{e}")

    try:
        for sql in bundle.ix_sqls: oracle_cursor.execute(sql.rstrip(';'))    
        print("index 생성 완료")
    except Exception as e:
        print(f"[index 생성중 오류 발생]{e}")

    try:
        for sql in bundle.comment_sqls: oracle_cursor.execute(sql.rstrip(';'))    
        print("commnet 생성 완료")
    except Exception as e:
        print(f"[comment 생성중 오류 발생]{e}")

    oracle_conn.commit()
    print(f"table: {table_name} 이관완료")

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