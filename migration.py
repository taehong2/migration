import mysql.connector 
import oracledb
import re
import sys
import unicodedata
oracledb.defaults.fetch_lobs = True

# 변수
table_name = 'XPD_CUSTOM_HIST_DTL'
oracle_username = 'altact'

# 연결 객체 초기화
mysql_conn = oracle_conn = None

try:
    # 1. MySQL 연결
    try:
        mysql_conn = mysql.connector.connect(
            host="182.162.96.167",
            port=3306,   
            user="memintgr",
            password="devpass12intgr#",
            database='memintgr'
        
        )
        mysql_cursor = mysql_conn.cursor()
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


    # 3. DDL 변환 함수
    def convert_mysql_to_oracle_ddl(mysql_ddl):
        ddl_lines = mysql_ddl.split('\n')
        oracle_lines = [f"CREATE TABLE {table_name} ("]
        primary_key = ""
        #comments = []
        column_comments = {}

        for line in ddl_lines[1:]:  # 기본키 처리
            line = line.strip().rstrip(',')
            if line.upper().startswith('PRIMARY KEY'):
                match = re.search(r'\((.*?)\)', line)
                if match:
                    pk_cols = match.group(1).replace('`', '').strip()
                    pk_constraint_name = f"PK_{table_name.upper()}"
                    primary_key = f"  CONSTRAINT {pk_constraint_name} PRIMARY KEY ({pk_cols})"
                else:
                    print(f"[WARNING] PRIMARY KEY 구문 파싱 실패: {line}")
                continue

            col_match = re.match(r'`(.+?)`\s+([^\s]+(?:\([^\)]*\))?)\s*(.*)', line)
            if col_match:
                col_name, col_type, rest = col_match.groups()
                print(f"col_type raw: '{col_type}'")
                col_type_upper = col_type.upper()
                if 'INT' in col_type_upper:
                    oracle_type = 'NUMBER'
                elif 'CHAR' in col_type_upper or 'TEXT' in col_type_upper:
                    size_match = re.search(r'\((\d+)\)', col_type)
                    size = size_match.group(1) if size_match else '255'
                    oracle_type = f"VARCHAR2({size})"
                elif 'DATE' in col_type_upper:
                    oracle_type = 'DATE'
                elif 'DECIMAL' in col_type_upper or 'NUMERIC' in col_type_upper:
                    size_match = re.search(r'\(\s*(\d+)\s*,\s*(\d+)\s*\)', col_type)
                    oracle_type = f"NUMBER({size_match.group(1)},{size_match.group(2)})" if size_match else 'NUMBER'
                else:
                    oracle_type = 'VARCHAR2(255)'
                
                #디폴트 처리
                disallowed_defaults = {
                'now()': 'TIMESTAMP',
                'current_timestamp': 'TIMESTAMP',
                'uuid()': 'RAW(16)',
                'getdate()': 'DATE',
                'curdate()': 'DATE',
                'newid()': 'RAW(16)',
                'sys_guid()': 'RAW(16)',
                }

                default_val = ''
                default_match = re.search(r'default\s+((?:\'[^\']*\'|"[^"]*"|[^\s,]+))', rest, re.IGNORECASE)
                if default_match:
                   val_raw = default_match.group(1)
                   val_clean = val_raw.strip().lower()
                   if val_clean.startswith('(') and val_clean.endswith(')'):
                        val_clean = val_clean[1:-1].strip()
                
                   if val_clean in disallowed_defaults:
                        oracle_type = disallowed_defaults[val_clean]
                   else:
                        default_val = f'DEFAULT {val_raw}'
               
                #not null처리
                not_null = 'NOT NULL' if 'not null' in rest.lower() else ''
                col_def = f'  {col_name} {oracle_type} {default_val} {not_null}'.strip()
                oracle_lines.append(col_def + ',')

                #comment처리
                comment_match = re.search(r'comment\s+\'(.*?)\'', rest, re.IGNORECASE)
                if comment_match:
                    column_comments[col_name.upper()] = comment_match.group(1)


                #oracle_lines.append(f'  {col_name} {oracle_type},')

        if primary_key:
        # 마지막 줄에 콤마가 있으면, 그 자리에 PK 추가
            if not oracle_lines[-1].endswith(','):
                oracle_lines[-1] += ','
            oracle_lines.append(f'  {primary_key}')
        else:
            # PK가 없을 경우, 마지막 줄 콤마만 제거
            if oracle_lines[-1].endswith(','):
                oracle_lines[-1] = oracle_lines[-1][:-1]

        oracle_lines.append(')')  
        oracle_ddl = '\n'.join(oracle_lines)


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
        
        comment_sqls = []
 
        for col, comment in column_comments.items():
            try:
             
                safe_comment = clean_comment(comment)

                # COMMENT 구문 생성
                comment_sql = (
                    f"COMMENT ON COLUMN {oracle_conn.username.upper()}."
                    f"{table_name}.{col} IS '{safe_comment}'"
                )
                comment_sqls.append(comment_sql)
            except Exception as e:
                print(f"[ERROR] '{col}' 컬럼 주석 처리 중 오류 발생: {e}")


        return '\n'.join(oracle_lines), comment_sqls
        
   
    # 4. MySQL DDL 추출
    try:
        mysql_cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
        _, create_stmt = mysql_cursor.fetchone()
        oracle_ddl, comment_sqls = convert_mysql_to_oracle_ddl(create_stmt)
        print("MySQL DDL 추출 성공")
    except Exception as e:
        print(f"[오류] MySQL 테이블 DDL 추출 실패: {e}")
        sys.exit(1)


    def extract_mysql_indexes(mysql_cursor, table_name):
        mysql_cursor.execute(f"SHOW INDEX FROM `{table_name}`")
        rows = mysql_cursor.fetchall()

        index_dict = {}
        for row in rows:
            key_name = row[2]
            non_unique = row[1]
            column_name = row[4]
            
            if key_name.upper() == 'PRIMARY' or non_unique == 0:
                continue

            if key_name not in index_dict:
                index_dict[key_name] = []
            index_dict[key_name].append(column_name)

        return index_dict
    
    def generate_oracle_index_ddls(index_dict, table_name):
        ddl_list = []
        for idx_name, columns in index_dict.items():
            col_list = ', '.join(columns)
            ddl = f"CREATE INDEX {idx_name.upper()} ON {table_name} ({col_list})"
            ddl_list.append(ddl)
        return ddl_list
    

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
        oracle_cursor.execute(oracle_ddl)
        print(f"Oracle 테이블 {table_name} 생성 완료")
        
    except Exception as e:
        print(f"[오류] Oracle 테이블 생성 중 실패: {e}")
        sys.exit(1)

        
    index_dict = extract_mysql_indexes(mysql_cursor, table_name)

    index_ddls = generate_oracle_index_ddls(index_dict, table_name)

    for ddl in index_ddls:
        print(f"[INFO] 인덱스 생성: {ddl}")
        oracle_cursor.execute(ddl)


    # 6. COMMENT 구문 실행
    try:
        for comment_sql in comment_sqls:
            print(f"{comment_sql}")
            oracle_cursor.execute(comment_sql)
        print("모든 COMMENT 구문 실행 완료")
    except Exception as e:
        print(f"[오류] COMMENT 구문 실행 중 실패: {e}")
        sys.exit(1)

    # 6. 데이터 이관
    try:
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(f"SELECT * FROM {table_name}")
        rows = mysql_cursor.fetchall()

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

    