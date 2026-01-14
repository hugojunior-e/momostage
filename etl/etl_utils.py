import oracledb
import mysql.connector
import json
import time
import uuid
import os
import requests
import snowflake.connector
import redshift_connector
import ibm_db
import multiprocessing
import functools
from datetime import datetime

oracledb.init_oracle_client()#lib_dir="/opt/oracle/instantclient_21_12")

CONSTANT_OWNER = "DWADM"

CONSTANT_SQL_JOB_BATCH_PARAMETERS = f"""
    select nvl(parameters,'-')
      from {CONSTANT_OWNER}.ms_job_batch
     where id = '%s'
       and job_name = '%s'
"""

CONSTANT_SQL_JOB_BATCH_STATUS_UPDATE = f"""
  declare
    v_id         number        := '<id>';
    v_status     varchar2(100) := '<status>';
    v_job_name   varchar2(100) := '<job_name>';
    v_created_by varchar2(100);
  begin
    update {CONSTANT_OWNER}.ms_job_batch
       set status = v_status,
           started_at  = case when v_status = 'E'        then sysdate else started_at  end,
           finished_at = case when v_status in ('F','A') then sysdate else finished_at end         
     where id = v_id
       and job_name = v_job_name
     returning created_by into v_created_by;

    if ( v_created_by = 'ALGARSCHEDULER' and v_status = 'A' ) then

      update {CONSTANT_OWNER}.ms_job_batch
         set status = 'AC'
       where rowid in (  
                       select b1.rowid
                         from {CONSTANT_OWNER}.ms_job_batch b1, 
                              {CONSTANT_OWNER}.ms_job_sequence s1,
                              (
                                    select order_pri, order_sec
                                      from {CONSTANT_OWNER}.ms_job_batch b, 
                                           {CONSTANT_OWNER}.ms_job_sequence s
                                     where b.id = v_id
                                       and b.job_name = v_job_name
                                       and s.SEQ_NAME = b.MANAGED_BY       
                                       and s.job_name = b.job_name
                              ) od
                        where b1.id         = v_id
                          and b1.STATUS     = 'P'
                          and s1.SEQ_NAME   = b1.MANAGED_BY
                          and s1.job_name   = b1.job_name
                          and s1.ORDER_PRI  = od.order_pri
                          and s1.ORDER_SEC >= od.order_sec
                     );      
    end if;
    commit;   
  end;     
"""

CONSTANT_SQL_JOB_BATCH_CREATE = f"""
declare
  v_id number;
begin
  SELECT {CONSTANT_OWNER}.ms_job_batch_seq.nextval
    INTO v_id
    FROM dual;
  
  insert into {CONSTANT_OWNER}.ms_job_batch(id, job_name, created_at, created_by, managed_by, parameters, status)
  values (
    v_id,
    '%s',
    sysdate,
    '%s',
    '%s',
    '%s',
    'P'
  );
  commit;
  :id := v_id;
end;
"""

CONSTANT_TRANSFORMS = """
def C_TRANS(records, lookups):
  C_OUTPUT = []
  for record in records:
    if %s :
%s
      C_OUTPUT.append( [%s
      ])   
  return C_OUTPUT
"""

CONSTANT_SQL_JOB = f"""
SELECT origin,
       target,
       hasheds,
       jsh
  FROM {CONSTANT_OWNER}.ms_job_def 
 where job_name = '%s'
   and reg_sts = 1
"""


CONSTANT_SQL_JOB_TRANSF = f"""
SELECT transf,
       filter,
       lookups
  FROM {CONSTANT_OWNER}.ms_job_transf 
 where job_name = '%s'
   and reg_sts = 1
 order by job_order
"""

CONSTANT_QTD_THREADS = 5
CONSTANT_TIMEOUT_THREAD = 300  # segundos

AS_GERA_LOTE = f"""
DECLARE
  v_id        NUMBER;
  v_pars      VARCHAR2(2000);
  v_key_value VARCHAR2(200);
  v_comma     NUMBER;
BEGIN
  SELECT {CONSTANT_OWNER}.ms_job_batch_seq.nextval
    INTO v_id
    FROM dual;

  INSERT INTO {CONSTANT_OWNER}.ms_job_batch (
    id,
    job_name,
    created_at,
    created_by,
    managed_by,
    parameters,
    status
  )
    SELECT v_id,
           job_name,
           sysdate,
           'ALGARSCHEDULER',
           seq_name,
           NULL,
           'P'
      FROM {CONSTANT_OWNER}.ms_job_sequence s
     WHERE s.SEQ_NAME = '$AGRUPAMENTO$'
       AND fl_active = 1;

  FOR cx IN ( SELECT s.PARAMETERS,
                     b.rowid,
                     def.PARAMETERS keys
               FROM {CONSTANT_OWNER}.ms_job_batch b,
                    {CONSTANT_OWNER}.ms_job_sequence s,
                    {CONSTANT_OWNER}.ms_job_def def
              WHERE b.id = v_id
                AND s.seq_name   = b.MANAGED_BY
                AND s.job_name   = b.JOB_NAME
                AND def.JOB_NAME = b.JOB_NAME
                and def.reg_sts  = 1
                AND s.PARAMETERS IS NOT NULL
                AND def.PARAMETERS IS NOT NULL
            ) LOOP
    v_pars  := chr(123) || chr(10);
    v_comma := 0;
    FOR k IN ( SELECT REGEXP_SUBSTR(cx.keys, '[^' || CHR(10) || ']+', 1, LEVEL) AS kkey
                 FROM dual CONNECT BY
                 LEVEL <= REGEXP_COUNT(cx.keys, CHR(10)) + 1
             ) 
    LOOP
      EXECUTE IMMEDIATE json_value(cx.parameters,'$.' || k.kkey)
        INTO v_key_value;
        
      IF ( v_comma = 1 ) THEN
        v_pars := v_pars || ',';
      END IF;
      
      v_pars  := v_pars || '"' || k.kkey || '":"' || v_key_value || '"';
      v_comma := 1;
    END LOOP;

    v_pars  := v_pars || chr(125);
    UPDATE {CONSTANT_OWNER}.ms_job_batch
       SET PARAMETERS = v_pars
     WHERE ROWID = cx.rowid;
  END LOOP;
  COMMIT;

  :id := v_id;
END;
"""


AS_VERIFICA_LOTE = f"""
select decode(count(1), 0, 'Finaliza', 'Aguarda') as retorno
  from {CONSTANT_OWNER}.ms_job_batch a
 where a.id = $p_lote$
   and a.status in ('P')
"""


AS_JOBS_AGENDA = f"""
select job_name, R_ID from
(
    select job_name, 
           r_id ,
           id,
           row_number() over(partition by id order by id, seq_name, order_pri, order_sec) LINHA
      from (
            select b.*,
                   b.rowid r_id,
                   seq_name, order_pri, order_sec,
                  row_number() over(partition by id, s.seq_name, order_pri order by id, s.seq_name, order_pri, order_sec) executar
              from {CONSTANT_OWNER}.ms_job_batch b, 
                   {CONSTANT_OWNER}.ms_job_sequence s
             where id = $p_lote$
               and s.seq_name = b.MANAGED_BY
               and s.job_name = b.JOB_NAME
               and b.STATUS = 'P'
               and not exists ( select 1
                                  from {CONSTANT_OWNER}.ms_job_batch     r2,
                                       {CONSTANT_OWNER}.ms_job_sequence  j2
                                  where r2.job_name(+) = j2.job_name
                                    and r2.id = b.id
                                    and j2.order_pri = s.order_pri
                                    AND j2.order_sec < s.order_sec --
                                    and r2.status <> 'F')   
           ) vw where executar = 1        
) vw2
  where linha <= (
                    (select  to_number( nvl( max(param_value) , '20' ) )
                       from {CONSTANT_OWNER}.ms_job_globals
                      where group_name = 'PARAMETERS'
                        AND param_name = 'QTD_INST_AS') 
                    - 
                    (select count(1)
                       from {CONSTANT_OWNER}.ms_job_batch rr
                      where rr.id = vw2.id
                        and rr.status = 'E')
                 )
"""

AS_VERIFICA_PENDENTES = f"""
  select count(1) from {CONSTANT_OWNER}.ms_job_batch where id = $p_lote_id$ and status not in ('K', 'F')
"""

CONSTANT_GLOBALS_parameters = {}

#=======================================================================================================
#
#=======================================================================================================

def local_db():
   x = oracledb.connect(dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=exa03-scan-stg.network.ctbc)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DWHOM)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))", user="dwadm", password="dwtst")
   return x

#=======================================================================================================
#
#=======================================================================================================

def load_globals():
  global CONSTANT_GLOBALS_parameters
  db  = local_db()
  cur = db.cursor()
  cur.execute( f"SELECT group_name, param_name, param_value FROM {CONSTANT_OWNER}.ms_job_globals" )
  dados = cur.fetchall()

  conf = {}
  for group_name, param_name, param_value in dados:
        if group_name not in conf:
              conf[group_name] = {}

        conf[group_name][param_name] = param_value
  CONSTANT_GLOBALS_parameters = conf
  cur.close()
  db.close()


#=======================================================================================================
#
#=======================================================================================================


def timeout(seconds=10, msg_on_error="Tempo limite atingido"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            q = multiprocessing.Queue()

            def target():
                try:
                    q.put(func(*args, **kwargs))
                except Exception as e:
                    q.put(e)

            p = multiprocessing.Process(target=target)
            p.start()
            p.join(seconds)

            if p.is_alive():
                p.terminate()
                raise TimeoutError(msg_on_error)

            result = q.get() if not q.empty() else None
            if isinstance(result, Exception):
                raise result
            return result
        return wrapper
    return decorator

#=======================================================================================================
#
#=======================================================================================================

def _do_connect_(dbname,timeout=10):
  dados   = json.loads(get_param_value("DATABASES", dbname.upper()))
  con     = None

  if "oracle" in dados['dbtype']:
    dsn            = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={dados['host']})(PORT={dados['port']})(CONNECT_TIMEOUT={timeout}))(CONNECT_DATA=(SERVICE_NAME={dados['database']})))"
    con            = oracledb.connect(user=dados['usr'], password=dados['pwd'], dsn=dsn, tcp_connect_timeout=timeout)

  if "mysql" in dados['dbtype']:
    con = mysql.connector.connect(
       host=dados['host'],
       port=3306,
       user=dados['usr'],
       password=dados['pwd'],
       database=dados['database'],
       connection_timeout=timeout)

  if "redshift" in dados['dbtype']:
    con = redshift_connector.connect( host=dados['host'],database=dados['database'],port=5439,user=dados['usr'],password=dados['pwd'],  timeout=60)
  
  if "db2" in dados['dbtype']:
    con = ibm_db.connect( f"DATABASE={ dados['database'] };HOSTNAME={ dados['host'] };PORT={ dados['port'] };PROTOCOL=TCPIP;UID={ dados['usr'] };PWD={ dados['pwd'] };CONNECTTIMEOUT={timeout}", "", "")

  if "snowflake" in dados['dbtype']:
    private_key_path = "/app_etl/rsa_key_algaretl.der"

    with open(private_key_path, "rb") as key_file:
        private_key = key_file.read()

    con = snowflake.connector.connect(
        user=dados.get('user'),
        account=dados.get('account'),
        private_key=private_key,
        warehouse=dados.get('warehouse'),
        database=dados.get('database'),
        schema=dados.get('schema'),
        role=dados.get('role'),
        login_timeout=timeout
    )  
  return con


#@timeout(seconds=15)        
def connect_db(dbname):
  return _do_connect_(dbname,10)


def diff_date(d1, d2):
   x = str( d1 - d2 ) 
   return x.split(".")[0]


def get_param_value(group_name, param_name):
  global CONSTANT_GLOBALS_parameters  
  return CONSTANT_GLOBALS_parameters[group_name][param_name]


def get_tmp_dir():
    return "/app/temp"

def get_log_dir():
    return "/app/logs"

def update_batch_status(batch_id, job_name, status):
  try:
    db  = local_db()
    cur = db.cursor()
    sql = CONSTANT_SQL_JOB_BATCH_STATUS_UPDATE.replace( "<id>",str(batch_id) ).replace("<status>",status).replace("<job_name>",job_name)
    cur.execute( sql )
    cur.close()
    db.close()
    return f"Sucess Update to {status}"
  except Exception as e:
     return f"Error Update to {status} " + str(e)


def create_job_batch(job_name, created_by, managed_by, parameters):
    db  = local_db()
    cur = db.cursor()
    id  = cur.var(int)
    cur.execute( CONSTANT_SQL_JOB_BATCH_CREATE%( job_name, created_by, managed_by, parameters ), id=id )
    ret =  id.getvalue() 
    cur.close()
    db.close()  
    return ret

def generate_hash(prefix="", with_hash=True):
  hash        = str(uuid.uuid4()) if with_hash else ""
  hash_sep    = "_" if with_hash and len(prefix) > 0 else ""
  path_folder = f"{ get_tmp_dir() }"
  return f"{ path_folder }{ os.path.sep }{ prefix }{ hash_sep }{ hash }"




REMOVE_TABLE_DEFAULT = str.maketrans("", "", "\n\t\";")

def clean_and_convert_tuples(data, remove_table=REMOVE_TABLE_DEFAULT):
    """
    Recebe um iterável de linhas (tuples)
    Retorna um generator de tuples limpas (streaming)
    """

    for row in data:
        yield tuple(
            field.translate(remove_table) if isinstance(field, str) else field
            for field in row
        )


#=======================================================================================================
# tratativa de log
#=======================================================================================================


def log(logger_id, msg, logbigdata=""):
  msg_log = "-"
  
  try:
    job_name    = logger_id.get('job_name')
    batch_id    = logger_id.get('batch_id')
    log_prefix  = logger_id.get( f"th_{ os.getpid() }"  )

    if log_prefix == None:
        log_prefix = ""
        
    if msg == "#":
        msg = "-" * 50

    msg_log = f'{ datetime.now().strftime("%d/%m/%Y %H:%M:%S") }:[{job_name}]: {log_prefix}{msg}'

    with open( f"{get_log_dir()}/{batch_id}.log", "a") as arq:
       arq.write( f"{msg_log}\n" )
       if len(logbigdata) > 1:
         arq.write( f'{ datetime.now().strftime("%d/%m/%Y %H:%M:%S") }:[{job_name}]: [BIGDATA]: {log_prefix}{  json.dumps({msg:logbigdata})   }\n'   )
  except Exception as e:
    print( str(e) )
  
  print( msg_log )

#=======================================================================================================
# envio de sms
#=======================================================================================================

def send_sms(too, phone):
  url = "http://172.25.76.21/kannel/cgi-bin/sendsms"
  params = {
      "username": "temp",
      "password": "t3mp0r4r10",
      "charset": "UTF-8",
      "text": phone,
      "to": too
  }
  response = requests.get(url, params=params)
  return response


#=======================================================================================================
# execute on snowflake
#=======================================================================================================

def execute_on_db(command, database, is_sql=False):
  conn   = connect_db(database)
  cursor = conn.cursor()
  cursor.execute(command)   
  
  if is_sql:
    result      = cursor.fetchone()
    columns     = [desc[0] for desc in cursor.description]
    row_dict    = dict(zip(columns, result))
    result_json = json.dumps(row_dict, ensure_ascii=False) 
    cursor.close()
    conn.close()
    return result_json
  else:
    cursor.close()
    conn.close()

  return "sucess: " + command
  
def execute_wait(command, database,timesleep=10):
  conn   = connect_db(database)
  cursor = conn.cursor()
  cursor.execute(command)   
  result      = cursor.fetchone()[0]
  while result > 0:
    cursor.execute(command)   
    result      = cursor.fetchone()[0]
    time.sleep( timesleep )
    
  cursor.close()
  conn.close()

  return "sucess: OK"
    
def human_readable_size(size_bytes):
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while size_bytes >= 1024 and i < len(units) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f} {units[i]}"


def kill_pids(list_pids, logger_id = None):
  for x in list_pids:
    try:
      os.kill(x.pid, 9)
      if logger_id:
         log(logger_id, f"Killed PID { x.pid }" )
    except Exception as e:
      pass
