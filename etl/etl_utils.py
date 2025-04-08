import oracledb
import json
import uuid
import os
from datetime import datetime

CONSTANT_OWNER = "DWADM"


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
       hasheds
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

CONSTANT_SQL_JOB_PARAMETERS = f"""
select parametro from {CONSTANT_OWNER}.vw_jobs_run where lote_id = %s and nome = '%s'
"""

CONSTANT_QTD_THREADS = 6

def local_db():
   x = oracledb.connect(dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=exa03-scan-stg.network.ctbc)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DWHOM)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))", user="dwadm", password="dwtst")
   return x


def connect_db(dbname):
  dados = json.loads(get_param_value("DATABASES", dbname.upper()))
  x = oracledb.connect(user=dados['usr'], password=dados['pwd'], dsn=dados['tns'])
  return x


def diff_date(d1, d2):
   x = str( d1 - d2 ) 
   return x.split(".")[0]

def get_logger_id():
  db  = local_db()
  cur = db.cursor()
  cur.execute( f"select {CONSTANT_OWNER}.ms_job_logger_id_seq.nextval from dual" )
  ret = cur.fetchone()[0]
  db.close()
  return ret

def get_param_value(group_name, param_name):
  db  = local_db()
  cur = db.cursor()
  cur.execute(f"""SELECT param_value FROM {CONSTANT_OWNER}.ms_job_globals where group_name = '%s' and param_name = '%s' """ % (group_name, param_name))
  ret = cur.fetchone()[0]
  cur.close()
  db.close()
  return ret

def get_tmp_dir():
   ret = get_param_value('PARAMETERS','TEMP_DIR')
   if "AlgarETL" in ret:
      return "/algar/temp_mms"
   return ret


def generate_hash(prefix="", with_hash=True):
  hash     = str(uuid.uuid4()) if with_hash else ""
  path_sep = "_" if with_hash and len(prefix) > 0 else ""

  return f"{ get_tmp_dir() }{ os.path.sep }{ prefix }{ path_sep }{ hash }"


def clean_and_convert_tuples(data, remove_chars=None):
  if remove_chars is None:
      remove_chars = "\n\t\";"

  def clean_field(field):
      if isinstance(field, str):
          for char in remove_chars:
              field = field.replace(char, "")
      return field

  result = [
      [clean_field(field) for field in row]
      for row in data
  ]

  return result


#=======================================================================================================
# tratativa de log
#=======================================================================================================

LOG_MAP        = {}
LOG_PREFIX     = {}  

def log_add_prefix(prefix):
  LOG_PREFIX[ f"th_{ os.getpid() }" ] = prefix   

def log(logger_id, msg, shortcut="I",logbigdata=""):
    job_name    = LOG_MAP[logger_id]['job_name']
    lote_id     = LOG_MAP[logger_id]['lote_id']
    log_seq     = LOG_MAP[logger_id]['log_seq']
    log_prefix  = LOG_PREFIX.get( f"th_{ os.getpid() }"  )

    if log_prefix == None:
        log_prefix = ""

    LOG_MAP[logger_id]['log_seq'] = log_seq + 1

    if msg == "#":
        msg = "-" * 50
       
    #mensagem =  (job_name, lote_id, msg, log_seq, logger_id, shortcut, logbigdata)
    
    #self.cur.execute(f"""begin
    #            INSERT INTO {etl_utils.CONSTANT_OWNER}.ms_job_LOGGER(JOB_NAME, PARAM_ID, CREATED_AT, LINE, LINE_ID, JOB_GROUP_ID, LINE_TYPE,LOGBIGDATA)  VALUES (:1,:2,SYSDATE,:3,:4,:5,:6,:7);
    #            commit;
    #            end;""", mensagem )            

    print(f'{ datetime.now().strftime("%m/%d/%Y %H:%M:%S") }:[{job_name}]: {log_prefix}{msg}')  