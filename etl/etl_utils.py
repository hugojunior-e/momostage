import cx_Oracle
from datetime import datetime
import json

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

CONSTANT_SQL_JOB = """
SELECT origin,
       target,
       hasheds
  FROM DWADM.ms_job_def 
 where job_name = '%s'
   and reg_sts = 1
"""


CONSTANT_SQL_JOB_TRANSF = """
SELECT transf,
       filter,
       lookups
  FROM DWADM.ms_job_transf 
 where job_name = '%s'
   and reg_sts = 1
 order by job_order
"""

CONSTANT_SQL_JOB_PARAMETERS = """
select parametro from dwadm.vw_jobs_run where lote_id = %s and nome = '%s'
"""

CONSTANT_QTD_THREADS = 6

LOG_MAP = {}

def local_db():
   x = cx_Oracle.connect("hugoa/BandoDados#147@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=exa03-scan-prd.network.ctbc)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DWPRD)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))", encoding='UTF-8', nencoding='UTF-8')
   return x


def connect_db(dbname):
  dados = json.loads(get_param_value("DATABASES", dbname.upper()))
  x = cx_Oracle.connect(f"{dados['usr']}/{dados['pwd']}@{dados['tns']}", encoding='UTF-8', nencoding='UTF-8')
  return x


def diff_date(d1, d2):
   x = str( d1 - d2 ) 
   return x.split(".")[0]

def get_logger_id():
  db  = local_db()
  cur = db.cursor()
  cur.execute("select DWADM.ms_job_logger_id_seq.nextval from dual")
  ret = cur.fetchone()[0]
  db.close()
  return ret


def get_job_type(job_name):
  db  = local_db()
  cur = db.cursor()
  cur.execute("""SELECT job_type
                  FROM DWADM.ms_job_def 
                where job_name = '%s'
                  and reg_sts = 1
                """ % (job_name))
  ret = cur.fetchone()[0]
  cur.close()
  db.close()
  return ret

def get_param_value(group_name, param_name):
  db  = local_db()
  cur = db.cursor()
  cur.execute("""SELECT param_value
                  FROM DWADM.ms_job_globals
                where group_name = '%s'
                  and param_name = '%s'
                """ % (group_name, param_name))
  ret = cur.fetchone()[0]
  cur.close()
  db.close()
  return ret

def get_job_list(job_name):
  db  = local_db()
  cur = db.cursor()
  cur.execute("""SELECT job_name
                  FROM DWADM.ms_job_def 
                where job_name LIKE '%s'
                  and reg_sts = 1
                """ % (job_name + ".OCI%"))
  lista = [r[0] for r in cur.fetchall()]
  cur.close()
  db.close()
  return lista



db_log    = local_db()

def log(logger_id, msg, shortcut="I",logbigdata=""):
  global LOG_MAP
  job_name  = LOG_MAP[logger_id]['job_name']
  lote_id   = LOG_MAP[logger_id]['lote_id']
  log_seq   = LOG_MAP[logger_id]['log_seq']
  log_out   = LOG_MAP[logger_id]['log_out']

  try:
      if msg == "#":
         msg = "-" * 50

      cur = db_log.cursor()
      cur.execute("""begin
                  INSERT INTO DWADM.ms_job_LOGGER(JOB_NAME, PARAM_ID, CREATED_AT, LINE, LINE_ID, JOB_GROUP_ID, LINE_TYPE,LOGBIGDATA)  VALUES (:1,:2,SYSDATE,:3,:4,:5,:6,:7);
                  commit;
                end;""", (job_name, lote_id, msg, log_seq, logger_id, shortcut, logbigdata))
      cur.close()
      LOG_MAP[logger_id]['log_seq'] = LOG_MAP[logger_id]['log_seq'] + 1
  except:
      print("erro")

  if log_out == None:
      print(f'{ datetime.now().strftime("%m/%d/%Y %H:%M:%S") }:[{job_name}]: {msg}')
  else:
      log_out(f"[{job_name}] : {msg}")

  