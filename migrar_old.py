#!/home/producao/Python/Python37/bin/python3

import cx_Oracle
import sys
import os
from unicodedata import normalize
import subprocess


os.environ["LD_LIBRARY_PATH"] += ":/opt/IBM/InformationServer/Server/biginsights/IHC/c++/Linux-amd64-64/lib:/opt/IBM/InformationServer/Server/branded_odbc/lib:/opt/IBM/InformationServer/Server/DSComponents/lib:/opt/IBM/InformationServer/Server/DSComponents/bin:/opt/IBM/InformationServer/Server/DSEngine/bin:/opt/IBM/InformationServer/Server/DSEngine/lib:/opt/IBM/InformationServer/Server/DSEngine/uvdlls:/opt/IBM/InformationServer/Server/PXEngine/lib:/opt/IBM/InformationServer/Server/PXEngine/bin:/opt/IBM/InformationServer/jdk/jre/lib/amd64/j9vm:/opt/IBM/InformationServer/jdk/jre/lib/amd64:/opt/IBM/InformationServer/ASBNode/lib/cpp:/opt/IBM/InformationServer/ASBNode/apps/proxy/cpp/linux-all-x86_64:/home/producao/Python/Extras/instantclient_10_2:/u01/app/oracle/product/11.2.0/client/lib:/u01/app/oracle/product/11.2.0/client/rdbms:/usr/lib64:/usr/lib:."
os.environ["PATH"] += ":/opt/IBM/InformationServer/Server/DSEngine/bin"

job = sys.argv[1]

proc = subprocess.run(
    ["dsjob" ,"-logdetail", "DWDiariaAlgar", job,  "-wave", "0"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)
txt = proc.stdout


idx_1      = txt.index("SELECT statement at runtime:")
idx_1      = txt.index(":", idx_1)
idx_2      = txt.index("Event Id:",idx_1)
sql_select = txt[idx_1: idx_2].strip()[1:-1]


idx_1 = txt.index("The connector connected to")
idx_2 = txt.index("\n",idx_1)
base  = txt[idx_1: idx_2].strip().split(" ")[-1][0:-1]

idx_1      = txt.index("INSERT statement at runtime:")
idx_1      = txt.index(":", idx_1)
idx_2      = txt.index("Event Id:",idx_1)
sql_insert = txt[idx_1: idx_2].strip()[1:-1]

idx_1      = txt.index("TRUNCATE TABLE statement")
idx_1      = txt.index(":", idx_1)
idx_2      = txt.index("Event Id:",idx_1)
truncate_table = txt[idx_1: idx_2].strip()[1:-1]


idx_1      = txt.index(f"Starting Job {job}")
idx_1      = txt.index("\n", idx_1)
idx_2      = txt.index("Event Id:",idx_1)
parametros = ""
for xx in (txt[idx_1: idx_2].strip()[0:-1]).split("\n"):
    xx_trim = xx.strip()
    if xx_trim.startswith("$") == False:
        parametros = parametros + "\n" + xx_trim.split(" ")[0]



origin_sql = f'''C_TYPE="db"
C_DATABASE="{base}"
C_SQL="""
{sql_select}
"""
C_SQL_AFTER=None
C_SQL_BEFORE=None
'''

origin_sql = normalize('NFKD', origin_sql).encode('ASCII','ignore').decode('ASCII')


target_sql = f'''C_OUT_COUNT = 1
C1_TYPE="db"
C1_DATABASE="dwprd"
C1_SQL=\"""
{sql_insert}
"""
C1_SQL_AFTER=None
C1_SQL_BEFORE="{truncate_table}"
'''

db = cx_Oracle.connect('hugoa/Peneira#754#asd@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=exa03-scan-prd.network.ctbc)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DWPRD)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))')
cur = db.cursor()
cur.execute( f"delete job_def where job_name='{job}' ")
cur.execute("insert into job_def values (:1,:2,:3,:4,:5,:6)", (job, 'ODS', origin_sql, target_sql,None,parametros.strip() ) )
db.commit()




