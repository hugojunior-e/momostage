#!/usr/bin/python3

import multiprocessing
import time
import etl_manager
import cx_Oracle

# ---------------------------------------------------------------------------------------------------
def db():
    return cx_Oracle.connect("dwadm/dwtst@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=exa03-scan-stg.network.ctbc)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=DWHOM)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))", encoding='UTF-8', nencoding='UTF-8')


def geraLote(sequencia):  
    cx = db()
    cur = cx.cursor()
    
    saida = cur.var(int)
    proc = "dwadm.pkg_jobs_agenda.pro_cria_lote"
    cur.callproc(proc,(sequencia, saida))
    vv = saida.getvalue()
    cur.close()
    cx.close()
    return vv

# ---------------------------------------------------------------------------------------------------

def executaViewJobsAgenda(lote_id, sequencia): 
    cx = db()
    cur = cx.cursor()
    view = f"""
        SELECT JOB_ID,NOME
          FROM dwadm.vw_jobs_agenda
         WHERE lote_id = {lote_id}
           AND sequencia = '{sequencia}'
      ORDER BY nivel,
               ordem
    """
    
    exec_view = cur.execute(view).fetchall()
    cur.close()
    cx.close()
    return exec_view


# ---------------------------------------------------------------------------------------------------

def atualizaJobRun(job_id, lote_id, status, msg_erro=""):
    cx = db()
    cur = cx.cursor()
    proc = "begin dwadm.pkg_jobs_agenda.pro_atualiza_job_run({},{},'{}','{}'); commit; end;".format(job_id, lote_id, status, msg_erro)
    cur.execute(proc)
    cur.close()
    cx.close()
    return True

# ---------------------------------------------------------------------------------------------------

def verificaLoteParaContinuar(lote_id):
    cx  = db()
    cur = cx.cursor()

    saida = cur.var(str)
    proc = "dwadm.pkg_jobs_agenda.pro_verifica_lote"
    cur.callproc(proc,(lote_id, saida))
    vv = saida.getvalue()
    cur.close()
    cx.close()
    return vv != "Finaliza"

# ---------------------------------------------------------------------------------------------------


def executaSubprocesso(job_id, lote_id,nome):
    atualizaJobRun(job_id, lote_id, "E")
    
    job = etl_manager.ETL_MANAGER(nome, lote_id)
    status_ret = job.run()

    if status_ret == 0:
        atualizaJobRun(job_id, lote_id, "F", "")
    else:
        atualizaJobRun(job_id, lote_id, "A", "")


def executaWait(job_id):
    cx = db()
    cur = cx.cursor()
    view = f"select valor from dwadm.tbl_jobs_parametros where job_id ={job_id}"
    sql_wait = cur.execute(view).fetchone()[0]
    while True:
        ret = cur.execute(sql_wait).fetchone()[0]
        if ret > 0:
            break
    cur.close()
    cx.close()

def executaCheckPoint(job_id):
    cx=db()
    cur = cx.cursor()
    view = f"select valor from dwadm.tbl_jobs_parametros where job_id ={job_id}"
    sql = cur.execute(view).fetchone()[0]
    cur.execute(sql)
    cur.close()
    cx.close()


def verificaStatusFinal(job_id):
    cx = db()
    cur = cx.cursor()
    view = f"select count(1) from dwadm.tbl_jobs_run where status <> 'F' and lote_id = {job_id}"
    qtd = cur.execute(view).fetchone()[0]
    cur.close()
    cx.close()
    return qtd == 0

# ---------------------------------------------------------------------------------------------------

def iniciaJobs(sequencia):
    loop = True
    lst_threads = []
    lote_id = geraLote(sequencia)
    
    
    while loop:
        lst_jobs = executaViewJobsAgenda(lote_id, sequencia)
        for JOB_ID,NOME in lst_jobs:  

            if NOME == "#WAIT":
                atualizaJobRun(JOB_ID, lote_id, "E")
                executaWait(JOB_ID)
                atualizaJobRun(JOB_ID, lote_id, "F")

            elif NOME == "#CHECKPOINT":
                try:
                    atualizaJobRun(JOB_ID, lote_id, "E")
                    executaCheckPoint(JOB_ID)
                    atualizaJobRun(JOB_ID, lote_id, "F")
                except:
                    atualizaJobRun(JOB_ID, lote_id, "A")
                    return False

            elif NOME == "-":
                atualizaJobRun(JOB_ID, lote_id, "F")

            elif NOME == "#WAIT_SQL":
                print( NOME )

            elif NOME == "#WAIT_ARQ":
                print( NOME )
            
            else:
                print( f"Liberando JOB {NOME}")
                th = multiprocessing.Process(target=executaSubprocesso, args=(JOB_ID, lote_id, NOME))
                th.start()
                lst_threads.append(th)                

        time.sleep(5)
        loop = verificaLoteParaContinuar(lote_id)

    for th in lst_threads:
        th.join()

    return verificaStatusFinal(lote_id)
    
# ---------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------


def main() :
    iniciaJobs( "DIARIA_TESTE" )
    print("STATUS#OK")

if __name__ == '__main__' :
   main()
