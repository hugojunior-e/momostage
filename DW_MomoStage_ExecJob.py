#!/home/producao/Python/Python37/bin/python3
# -*- coding: utf-8 -*-


import sys
import os
import cx_Oracle
import multiprocessing
import time
import atexit
import csv
import datetime
import codecs
import json
import re
import logging

import etl_manager


sys.path.append("/home/producao/pyApiAlgar/PRD")
os.environ['SISTEMA'] = 'DW'
os.environ['SCRIPT'] = 'DW_MomoStage_ExecJob'

from api import G
import api.script
import api.db as db
import api.db.sql as api_sql
import api.so.utils as api_util
import config.definicoes.basedados as def_db
import api.email as email
import config.definicoes.servidor as def_srv
import api.rede as api_rede
import config.ambiente

S = api.script.Script( "DW_MomoStage_ExecJob" )
S.P.valor("AGRUPAMENTO", "-A", "Nome do processo generico a ser executado", False, True)

def inicializa() :
    return True

# ---------------------------------------------------------------------------------------------------



def geraLote(sequencia):  
    cx  = db.conectar(def_db.DWPRD)
    cur = cx.cursor()
    
    saida = cur.var(int)
    proc = "dwadm.pkg_jobs_agenda.pro_cria_lote"
    cur.callproc(proc,(sequencia, saida))
    vv = saida.getvalue()
    cur.close()
    return vv

# ---------------------------------------------------------------------------------------------------

def executaViewJobsAgenda(lote_id, sequencia): 
    cx  = db.conectar(def_db.DWPRD)   
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
    return exec_view


# ---------------------------------------------------------------------------------------------------

def atualizaJobRun(job_id, lote_id, status, msg_erro=""):
    cx  = db.conectar(def_db.DWPRD)
    cur = cx.cursor()
    proc = "begin dwadm.pkg_jobs_agenda.pro_atualiza_job_run({},{},'{}','{}'); commit; end;".format(job_id, lote_id, status, msg_erro)
    cur.execute(proc)
    cur.close()
    return True

# ---------------------------------------------------------------------------------------------------

def verificaLoteParaContinuar(lote_id):
    cx  = db.conectar(def_db.DWPRD)
    cur = cx.cursor()

    saida = cur.var(str)
    proc = "dwadm.pkg_jobs_agenda.pro_verifica_lote"
    cur.callproc(proc,(lote_id, saida))
    vv = saida.getvalue()
    cur.close()
    return vv != "Finaliza"

# ---------------------------------------------------------------------------------------------------


def executaSubprocesso(job_id, lote_id,nome):
    atualizaJobRun(job_id, lote_id, "E")
    
    job = etl_manager.ETL_MANAGER(nome, lote_id, G.INFO)
    status_ret = job.run()

    if status_ret == 0:
        atualizaJobRun(job_id, lote_id, "F", "")
    else:
        atualizaJobRun(job_id, lote_id, "A", "")


def executaWait(job_id):
    cx  = db.conectar(def_db.DWPRD)
    cur = cx.cursor()
    view = f"select valor from dwadm.tbl_jobs_parametros where job_id ={job_id}"
    sql_wait = cur.execute(view).fetchone()[0]
    while True:
        ret = cur.execute(sql_wait).fetchone()[0]
        if ret > 0:
            break
    cur.close()

def executaCheckPoint(job_id):
    cx  = db.conectar(def_db.DWPRD)
    cur = cx.cursor()
    view = f"select valor from dwadm.tbl_jobs_parametros where job_id ={job_id}"
    sql = cur.execute(view).fetchone()[0]
    cur.execute(sql)
    cur.close()


def verificaStatusFinal(job_id):
    cx  = db.conectar(def_db.DWPRD)
    cur = cx.cursor()
    view = f"select count(1) from dwadm.tbl_jobs_run where status <> 'F' and lote_id = {job_id}"
    qtd = cur.execute(view).fetchone()[0]
    cur.close()
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
                G.INFO( f"Liberando JOB {NOME}")
                th = multiprocessing.Process(target=executaSubprocesso, args=(JOB_ID, lote_id, NOME))
                th.start()
                lst_threads.append(th)                

        time.sleep(5)
        loop = verificaLoteParaContinuar(lote_id)

    for th in lst_threads:
        th.join()

    
    return verificaStatusFinal(lote_id)
    
# ---------------------------------------------------------------------------------------------------


def processar() :
    G.INFO( f"Iniciando processo {S.P.AGRUPAMENTO.valor}")
    return iniciaJobs( S.P.AGRUPAMENTO.valor )


# ---------------------------------------------------------------------------------------------------


def main() :
    S.flxExecucao.add("INICIALIZAR", inicializa )
    S.flxExecucao.add("PROCESSA JOB", processar)

    prefix_paths = os.path.join(os.environ.get('DIR_BASE_PYTHON', '/home/producao'), os.environ['SISTEMA'])
    os.environ['DIR_DATA'] = os.path.join(prefix_paths, 'dados', sys.argv[-1] )
    os.environ['DIR_LOG'] = os.path.join(prefix_paths, 'dados', sys.argv[-1], datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    S.executa( sufixo_script=sys.argv[-1] )

if __name__ == '__main__' :
   main()