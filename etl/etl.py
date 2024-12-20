#!/home/producao/Python/Python37/bin/python3

import etl_utils
import etl_hash
import etl_in
import etl_out
import multiprocessing
import time
import os
import gc
from datetime import datetime


#######################################################


class ETL:
  def __init__(self, job_name, lote_id, logger_id, log_out=None):
    etl_utils.LOG_MAP[logger_id] = {"job_name":job_name, "lote_id":lote_id, "log_out":log_out, "log_seq":0}
    self.logger_id       = logger_id
    self.job_name        = job_name
    self.lote_id         = lote_id
    self.logger_id       = logger_id

    self.transformations = []
    self.config_orig     = {}
    self.config_target   = {}


  #---------------------------------------------------------

  def transform(self, dados, lookups):
    if len(self.transformations) == 0:
      return dados

    for r in self.transformations:
      return r['C_TRANS'](dados, lookups)


  #---------------------------------------------------------

  def run_job_thread(self,m_resto, m_qtd, l_status):
    etapa = "S"
    try:
      inn       = etl_in.ETL_IN(self.config_orig,logger_id=self.logger_id)
      
      l_saidas  = []

      for idx in range( self.config_target['C_OUT_COUNT'] ):
        ss = etl_out.ETL_OUT(idx, self.config_target,m_resto,logger_id=self.logger_id)
        l_saidas.append(ss)

      sql = None
      if self.config_orig['C_TYPE'] == "sql":
        sql = self.config_orig['C_SQL'].replace("#RESTO#", str(m_resto)).replace("#MOD#", str(m_qtd)) 
      
      etl_utils.log(self.logger_id, "Loading hasheds.")
      lookups = {}
      for r in self.c_hasheds.strip().split("\n"):
        if len(r) > 1:
          lookups[ os.path.basename(r) ] = etl_hash.ETL_HASH( r )
            
      qtd  = 0
      while True:
        etapa = "Loading Data..."
        dados = inn.getData( sql )

        if len(dados) == 0:
          break
        
        qtd = qtd + len(dados)

        if qtd % 250000 == 0:
          etl_utils.log(self.logger_id,  f"qtd_parc={qtd:,}" if m_qtd == 1 else f"INST({m_resto}/{m_qtd}): qtd_parc={qtd:,}" , shortcut="D" )

        etapa = "Transforming..."
        x   = self.transform(dados, lookups)

        etapa = "ProcessOUT..."
        for saida in l_saidas:
          saida.execute(x)

        del dados
        del x
        gc.collect()

      for saida in l_saidas:
        saida.finishing()
          
      etl_utils.log(self.logger_id,  f"qtd_total={qtd:,}" if m_qtd == 1 else  f"INST({m_resto}/{m_qtd}): qtd_total={qtd:,}"  )

      l_status[m_resto] = 0
      multiprocessing.current_process().status = 0
    except Exception as e:
      l_status[m_resto] = -1
      etl_utils.log(self.logger_id,  f"Mod: {m_resto}/{m_qtd} Point: {etapa} Error: {str(e)}" )



  #---------------------------------------------------------

  def run_job(self):
    statusExecutionSucess = True

    etl_utils.log(self.logger_id, "Executing IN.before" )
    inn = etl_in.ETL_IN(self.config_orig,logger_id=self.logger_id)
    inn.prepareBefore()


    etl_utils.log(self.logger_id, "Executing OUT.before" )
    for idx in range( self.config_target['C_OUT_COUNT'] ):
      ou = etl_out.ETL_OUT(idx, self.config_target, -1,logger_id=self.logger_id)
      ou.prepareBefore()

    l_thread = []
    l_thread_status = multiprocessing.Manager().list()
    qtd_mod  = inn.getQtdInst()

    etl_utils.log(self.logger_id, "Preparing..." )
    
    for i in range(qtd_mod):
      if qtd_mod > 1:
        etl_utils.log(self.logger_id,  f"Threading Instance {i}" )
        t        = multiprocessing.Process(target=self.run_job_thread, args=(i,qtd_mod,l_thread_status,) )
        l_thread_status.append(1)
        l_thread.append(t)
        t.start()
        time.sleep(2)
      else:
        etl_utils.log(self.logger_id,  f"Executing Job...")
        l_thread_status.append(1)
        self.run_job_thread(i,qtd_mod, l_thread_status)
        etl_utils.log(self.logger_id,  f"Direct: Status: {l_thread_status[i]}" )
        statusExecutionSucess = l_thread_status[i] == 0

    for i, x in enumerate(l_thread):
      x.join()
      etl_utils.log(self.logger_id,  f"Thread: {i} Status: {l_thread_status[i]}" )
      if l_thread_status[i] != 0:
        statusExecutionSucess = False
    
    if statusExecutionSucess == False:
      raise Exception("Error")
    
    etl_utils.log(self.logger_id, "Executing OUT.after" )
    for idx in range( self.config_target['C_OUT_COUNT'] ):
      ou = etl_out.ETL_OUT(idx, self.config_target, -1,logger_id=self.logger_id)
      ou.prepareAfter()


  #--------------------------------------------------------------------------------------------
  # bloco principal
  #--------------------------------------------------------------------------------------------


  def apply_filter(self, parameter_job, parameter_text):
    ret   = parameter_text
    if parameter_job != None:
        lista = parameter_job[0].split(" ")
        for idx, l in enumerate(lista):
          if l == "-param":
            pname,pvalue = lista[idx+1].replace('"','').split("=")
            ret   = ret.replace( f"#{pname}#", pvalue)
    return ret



  def run(self):
    try:
      etl_utils.log(self.logger_id, "#")
      etl_utils.log(self.logger_id, "### Preparing Parameters/Configuration")
      etl_utils.log(self.logger_id, "#")


      dt_ini = datetime.now()

      etl_utils.log(self.logger_id, f"lote_id= {self.lote_id}")
      etl_utils.log(self.logger_id, "Loading Params.")

      cur_dw                = etl_utils.local_db().cursor()
      origin,target,hasheds = cur_dw.execute( etl_utils.CONSTANT_SQL_JOB % (self.job_name) ).fetchone()
      transforms            = cur_dw.execute( etl_utils.CONSTANT_SQL_JOB_TRANSF % (self.job_name) ).fetchall()
      parameter_job         = cur_dw.execute( etl_utils.CONSTANT_SQL_JOB_PARAMETERS % (self.lote_id, self.job_name) ).fetchone()
      c_origin              = self.apply_filter( parameter_job, origin.read() )
      c_target              = self.apply_filter( parameter_job, target.read() )
      self.c_hasheds        = self.apply_filter( parameter_job, "" if hasheds == None else hasheds.read() )

      if parameter_job != None:
          lista = parameter_job[0].split(" ")
          for idx, l in enumerate(lista):
            if l == "-param":
              pname,pvalue = lista[idx+1].replace('"','').split("=")
              etl_utils.log(self.logger_id, f"{pname} = {pvalue}")
              
      etl_utils.log(self.logger_id, "Configuring Params input.")
      exec(c_origin, self.config_orig)
      etl_utils.log(self.logger_id, "Configuring Params output.")
      exec(c_target , self.config_target)


      etl_utils.log(self.logger_id, "Configuring SQL Auto.")

      for idx in range( self.config_target['C_OUT_COUNT'] ):
        if self.config_target[ f'C{idx+1}_TYPE'] == 'sql' and self.config_target[ f'C{idx+1}_SQL_AUTO']:
          l_fields   = self.config_target[ f'C{idx+1}_SQL_FIELDS'].split("\n")

          tabela     = self.config_target[ f'C{idx+1}_SQL']
          campos     = [ f.replace("*", "") for f in l_fields ]
          binds      = [ f":{xx+1}" for xx in range(len(campos))]
          campos_cur = [ f":{ xx + 1 } as {fn}" for xx,fn in enumerate(campos)  ]
          campos_key = ""

          for ff in l_fields:
            if "*" in ff:
              campos_key = campos_key + " AND " + ff.replace("*", "") + " = cx." + ff.replace("*", "")

         
          if self.config_target[ f'C{idx+1}_SQL_TYPE'] == "Insert":
            self.config_target[ f'C{idx+1}_SQL'] = f"""INSERT INTO {tabela}({ ",".join(campos) }) values ({ ",".join(binds) })"""

          if self.config_target[ f'C{idx+1}_SQL_TYPE'] == "Update Then Insert":
            self.config_target[ f'C{idx+1}_SQL'] = f"""
            BEGIN
              FOR CX IN ( SELECT { ','.join(campos_cur) }
                            FROM DUAL )
              loop
                update {tabela}  set
                  { ','.join([ xx + " = cx." + xx for xx in campos]) }
                where 1=1
                  {campos_key};

                if (SQL%ROWCOUNT = 0) then
                  insert into {tabela} ( { ','.join(campos) } )
                  values ( { ','.join([ "cx." + xx for xx in campos]) } );
                end if;
              end loop;
            END;
            """
       
      etl_utils.log(self.logger_id, "Loading Params Transforms.")
      for transf,filter,lookups in transforms:
        sps     = "      "
        sps_txt = ""
        if lookups != None:
          for r in lookups.split("\n"):
            sps_txt = sps_txt + sps + r + "\n"
        
        x={"datetime":datetime}
        exec( etl_utils.CONSTANT_TRANSFORMS % (filter, sps_txt, self.apply_filter( parameter_job, transf.read())  ), x)
        self.transformations.append(x)

      etl_utils.log(self.logger_id, "#")
      etl_utils.log(self.logger_id, "### Starting Execution")
      etl_utils.log(self.logger_id, "#")

      self.run_job()
      etl_utils.log(self.logger_id, "Time Elapsed: " + etl_utils.diff_date(datetime.now() , dt_ini ) )

      return 0
    except Exception as e:
      etl_utils.log(self.logger_id,  str(e)  )
      return 1