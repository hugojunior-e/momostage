import etl_utils
import etl_hash
import etl_in
import etl_out
import multiprocessing
import threading
import os
import gc
import json
import sys
import time
from datetime import datetime


class ETL:
  def __init__(self, job_name, batch_id):
    self.job_name        = job_name
    self.batch_id        = batch_id
    self.logger_id       = etl_utils.get_logger_id()
    self.transformations = []
    self.config_orig     = {}
    self.config_target   = {}


    etl_utils.LOG_MAP[self.logger_id] = { "job_name":job_name, "batch_id":batch_id }

    


  #=======================================================================================================
  #
  #=======================================================================================================


  def transform(self, dados, lookups):
    if len(self.transformations) == 0:
      return dados

    for r in self.transformations:
      return r['C_TRANS'](dados, lookups)


  #=======================================================================================================
  #
  #=======================================================================================================


  def run_job_thread(self,m_resto=0, m_qtd=1, l_thread_list=None):
    etl_utils.log_add_prefix( "" if m_qtd == 1 else f"INST=[{m_resto}/{m_qtd}] " )
    
    etapa      = "S"
    try:
      inn       = etl_in.ETL_IN(self.config_orig,logger_id=self.logger_id, m_qtd=m_qtd, m_resto=m_resto)
      l_saidas  = []

      for idx in range( self.config_target['C_OUT_COUNT'] ):
        ss = etl_out.ETL_OUT(idx, self.config_target,m_resto,logger_id=self.logger_id)
        l_saidas.append(ss)

      etl_utils.log(self.logger_id, "Loading hasheds.")

      lookups = {}
      for r in self.c_hasheds.strip().split("\n"):
        if len(r) > 1:
          lookups[ os.path.basename(r) ] = etl_hash.ETL_HASH( r )
            
      qtd  = 0
      while True:
        etapa = "Loading Data..."
        dados = inn.getData()

        if len(dados) == 0:
          break
        
        qtd = qtd + len(dados)

        if qtd % 250000 == 0:
          etl_utils.log(self.logger_id,  f"qtd_parc={qtd:,}" , shortcut="D" )

        etapa = "Transforming..."
        x   = self.transform(dados, lookups)

        etapa = "ProcessOUT..."
        for saida in l_saidas:
          saida.execute(x)

        del dados
        del x
        gc.collect()

      etl_utils.log(self.logger_id,  f"FetchALL={qtd:,}"  )

      for saida in l_saidas:
        saida.finishing()

      if l_thread_list:
        l_thread_list[m_resto] = [0,'SUCESSO']
      return (0,"SUCESSO")
    except Exception as e:
      MSG                         = f"POINT: {etapa} MSG: {str(e)}"
      if l_thread_list:
        l_thread_list[m_resto] = [-1, MSG]
      return (-1, MSG)



  #=======================================================================================================
  #
  #=======================================================================================================


  def run_job(self):
    etl_utils.log(self.logger_id, "Executing IN.before" )
    inn = etl_in.ETL_IN(self.config_orig,logger_id=self.logger_id)
    inn.prepareBefore()


    etl_utils.log(self.logger_id, "Executing OUT.before" )
    for idx in range( self.config_target['C_OUT_COUNT'] ):
      ou = etl_out.ETL_OUT(idx, self.config_target, -1,logger_id=self.logger_id)
      ou.prepareBefore()

    
    qtd_mod         = inn.qtd_inst

    etl_utils.log(self.logger_id, "Preparing..." )

    if qtd_mod == 1:
      etl_utils.log(self.logger_id,  f"Executing Job...Start")
      statusCode, statusMsg = self.run_job_thread()
      etl_utils.log(self.logger_id,  f"Executing Job...Finish - { statusCode }")

      if statusCode == -1:
        raise Exception(statusMsg)
      
    else:
      l_thread      = []
      manager       = multiprocessing.Manager()
      l_thread_list = manager.list()

      for i in range(qtd_mod):
        etl_utils.log(self.logger_id,  f"Threading Instance {i}" )

        l_thread_list.append( [1,'-']  )

        t        = multiprocessing.Process(target=self.run_job_thread, args=(i,qtd_mod,l_thread_list,) )
        t.index  = i
        l_thread.append(t)
        t.start()

      while len(l_thread) > 0:
        for x in l_thread:
          if x.is_alive() == False:
            x.join()
            s,m = l_thread_list[ x.index ]
            etl_utils.log(self.logger_id,  f"Thread: { x.index } Status: {s} {m}" )
            l_thread.remove(x)
            if s != 0:
              l_thread.clear()
              raise Exception(m)      
        time.sleep(3)
        
      manager.shutdown()
      
    etl_utils.log(self.logger_id, "Executing OUT.after" )
    for idx in range( self.config_target['C_OUT_COUNT'] ):
      ou = etl_out.ETL_OUT(idx, self.config_target, -1,logger_id=self.logger_id)
      ou.prepareAfter()


  #=======================================================================================================
  #
  #=======================================================================================================



  def apply_filter(self, p_job, parameter_text):
    ret   = parameter_text
    if p_job != None:
        param_job = p_job[0]
        if param_job != None:
          dados = json.loads(param_job)
          for l in dados:
            ret  = ret.replace( f"#{ l }#", dados[l] )
    return ret


  def apply_jsh(self, commands, opt):
    try:
      if commands != None:
        results = {}
        exec(commands.read(), results)
        s_sh = results[ "C_JOB_SH_BEFORE" if opt == "job.start" else "C_JOB_SH_AFTER"  ]
        l_sh = s_sh.split("\n")
        for x in l_sh:
          if x.startswith("#SNOWFLAKE_EXECUTE"):
            cmd   = x.strip().split(" ")
            cmd   = " ".join( cmd[1:] )
            r = etl_utils.execute_on_snowflake(cmd)
            etl_utils.log(self.logger_id, "Executing on Snow: " + r)

          if x.startswith("#SMS"):
            cmd   = x.strip().split(" ")
            phone = cmd[1] 
            msg   = " ".join( cmd[2:] )
            r = etl_utils.send_sms(phone, msg)
            etl_utils.log(self.logger_id, "Sending SMS to " + phone + " " + r.text)
      return 1
    except:
      return 0
  


  def run(self):
    dt_ini     = datetime.now()
    status_ret = 0
    jsh        = ""
    m          = etl_utils.update_batch_status( self.batch_id , self.job_name  , "E" )
    etl_utils.log(self.logger_id, m)

    try:
      etl_utils.log(self.logger_id, "#")
      etl_utils.log(self.logger_id, "### Preparing Parameters/Configuration")
      etl_utils.log(self.logger_id, "#")

      cur_dw                     = etl_utils.local_db().cursor()
      origin,target,hasheds,jsh  = cur_dw.execute( etl_utils.CONSTANT_SQL_JOB % (self.job_name) ).fetchone()
      transforms                 = cur_dw.execute( etl_utils.CONSTANT_SQL_JOB_TRANSF % (self.job_name) ).fetchall()
      parameter_job              = cur_dw.execute( etl_utils.CONSTANT_SQL_JOB_BATCH_PARAMETERS % ( self.batch_id  , self.job_name ) ).fetchone()

      etl_utils.log(self.logger_id, "Input Params.")
      etl_utils.log(self.logger_id, f"sys       = {sys.argv}")
      etl_utils.log(self.logger_id, f"batch_id  = {self.batch_id}")
      etl_utils.log(self.logger_id, f"logger_id = {self.logger_id}")
      etl_utils.log(self.logger_id, f"params    = {parameter_job}")
      etl_utils.log(self.logger_id, f"job_sh    = { '' if jsh == None else jsh }")

      c_origin              = self.apply_filter( parameter_job, origin.read() )
      c_target              = self.apply_filter( parameter_job, target.read() )
      self.c_hasheds        = self.apply_filter( parameter_job, "" if hasheds == None else hasheds.read() )

      etl_utils.log(self.logger_id, "job_def-origin",shortcut="I",logbigdata=c_origin)
      etl_utils.log(self.logger_id, "job_def-target",shortcut="I",logbigdata=c_target)

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

      self.apply_jsh(jsh, "job.start")
      self.run_job()
    except Exception as e:
      etl_utils.log(self.logger_id, "ERROR: " + str(e)  )
      status_ret = 1

    self.apply_jsh(jsh, "job.end")
    
    m = etl_utils.update_batch_status( self.batch_id  , self.job_name , ("F" if status_ret == 0 else "A")  )
    etl_utils.log(self.logger_id, m )
    
    etl_utils.log(self.logger_id, "Time Elapsed: " + etl_utils.diff_date(datetime.now() , dt_ini ) )
    etl_utils.log(self.logger_id, f"STATUS:{ 'OK' if status_ret == 0 else 'ERRO'}")
    return status_ret
    
