import etl_utils
import etl_odata
import boto3
import json
import os
import re
import ibm_db
import xml.etree.ElementTree as ET
from unicodedata import normalize

class ETL_IN:
  def __init__(self, config, logger_id, m_resto=0, m_qtd=1):
    self.config      = config
    self.db          = None
    self.qtd_inst    = 1
    self.cur         = None
    self.fp          = None
    self.logger_id   = logger_id
    self.py_code     = None
    self.py_code_ret = {"consoleLog": self.consoleLog}

    if self.config['C_TYPE'] == "filename":
      self.filename_file_required = self.config['C_FILENAME_FILE_REQUIRED'] == "1"
      self.filename               = self.config['C_FILENAME']
      self.filename_fd            = self.config["C_FILENAME_FD"]


    if self.config['C_TYPE'] == 'boto3':
      self.boto3_name        = etl_utils.generate_hash()
      self.boto3_filename    = self.config['C_BOTO3_FILENAME']
      self.boto3_fd          = self.config["C_BOTO3_FD"]
      self.boto3_bucket_data = json.loads( etl_utils.get_param_value("AUTHS.S3", self.config['C_BOTO3_BUCKET'] ) )

    if self.config['C_TYPE'] == "sql":
      if "#MOD#" in self.config['C_SQL']:
        match = re.search(r"#SQL_MODMAX_(\d+)#", self.config['C_SQL'])
        self.qtd_inst = int( match.group(1) ) if match else etl_utils.CONSTANT_QTD_THREADS
      else:
          self.qtd_inst = 1  

      arsize             = self.config.get('C_SQL_ARRAYSIZE')
      self.sql_arraysize = int( arsize ) if arsize != None else 5000

      AUX = self.config['C_SQL_DB'] 

      etl_utils.log(self.logger_id,  f"DB: { AUX } : Opening : {self.sql_arraysize}")
      self.db      = etl_utils.connect_db( AUX )
      self.db_type = str(type(self.db)).lower().replace("<class '","").replace("'>","")
      etl_utils.log(self.logger_id,  f"DB: { AUX } : success : { self.db_type }")

      if "ibm" not in self.db_type:
        self.cur           = self.db.cursor()

      self.sql_before    = self.config['C_SQL_BEFORE']
      self.sql_query     = self.config['C_SQL'].replace("#RESTO#", str(m_resto)).replace("#MOD#", str(m_qtd)) 
      self.sql_is_open   = False

    if self.config['C_TYPE'] == "xml":        
        self.xml_file_required = self.config['C_XML_FILE_REQUIRED'] == "1"
        self.xm_filename       = self.config['C_XML_FILENAME']
        self.xml_fields        = self.config['C_XML_FIELDS'].split("\n")

    if self.config['C_TYPE'] == "odata":
      self.odata  = etl_odata.ETL_ODATA(self.config,logger_id=logger_id)

  #=======================================================================================================
  #
  #=======================================================================================================

  def consoleLog(self, msg):
    etl_utils.log(self.logger_id, msg)    

  #=======================================================================================================
  #
  #=======================================================================================================

  def getData(self):

    # ----------------------------------------
    # executa entrada para xml
    # ----------------------------------------

    if self.config['C_TYPE'] == "xml":
      dados = []
      
      if self.fp == None:
        if self.xml_file_required == False and os.path.isfile(self.xml_filename) == False:
          etl_utils.log(self.logger_id, f"*** file not found ***")
          return []
        etl_utils.log(self.logger_id, f"File {self.xml_filename}")
        self.fp  = open( self.xml_filename )
        self.xml = self.fp.readlines()
        self.fp.close()
        root = ET.fromstring("".join(self.xml))
        filtro1 = self.xml_fields[0].split("/")[-2]
        for child in root.findall( filtro1 ):
          reg = []
          for r in self.xml_fields:
            f = r.split("/")[-1]
            reg.append(  child.find(f).text )
          dados.append( reg )
      return dados

    # ----------------------------------------
    # executa entrada para odata
    # ----------------------------------------

    if self.config['C_TYPE'] == "odata":
      return self.odata.get_data()

    # ----------------------------------------
    # executa entrada para py
    # ----------------------------------------

    if self.config['C_TYPE'] == "py":
      if self.py_code is None:
        self.py_code = self.config["C_PY_CODE"]
        exec(self.py_code, self.py_code_ret)
      return self.py_code_ret['getData']()

    # ----------------------------------------
    # executa entrada para database
    # ----------------------------------------

    if self.config['C_TYPE'] == "sql":
      if self.sql_is_open == False:
        if "ibm" not in self.db_type:
          self.cur.arraysize    = 10000
          self.cur.prefetchrows = 10000
          etl_utils.log(self.logger_id,  f"DB Executing...")
          self.cur.execute(self.sql_query)
          etl_utils.log(self.logger_id,  f"DB Open Success...")
        else:
          self.sql_stmt_db2 = ibm_db.exec_immediate(self.db, self.sql_query)

        self.sql_is_open = True

      if "ibm" not in self.db_type:
        return self.cur.fetchmany( self.sql_arraysize )
      else:
        x = ibm_db.fetchmany(self.sql_stmt_db2, self.sql_arraysize)
        return [] if x == None else x

    # ----------------------------------------
    # executa entrada para arquivos csv
    # ----------------------------------------

    if self.config['C_TYPE'] == "filename":
      if self.filename_file_required == False and os.path.isfile(self.filename) == False:
        etl_utils.log(self.logger_id, f"*** file not found ***")
        return []

      if self.fp == None:
        etl_utils.log(self.logger_id, f"File {self.filename}")
        self.fp = open( self.filename)
      dados = []
      loop  = True
      while loop:
        line = self.fp.readline()
        if not line:
          break
        line = normalize('NFKD', line).encode('ASCII','ignore').decode('ASCII')
        dados.append( line.strip().split( self.filename_fd ) )
        loop = len(dados) < 50000
      return dados

    # ----------------------------------------
    # executa entrada para S3(boto3)
    # ----------------------------------------

    if self.config['C_TYPE'] == "boto3":
      if self.fp == None:
          self.fp = open( self.boto3_name )
      dados = []
      loop  = True
      while loop:
        line = self.fp.readline()
        if not line:
          break
        line = normalize('NFKD', line).encode('ASCII','ignore').decode('ASCII')
        dados.append( line.strip().split( self.boto3_fd ) )
        loop = len(dados) < 50000
      return dados



  #=======================================================================================================
  #
  #=======================================================================================================

  def prepareBefore(self):
    if self.config['C_TYPE'] == "boto3":
      session = boto3.Session(aws_access_key_id=self.boto3_bucket_data['access_key'],aws_secret_access_key=self.boto3_bucket_data['secret_key'])      
      s3 = session.resource('s3')
      s3.meta.client.download_file( self.boto3_bucket_data['bucket'], self.boto3_filename, self.boto3_name )

    if self.config['C_TYPE'] == "sql" and self.sql_before != None:
      print(self.sql_before)
      self.cur.execute(  self.sql_before   )     

