from genericpath import isfile
import etl_utils
import etl_odata
import boto3
import random
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime


from unicodedata import normalize

class ETL_IN:
  def __init__(self, config, logger_id):
    self.config     = config
    self.db         = None
    self.cur        = None
    self.fp         = None
    self.filename   = None
    self.opened_sql = False
    self.logger_id  = logger_id

    if self.config['C_TYPE'] == 'boto3':
      self.hash_name    = "/tmp/" + ("%032x" % random.getrandbits(128))

    if self.config['C_TYPE'] == "sql":
        self.db  = etl_utils.connect_db( self.config['C_SQL_DB'] )
        self.cur = self.db.cursor()

    if self.config['C_TYPE'] == "odata":
        ccc         = self.config['C_ODATA_FIELDS']
        self.odata  = etl_odata.ETL_ODATA(self.config['C_ODATA_TH_COUNT'], 
                                          self.config['C_ODATA_TH_SIZE'],
                                          self.config['C_ODATA_URL'],
                                          self.config['C_ODATA_AUTH'],
                                          ccc.strip().split("\n"),
                                          logger_id=logger_id)

  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------
  #   
  def getQtdInst(self):
    if self.config['C_TYPE'] in ["filename", "odata", "xml"]:
        return 1
    else:
        return 1 if self.config['C_SQL'].find("#MOD#") < 0 else etl_utils.CONSTANT_QTD_THREADS

  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def getData(self, sql=None):


    #########
    # executa entrada para xml
    #########

    if self.config['C_TYPE'] == "xml":
        dados = []
       
        if self.fp == None:
            if self.config['C_XML_FILE_REQUIRED'] == False and os.path.isfile(self.config['C_XML_FILENAME']) == False:
                etl_utils.log(self.logger_id, "*** file not found ***")
                return []
            etl_utils.log(self.logger_id, f"File {self.config['C_XML_FILENAME']}")
            self.fp  = open( self.config['C_XML_FILENAME'] )
            self.xml = self.fp.readlines()
            self.fp.close()
            root = ET.fromstring("".join(self.xml))
            campos = self.config['C_XML_FIELDS'].split("\n")
            filtro1 = campos[0].split("/")[-2]
            for child in root.findall( filtro1 ):
                reg = []
                for r in campos:
                    f = r.split("/")[-1]
                    reg.append(  child.find(f).text )
                dados.append( reg )
        return dados

    #########
    # executa entrada para odata
    #########

    if self.config['C_TYPE'] == "odata":
        return self.odata.get_data()


    #########
    # executa entrada para database
    #########

    if self.config['C_TYPE'] == "sql":
        if self.opened_sql == False:
            self.cur.arraysize = 100000
            etl_utils.log(self.logger_id,  "Opening Cursor on DB...")
            self.cur.execute(sql)
            etl_utils.log(self.logger_id,  "Open Success...")
            self.opened_sql = True
        return self.cur.fetchmany(50000)

    #########
    # executa entrada para arquivos csv
    #########

    if self.config['C_TYPE'] == "filename":
        if self.config['C_FILENAME_FILE_REQUIRED'] == False and os.path.isfile(self.config['C_FILENAME']) == False:
            etl_utils.log(self.logger_id, "*** file not found ***")
            return []

        if self.fp == None:
            etl_utils.log(self.logger_id, f"File {self.config['C_FILENAME']}")
            self.fp = open( self.config['C_FILENAME'] )
        dados = []
        loop  = True
        while loop:
            line = self.fp.readline()
            if not line:
                break
            line = normalize('NFKD', line).encode('ASCII','ignore').decode('ASCII')
            dados.append( line.strip().split( self.config["C_FILENAME_FD"] ) )
            loop = len(dados) < 50000
        return dados
    
    #########
    # executa entrada para S3(boto3)
    #########

    if self.config['C_TYPE'] == "boto3":
        if self.fp == None:
            self.fp = open( self.hash_name )
        dados = []
        loop  = True
        while loop:
            line = self.fp.readline()
            if not line:
                break
            line = normalize('NFKD', line).encode('ASCII','ignore').decode('ASCII')
            dados.append( line.strip().split( self.config["C_BOTO3_FD"] ) )
            loop = len(dados) < 50000
        return dados



  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def prepareBefore(self):
    if self.config['C_TYPE'] == "boto3":
      dados   = json.loads( etl_utils.get_param_value("AUTHS.S3", self.config['C_BOTO3_BUCKET'] ) )
      session = boto3.Session(aws_access_key_id=dados['access_key'],aws_secret_access_key=dados['secret_key'])      
      s3 = session.resource('s3')
      s3.meta.client.download_file( dados['bucket'], self.config['C_BOTO3_FILENAME'], self.hash_name )

    if self.config['C_TYPE'] == "sql":
        sql_before = self.config['C_SQL_BEFORE']
        if sql_before != None:
            self.cur.execute(  sql_before   )     
    
