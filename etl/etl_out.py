import csv
import etl_utils
import os
import sqlite3
import json
import boto3
import random

class ETL_OUT:
  def __init__(self, idx, config, inst_mod=0,logger_id=0):
    self.config     = config
    self.idx        = idx
    self.db         = None
    self.cur        = None
    self.fp         = None
    self.data       = []
    self.inst_mod   = inst_mod
    self.hash_name  = None
    self.hash_cols  = None
    self.hash_binds = None
    self.hash_idx   = []
    self.logger_id  = logger_id

    if self.value('C_TYPE') == 'boto3':
      self.hash_name    = "/tmp/" + ("%032x" % random.getrandbits(128))

    if self.value('C_TYPE') == 'dataset':
      self.hash_name    = self.value("C_DATASET")
      self.hash_cols    = [ r.replace("*","") + "  varchar(800)" for r in self.value("C_DATASET_FIELDS").split("\n") ]
      self.hash_binds   = [ f":{i+1}" for i in range(len(self.hash_cols)) ]  
      for r in self.value("C_DATASET_FIELDS").split("\n"):
        if "*" in r:
          self.hash_idx.append( r.replace("*","") )

  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def value(self, key):
    return self.config[ key.replace("C_", f"C{self.idx + 1}_") ]


  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def execute(self, data):
    #########
    # executa saidas para database
    #########

    if self.value('C_TYPE') == 'sql':
      if self.db == None:
        self.db = etl_utils.connect_db( self.value('C_SQL_DB') )
        self.cur = self.db.cursor()
      self.cur.executemany( self.value('C_SQL') , data)
      self.db.commit()


    #########
    # executa saidas boto3 AWS
    #########

    if self.value('C_TYPE') == 'boto3':
      if self.fp == None:
        self.fp     = open(self.hash_name,"w", encoding='utf-8')
        self.myFile = csv.writer(self.fp, lineterminator = '\n', delimiter=self.value('C_BOTO3_FD')  )
        colunas     = self.value("C_BOTO3_FIELDS").split("\n")
        self.myFile.writerow( colunas )
      self.myFile.writerows(data)

    #########
    # executa saidas arquivos de hasheds
    #########

    if self.value('C_TYPE') == 'dataset':
      if self.db == None:
        self.db = sqlite3.connect( self.hash_name + "_" + str(self.inst_mod) )
        
        for r in ["PRAGMA journal_mode = wal;", "PRAGMA synchronous = 0;","PRAGMA cache_size = 1000000;","PRAGMA temp_store = MEMORY;"]:
          self.db.execute(r)

        self.db.execute( f"CREATE TABLE tab ( { ','.join(self.hash_cols) } )"  )
        self.db.execute( f"CREATE INDEX idx on tab ( { ','.join(self.hash_idx) } )"  )
        self.insert_sql = f"INSERT INTO  tab values  ( { ','.join(self.hash_binds)   }) "  
                 
      self.db.executemany(self.insert_sql, data)
      self.db.commit()


    #########
    # executa saidas arquivos csv
    #########

    if self.value('C_TYPE') == 'filename':
      if self.fp == None:
        self.fp     = open(self.value("C_FILENAME"),"w", encoding='utf-8')
        self.myFile = csv.writer(self.fp, lineterminator = '\n', delimiter=self.value('C_FILENAME_FD'))
        self.myFile.writerow( self.value("C_FILENAME_FIELDS").split("\n") )
      self.myFile.writerows(data)


  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def prepareBefore(self):
    if self.value('C_TYPE') == "sql" and self.value('C_SQL_BEFORE') != None:
      dbt   = etl_utils.connect_db( self.value('C_SQL_DB') )
      dbt_c = dbt.cursor()
      sql   = self.value('C_SQL_BEFORE')
      dbt_c.execute(  sql   )     

    if self.value('C_TYPE') == 'dataset':
      for i in range(8):
        if os.path.isfile( self.hash_name + "_" + str(i) ):
          os.remove( self.hash_name + "_" + str(i) )


  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def prepareAfter(self):
    if self.value('C_TYPE') == "sql" and self.value('C_SQL_AFTER') != None:
      dbt   = etl_utils.connect_db( self.value('C_SQL_DB') )
      dbt_c = dbt.cursor()
      sql   = self.value('C_SQL_AFTER')
      dbt_c.execute(  sql   )     


  #-----------------------------------------------------------------------------------------
  #-----------------------------------------------------------------------------------------

  def finishing(self):

    if self.value('C_TYPE') == 'filename':
      self.fp.close()

    if self.value('C_TYPE') == 'boto3':
      self.fp.close()
      dados   = json.loads( etl_utils.get_param_value("AUTHS.S3", self.value('C_BOTO3_BUCKET')) )

      etl_utils.log(self.logger_id, "sending to S3....session")
      session = boto3.Session(aws_access_key_id=dados['access_key'],aws_secret_access_key=dados['secret_key'])      

      etl_utils.log(self.logger_id, "sending to S3....client")
      s3 = session.client('s3')

      etl_utils.log(self.logger_id, "sending to S3....upload_file " + self.value('C_BOTO3_FILENAME'))
      s3.upload_file( self.hash_name, dados['bucket'], self.value('C_BOTO3_FILENAME'))

      etl_utils.log(self.logger_id, "sending to S3....OK")
