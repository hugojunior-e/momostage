import csv
import gzip
import etl_utils
import os
import sqlite3
import json
import boto3
import sys

class ETL_OUT:
  def __init__(self, idx, config, inst_mod=0,logger_id=0):
    self.config              = config
    self.idx                 = idx
    self.db                  = None
    self.cur                 = None
    self.fp                  = None
    self.data                = []
    self.inst_mod            = inst_mod
    self.logger_id           = logger_id
    self.temp_file_operation = etl_utils.get_param_value('PARAMETERS','TEMP_FILE_OPERATION')

    #-----------
    if self.value('C_TYPE') == 'sql':
      self.sql_db     = self.value('C_SQL_DB')
      self.sql        = self.value('C_SQL')
      self.sql_after  = self.value('C_SQL_AFTER')
      self.sql_before = self.value('C_SQL_BEFORE')

    #-----------

    if self.value('C_TYPE') == 'filename':
      self.filename        = self.value("C_FILENAME")      
      self.filename_fd     = self.value('C_FILENAME_FD')
      self.filename_fields = self.value("C_FILENAME_FIELDS").split("\n")

    #-----------

    if self.value('C_TYPE') == 'boto3':
      self.boto3_format      = self.value('C_BOTO3_FORMAT')
      self.boto3_fields      = [ x.strip() for x in self.value("C_BOTO3_FIELDS").split("\n") ]
      self.boto3_exists      = False
      self.boto3_filename    = self.value('C_BOTO3_FILENAME')
      self.boto3_fd          = self.value('C_BOTO3_FD')
      self.boto3_file_list   = []
      self.boto3_bucket_data = json.loads( etl_utils.get_param_value("AUTHS.S3", self.value('C_BOTO3_BUCKET')) )
      

    #-----------

    if self.value('C_TYPE') == 'dataset':
      self.dataset           = self.value("C_DATASET")
      self.dataset_idx       = []
      self.dataset_fields    = [ r.replace("*","") + "  varchar(800)" for r in self.value("C_DATASET_FIELDS").split("\n") ]
      self.dataset_binds     = [ f":{i+1}" for i in range(len(self.dataset_fields)) ]  
      for r in self.value("C_DATASET_FIELDS").split("\n"):
        if "*" in r:
          self.dataset_idx.append( r.replace("*","") )


  #=======================================================================================================
  #
  #=======================================================================================================


  def value(self, key):
    return self.config[ key.replace("C_", f"C{self.idx + 1}_") ]


  #=======================================================================================================
  #
  #=======================================================================================================


  def boto3NewFileName(self):
    last_name  = self.boto3_filename.split("/")[-1]
    gen_hash   = "gzip" in self.boto3_format
    name       = etl_utils.generate_hash( prefix=last_name , with_hash=gen_hash)
    self.boto3_file_list.append( name )
    etl_utils.log(self.logger_id, "Generated File: " + name)
    return name
  
  def boto3CheckFileSize(self):
    tamanho = os.path.getsize( self.boto3_file_local ) / 1024 / 1024
    if tamanho > 250:
      if self.fp:
        self.fp.close()

      self.boto3_exists     = False
      self.fp               = None

  def boto3SendFiles(self):
      if "__fake__" in self.boto3_filename:
        for file_orig in self.boto3_file_list:
          if os.path.exists(file_orig):
            etl_utils.log(self.logger_id, f"Removing {file_orig}")
            os.remove(file_orig)        
      else:  
        etl_utils.log(self.logger_id, "Start Upload Files")
        session = boto3.Session(aws_access_key_id=self.boto3_bucket_data['access_key'],aws_secret_access_key=self.boto3_bucket_data['secret_key'])      
        s3      = session.client('s3')

        for file_orig in self.boto3_file_list:
          if os.path.exists(file_orig):
            prefix         = self.boto3_filename.split("/")
            prefix[-1]     = file_orig.split( os.path.sep )[-1]
            file_remote    = "/".join( prefix )

            s3.upload_file( file_orig , self.boto3_bucket_data['bucket'], file_remote )
            etl_utils.log(self.logger_id, f"Uploaded File {file_orig} -> {file_remote} ")

            if self.temp_file_operation == "1":
              os.remove(file_orig)
            if self.temp_file_operation == "2":
              os.rename(file_orig, file_orig + ".done")
          else:
            etl_utils.log(self.logger_id, f"ERROR(not found) Upload File {file_orig}")

  #=======================================================================================================
  #
  #=======================================================================================================

  def execute(self, data):

    # ----------------------------------------
    # executa saidas para database
    # ----------------------------------------

    if self.value('C_TYPE') == 'sql':
      if self.db == None:
        self.db = etl_utils.connect_db( self.sql_db )
        self.cur = self.db.cursor()
      self.cur.executemany( self.sql , data)
      self.db.commit()


    # ----------------------------------------
    # executa saidas boto3 AWS
    # ----------------------------------------
            
    if self.value('C_TYPE') == 'boto3':
      if self.fp == None:
        self.boto3_file_local  = self.boto3NewFileName()

        if "gzip" in self.boto3_format:
          self.fp     = gzip.open(self.boto3_file_local,"wt", newline="", encoding='utf-8', compresslevel=1)
        else:
          self.fp     = open(self.boto3_file_local,"w", newline="", encoding='utf-8')

        self.myFile = csv.writer(self.fp, lineterminator = '\n', delimiter=self.boto3_fd  , quoting=csv.QUOTE_ALL)
        self.myFile.writerow( self.boto3_fields )
      self.myFile.writerows( etl_utils.clean_and_convert_tuples(data) )

      if "gzip" in self.boto3_format:
        self.boto3CheckFileSize()


      
        

    # ----------------------------------------
    # executa saidas arquivos de hasheds """
    # ----------------------------------------

    if self.value('C_TYPE') == 'dataset':
      if self.db == None:
        self.db = sqlite3.connect( self.dataset + "_" + str(self.inst_mod) )
        
        for r in ["PRAGMA journal_mode = wal;", "PRAGMA synchronous = 0;","PRAGMA cache_size = 1000000;","PRAGMA temp_store = MEMORY;"]:
          self.db.execute(r)

        self.db.execute( f"CREATE TABLE tab ( { ','.join(self.dataset_fields) } )"  )
        self.db.execute( f"CREATE INDEX idx on tab ( { ','.join(self.dataset_idx) } )"  )
        self.dataset_insert_sql = f"INSERT INTO  tab values  ( { ','.join(self.dataset_binds)   }) "  
                 
      self.db.executemany(self.dataset_insert_sql, data)
      self.db.commit()


    # ----------------------------------------
    # executa saidas arquivos csv """
    # ----------------------------------------

    if self.value('C_TYPE') == 'filename':
      if self.fp == None:
        self.fp     = open(self.filename,"w", encoding='utf-8')
        self.myFile = csv.writer(self.fp, lineterminator = '\n', delimiter=self.filename_fd)
        self.myFile.writerow( self.filename_fields )
      self.myFile.writerows(data)


  #=======================================================================================================
  #
  #=======================================================================================================


  def prepareBefore(self):
    if self.value('C_TYPE') == "sql" and self.sql_before != None:
      dbt   = etl_utils.connect_db( self.sql_db )
      dbt_c = dbt.cursor()
      dbt_c.execute(  self.sql_before   )     

    if self.value('C_TYPE') == 'dataset':
      for i in range(8):
        if os.path.isfile( self.dataset + "_" + str(i) ):
          os.remove( self.dataset + "_" + str(i) )


  #=======================================================================================================
  #
  #=======================================================================================================


  def prepareAfter(self):
    if self.value('C_TYPE') == "sql" and self.sql_after != None:
      dbt   = etl_utils.connect_db( self.sql_db )
      dbt_c = dbt.cursor()
      dbt_c.execute(  self.sql_after   )     


  #=======================================================================================================
  #
  #=======================================================================================================


  def finishing(self):
    if self.fp != None:
      self.fp.close()

    if self.value('C_TYPE') == 'boto3':
      self.boto3SendFiles()
      etl_utils.log(self.logger_id, "Send Files Finished...")
