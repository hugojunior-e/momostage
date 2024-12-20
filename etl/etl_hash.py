import sqlite3
import etl_utils
import os
class ETL_HASH:
    def __init__(self, hash_file):
        self.key        = None
        self.db         = []
        self.cols       = []
        self.ret        = {}

        for i in range(etl_utils.CONSTANT_QTD_THREADS):
            if os.path.isfile(hash_file + "_" + str(i)):
                tmp = sqlite3.connect(hash_file + "_" + str(i))
                tmp.execute("PRAGMA journal_mode = wal;")      

                x = tmp.execute("pragma table_info(tab)")      
                self.cols = [ cc[1] for cc in x.fetchall() ]

                x = tmp.execute("select * from sqlite_master WHERE type = 'index' and tbl_name='tab'")
                self.key = (x.fetchone()[4]).split(" ")[6]

                self.db.append(tmp.cursor())

                for c in self.cols:
                    self.ret[ c ] = ""                

    def value(self, key_value):
        for c in self.cols:
            self.ret[ c ] = ""

        if key_value == None or len(key_value) == 0:
            return self.ret

        idx = 0 if len(self.db) == 1 else ord(key_value[-1]) % etl_utils.CONSTANT_QTD_THREADS
        
        tt = self.db[idx].execute( f"SELECT * FROM tab where {self.key} = '{key_value}' ")
        dd = tt.fetchone()
        if dd != None:
            for i, a in enumerate(dd):
                self.ret[ self.cols[i] ] = a
        
        return self.ret
