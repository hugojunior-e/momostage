#!/usr/bin/python3

import etl
import platform
import multiprocessing


if platform.system() == "Windows":
    multiprocessing.freeze_support()    
job_name = "d_customer"
job_lote = 999

mng = etl.ETL( job_name , job_lote)
mng.run()
