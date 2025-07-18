#!/usr/bin/python3

import etl
import sys
import threading
import etl_utils

job_name = sys.argv[1]
batch_id = sys.argv[3]

thread_log = threading.Thread(target=etl_utils.log_buffer, args=())
thread_log.start()

mng = etl.ETL( job_name , batch_id)
mng.run()

etl_utils.LOG_verificar = False
thread_log.join()