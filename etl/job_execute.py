#!/usr/bin/python3

import sys
import os
import etl
import etl_utils

def main():
    job_name    = sys.argv[1]
    batch_id    = sys.argv[3]
    loop_values = os.environ.get("LOOP_VALUES")
    
    if loop_values is not None:
        loop_list = loop_values.split(",")
        for lv in loop_list:
            mng = etl.ETL(job_name , batch_id, loop_value=lv)
            ret = mng.run()
            if ret != 0:
                break
    else:
        mng = etl.ETL(job_name , batch_id)
        mng.run()

if __name__ == "__main__":
    etl_utils.load_globals()
    main()