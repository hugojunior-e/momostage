#!/usr/bin/python3

import etl
import sys

def main():
    job_name = sys.argv[1]
    batch_id = sys.argv[3]

    mng = etl.ETL( job_name , batch_id)
    mng.run()

if __name__ == "__main__":
    main()