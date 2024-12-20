#!/usr/bin/python3

import etl_manager
import sys

mng = etl_manager.ETL_MANAGER(sys.argv[1], -999)
mng.run()