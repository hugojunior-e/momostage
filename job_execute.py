#!/home/producao/Python/Python37/bin/python3

import etl_manager
import sys
#sys.argv[1]
mng = etl_manager.ETL_MANAGER("book_variavel_baixas_completa_S3", -999)
mng.run()