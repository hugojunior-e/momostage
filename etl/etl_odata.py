from encodings import utf_8
import requests
import base64
import xml.etree.ElementTree as ET
import threading
import etl_utils
import json
import time
import urllib3
from datetime import datetime



class ETL_ODATA:
    data     = []
    data_qtd = []
    error    = None
    error_bd = None

    def __init__(self,config,logger_id):
        self.config       = config
        self.P_QTD_TH     = int( self.config['C_ODATA_TH_COUNT'] )
        self.P_QTD_TH_SZ  = int( self.config['C_ODATA_TH_SIZE'] )
        self.P_URL        = self.config['C_ODATA_URL']
        self.logger_id    = logger_id

        urllib3.disable_warnings()

        d                 = json.loads( etl_utils.get_param_value("AUTHS.SAP", self.config['C_ODATA_AUTH']  ) )
        self.P_USR        = d['usr']
        self.P_PWD        = d['pwd']
        self.P_FLD        = self.config['C_ODATA_FIELDS'].strip().split("\n")
        self.i_page       = 0

        etl_utils.log(self.logger_id, f'URL: { self.P_URL + "&$top=500&$skip=0" }')

    ##---------------------------------------------------------------
    ##---------------------------------------------------------------

    def ws_page(self, p_page, p_qtd_by_page, idx):
        global data
        global data_qtd
        global error
        global error_bd

        dados_o = ""
        threading.current_thread().page   = p_page
        threading.current_thread().dt_ini = datetime.now()
        threading.current_thread().count  = 0
        threading.current_thread().dt_fim = datetime.now()
        try:
            v_url = self.P_URL + "&$top=" + str(p_qtd_by_page) + "&$skip=" + str(p_page * p_qtd_by_page) 
            #print(v_url)
            encoded = base64.b64encode( (self.P_USR + ":" + self.P_PWD).encode('utf-8') )
            my_headers = {
                'Authorization': 'Basic ' + encoded.decode("utf-8"),
                'Content-type': 'text/plain; charset=utf-8',
                'Accept-Charset': 'UTF-8'
            }

            namespaces = {'m': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
                        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices' }

            dados_x    = requests.get( v_url , headers=my_headers, verify=False).content.decode("UTF-8") 
            dados_o    = dados_x.replace( chr(29), "")
            error_bd   = dados_o

            if dados_o.find('401 Nicht autorisiert') >= 0:
                error = f"LOG - 401 Nicht autorisiert page {p_page}"
                return True
            
            if dados_o.find('Connection Timed Out') >= 0:
                error = f"LOG - Connection Timed Out page {p_page}"
                return True
            

            root           = ET.fromstring(dados_o)
            qtd = 0
            for child in root.findall("{http://www.w3.org/2005/Atom}entry"):
                reg_item = []
                for child2 in child.findall("{http://www.w3.org/2005/Atom}content"):
                    for child3 in child2.findall("m:properties",namespaces):
                        for info_field_name in self.P_FLD:
                            info           = child3.find("d:" + info_field_name,namespaces)

                            if info == None or info.text == None:
                                info_value = ""
                            else:
                                info_value = info.text.replace("\n","").replace('"','')

                            reg_item.append(info_value)

                        qtd = qtd + 1
                        data.append( reg_item )

            data_qtd[idx]                       = qtd 
            threading.current_thread().count    = qtd 
            threading.current_thread().dt_fim   = datetime.now()

        except Exception as e:
            data_qtd[idx] = 0
            error = "ERROR: " + str(e)

##---------------------------------------------------------------
##---------------------------------------------------------------

    def get_data(self):
        global data
        global data_qtd
        global error
        global error_bd

        qtd_th   = self.P_QTD_TH
        threads  = []
        data     = []  
        data_qtd = [0 for _ in range(qtd_th)]    
        error    = None
        error_bd = None


        for x in range(qtd_th):
            etl_utils.log(self.logger_id, f'INI page {self.i_page}')

            th        = threading.Thread(   target=self.ws_page    , args=(self.i_page ,self.P_QTD_TH_SZ, x)    )
            th.start()
            threads.append(th)
            self.i_page = self.i_page + 1
            time.sleep(5)

        for t in threads:
            t.join()
            etl_utils.log(self.logger_id, f"END page {t.page} QTD {  t.count } Time Elapsed: { etl_utils.diff_date(t.dt_fim,t.dt_ini )  } " )

        if error != None:
            etl_utils.log(self.logger_id, error,shortcut="I",logbigdata=error_bd)
            raise Exception(error)
        
        x = 0
        while x < qtd_th-1:
            if data_qtd[x] == 0 and data_qtd[x+1] > 0:
                raise Exception("ERROR: possible COREDUMP_001")
            x = x+1

        return data
