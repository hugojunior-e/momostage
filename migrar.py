#!/home/producao/Python/Python37/bin/python3

import cx_Oracle
import sys
import os
from unicodedata import normalize
import subprocess


os.environ["LD_LIBRARY_PATH"] += ":/opt/IBM/InformationServer/Server/biginsights/IHC/c++/Linux-amd64-64/lib:/opt/IBM/InformationServer/Server/branded_odbc/lib:/opt/IBM/InformationServer/Server/DSComponents/lib:/opt/IBM/InformationServer/Server/DSComponents/bin:/opt/IBM/InformationServer/Server/DSEngine/bin:/opt/IBM/InformationServer/Server/DSEngine/lib:/opt/IBM/InformationServer/Server/DSEngine/uvdlls:/opt/IBM/InformationServer/Server/PXEngine/lib:/opt/IBM/InformationServer/Server/PXEngine/bin:/opt/IBM/InformationServer/jdk/jre/lib/amd64/j9vm:/opt/IBM/InformationServer/jdk/jre/lib/amd64:/opt/IBM/InformationServer/ASBNode/lib/cpp:/opt/IBM/InformationServer/ASBNode/apps/proxy/cpp/linux-all-x86_64:/home/producao/Python/Extras/instantclient_10_2:/u01/app/oracle/product/11.2.0/client/lib:/u01/app/oracle/product/11.2.0/client/rdbms:/usr/lib64:/usr/lib:."
os.environ["PATH"] += ":/opt/IBM/InformationServer/Server/DSEngine/bin"

jobs = ["ODSCMCAContestFaturaDet"]

for job in jobs:
    proc = subprocess.run(
        ["dsjob" ,"-logdetail", "DWDiariaAlgar", job,  "-wave", "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    txt = proc.stdout
    i = ""
    y = ""
    for x in txt.split("\n"):
        if x.find("Number of rows inserted on the current node") > 0:
            print( "--" + x )
