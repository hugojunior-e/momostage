#!/usr/bin/python3

import etl.etl_utils as eu
import subprocess
import os
import glob
import re
import paramiko
from ldap3 import Server, Connection, ALL, core
from flask import Flask, render_template, redirect, request, session
from markupsafe import Markup
from flask_session import Session
from datetime import datetime

app                 = Flask(__name__)

# ---------------- Configuration ----------------
app.config["SESSION_PERMANENT"] = False          # Sessions expire when browser closes
app.config["SESSION_TYPE"]      = "filesystem"   # Store session data on the filesystem
app.config["SESSION_FILE_DIR"]  = "/app/cache"

Session(app)
# ---------------- /Configuration ---------------


app.db              = eu.local_db()


#######################################################################################
#  autenticacao ldap
#######################################################################################

def ldap_check(ad_servidor, usuario_dn, senha):
    try:
        servidor = Server(ad_servidor, get_info=ALL)
        conexao = Connection(
            servidor,
            user=usuario_dn, 
            password=senha,
            authentication='SIMPLE',
            auto_bind=True
        )
        conexao.unbind()
        return True
    except core.exceptions.LDAPBindError as e:
        return False

def ldap_login(username, password):
    ad_servidor = "10.51.47.125"
    for domains in ["associado","terceiro","temporario"]:
        user_dn = f"CN={username},ou={domains},cn=Users,dc=network,dc=ctbc"
        if ldap_check(ad_servidor=ad_servidor, usuario_dn=user_dn, senha=password):
            return True
    return False


#######################################################################################
#  rotinas gerais
#######################################################################################


class db_type:
    data = []
    fields = []


def db_sql(sql_name,params=[]):
    cur = app.db.cursor()
    cur.execute( f"SELECT PARAM_VALUE FROM DWADM.MS_JOB_GLOBALS WHERE GROUP_NAME = 'SQL' AND PARAM_NAME='{sql_name}'" )
    ret = cur.fetchone()[0]
    cur.close()
    for i,x in enumerate(params):
        ret = ret.replace(f"<{i+1}>", x)
    return ret



def db_execute(sql,params=None, fetch=True):
    ret = db_type()
    cur = app.db.cursor()
    if params != None:
        cur.execute(sql,params)  
    else:
        cur.execute(sql)    
    if fetch:
        ret.fields = [  cur.description[i][0] for i in range(0, len(cur.description))  ]
        ret.data   = cur.fetchall()
    cur.close()
    return ret


def add_conf(tag, value, nosep=False):
    svalue = ""    if value == None  else value.strip() 
    sep    = '"""' if "\n" in svalue else '"'
    
    if svalue == "" or len( svalue.strip() ) == 0:
        sep=""
        svalue="None"

    if nosep:
        sep = ""

    if svalue == 'true':
        sep = ""
        svalue = "True"

    if svalue == 'false':
        sep = ""
        svalue = "False"

    ret = f'''{tag}={sep}{ svalue }{sep}'''
    return ret.strip()


def create_component(local, config, addin="", index=""):
    type = config[f'C{index}_TYPE']
   
    if (type == 'transf'):
        name = "TRANSF.tr"
    else:
        name = config[f'C{index}_NAME']
    menu = """
        <div class="dropdown-content">
            <a href="#" onClick="jsComponentAction(this, 1)">Edit</a>
            <a href="#" onClick="jsComponentAction(this, 2)">Remove</a>
        </div>    
    """
    ret = Markup(f'<div class="dropdown"><input class="dropbtn" {addin} type=image src=/static/img/icon_{type}.png  local="{local}" type_obj="{type}" ><br><span>{name}</span> {menu} </div><br>')
    return ret



#######################################################################################
#  rotinas gerais
#######################################################################################


def importar_job_datastage():
    host = "datastageprd01"
    usuario = "producao"
    senha = "Pr0duc@0s02010"
    comando = f"""/opt/IBM/pastasletras/scripts/carga/lixo/migrar.py { request.form.get("job_name") }"""

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Conecta no servidor
        client.connect(hostname=host, username=usuario, password=senha)

        # Executa o comando
        stdin, stdout, stderr = client.exec_command(comando)

        # Captura saída
        output = stdout.read().decode("utf-8")
        error = stderr.read().decode("utf-8")

        client.close()

        # Se tiver erro, você pode tratar aqui
        if error:
            return f"ERRO:\n{error}"

        return {"message": output}
    except Exception as e:
        return {"message": f"Falha na execucao: {str(e)}" }




def metricas_log(log_texto: str):
    loop_value_ret = ""

    if "LOOP_VALUE" in log_texto:
        linhas = log_texto.splitlines()
        idx_loop = None

        for i, linha in enumerate(linhas):
            if "LOOP_VALUE" in linha:
                match = re.search(r'LOOP_VALUE=([^\]\s]+)', linha)
                loop_value_ret = f" [ { match.group(1) } ]"
                idx_loop = i

        if idx_loop is not None:
            log_texto = "\n".join(linhas[idx_loop:])

    # --- Datas ---
    padrao_data = re.compile(
        r'^(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})',
        re.MULTILINE
    )
    datas = padrao_data.findall(log_texto)

    if len(datas) < 2:
        tempo_decorrido = None
    else:
        inicio     = datetime.strptime(datas[0], "%d/%m/%Y %H:%M:%S")
        fim        = datetime.strptime(datas[-1], "%d/%m/%Y %H:%M:%S")
        fim_online = fim

        if not log_texto.strip().endswith("STATUS:OK") and not log_texto.strip().endswith("STATUS:ERRO"):
            fim_online = datetime.now().replace(microsecond=0)

        tempo_decorrido = fim_online - inicio

    # --- Últimos valores ---
    ult_qtd_parc = {}
    ult_fetchall = {}

    encontrou_inst = False
    ultimo_qtd_sem_inst = None
    ultimo_fetch_sem_inst = None

    # --- Processa linha a linha ---
    for linha in log_texto.splitlines():
        m_inst = re.search(r'INST=\[(\d+)/\d+\]', linha)

        if m_inst:
            encontrou_inst = True
            inst = int(m_inst.group(1))
        else:
            inst = None

        # qtd_parc
        m_qtd = re.search(r'qtd_parc=([\d,]+)', linha)
        if m_qtd:
            valor = int(m_qtd.group(1).replace(',', ''))
            if inst is not None:
                ult_qtd_parc[inst] = valor
            else:
                ultimo_qtd_sem_inst = valor

        # FetchALL
        m_fetch = re.search(r'FetchALL=([\d,]+)', linha)
        if m_fetch:
            valor = int(m_fetch.group(1).replace(',', ''))
            if inst is not None:
                ult_fetchall[inst] = valor
            else:
                ultimo_fetch_sem_inst = valor

    # --- Cálculo final ---
    total_records = 0
    qtd_text = "Total  :"

    if encontrou_inst:
        for inst in set(ult_qtd_parc) | set(ult_fetchall):
            if inst in ult_fetchall:
                total_records += ult_fetchall[inst]
            else:
                total_records += ult_qtd_parc.get(inst, 0)
                qtd_text = "Partial:"
    else:
        if ultimo_fetch_sem_inst is not None:
            total_records = ultimo_fetch_sem_inst
            qtd_text = "Total  :"
        elif ultimo_qtd_sem_inst is not None:
            total_records = ultimo_qtd_sem_inst
            qtd_text = "Partial:"
        else:
            total_records = 0
            qtd_text = "Total  :"

    # --- QtdFiles ---
    qtd_files = len(re.findall(r'Generated File:', log_texto))

    # --- Throughput ---
    tempo_rec     = fim - inicio
    tempo_rec_seg = tempo_rec.total_seconds()

    if tempo_rec_seg > 0:
        rec_por_seg = int(total_records / tempo_rec_seg)
    else:
        rec_por_seg = 0

    return f"Time: {tempo_decorrido} ".ljust(20) + f"{qtd_text} {total_records:,} ".ljust(25) + f"Speed: {rec_por_seg}/seg ".ljust(20) + f"Files Count: {qtd_files}", loop_value_ret


    

def job_info_memory(info):
    titulo      = ["", "total", "used", "free", "%"]
    mem         = info.get("mem_data", [0,0,0])
    swap        = info.get("swap_data", [0,0,0])
    usage       = info.get("app_data", [0,0,0])

    a,b,c        = info.get("load_average", [0,0,0])
    cpu_percent  = info.get("cpu_usage", 0)
    cpu_count    = info.get("cpu_count", 1)
    

    infos = [
        ["Mem:", mem[0], mem[1], mem[2], mem[3] ],
        ["Swap:", swap[0], swap[1], swap[2], swap[3] ],
        ["/app:", usage[0], usage[1], usage[2], usage[3] ]
    ]

    return {"data"  : infos, 
            "fields": titulo, 
            "gauge1": f"<span>CPU%</span><h2>{ round(cpu_percent,2) }</h2><span>{a:.2f},{b:.2f},{c:.2f}<br>{cpu_count}</span>", 
    }


def job_info_last_abort():
    titulo   = ["#", "name", "batch_id", "started_at", "managed_by", "time", ""]
    sql      = db_sql("job_get_last_executions")
    data     = db_execute(sql)
    return {"data":data.data, "fields": titulo}




def job_info_kill_process():
    eu.load_globals()

    users_permit = eu.get_param_value("PARAMETERS","KILL_PROCESS")
    pid          = request.form.get("pid")
    tipo_cmd     = request.form.get("tipo_cmd")
    comando      = request.form.get("cmd")
    userId       = session.get("status_login").upper()

    if pid != None and pid != "":
        if "local" in tipo_cmd:
            subprocess.run( f"ps -eo pid,ppid | awk '$1 == {pid} || $2 == {pid} {{ print $1 }}' | xargs -r kill -9", shell=True )
        else:
            if userId in users_permit:
                subprocess.run( f"pgrep -f { comando } | xargs kill -9", shell=True )
                arquivos = glob.glob( f"/home/producao/DW/py/lock/*{ comando }*lock" )
                for arquivo in arquivos:
                    os.remove(arquivo)
            else:
                return {"message" : "User not permited kill process"}            
        ret = f"Process {pid} killed."
    return {"message":ret}



def job_info_sequence_save():
    sql = db_sql("job_sequence_save", [])
    pars = (
                request.form.get("rowid"),
                request.form.get("SEQ_NAME"),
                request.form.get("JOB_NAME"),
                request.form.get("PARAMETERS"),
                request.form.get("ORDER_PRI"),
                request.form.get("ORDER_SEC"),
                request.form.get("FL_ACTIVE"),
                request.form.get("action"),
    )
    db_execute(sql, params=pars, fetch=False)
    return { "message": "OK" }


def job_info_globals_save():
    sql = db_sql("job_globals.save", [])
    pars = (
                request.form.get("rowid"),
                request.form.get("GROUP_NAME"),
                request.form.get("PARAM_NAME"),
                request.form.get("PARAM_VALUE"),
                request.form.get("DESCRIPTION")
    )
    db_execute(sql, params=pars, fetch=False)
    return { "message": "OK" }


def job_info_result_set():
    sql        = db_sql(request.form.get("sql"))
    sql_pars   = request.form.get("sql_pars").split("|")
    for i,x in enumerate(sql_pars):
        sql = sql.replace( f"<{i+1}>", x)

    ret = db_execute(sql)    
    return {"data":ret.data, "fields":ret.fields}


def job_info_logger_batch_id():
    log_id   = request.form.get("batch_id")
    log_job  = request.form.get("job_name")
    log_file = f"{ eu.get_log_dir() }/{ log_id }.log"
    log_data = {}
    ret1     = ""
    ret2     = ""
    ret3     = ""

    if os.path.exists(log_file):
        with open(log_file, "r") as arq:
            for idx, x in enumerate(arq.readlines()):
                match    = re.search(r'\[(.*?)\]', x)
                if match:
                    job_name = match.group(1)
                    job_line = x
                    if log_job == None or log_job == job_name:
                        if "[BIGDATA]" in x:
                            xxx             = x.split("[BIGDATA]")
                            job_line        = f"{xxx[0]}<a onclick=js_bigdata({idx}) href=#>[BIGDATA]</a>\n<vv id=big_{idx}>{xxx[1][1:]}</vv>"

                        job_val  = log_data.get(job_name)
                        log_data[job_name] = ("" if job_val == None else job_val ) + job_line

        xx_id = 0
        for x in log_data:
            xx_id += 1
            icone = "▶️"

            log_txt             = log_data[x]
            log_metrica, log_lv = metricas_log(log_txt)
            
            if log_txt.strip().endswith("STATUS:OK"):
                icone = "🟢" 

            if log_txt.strip().endswith("STATUS:ERRO"):
                icone = "🔴" 
                
            ret_header = f"""{icone} <a href=/designer?job_name={x} target=__blank>[edit]</a> - <a onclick=js_log_in_row('{xx_id}') style='cursor:pointer'>{ (x + log_lv).ljust(50) }</a> {log_metrica} <hr>"""
            ret_data   = f"<div id=id_log_{ xx_id } style='display:none'>{ log_txt }<hr></div>"
            if icone == "▶️":
                ret1   = ret1 + ret_header + ret_data
            if icone == "🔴":
                ret2   = ret2 + ret_header + ret_data
            if icone == "🟢":
                ret3   = ret3 + ret_header + ret_data
    else:
        ret = "Logfile not found!"
        
    return { "message": ret1+ret2+ret3 }


def job_info_logger_big():
    sql = db_sql("job_logger_big", [request.form.get("rid")])
    x   = db_execute(sql).data
    return { "message": x[0][0].read() }


def job_execute():
    job_name            = request.form.get("job_name")
    job_run_parameters  = request.form.get("job_run_parameters").strip().split("\n")
    job_run_loop_values = request.form.get("job_run_loop_values").strip()
    path_etl            = os.path.dirname(os.path.abspath(__file__))
    lista               = []
    env                 = os.environ.copy()
    if len(job_run_loop_values) > 1:
        env['LOOP_VALUES'] = job_run_loop_values

    for x in job_run_parameters:
        if len(x.strip()) > 0:
            a,b = x.split("=",1)
            lista.append( f'''"{a}":"{b}"''' )

    batch_id = eu.create_job_batch(job_name=job_name,created_by=session.get("status_login"), managed_by="algaretl", parameters=("{\n" + ",".join(lista) + "\n}") )
    subprocess.Popen( f"""nohup {path_etl}/etl/job_execute.py { job_name } { session.get("status_login") } {batch_id} &""", shell=True, env=env)
    return {"message" :"Started..." }




def job_sequence_start():
    seq_name = request.form.get("SEQ_NAME")
    print(f"""nohup /home/producao/DW/py/ALGAR_PRD_ExecGen.py -P {seq_name} &""")
    subprocess.Popen( f"""nohup /home/producao/DW/py/ALGAR_PRD_ExecGen.py -P {seq_name} &""", shell=True)
    return {"message": "Sequence started."}



def job_info_check_abort():
    eu.load_globals()

    batch_id     = request.form.get("batch_id")
    userId       = session.get("status_login").upper()
    users_permit = eu.get_param_value("PARAMETERS","OPERATION_CHECK")
    if userId in users_permit:
        sql      = db_sql("job_check_abort", [batch_id,userId])
        try:
            db_execute(sql,fetch=False)
        except Exception as e:
            return { "status_code": "1", "message": str(e) }
        return { "status_code": "0", "message": "OK" }
    else:
        return { "status_code": "1", "message": "User not permit check aborted process!" }

def job_info_detect_fields():
    eu.load_globals()
    db_name = request.form.get("db_name")
    db_sql  = request.form.get("db_sql")
    ret     = ""
    if db_name == None or db_name == "":
        return {"message": "No database selected."}
    try:
        tt   = eu.connect_db(db_name)
        cc   = tt.cursor()
        cc.execute("SELECT * FROM (\n" + db_sql.replace("#MOD#","0").replace("#RESTO#","0") + "\n) where 1=2")
        ret = "\n".join( [ desc[0] for desc in cc.description] )
    except Exception as e:
        ret = str(e)

    return { "message": ret }


def test_connection():
    eu.load_globals()
    db_name = request.form.get("db_name")
    if db_name == None or db_name == "":
        return {"message": "No database selected."}
    try:
        tt = eu.connect_db(db_name)
        if tt is None:
            return {"message": f"Could not connect to {db_name}."}
        return {"message": f"Connection to {db_name} successful."}
    except Exception as e:
        return {"message": f"Error connecting to {db_name}: {str(e)}"}
    

def job_info_init_values():
    sql                 = db_sql("job_combobox")
    l_cbo_databases     = ",".join(  [x[0] for x in db_execute(sql.replace("<1>", 'DATABASES'  )).data]   )
    l_cbo_auths_sap     = ",".join(  [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.SAP'  )).data]   )
    l_cbo_projetos      = ",".join(  [x[0] for x in db_execute(sql.replace("<1>", 'FOLDER_ROOT')).data]   )
    l_cbo_boto3         = ",".join(  [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.S3'   )).data]   )
    l_cbo_sharepoint    = ",".join(  [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.SHAREPOINT'   )).data]   )

    return {"cbo_databases"  : l_cbo_databases,
            "cbo_auths_sap"  : l_cbo_auths_sap,
            "cbo_projetos"   : l_cbo_projetos,
            "cbo_boto3"      : l_cbo_boto3,
            "cbo_sharepoint" : l_cbo_sharepoint   }


def job_info_delete_job():
    eu.load_globals()
    
    users_permit = eu.get_param_value("PARAMETERS","DELETE_JOB")
    job_name     = request.form.get("job_name")
    userId       = session.get("status_login").upper()

    if userId in users_permit:
        sql      = db_sql("job_delete", [job_name])
        db_execute(sql, fetch=False)
        return { "status_code": "0", "message": "Delete Successful" }
    else:
        return { "status_code": "1", "message": "User not permit to delete job!" }

#######################################################################################
#  
#######################################################################################



@app.route("/info",methods = ['POST'])
def info():
    name = request.form.get("name")

    if name == "init_values":
        return job_info_init_values()
    
    if name == "check_abort":
        return job_info_check_abort()
    
    if name == "delete_job":
        return job_info_delete_job()
    
    if name == "job_sequence_save":
        return job_info_sequence_save()

    if name == "job_globals_save":
        return job_info_globals_save()

    if name == "kill_process":
        return job_info_kill_process()

    if name == "result_set":
        return job_info_result_set()

    if name == "log_batch_id":
        return job_info_logger_batch_id()

    if name == "log_big":
        return job_info_logger_big()

    if name == "job_execute":
        return job_execute()
    
    if name == "test_connection":
        return test_connection()
    
    if name == "job_sequence_start":
        return job_sequence_start()
    
    if name == "detect_fields":
        return job_info_detect_fields()
    
    if name == "importar_job_datastage":
        return importar_job_datastage()

    if name == "dashboard_index":
        info            = eu.get_server_info()
        titulo          = ["pid", "ppid", "start", "%mem", "pcpu", "command", ""]
        process_running = {"data": info.get("process_run", []), "fields": titulo}

        return {"memory": job_info_memory(info), "process_aborted": job_info_last_abort(), "process_running": process_running }




#######################################################################################
#  load and save job
#######################################################################################



@app.route("/job_save",methods = ['POST'])
def job_save():
    job_name              = request.form.get("job_name")
    job_parameters        = request.form.get("C_JOB_DEF_PARAMETERS")
    job_folders_name      = request.form.get("C_JOB_DEF_FOLDERS_NAME")
    job_components        = request.form.get("job_components").split(",")
    jsh                   = add_conf("C_JOB_SH_BEFORE", request.form.get("C_JOB_SH_BEFORE") ) + "\n" + add_conf("C_JOB_SH_AFTER", request.form.get("C_JOB_SH_AFTER") )
    sql_job_save_transf   = db_sql("job_save_transf" )
    sql_job_save_def      = db_sql("job_save_def" )
    sql_job_save_versiona = db_sql("job_save_versiona", [job_name])

    db_execute( sql_job_save_versiona , fetch=False)

    DATASETS = request.form.get("DATASETS")
    TARGET   = []
    ORIGIN   = []
    
    
    if len(request.form.get("TRANSFORM_DATA")) > 1:
        db_execute(sql_job_save_transf, 
                    params=(job_name, 1, request.form.get("TRANSFORM_DATA") + "\n", request.form.get("TRANSFORM_FILTER"), request.form.get(f"TRANSFORM_LOOKUPS") ),
                    fetch=False )        
    
    qtd_outs = 1

    for cc in job_components:
        cc0       = cc.split("/")[0]

        cc_orig   = request.form.get(f"ORIG_{cc0}")    

        if cc_orig == "null":
            cc_orig = ""

        if cc_orig != None and cc_orig != "undefined":
            ORIGIN.append(add_conf(cc0,cc_orig))    

        for cx in [1,2,3,4]:
            cc_target = request.form.get(f"TARGET_C{cx}{cc0[1:]}") 
            if cc_target != None:
                if cc_target == "null":
                    cc_target = ""                
                if cc_target != None and cc_target != "undefined":
                    TARGET.append(add_conf( f"C{cx}{cc0[1:]}" ,cc_target))    
                    qtd_outs = qtd_outs if qtd_outs < cx else cx
                
    TARGET.insert(0, add_conf('C_OUT_COUNT', str(qtd_outs), nosep=True)) 

    db_execute(sql_job_save_def, params=(job_name, 'JOB', "\n".join(ORIGIN) , "\n".join(TARGET), DATASETS , job_folders_name, jsh, job_parameters ),fetch=False)
    app.db.commit()

    return { "message": "Saved successfully" }



@app.route("/job_load",methods = ['POST'])
def job_load():
    ret                           = {}
    ret['C_JOB_SH_BEFORE']        = ""
    ret['C_JOB_SH_AFTER']         = ""
    ret['C_JOB_DEF_PARAMETERS']   = ""
    ret['C_JOB_DEF_FOLDERS_NAME'] = ""
    job_name                      = request.form.get("job_name")
    
    id_comp = 0

    if "newjob" not in job_name :
        job_components = request.form.get("job_components").split(",")

        confs          = db_execute( db_sql("job_def_conf",[job_name])    ).data[0]
        transfs        = db_execute( db_sql("job_def_transf",[job_name])  ).data
        
        dados_orig     = {}
        dados_target   = {}

        exec(confs[0].read(), dados_orig)
        exec(confs[1].read(), dados_target)

        ret['C_JOB_DEF_PARAMETERS']   = confs[3]
        ret['C_JOB_DEF_FOLDERS_NAME'] = confs[4]

        ## ----------------------------------------------------------------------
        ## ----- job shell before / after
        job_sh = confs[5]
        if job_sh and len(job_sh.read()) > 0:
            a = {}
            exec( job_sh.read() ,a )
            ret['C_JOB_SH_BEFORE']  = a['C_JOB_SH_BEFORE']
            ret['C_JOB_SH_AFTER']   = a['C_JOB_SH_AFTER' ]

        ## ----------------------------------------------------------------------
        ## ----- origem -------------------
        ## ----------------------------------------------------------------------

        id_comp += 1
        origem   = create_component("orig",dados_orig, addin=f"id=COMP_{id_comp}", index="")

        for x in job_components:
            x1 = x.split("/")[0]
            if x1 in dados_orig:
                ret[f"COMP_{id_comp}.{x1}"] = dados_orig[x1]

        ## ----------------------------------------------------------------------
        ## ----- destino -------------------
        ## ----------------------------------------------------------------------

        destino = ""
        qtd     = int(  dados_target['C_OUT_COUNT']  )
        for i in range(qtd):
            id_comp += 1
            destino = destino + create_component("target",dados_target,addin=f"id=COMP_{id_comp}", index=str(i+1))
            for x in job_components:
                x1 = x.split("/")[0]
                if ( "C" + str(i+1) + x1[1:] ) in dados_target:
                    ret[ f"COMP_{id_comp}.{x1}" ] = dados_target[ "C" + str(i+1) + x1[1:]]

        ## ----------------------------------------------------------------------
        ## ----- datasets -------------------
        ## ----------------------------------------------------------------------

        dataset      = ""
        dataset_data = ""
        if confs[2] != None:
            dataset_data = confs[2].read()
            for x in dataset_data.strip().split("\n"):
                id_comp += 1
                dataset = dataset + create_component("dataset",{"C_TYPE":"dataset","C_NAME":x.split("/")[-1] } , addin=f"id=COMP_{id_comp}", index="")
                ret[ f"COMP_{id_comp}.DATASETS" ] = x

        ## ----------------------------------------------------------------------
        ## ----- transform -------------------
        ## ----------------------------------------------------------------------
        
        transform                = "" if len(transfs) == 0 else create_component("transf",{"C_TYPE":"transf"}) 
        ret["TRANSFORM_DATA"]    = "" if len(transform) == 0 else transfs[0][0].read()
        ret["TRANSFORM_FILTER"]  = "" if len(transform) == 0 else transfs[0][1]
        ret["TRANSFORM_LOOKUPS"] = "" if len(transform) == 0 else transfs[0][2]

        ## ----------------------------------------------------------------------
        ## ----- jobs data -------------------
        ## ----------------------------------------------------------------------

        ret['JOB'] = { "origem"        : origem,
                       "transform"     : transform,
                       "destino"       : destino,
                       "dataset"       : dataset }
    return ret





#######################################################################################
# pages
#######################################################################################


@app.route("/doc", methods=["GET", "POST"])
def doc():
    eu.load_globals()
    documentation       = eu.get_param_value("PARAMETERS","DOCUMENTATION")
    return render_template( 'documentation.html',documentation=documentation )



@app.route("/error")
def error():
    return render_template("error.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("username")
        senha   = request.form.get("password")
        if ldap_login(username=usuario, password=senha):
            session["status_login"] = usuario
            return redirect("/")
        else:
            return redirect("/error")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session["status_login"] = None
    return redirect("/")


@app.route("/logger", methods=["GET", "POST"])
def job_logger():
    return render_template( 'job_logger.html' )


@app.route("/sequences")
def job_sequences():
    if not session.get("status_login"):
        return redirect("/login")
    return render_template('job_sequences.html')



@app.route("/globals")
def job_globals():
    if not session.get("status_login"):
        return redirect("/login")
    return render_template('job_globals.html')



@app.route("/designer", methods=["GET", "POST"])
def job_designer():
    if not session.get("status_login"):
        return redirect("/login")
    return render_template('job_designer.html')



@app.route("/")
def index():
    if not session.get("status_login"):
        return redirect("/login")
    return render_template('index.html', login=session["status_login"])


#######################################################################################
# main
#######################################################################################

if __name__ == "__main__":
    app.run()
