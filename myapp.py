#!/usr/bin/python3

import etl.etl_utils as eu
import subprocess
import os
import psutil
import time

from ldap3 import Server, Connection, ALL, core
from flask import Flask, render_template, redirect, request, session
from markupsafe import Markup
from flask_session import Session
from datetime import datetime

app                 = Flask(__name__)

# ---------------- Configuration ----------------
app.config["SESSION_PERMANENT"] = False     # Sessions expire when browser closes
app.config["SESSION_TYPE"] = "filesystem"   # Store session data on the filesystem
app.config["SESSION_FILE_DIR"] = "/opt/apache2/run"

Session(app)
# ---------------- /Configuration ---------------



app.lista_databases = []
app.lista_auths_sap = []
app.lista_projetos  = []
app.lista_boto3     = []
app.db              = eu.local_db()
app.disk_io         = psutil.disk_io_counters()
app.disk_io_now     = datetime.now()


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
    svalue = ""    if value == None  else value 
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


def create_menu_add(local=[]):
    ret =  '<div class="dropdown-content">'
    for x in local:
        ret += f'''<a href="#" onClick="jsComponentActionAdd(this, '{x}')">Add to {x}</a>'''
    ret +=  '</div>'
    return Markup(ret)



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

def job_info_memory():
    mem   = psutil.virtual_memory()
    swap  = psutil.swap_memory()
    usage = psutil.disk_usage('/app')
    io2         = psutil.disk_io_counters()
    io2_now     = datetime.now()
    io2_sec     = (io2_now - app.disk_io_now).total_seconds()
    read_speed  = (io2.read_bytes - app.disk_io.read_bytes) / io2_sec  # bytes lidos por segundo
    write_speed = (io2.write_bytes - app.disk_io.write_bytes) / io2_sec  # bytes escritos por segundo

    ret  = f"""
        <table width=600>
        <tr><td></td><td>Total</td><td>Used</td><td>Free</td><td>%</td>
        <tr><td>Mem:</td><td>  { eu.human_readable_size(mem.total) }</td><td>{ eu.human_readable_size(mem.used) }</td><td>{ eu.human_readable_size(mem.free) }</td><td>{mem.percent:.2f}%</td>
        <tr><td>Swap:</td><td> { eu.human_readable_size(swap.total) }</td><td>{ eu.human_readable_size(swap.used) }</td><td>{ eu.human_readable_size(swap.free) }</td><td>{swap.percent:.2f}%</td>
        <tr><td>/app:</td><td> { eu.human_readable_size(usage.total) }</td><td>{ eu.human_readable_size(usage.used) }</td><td>{ eu.human_readable_size(usage.free) }</td><td>{usage.percent:.2f}%</td>
        <tr><td>CPU Load:</td>
            <td colspan=4>{psutil.getloadavg()} - {psutil.cpu_count()} cpus </td>
        </tr>
        <tr><td>IO Write / Read:</td>
            <td colspan=2>{write_speed / (1024 * 1024):.2f} MB/s</td>
            <td colspan=2>{read_speed / (1024 * 1024):.2f} MB/s</td>
        </tr>
        </table> 
    """  
    app.disk_io      = io2
    app.disk_io_now  = io2_now
    return {"message":ret}


def job_info_last_abort():
    sql   = db_sql("job_get_last_executions")
    dados = db_execute(sql)
    ret = []
    for x in dados.data:
        bits = [ "" if a == None else str(a) for a in x ]
        ret.append( "<tr><td>" + "</td><td>".join(bits) + "</td></tr>" )
    ret = "<table width=100% id=id_table_proc>" + "".join(ret) + "</table>"
    return {"message":ret}


def job_info_kill_process():
    pid = request.form.get("pid")
    if pid != None and pid != "":
        subprocess.run( f"ps -eo pid,ppid | awk '$1 == {pid} || $2 == {pid} {{ print $1 }}' | xargs -r kill -9", shell=True )
        ret = f"Process {pid} killed."
    return {"message":ret}


def job_info_process_running():
    lista_as  = []
    tabela    = "<td>pid</td><td>ppid</td><td>start</td><td>%mem</td><td>pcpu</td><td>command</td>"  

    for proc in psutil.process_iter(['pid', 'ppid', 'name', 'username', 'cpu_percent', 'memory_percent']):
        start_time = datetime.fromtimestamp(proc.create_time()).strftime("%d/%m/%Y %H:%M:%S")
        proc_cmd   = proc.cmdline()
        is_as      = (" ".join(proc.cmdline())).lower().__contains__("algar_prd_execgen")
        is_mms     = (" ".join(proc.cmdline())).lower().__contains__("job_execute")
        
        if is_mms or is_as:
            pid_kill = ""
            pid_show = ""
            pai      = proc.info['ppid'] == 1
            pid_link = ""

            if pai:
                pid_kill = f"""<a href=# onclick="jsKillProcess('{ proc.info['pid'] }')">kill</a>"""
                pid_link = f"<a href=# onclick=jsShowPidDetail({  proc.info['pid']  })> show </a>"
            elif is_as:
                pass
            else:
                pid_show = f""" id=pid{ proc.info['ppid'] } style="display:none" """
            
            mem     = ( f"{ proc.info['memory_percent'] }" ).split(".")
            mem_fmt = mem[0] + "." + mem[1][0:2]
            if proc.info['name'] not in lista_as:
                tabela = f"""
                    {tabela}
                    <tr {pid_show}> 
                        <td>{ proc.info['pid']  }</td>
                        <td>{ proc.info['ppid'] }</td>
                        <td>{ start_time        }</td>
                        <td>{ mem_fmt } </td>
                        <td>{ proc.info['cpu_percent']    }</td>
                        <td>{ " ".join(proc_cmd[2 if is_mms else 3:]) }</td>
                        <td>{pid_kill} {pid_link}</td>
                    </tr>
                """
                if is_as:
                    lista_as.append( proc.info['name'] )

    ret =  "<table width=100%>" + tabela + "</table>"    
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
                request.form.get("FL_ACTIVE")
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


def job_info_tablemodel():
    sql        = db_sql(request.form.get("sql"))
    sql_pars   = request.form.get("sql_pars").split("|")
    lista_pars = []
    for i,x in enumerate(sql_pars):
        sql = sql.replace( f"<{i+1}>", x)

    ret = db_execute(sql)    
    for idx,x in enumerate(ret.data):
        if idx == 0:
            l = "<tr>"
            for col in ret.fields:
                l = l + "<td id=_><b>" + col + "</b></td>"
            l = l + "</tr>"
            lista_pars.append(l)

        l = "<tr>"
        for col in x:
            l = l + "<td>" + ("" if col == None else str(col)) + "</td>"
        l = l + "</tr>"
        lista_pars.append(l)

    return {"message": "<table width=100%>" + "".join(lista_pars) + "</table>" }


def job_info_logger_batch_id():
    sql = db_sql("job_logger_batch_id", [request.form.get("batch_id")])
    ret = ""
    j   = "-"

    for x in db_execute(sql).data:
        if j != x[0]:
            ret = ret + "<hr>" + x[0] + "<hr>"
        info = x[2].strip()
        if x[3] == 1:
            info = f"""<a href=# onclick="js_log_big_view('{x[4]}')"> {info} </a> """
        ret = ret + x[1] + "  " + info + "\n" 
        j   = x[0]
    return {"message": ret }


def job_info_logger_big():
    sql = db_sql("job_logger_big", [request.form.get("rid")])
    x   = db_execute(sql).data
    return { "message": x[0][0].read() }


def job_info_logger_lotes():
    job_name = request.form.get("job_name")
    sql      = db_sql("job_logger_lotes", [job_name])
    ret      = ""
    for x in db_execute(sql).data:
        ret = ret + f'<option value="{x[0]}">{x[1]}</option>'
    return { "message": f'<select id=id_logger_job_id onChange="js_log_by_id()">' + ret + """</select> <button onclick="js_log_by_id()">refresh</button>""" }


def job_info_logger():
    sql = db_sql("job_logger", [request.form.get("job_logger_id")])
    ret = ""
    j   = "-"

    for x in db_execute(sql).data:
        if j != x[0]:
            ret = ret + x[0] + "<hr>"
        info = x[2].strip()
        if x[3] == 1:
            info = f"""<a href=# onclick="js_log_big_view('{x[4]}')"> {info} </a> """
        ret = ret + x[1] + "  " + info + "\n" 
        j   = x[0]
    return {"message": ret }


def job_execute():
    job_name            = request.form.get("job_name")
    job_run_parameters  = request.form.get("job_run_parameters").strip().split("\n")
    path_etl            = os.path.dirname(os.path.abspath(__file__))

    lista = []
    print(job_run_parameters)
    for x in job_run_parameters:
        if len(x.strip()) > 0:
            a,b = x.split("=",1)
            lista.append( f'''"{a}":"{b}"''' )

    batch_id = eu.create_job_batch(job_name=job_name,created_by=session.get("status_login"), managed_by="algaretl", parameters=("{\n" + ",".join(lista) + "\n}") )
    subprocess.Popen( f"""nohup {path_etl}/etl/job_execute.py { job_name } { session.get("status_login") } {batch_id} &""", shell=True)
    return {"message" :"Started..." }


#######################################################################################
#  
#######################################################################################



@app.route("/info",methods = ['POST'])
def info():
    name = request.form.get("name")

    if name == "job_sequence_save":
        return job_info_sequence_save()

    if name == "job_globals_save":
        return job_info_globals_save()

    if name == "memory":
        return job_info_memory()

    if name == "process_aborted":
        return job_info_last_abort()

    if name == "kill_process":
        return job_info_kill_process()

    if name == "process_running":
        return job_info_process_running()

    if name == "tablemodel":
        return job_info_tablemodel()

    if name == "log_batch_id":
        return job_info_logger_batch_id()

    if name == "log_big":
        return job_info_logger_big()

    if name == "log_lotes":
        return job_info_logger_lotes()

    if name == "log":
        return job_info_logger()

    if name == "job_execute":
        return job_execute()





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

        ret['JOB'] = { "origem"    : origem,
                       "transform" : transform,
                       "destino"   : destino,
                       "dataset"   : dataset }
    return ret





#######################################################################################
# login / loggout
#######################################################################################

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


#######################################################################################
#  open pages /  globals  designer
#######################################################################################


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
    
    job_name = request.args.get("job_name")    

    return render_template('job_designer.html', 
                           job_name          = job_name,
                           menu1             = create_menu_add(['origin','target']),
                           menu2             = create_menu_add(['origin']),
                           menu3             = create_menu_add(['transf']),
                           menu4             = create_menu_add(['target', 'dataset']),
                           CBO_DATABASES     = ",".join(app.lista_databases),
                           CBO_AUTHS_SAP     = ",".join(app.lista_auths_sap),
                           CBO_PROJETOS      = ",".join(app.lista_projetos),
                           CBO_BOTO3         = ",".join(app.lista_boto3)    )



@app.route("/")
def index():
    if not session.get("status_login"):
        return redirect("/login")
        
    sql                 = db_sql("job_combobox")
    app.lista_databases = [x[0] for x in db_execute(sql.replace("<1>", 'DATABASES'  )).data]
    app.lista_auths_sap = [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.SAP'  )).data]
    app.lista_projetos  = [x[0] for x in db_execute(sql.replace("<1>", 'FOLDER_ROOT')).data]
    app.lista_boto3     = [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.S3'   )).data]

    sql     = db_sql("job_treeview")
    x       = ""
    for y in db_execute(sql).data:
        x = x + "\n" + y[0]

    return render_template('index.html', treeview=x.strip(), login=session["status_login"])


#######################################################################################
# main
#######################################################################################

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=8080)
