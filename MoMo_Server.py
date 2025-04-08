#!/opt/IBM/pastasletras/scripts/producao/Python/Python37/bin/python3.7

import etl.etl_utils
import subprocess
from flask import Flask, render_template, request
from markupsafe import Markup
import os

app                 = Flask(__name__)
app.db              = etl.etl_utils.local_db()
app.lista_databases = []
app.lista_auths_sap = []
app.lista_projetos  = []
app.lista_boto3     = []


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



@app.route("/get_sql_values",methods = ['POST'])
def get_sql_values():
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
            l = l + "<td>" + str(col) + "</td>"
        l = l + "</tr>"
        lista_pars.append(l)

    return Markup( "<table width=100%>" + "".join(lista_pars) + "</table>" )




def add_conf(tag, value, nosep=False):
    svalue = ""    if value == None  else value 
    sep    = '"""' if "\n" in svalue else '"'
    
    if svalue == "":
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
# 
#######################################################################################


@app.route("/job_logger_lotes",methods = ['POST'])
def job_logger_lotes():
    job_name = request.form.get("job_name")
    sql      = db_sql("job_logger_lotes", [job_name])
    ret      = ""
    for x in db_execute(sql).data:
        ret = ret + f'<option value="{x[0]}">{x[1]}</option>'
    return Markup( f'<select id=id_logger_job_id onChange="js_log_by_id()">' + ret + """</select> <input type=button value=refresh onclick="js_log_by_id()">""")



@app.route("/job_logger_big",methods = ['POST'])
def job_logger_big():
    sql = db_sql("job_logger_big", [request.form.get("rid")])
    x   = db_execute(x).data
    return Markup(x[0][0].read())



@app.route("/job_logger",methods = ['POST'])
def job_logger():
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
    return Markup(ret)






#######################################################################################
# 
#######################################################################################

@app.route("/job_save",methods = ['POST'])
def job_save():
    job_name              = request.form.get("job_name")
    job_components        = request.form.get("job_components").split(",")
    folders_name          = request.form.get("job_folders_name")
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
        if cc_orig != None:
            ORIGIN.append(add_conf(cc0,cc_orig))    

        for cx in [1,2,3,4]:
            cc_target = request.form.get(f"TARGET_C{cx}{cc0[1:]}") 
            if cc_target != None:
                TARGET.append(add_conf( f"C{cx}{cc0[1:]}" ,cc_target))    
                qtd_outs = qtd_outs if qtd_outs < cx else cx
                
    TARGET.insert(0, add_conf('C_OUT_COUNT', str(qtd_outs), nosep=True)) 

    db_execute(sql_job_save_def, params=(job_name, 'JOB', "\n".join(ORIGIN) , "\n".join(TARGET), DATASETS , folders_name, jsh ),fetch=False)
    app.db.commit()

    return "Saved successfully"



@app.route("/job_load",methods = ['POST'])
def job_load():
    ret                     = {}
    ret['C_JOB_SH_BEFORE']  = ""
    ret['C_JOB_SH_AFTER']   = ""
    
    job_name        = request.form.get("job_name")
    job_components  = request.form.get("job_components").split(",")
    job_sh          = db_execute( db_sql("job_def_sh",[job_name]) ).data


    ## ----------------------------------------------------------------------
    ## ----- job shell before / after  -------------------
    ## ----------------------------------------------------------------------

    if len(job_sh) > 0:
        a = {}
        b = job_sh[0][0]
        if b != None:
            exec( b.read() ,a )
            ret['C_JOB_SH_BEFORE']  = a['C_JOB_SH_BEFORE']
            ret['C_JOB_SH_AFTER']   = a['C_JOB_SH_AFTER' ]

    id_comp = 0

    if "newjob" not in job_name :
        confs        = db_execute( db_sql("job_def_conf",[job_name])    ).data[0]
        transfs      = db_execute( db_sql("job_def_transf",[job_name])  ).data
        
        dados_orig   = {}
        dados_target = {}
    

        exec(confs[0].read(), dados_orig)
        exec(confs[1].read(), dados_target)

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
# 
#######################################################################################



@app.route("/job_execute",methods = ['POST'])
def job_execute():
    job_pars_salvar    = request.form.get("job_pars_salvar")
    job_name           = request.form.get("job_name")
    sql                = db_sql("job_execute_prepare", [job_pars_salvar,job_name])
    db_execute(sql,fetch=False)
    app.db.commit()
    path_etl = os.path.dirname(os.path.abspath(__file__))

    subprocess.Popen( f"nohup {path_etl}/etl/job_execute.py {job_name} -999 &", shell=True)
    return "Started..."


#######################################################################################
# 
#######################################################################################


@app.route("/<name>")
def job_page(name):
    if name == "globals":  
        return render_template('job_globals.html')

    if "..." not in name:
        return ""

    job_name      = name.replace("...","")
    sql           = db_sql("job_folders_name",[job_name])
    folders_name  = "" if "newjob" in name else db_execute(sql).data[0][0]
    
    sql           = db_sql("job_pars_sql",[job_name])
    job_pars      = [x[0] for x in db_execute(sql).data]

    return render_template('job_designer.html', 
                           job_name          = job_name,
                           job_pars          = "/".join(job_pars),
                           job_folders_name  = folders_name,
                           menu1             = create_menu_add(['origin','target']),
                           menu2             = create_menu_add(['origin']),
                           menu3             = create_menu_add(['transf']),
                           menu4             = create_menu_add(['target', 'dataset']),
                           CBO_DATABASES     = ",".join(app.lista_databases),
                           CBO_AUTHS_SAP     = ",".join(app.lista_auths_sap),
                           CBO_PROJETOS      = ",".join(app.lista_projetos),
                           CBO_BOTO3         = ",".join(app.lista_boto3)    )



#######################################################################################
#
#######################################################################################


@app.route("/status_cpu_mem",methods = ['POST'])
def status_cpu_mem():
    x = subprocess.run(['free', '-m'], capture_output=True, text=True)
    h = subprocess.run(['df', '-h'], capture_output=True, text=True)
    return Markup(x.stdout + "<hr>" + h.stdout)



#######################################################################################
#
#######################################################################################


@app.route("/list_process_running",methods = ['POST'])
def list_process_running():
    ret = []
    x = subprocess.run("ps -eo user,pid,lstart,%mem,pcpu,command --sort=%mem".split(" "), capture_output=True, text=True)
    for info_line in x.stdout.split("\n"):
        if "-A" in info_line and "Momo" in info_line:
            a = info_line[0 : info_line.index("/")] + info_line[info_line.index(" -A ") : ]
            ret.append(a)
    return Markup( "\n".join(ret) )


#######################################################################################
#
#######################################################################################

@app.route("/")
def index():
    sql                 = db_sql("job_combobox")
    app.lista_databases = [x[0] for x in db_execute(sql.replace("<1>", 'DATABASES'  )).data]
    app.lista_auths_sap = [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.SAP'  )).data]
    app.lista_projetos  = [x[0] for x in db_execute(sql.replace("<1>", 'FOLDER_ROOT')).data]
    app.lista_boto3     = [x[0] for x in db_execute(sql.replace("<1>", 'AUTHS.S3'   )).data]

    ret     = []
    projeto = '-'
    pasta   = '-'
    sql     = db_sql("job_tree")
    id      = 0
    pai_1   = 0
    pai_2   = 0   
    for x in db_execute(sql).data:
        if projeto != x[0]:
            ret.append( f'Tree[{id}]  = "{id+1}|{0}|{x[0]}|#";')
            projeto = x[0]
            id = id + 1
            pai_1 = id

        if pasta != x[1]:
            ret.append( f'Tree[{id}]  = "{id+1}|{pai_1}|{x[1]}|#";')
            pasta = x[1]
            id = id + 1
            pai_2 = id

        ret.append( f'Tree[{id}]  = "{id+1}|{pai_2}|{x[2]}|{x[2]}...";')
        id = id + 1
    x = Markup("var Tree = new Array;\n" + "\n".join(ret))

    return render_template('index.html', load_repository=x)

#######################################################################################
# main
#######################################################################################

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=8080)
