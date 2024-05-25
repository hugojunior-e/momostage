g_novo_job  = '<tr><td align=center width="300" id="id_origin_0"  ></td>'+
                  '<td align=center width="300" id="id_transf_0"  ></td>'+
                  '<td align=center width="300" id="id_target_0"  ></td>'+
                  '<td align=center             id="id_dataset_0" ></td></tr>';



g_transform = ['TRANSFORM_DATA','TRANSFORM_FILTER','TRANSFORM_LOOKUPS'];

g_components = [
    'C_TYPE','C_NAME',
    'C_ODATA_TH_COUNT','C_ODATA_TH_SIZE','C_ODATA_URL','C_ODATA_AUTH','C_ODATA_FIELDS',
    'C_SQL_DB','C_SQL','C_SQL_AFTER','C_SQL_BEFORE','C_SQL_FIELDS/target','C_SQL_TYPE/target','C_SQL_AUTO/target',
    'C_FILENAME','C_FILENAME_FD','C_FILENAME_FIELDS/target','C_FILENAME_FILE_REQUIRED/orig',
    'C_BOTO3_BUCKET','C_BOTO3_FILENAME','C_BOTO3_FD','C_BOTO3_FIELDS',
    'C_XML_FILENAME','C_XML_FIELDS','C_XML_FILE_REQUIRED/orig',
    'C_DATASET','C_DATASET_FIELDS'
];


function do_result_set(sql_name,sql_pars, ff) {
    $.ajax({url:"/get_sql_values",type:"POST",data:{"sql":sql_name,"sql_pars":sql_pars.join("|")}, success:function(ret){
        ff(ret);
    }});
}

function fill_combo(cbo, data_cbo) {
    dbs = data_cbo.split(",");
    for (i=0; i < dbs.length; i++) {
        option = document.createElement( 'option' );
        option.value = option.text = dbs[i];
        cbo.add(option);
    }            
}



function setTitles() {
    document.title  = 'MoMoStage Designer - ' + job_name;
    document.all.id_title_page.innerHTML = job_name;
}


function getatt(id, idx, attname) {
    elemento = document.getElementById(id);
    if (elemento == null)
      return "";

    minputs = elemento.getElementsByTagName("input")

    if (idx == -1) {
        ret = ""
        for (i=0 ; i < minputs.length; i++)
          ret = ret + "\n" + minputs[i].attributes[attname]
        return ret.trim();
    }
    
    if ( idx >= minputs.length )
        return "";

    return minputs[idx].attributes[attname];
}



function data_populate(data, data_fields, index, id_data, idx_loop=0, idx_header="") {
    v_id = id_data + '_' + ii;
    if ( document.getElementById(v_id) != null ) {
        data_fields.forEach(e => { 
            xx  = e.split("/");
            tag = xx[0];            
            data[idx_header + tag + "_" + index]   = getatt(v_id, idx_loop , tag);
        });    
    }
}


function data_get(data, data_fields, local) {
    data_fields.forEach(e => { 
        xx  = e.split("/");
        tag = xx[0];
        el  = document.getElementById(tag);
        if (el != null) {
            el.disabled = false;

            if ( xx.length == 1 || xx[1] == local ) {
                if ( el.type == "checkbox")
                    el.checked   = data[tag];
                else
                    el.value   = data[tag] == null ? "" : data[tag];
            }
            else {
              el.disabled = true;
              if ( el.type != "checkbox")
                el.value   = "";
            }
        }
    });    
}


function data_put(list_obj, g_objeto) {
    list_obj.forEach(e => { 
        dados = e.split("/");
        if ( g_objeto.attributes[ dados[0] ] != null ) {
            ele   = document.getElementById( dados[0] );
            if (ele != null )
            {
                g_objeto.attributes[ dados[0] ]  = ele.value;
            }
        }
    });
}