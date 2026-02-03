g_components = [
    'C_TYPE','C_NAME',
    'C_SQL_DB','C_SQL','C_SQL_AFTER','C_SQL_BEFORE','C_SQL_FIELDS/target','C_SQL_TYPE/target','C_SQL_AUTO/target','C_SQL_ARRAYSIZE/orig',
    'C_FILENAME','C_FILENAME_FD','C_FILENAME_FIELDS/target','C_FILENAME_FILE_REQUIRED/orig',
    'C_ODATA_TH_COUNT','C_ODATA_TH_SIZE','C_ODATA_URL','C_ODATA_AUTH','C_ODATA_FIELDS',
    'C_DATASET','C_DATASET_FIELDS',
    'C_BOTO3_BUCKET','C_BOTO3_FILENAME','C_BOTO3_FD','C_BOTO3_FIELDS','C_BOTO3_FORMAT',
    'C_XML_FILENAME','C_XML_FIELDS','C_XML_FILE_REQUIRED/orig',
    'C_PY_NAME', 'C_PY_CODE'
];


const ICO_OK      = "🟢";
const ICO_ERROR   = "🔴";
const ICO_RUNNING = "▶️"
const ICO_WARNING = "❌";



function ajax(url, dataBody, jsAction, jsError) {
    fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        body: new URLSearchParams(dataBody)
    })
        .then(response => {
            if (!response.ok) {
                throw new Error("Erro na requisição: " + response.statusText);
            }
            return response.json();
        })
        .then(a => {
            if (jsAction !== null) {
                jsAction(a);
            }
        })
        .catch(error => {
            if (jsError !== null) {
                jsError(error);
            } else {
                alert("Erro ao executar consulta." + error);
            }
        });
}

function fill_combo(cbo, data_cbo) {
    dbs = data_cbo.split(",");
    for (i=0; i < dbs.length; i++) {
        option = document.createElement( 'option' );
        option.value = option.text = dbs[i];
        cbo.add(option);
    }            
}



function data_populate(data, data_fields, id_data, idx_header="") {
    elemento = document.getElementById(id_data);

    if ( elemento != null ) {
        lista_inputs = elemento.getElementsByTagName("input")
            
        data_fields.forEach(e => { 
            xx  = e.split("/");
            tag = xx[0];            

            if (id_data == "id_dataset") {
                ret = ""
                for (i=0 ; i < lista_inputs.length; i++)
                  ret = ret + "\n" + lista_inputs[i].attributes[attname]

                data[idx_header + tag] =  ret.trim();

            } else{
                for (i=0 ; i < lista_inputs.length; i++) {
                    data[idx_header + "C" + (id_data == "id_target" ? String(i+1) : "") + tag.substring(1) ]   = lista_inputs[i].attributes[tag];
                }
            }

            
        });    
    }
}


// pega o atributo e "poe" no valor
function data_get(data, data_fields, local) {
    data_fields.forEach(e => { 
        xx  = e.split("/");
        tag = xx[0];
        el  = document.getElementById(tag);
        if (el != null) {
            el.disabled = false;
            el.value    = data[tag] == null ? "" : data[tag];

            if ( xx.length > 1 && xx[1] != local ) {
              el.disabled = true;
            }
        }
    });    
}


// pega o valor e "poe" no atributo
function data_put(list_obj, g_objeto) {
    list_obj.forEach(e => { 
        dados = e.split("/");

        if ( dados[0].startsWith("C_" + g_objeto.attributes['C_TYPE'].toUpperCase()) ) {
            ele   = document.getElementById( dados[0] );
            if (ele != null )   {
                g_objeto.attributes[ dados[0] ]  = ele.value;
            }
        }
    });
}


function jsNoSpeel() {
        document.querySelectorAll('input, textarea, [contenteditable]').forEach(el => {
            el.setAttribute('spellcheck', 'false');
            el.classList.add('no-spellcheck');
        });
}


function js_create_menu_add(local = []) {
    let ret = '<div class="dropdown-content">';

    for (const x of local) {
        ret += `<a href="#" onClick="jsComponentActionAdd(this, '${x}')">Add to ${x}</a>`;
    }

    ret += '</div>';
    return ret;
}
