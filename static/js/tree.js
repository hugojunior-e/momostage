class TreeView {
    constructor() {
        this.nodes = [];
        this.openNodes = [];
        this.treeBuffer = [];
        this.endNodeClick = null;
        this.endNodeParamClick = null;
        this.endNodeText = null;
    }


    writeTreeString(txt) {
        this.treeBuffer.push(txt);
    }

    montaArvoreDados(arrNodes, startNode = 0, openNode = null) {
        this.treeBuffer = [];
        this.nodes = this.parseCsvToTreeArray(arrNodes);
        this.openNodes = [];

        if (this.nodes.length > 0) {
            if (openNode != 0 && openNode != null) {
                this.setOpenNodes(openNode);
            }

            if (startNode !== 0) {
                const nodeValues = this.nodes[this.getArrayId(startNode)].split("|");
                this.writeTreeString(
                    `<a href="${nodeValues[3]}" onmouseover="window.status='${nodeValues[2]}';return true;" onmouseout="window.status=' ';return true;">${nodeValues[2]}</a><br />`
                );
            }

            const recursedNodes = [];
            this.addNode(startNode, recursedNodes);
        }

        return this.treeBuffer.join("\n");
    }

    getArrayId(node) {
        for (let i = 0; i < this.nodes.length; i++) {
            const values = this.nodes[i].split("|");
            if (values[0] == node) return i;
        }
        return null;
    }

    setOpenNodes(openNode) {
        for (let i = 0; i < this.nodes.length; i++) {
            const nodeValues = this.nodes[i].split("|");
            if (nodeValues[0] == openNode) {
                this.openNodes.push(nodeValues[0]);
                this.setOpenNodes(nodeValues[1]);
            }
        }
    }

    isNodeOpen(node) {
        return this.openNodes.includes(node);
    }

    hasChildNode(parentNode) {
        return this.nodes.some(n => n.split("|")[1] == parentNode);
    }

    lastSibling(node, parentNode) {
        let lastChild = 0;
        for (let i = 0; i < this.nodes.length; i++) {
            const nodeValues = this.nodes[i].split("|");
            if (nodeValues[1] == parentNode) {
                lastChild = nodeValues[0];
            }
        }
        return lastChild == node;
    }

    addNode(parentNode, recursedNodes) {
        for (let i = 0; i < this.nodes.length; i++) {
            const nodeValues = this.nodes[i].split("|");
            if (nodeValues[1] == parentNode) {

                const ls = this.lastSibling(nodeValues[0], nodeValues[1]);
                const hcn = this.hasChildNode(nodeValues[0]);
                const ino = this.isNodeOpen(nodeValues[0]);

                // linhas do layout
                for (let g = 0; g < recursedNodes.length; g++) {
                    if (recursedNodes[g] == 1) this.writeTreeString("  ┊");
                    else this.writeTreeString("&nbsp;&nbsp;&nbsp;");
                }

                recursedNodes.push(ls ? 0 : 1);

                const vv_div = this.generate_session_id();

                // Nó com filhos
                if (hcn) {
                    const isLast = ls ? 1 : 0;
                    this.writeTreeString(
                        `<a href="javascript: oc('${vv_div}_${nodeValues[0]}', ${isLast});">➕📁${nodeValues[2]}</a>`
                    );
                } else {
                    // Nó folha
                    this.writeTreeString("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;🔵");
                    const nodeValue       = nodeValues[2];
                    let endNodeText       = nodeValue;
                    let endNodeParameter  = nodeValue;

                    if ( this.endNodeText != null ) {
                        endNodeText = eval(this.endNodeText);
                    }

                    if ( this.endNodeParamClick != null ) {
                        endNodeParameter = eval(this.endNodeParamClick);
                    }

                    if ( this.endNodeClick != null ) {
                        this.writeTreeString(
                            `<a href=# onclick="${this.endNodeClick}('${endNodeParameter}')">${endNodeText}</a>`
                        );
                    } else {
                        this.writeTreeString(endNodeText);
                    } 
                }

                this.writeTreeString("<br/>");

                // Se tiver filhos → abre div e recursa
                if (hcn) {
                    this.writeTreeString(`<div id="div${vv_div}_${nodeValues[0]}"`);
                    if (!ino) this.writeTreeString(` style="display: none;"`);
                    this.writeTreeString(">");

                    this.addNode(nodeValues[0], recursedNodes);

                    this.writeTreeString("</div>");
                }

                recursedNodes.pop();
            }
        }
    }

    // ID fake como no original
    generate_session_id() {
        return Math.random().toString(36).substring(2, 10);
    }

    // parser CSV → array de strings "id|parent|name|link"
    parseCsvToTreeArray(csv) {
        const lines = csv.split("\n");
        const map = {};
        const result = [];
        let currentId = 1;

        for (let line of lines) {
            const parts = line.trim().split("|");
            let path = "";
            let parentPath = "";
            let parentId = 0;

            for (let i = 0; i < parts.length; i++) {
                parentPath = path;
                path = path ? path + "|" + parts[i] : parts[i];

                if (!(path in map)) {
                    const id = currentId++;
                    map[path] = id;
                    const name = parts[i];
                    const link = parts.slice(0, i + 1).join("...");
                    const parent = map[parentPath] || 0;
                    result.push(`${id}|${parent}|${name}|${link}`);
                }
            }
        }
        return result;
    }


}

// Função global necessária pelo HTML existente
function oc(node, bottom) {
    const div = document.getElementById("div" + node);
    div.style.display = div.style.display == "none" ? "" : "none";
}
