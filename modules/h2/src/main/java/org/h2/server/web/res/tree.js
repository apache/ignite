/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 *  * Initial Developer: H2 Group
 */

var nodeList = new Array();
var icons = new Array();
var tables = new Array();
var tablesByName = new Object();

function Table(name, columns, i) {
    this.name = name;
    this.columns = columns;
    this.id = i;
}

function addTable(name, columns, i) {
    var t = new Table(name, columns, i);
    tables[tables.length] = t;
    tablesByName[name] = t;
}

function ins(s, isTable) {
    if (parent.h2query) {
        if (parent.h2query.insertText) {
            parent.h2query.insertText(s, isTable);
        }
    }
}

function refreshQueryTables() {
    if (parent.h2query) {
        if (parent.h2query.refreshTables) {
            parent.h2query.refreshTables();
        }
    }
}

function goToTable(s) {
    var t = tablesByName[s];
    if (t) {
        hitOpen(t.id);
        return true;
    }
    return false;
}

function loadIcons() {
    icons[0] = new Image();
    icons[0].src = "tree_minus.gif";
    icons[1] = new Image();
    icons[1].src = "tree_plus.gif";
}

function Node(level, type, icon, text, link) {
    this.level = level;
    this.type = type;
    this.icon = icon;
    this.text = text;
    this.link = link;
}

function setNode(id, level, type, icon, text, link) {
    nodeList[id] = new Node(level, type, icon, text, link);
}

function writeDiv(i, level, dist) {
    if (dist>0) {
        document.write("<div id=\"div"+(i-1)+"\" style=\"display: none;\">");
    } else {
        while (dist++<0) {
            document.write("</div>");
        }
    }
}

function writeTree() {
    loadIcons();
    var last=nodeList[0];
    for (var i=0; i<nodeList.length; i++) {
        var node=nodeList[i];
        writeDiv(i, node.level, node.level-last.level);
        last=node;
        var j=node.level;
        while (j-->0) {
            document.write("<img src=\"tree_empty.gif\"/>");
        }
        if (node.type==1) {
            if (i < nodeList.length-1 && nodeList[i+1].level > node.level) {
                document.write("<img onclick=\"hit("+i+");\" id=\"join"+i+"\" src=\"tree_plus.gif\"/>");
            } else {
                document.write("<img src=\"tree_empty.gif\"/>");
            }
        }
        document.write("<img src=\"tree_"+node.icon+".gif\"/>&nbsp;");
        if (node.link==null) {
            document.write(node.text);
        } else {
            document.write("<a id='"+node.text+"' href=\""+node.link+"\" >"+node.text+"</a>");
        }
        document.write("<br />");
    }
    writeDiv(0, 0, -last.type);
}

function hit(i) {
    var theDiv = document.getElementById("div"+i);
    var theJoin    = document.getElementById("join"+i);
    if (theDiv.style.display == 'none') {
        theJoin.src = icons[0].src;
        theDiv.style.display = '';
    } else {
        theJoin.src = icons[1].src;
        theDiv.style.display = 'none';
    }
}

function hitOpen(i) {
    var theDiv = document.getElementById("div"+i);
    var theJoin    = document.getElementById("join"+i);
    theJoin.src = icons[0].src;
    theDiv.style.display = '';
}