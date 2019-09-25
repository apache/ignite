<!-- can not use doctype -->
<!--
Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
and the EPL 1.0 (http://h2database.com/html/license.html).
Initial Developer: H2 Group
-->
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
        <title>${text.a.title}</title>
        <link rel="stylesheet" type="text/css" href="stylesheet.css" />
        <script type="text/javascript">
//<!--

var agent=navigator.userAgent.toLowerCase();
var is_opera = agent.indexOf("opera") >= 0;
var autoComplete = 0; // 0: off, 1: normal, 2: full
var autoSelect = 1; // 0: off, 1: on
var selectedRow = -1;
var lastList = '';
var lastQuery = null;
var columnsByTable = new Object();
var tableAliases = new Object();
var showAutoCompleteWait = 0;
var autoCompleteManual = false;
var req;

function refreshTables() {
    columnsByTable = new Object();
    var tables = parent.h2menu.tables;
    for(var i=0; i<tables.length; i++) {
        columnsByTable[tables[i].name] = tables[i].columns;
    }
}

function sizeTextArea() {
    var height=document.body.clientHeight;
    var sql = document.h2query.sql;
    sql.style.height=(height-sql.offsetTop)+'px';
}

function buildTableAliases(input) {
    tableAliases = new Object();
    var list = splitSQL(input);
    var last = "";
    for(var i=0; i<list.length; i++) {
        var word = list[i].toUpperCase();
        if (word != "AS") {
            if (columnsByTable[last]) {
                tableAliases[word] = last;
            }
            last = word;
        }
    }
}

function splitSQL(s) {
    var list = new Array();
    s = s.toUpperCase() + ' ';
    var e = s.length;
    for(var i=0; i<e; i++) {
        var ch = s.charAt(i);
        if (ch == '_' || (ch >= 'A' && ch <= 'Z')) {
            var start = i;
            do {
                ch = s.charAt(++i);
            } while (ch == '_' || (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z'));
            list[list.length] = s.substring(start, i);
        }
    }
    return list;
}

function help() {
    var input = document.h2query.sql;
    setSelection(input);
    var pos = input.selectionStart;
    if (pos > 0) {
        var s = input.value.substring(0, pos).toUpperCase();
        var e = pos-1;
        for(; e>=0; e-=1) {
            var ch = s.charAt(e);
            if (ch != '_' && (ch < '0' || ch > '9') && (ch < 'A' || ch > 'Z')) {
                break;
            }
        }
        s = s.substring(e+1, s.length);
        buildTableAliases(input.value);
        if (!columnsByTable[s]) {
            s = tableAliases[s];
        }
        if (columnsByTable[s]) {
            if (parent.h2menu.goToTable(s)) {
                parent.h2menu.document.location='tables.do?jsessionid=${sessionId}#' + s;
                // parent.h2menu.window.blur();
                document.h2query.sql.focus();
            }
        }
    }
}

function setSelection(element) {
    if (document.all && !is_opera) {
        try {
            var range = document.selection.createRange();
            var copy = range.duplicate();
            copy.moveToElementText(element);
            copy.setEndPoint('EndToEnd', range);
            element.selectionStart = copy.text.length - range.text.length;
            element.selectionEnd = element.selectionStart + range.text.length;
        } catch (e) {
            element.selectionEnd = element.selectionStart = 0;
        }
    }
}

function set(field, combo) {
    field.value=combo.value;
    combo.value='';
    field.focus();
}

function trim(s) {
    while (s.charAt(0)==' ' && s.length>0) {
        s=s.substring(1);
    }
    while (s.charAt(s.length-1)==' ' && s.length>0) {
        s=s.substring(0, s.length-1);
    }
    return s;
}

function trimCommas(s) {
    while (s.charAt(0)==',' && s.length>0) {
        s=s.substring(1);
    }
    while (s.charAt(s.length-1)==',' && s.length>0) {
        s=s.substring(0, s.length-1);
    }
    return s;
}

function insert(field, combo) {
    insertText(combo.value);
    combo.value='';
}

function insertText(s, isTable) {
    s = decodeURIComponent(s);
    var field = document.h2query.sql;
    var last = s.substring(s.length-1);
    if (last != '.' && last != '\'' && last != '"' && last > ' ') {
        s += ' ';
    }
    if (isTable && trim(field.value)=='') {
        field.value = 'SELECT * FROM ' + s;
    } else {
        if (document.selection) {
            // IE
            field.focus();
            selection = document.selection.createRange();
            selection.text = s;
        } else if (field.selectionStart || field.selectionStart == '0') {
            // Firefox
            var startPos = field.selectionStart;
            var endPos = field.selectionEnd;
            field.value = field.value.substring(0, startPos) + s + field.value.substring(endPos, field.value.length);
            var pos = endPos + s.length;
            field.selectionStart = pos;
            field.selectionEnd = pos;
        } else {
            field.value += s;
        }
    }
    field.focus();
}

function showAutoComplete() {
    if (showAutoCompleteWait==0) {
        showAutoCompleteWait=5;
        setTimeout('showAutoCompleteNow()', 100);
    } else {
        showAutoCompleteWait-=1;
    }
}

function showAutoCompleteNow() {
    var input = document.h2query.sql;
    setSelection(input);
    var pos = input.selectionStart;
    var s = input.value.substring(0, pos);
    if (s != lastQuery) {
        lastQuery = s;
        retrieveList(s);
    }
    showAutoCompleteWait = 0;
}

function keyDown(event) {
    var key=event.keyCode? event.keyCode : event.charCode;
    if (key == null) {
        return false;
    }
    if (key == 13 && (event.ctrlKey || event.metaKey)) {
        // ctrl + return, cmd + return
        submitAll();
        return false;
    } else if (key == 13 && event.shiftKey) {
        // shift + return
        submitSelected();
        return false;
    } else if (key == 32 && (event.ctrlKey || event.altKey)) {
        // ctrl + space
        manualAutoComplete();
        return false;
    } else if (key == 190 && autoComplete == 0) {
        // dot
        help();
        return true;
    }
    var table = getAutoCompleteTable();
    if (table.rows.length > 0) {
        if (key == 27) {
            // escape
            while (table.rows.length > 0) {
                table.deleteRow(0);
            }
            showOutput('');
            return false;
        } else if ((key == 9 && !event.shiftKey) || (key == 13 && !event.shiftKey && !event.ctrlKey && !event.altKey)) {
            // tab
            if (table.rows.length > selectedRow) {
                var row = table.rows[selectedRow];
                if (row.cells.length>1) {
                    insertText(row.cells[1].innerHTML);
                }
                removeAutoComplete();
                return false;
            }
        } else if (key == 38 && !event.shiftKey) {
            // up
            if (table.rows.length > selectedRow) {
                selectedRow = selectedRow <= 0 ? table.rows.length-1 : selectedRow-1;
                highlightRow(selectedRow);
                return false;
            }
        } else if (key == 40 && !event.shiftKey) {
            // down
            if (table.rows.length > selectedRow) {
                selectedRow = selectedRow >= table.rows.length-1 ? 0 : selectedRow+1;
                highlightRow(selectedRow);
                return false;
            }
        }
        if (autoComplete == 0) {
            // remove auto-complete if manually started
            while (table.rows.length > 0) {
                table.deleteRow(0);
            }
            showOutput('');
        }
    }
    // alert('key:' + key);
    return true;
    // bs:8 ret:13 lt:37 up:38 rt:39 dn:40 tab:9
    // pgup:33 pgdn:34 home:36 end:35 del:46 shift:16
    // ctrl, alt gr:17 alt:18 caps:20 5(num):12 ins:45
    // pause:19 f1..13:112..123 win-start:91 win-ctx:93 esc:27
    // cmd:224
}

function keyUp(event) {
    var key = event == null ? 0 : (event.keyCode ? event.keyCode : event.charCode);
    if (autoComplete != 0) {
        if (key != 37 && key != 38 && key != 39 && key != 40) {
            // left, right, up, down: don't show autocomplete
            showAutoComplete();
        }
    }
    if (key == 13 && event.shiftKey) {
        // shift + return
        return false;
    }
    return true;
}

function setAutoComplete(value) {
    autoComplete = value;
    if (value == 0) {
        removeAutoComplete();
    } else {
        var s = lastList;
        lastList = '';
        showList(s);
    }
}

function setAutoSelect(value) {
    autoSelect = value;
}

function manualAutoComplete() {
    autoCompleteManual = true;
    lastQuery = null;
    lastList = '';
    showAutoCompleteNow();
}

function removeAutoComplete() {
    var table = getAutoCompleteTable();
    while (table.rows.length > 0) {
        table.deleteRow(0);
    }
    showOutput('');
}

function highlightRow(row) {
    if (row != null) {
        selectedRow = row;
    }
    var table = getAutoCompleteTable();
    highlightThisRow(table.rows[selectedRow]);
}

function highlightThisRow(row) {
    var table = getAutoCompleteTable();
    for(var i=0; i<table.rows.length; i++) {
        var r = table.rows[i];
        var col = (r == row) ? '#95beff' : '';
        var cells = r.cells;
        if (cells.length > 0) {
            cells[0].style.backgroundColor = col;
        }
    }
    showOutput('none');
}

function getAutoCompleteTable() {
    return parent.h2result.document.getElementById('h2auto');
}

function showOutput(x) {
//     parent.h2result.document.getElementById('output').style.display=x;
}

function showList(s) {
    if (lastList == s) {
        return;
    }
    lastList = s;
    var list = s.length == 0 ? null : s.split('|');
    var table = getAutoCompleteTable();
    if (table == null) {
        return;
    }
    while (table.rows.length > 0) {
        table.deleteRow(0);
    }
    selectedRow = 0;
    var count = 0;
    var doc = parent.h2result.document;
    var tbody = table.tBodies[0];
    for(var i=0; list != null && i<list.length; i++) {
        var kv = list[i].split('#');
        var type = kv[0];
        if (type > 0 && autoComplete != 2 && !autoCompleteManual) {
            continue;
        }
        var row = doc.createElement("tr");
        tbody.appendChild(row);
        var cell = doc.createElement("td");
        var key = kv[1];
        var value = kv[2];
        if (!key || !value) {
            break;
        }
        count++;
        cell.className = 'autoComp' + type;
        key = decodeURIComponent(key);
        row.onmouseover = function(){highlightThisRow(this)};
        if (!document.all || is_opera) {
            row.onclick = function(){insertText(this.cells[1].innerHTML);keyUp()};
        }
    //    addEvent(row, "click", function(e){var row=e?e.target:window.event.srcElement;alert(row);insertText(row.cells[1].innerHTML)});
//        addEvent(row, "mouseover", function(e){var row=e?e.target:window.event.srcElement;alert(row);highlightThisRow(row)});
//        addEvent(row, "click", function(e){var row=e?e.target:window.event.srcElement;alert(row);insertText(row.cells[1].innerHTML)});
//            addEvent(row, "mouseover", eval('function(){highlightRow('+i+')}'));
//            addEvent(row, "click", eval('function(){insertText(\''+value+'\')}'));
        var text = doc.createTextNode(key);
        cell.appendChild(text);
        row.appendChild(cell);
        cell = doc.createElement("td");
        cell.style.display='none';
        text = doc.createTextNode(value);
        cell.appendChild(text);
        row.appendChild(cell);
    }
    if (count > 0) {
        highlightRow();
        showOutput('none');
    } else {
        showOutput('');
    }
    // scroll to the top left
    parent.h2result.scrollTo(0, 0);
    autoCompleteManual = false;
}

function retrieveList(s) {
    if (s.length > 2000) {
        s = s.substring(s.length - 2000);
    }
    sendAsyncRequest('autoCompleteList.do?jsessionid=${sessionId}&query=' + encodeURIComponent(s));
}

function addEvent(element, eventType, fn) {
    // cross-browser event handling for IE5+,  NS6 and Mozilla by Scott Andrew
    if (fn == null) {
        return;
    }
    if (element.addEventListener) {
        element.addEventListener(eventType, fn, true);
        return true;
    } else if (element.attachEvent){
        return element.attachEvent('on'+eventType, fn);
    } else {
        alert('Event handler could not be added');
    }
}

function sendAsyncRequest(url) {
    req = false;
    if (window.XMLHttpRequest) {
        try {
            req = new XMLHttpRequest();
        } catch(e) {
            req = false;
        }
    } else if (window.ActiveXObject) {
        try {
            req = new ActiveXObject("Msxml2.XMLHTTP");
        } catch(e) {
            try {
                req = new ActiveXObject("Microsoft.XMLHTTP");
            } catch(e) {
                req = false;
            }
        }
    }
    if (req) {
        req.onreadystatechange = processAsyncResponse;
        req.open("GET", url, true);
        req.send("");
    } else {
        var getList = document.getElementById('h2iframeTransport');
        getList.src = url;
    }
}

function processAsyncResponse() {
    if (req.readyState == 4) {
        if (req.status == 200) {
            showList(req.responseText);
        } else {
            // alert('Could not retrieve data');
        }
    }
}

function submitAll() {
    document.h2querysubmit.sql.value = document.h2query.sql.value;
    document.h2querysubmit.submit();
}

function submitSelected() {
    var field = document.h2query.sql;
    //alert('contents ' + field.selectionStart + '  ' + field.selectionEnd);
    if (field.selectionStart == field.selectionEnd) {
        if (autoSelect == 0) {
            return;
        }
        doAutoSelect();
        if (field.selectionStart == field.selectionEnd) {
            return;
        }
    }
    var startPos = field.selectionStart;
    var endPos = field.selectionEnd;
    var selectedText = field.value.substring(startPos, endPos);
    document.h2querysubmit.sql.value = selectedText;
    document.h2querysubmit.submit();
}

function doAutoSelect() {
    var field = document.h2query.sql;
    var position = field.selectionStart;
    try {
        var prevDoubleLine = field.value.lastIndexOf('\n\n',position - 1) + 2;
        if (prevDoubleLine == 1) prevDoubleLine = 0;
        var nextDoubleLine = field.value.indexOf('\n\n',position);
        if (nextDoubleLine == -1) nextDoubleLine = field.value.length;
        field.setSelectionRange(prevDoubleLine,nextDoubleLine);
    } catch (e) {
        field.selectionStart = field.selectionEnd = position;
    }
}

//-->
</script>
</head>
    <body onresize="sizeTextArea();" onload="sizeTextArea();" style="margin: 0px; padding: 0px;">
        <form name="h2query" action="javascript:alert('should not be submitting this form');">
            <span style="white-space:nowrap">
                <input type="button" class="button" value="${text.toolbar.run}" onclick="javascript:submitAll();sql.focus();return true;" />
                <input type="button" class="button" value="${text.toolbar.runSelected}" onclick="javascript:submitSelected();sql.focus();return true;" />
                <input type="button" class="button" value="${text.toolbar.autoComplete}" onclick="javascript:manualAutoComplete();sql.focus();return true;" />
                <input type="button" class="button" value="${text.toolbar.clear}" onclick="javascript:sql.value='';keyUp();sql.focus();return true;" />
                ${text.toolbar.sqlStatement}:
            </span>
            <div style="display:none">
                <iframe id="h2iframeTransport" src="" onload="showList(this.contentWindow.document.body.innerHTML);"></iframe>
            </div>
            <textarea id="sql" name="sql" cols="80" rows="5" onkeydown="return keyDown(event)" onkeyup="return keyUp(event)"
                onfocus="keyUp()" onchange="return keyUp()">${query}</textarea>
        </form>
        <form name="h2querysubmit" method="post" action="query.do?jsessionid=${sessionId}" target="h2result" style="display:none">
            <textarea id="sql" name="sql" style="display:none"></textarea>
        </form>
    </body>
</html>