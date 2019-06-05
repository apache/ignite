<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!--
Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
and the EPL 1.0 (http://h2database.com/html/license.html).
Initial Developer: H2 Group
-->
<html><head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=0.9" />
    <title>${text.a.tools}</title>
    <link rel="stylesheet" type="text/css" href="stylesheet.css" />
    <script type="text/javascript">
//<!--
var current = '';
function go(name) {
    if (name == current) {
        return;
    }
    var tools = document.getElementsByTagName('div');
    for (i = 0; i < tools.length; i++) {
        var div = tools[i];
        if (div.id.substring(0, 4) == 'tool') {
            div.style.display = (div.id == 'tool' + name) ? '' : 'none';
        }
    }
    document.getElementById('commandLine').style.display='';
    document.getElementById('toolName').innerHTML = name;
    document.getElementById('tool').value = name;
    document.getElementById('result').style.display = 'none';
    current = name;
    update();
}
function quote(x) {
    var q = '';
    for (var i=0; i<x.length; i++) {
        var c = x.charAt(i);
        if (c == '"' || c == '\\' || c == ',') {
            q += '\\';
        }
        q += c;
    }
    return q;
}
function update() {
    var line = '', args = '';
    for (var i = 0; i < 9; i++) {
        var f = document.getElementById('option' + current + '.' + i);
        if (f != null && f.value.length > 0) {
            var x = quote(f.value);
            if (f.type == 'password') {
                x = '';
                for (var j = 0; j < f.value.length; j++) {
                    x += '*';
                }
            }
            line += ' -' + f.name + ' "' + x + '"';
            if (args.length > 0) {
                args += ',';
            }
            args += '-' + f.name + ',' + quote(f.value);
        }
    }
    document.getElementById('toolOptions').innerHTML = line;
    document.getElementById('args').value = args;
}
//-->
    </script>
</head>
<body style="margin: 20px">
<form name="tools" method="post" action="tools.do?jsessionid=${sessionId}" id="tools">

<h1>${text.a.tools}</h1>
<p>
<a href="logout.do?jsessionid=${sessionId}">${text.adminLogout}</a>
</p>
<hr />
<p>
<a href="javascript:go('Backup')">${text.tools.backup}</a>&nbsp;&nbsp;
<a href="javascript:go('Restore')">${text.tools.restore}</a>&nbsp;&nbsp;
<a href="javascript:go('Recover')">${text.tools.recover}</a>&nbsp;&nbsp;
<a href="javascript:go('DeleteDbFiles')">${text.tools.deleteDbFiles}</a>&nbsp;&nbsp;
<a href="javascript:go('ChangeFileEncryption')">${text.tools.changeFileEncryption}</a>
</p><p>
<a href="javascript:go('Script')">${text.tools.script}</a>&nbsp;&nbsp;
<a href="javascript:go('RunScript')">${text.tools.runScript}</a>&nbsp;&nbsp;
<a href="javascript:go('ConvertTraceFile')">${text.tools.convertTraceFile}</a>&nbsp;&nbsp;
<a href="javascript:go('CreateCluster')">${text.tools.createCluster}</a>
</p>
<hr />
<div id="toolBackup" style="display: none">
    <h2>${text.tools.backup}</h2>
    <p>${text.tools.backup.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.targetFileName}:&nbsp;</td><td><input id="optionBackup.0" name="file" onkeyup="update()" onchange="update()" value="~/backup.zip" size="50" />
        </td></tr><tr><td>
        ${text.tools.sourceDirectory}:&nbsp;</td><td><input id="optionBackup.1" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
        </td></tr><tr><td>
        ${text.tools.sourceDatabaseName}:&nbsp;</td><td><input id="optionBackup.2" name="db" onkeyup="update()" onchange="update()" value="" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolRestore" name="Restore" style="display: none">
    <h2>${text.tools.restore}</h2>
    <p>${text.tools.restore.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.sourceFileName}:&nbsp;</td><td><input id="optionRestore.0" name="file" onkeyup="update()" onchange="update()" value="~/backup.zip" size="50" />
        </td></tr><tr><td>
        ${text.tools.targetDirectory}:&nbsp;</td><td><input id="optionRestore.1" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
        </td></tr><tr><td>
        ${text.tools.targetDatabaseName}:&nbsp;</td><td><input id="optionRestore.2" name="db" onkeyup="update()" onchange="update()" value="" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolRecover" style="display: none">
    <h2>${text.tools.recover}</h2>
    <p>${text.tools.recover.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.directory}:&nbsp;</td><td><input id="optionRecover.0" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
        </td></tr><tr><td>
        ${text.tools.databaseName}:&nbsp;</td><td><input id="optionRecover.1" name="db" onkeyup="update()" onchange="update()" value="" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolDeleteDbFiles" style="display: none">
    <h2>${text.tools.deleteDbFiles}</h2>
    <p>${text.tools.deleteDbFiles.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.directory}:&nbsp;</td><td><input id="optionDeleteDbFiles.0" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
        </td></tr><tr><td>
        ${text.tools.databaseName}:&nbsp;</td><td><input id="optionDeleteDbFiles.1" name="db" onkeyup="update()" onchange="update()" value="delete" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolChangeFileEncryption" style="display: none">
    <h2>${text.tools.changeFileEncryption}</h2>
    <p>${text.tools.changeFileEncryption.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.cipher}:&nbsp;</td><td><input id="optionChangeFileEncryption.0" name="cipher" onkeyup="update()" onchange="update()" value="XTEA" />
        </td></tr><tr><td>
        ${text.tools.directory}:&nbsp;</td><td><input id="optionChangeFileEncryption.1" name="dir" onkeyup="update()" onchange="update()" value="~" size="50" />
        </td></tr><tr><td>
        ${text.tools.databaseName}:&nbsp;</td><td><input id="optionChangeFileEncryption.2" name="db" onkeyup="update()" onchange="update()" value="test" size="50" />
        </td></tr><tr><td>
        ${text.tools.decryptionPassword}:&nbsp;</td><td><input type="password" id="optionChangeFileEncryption.3" name="decrypt" onkeyup="update()" onchange="update()" value="" />
        </td></tr><tr><td>
        ${text.tools.encryptionPassword}:&nbsp;</td><td><input type="password" id="optionChangeFileEncryption.4" name="encrypt" onkeyup="update()" onchange="update()" value="" />
        </td></tr>
    </table>
</div>
<div id="toolScript" style="display: none">
    <h2>${text.tools.script}</h2>
    <p>${text.tools.script.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.sourceDatabaseURL}:&nbsp;</td><td><input id="optionScript.0" name="url" onkeyup="update()" onchange="update()" value="jdbc:h2:~/test" size="50" />
        </td></tr><tr><td>
        ${text.a.user}:&nbsp;</td><td><input id="optionScript.1" name="user" onkeyup="update()" onchange="update()" value="sa" />
        </td></tr><tr><td>
        ${text.a.password}:&nbsp;</td><td><input type="password" id="optionScript.2" name="password" onkeyup="update()" onchange="update()" value="" />
        </td></tr><tr><td>
        ${text.tools.targetScriptFileName}:&nbsp;</td><td><input id="optionScript.3" name="script" onkeyup="update()" onchange="update()" value="~/backup.sql" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolRunScript" style="display: none">
    <h2>${text.tools.runScript}</h2>
    <p>${text.tools.runScript.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.targetDatabaseURL}:&nbsp;</td><td><input id="optionRunScript.0" name="url" onkeyup="update()" onchange="update()" value="jdbc:h2:~/test" size="50" />
        </td></tr><tr><td>
        ${text.a.user}:&nbsp;</td><td><input id="optionRunScript.1" name="user" onkeyup="update()" onchange="update()" value="sa" />
        </td></tr><tr><td>
        ${text.a.password}:&nbsp;</td><td><input type="password" id="optionRunScript.2" name="password" onkeyup="update()" onchange="update()" value="" />
        </td></tr><tr><td>
        ${text.tools.sourceScriptFileName}:&nbsp;</td><td><input id="optionRunScript.3" name="script" onkeyup="update()" onchange="update()" value="~/backup.sql" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolConvertTraceFile" style="display: none">
    <h2>${text.tools.convertTraceFile}</h2>
    <p>${text.tools.convertTraceFile.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.traceFileName}:&nbsp;</td><td><input id="optionConvertTraceFile.0" name="traceFile" onkeyup="update()" onchange="update()" value="~/test.trace.db" size="50" />
        </td></tr><tr><td>
        ${text.tools.scriptFileName}:&nbsp;</td><td><input id="optionConvertTraceFile.1" name="script" onkeyup="update()" onchange="update()" value="~/test.sql" size="50" />
        </td></tr><tr><td>
        ${text.tools.javaDirectoryClassName}:&nbsp;</td><td><input id="optionConvertTraceFile.2" name="javaClass" onkeyup="update()" onchange="update()" value="~/Test" size="50" />
        </td></tr>
    </table>
</div>
<div id="toolCreateCluster" style="display: none">
    <h2>${text.tools.createCluster}</h2>
    <p>${text.tools.createCluster.help}</p>
    <table class="tool">
        <tr><td>
        ${text.tools.sourceDatabaseURL}:&nbsp;</td><td><input id="optionCreateCluster.0" name="urlSource" onkeyup="update()" onchange="update()" value="jdbc:h2:~/test" size="50" />
        </td></tr><tr><td>
        ${text.tools.targetDatabaseURL}:&nbsp;</td><td><input id="optionCreateCluster.1" name="urlTarget" onkeyup="update()" onchange="update()" value="jdbc:h2:~/copy/test" size="50" />
        </td></tr><tr><td>
        ${text.a.user}:&nbsp;</td><td><input id="optionCreateCluster.2" name="user" onkeyup="update()" onchange="update()" value="sa" />
        </td></tr><tr><td>
        ${text.a.password}:&nbsp;</td><td><input type="password" id="optionCreateCluster.3" name="password" onkeyup="update()" onchange="update()" value="" />
        </td></tr><tr><td>
        ${text.tools.serverList}:&nbsp;</td><td><input id="optionCreateCluster.4" name="serverList" onkeyup="update()" onchange="update()" value="server1,server2" size="50" />
        </td></tr>
    </table>
</div>

<div id="commandLine" style="display: none">
        <input type="submit" class="button" value="${text.tools.run}" />
        <input type="hidden" name="tool" id="tool" value=""/>
        <input type="hidden" name="args" id="args" value=""/>
        <h4>${text.tools.commandLine}:</h4>
        java -cp h2*.jar org.h2.tools.<span id="toolName"></span>
        <span id="toolOptions">${tool}</span>
</div>

<div id="result" style="display: none">
        <h4>${text.tools.result}:</h4>
        <p>${toolResult}</p>
</div>

</form>

<script type="text/javascript">
//<!--
var t = '${tool}';
if (t != '') {
    go(t);
    document.getElementById('result').style.display = '';
}
//-->
</script>
</body></html>
