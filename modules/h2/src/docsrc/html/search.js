/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 *  * Initial Developer: H2 Group
 */

var pages = new Array();
var ref = new Array();
var ignored = '';
var firstLink = null;
var firstLinkWord = null;

String.prototype.endsWith = function(suffix) {
    var startPos = this.length - suffix.length;
    if (startPos < 0) {
        return false;
    }
    return this.lastIndexOf(suffix, startPos) == startPos;
};

function listWords(value, open) {
    value = replaceOtherChars(value);
    value = trim(value);
    if (pages.length == 0) {
        load();
    }
    var table = document.getElementById('result');
    while (table.rows.length > 0) {
        table.deleteRow(0);
    }
    firstLink = null;
    var clear = document.getElementById('clear');
    if (value.length == 0) {
        clear.style.display = 'none';
        return;
    }
    clear.style.display = '';
    var keywords = value.split(' ');
    if (keywords.length > 1) {
        listMultipleWords(keywords);
        return;
    }
    if (value.length < 3) {
        max = 100;
    } else {
        max = 1000;
    }
    value = value.toLowerCase();
    var r = ref[value.substring(0, 1)];
    if (r == undefined) {
        return;
    }
    var x = 0;
    var words = r.split(';');
    var count = 0;
    for (var i = 0; i < words.length; i++) {
        var wordRef = words[i];
        if (wordRef.toLowerCase().indexOf(value) == 0) {
            count++;
        }
    }
    for (var i = 0; i < words.length && (x <= max); i++) {
        var wordRef = words[i];
        if (wordRef.toLowerCase().indexOf(value) == 0) {
            word = wordRef.split("=")[0];
            var tr = table.insertRow(x++);
            var td = document.createElement('td');
            var tdClass = document.createAttribute('class');
            tdClass.nodeValue = 'searchKeyword';
            td.setAttributeNode(tdClass);

            var ah = document.createElement('a');
            var href = document.createAttribute('href');
            href.nodeValue = 'javascript:set("' + word + '");';
            var link = document.createTextNode(word);
            ah.setAttributeNode(href);
            ah.appendChild(link);
            td.appendChild(ah);
            tr.appendChild(td);
            piList = wordRef.split("=")[1].split(",");
            if (count < 20 || open == word) {
                x = addReferences(x, piList, word);
            }
        }
    }
    if (x == 0) {
        if (ignored.indexOf(';' + value + ';') >= 0) {
            noResults(table, 'Common word (not indexed)');
        } else {
            noResults(table, 'No results found!');
        }
    }
}

function set(v) {
    if (pages.length == 0) {
        load();
    }
    var search = document.getElementById('search').value;
    listWords(search, v);
    document.getElementById('search').focus();
    window.scrollBy(-20, 0);
}

function goFirst() {
    var table = document.getElementById('result');
    if (firstLink != null) {
        go(firstLink, firstLinkWord);
    }
    return false;
}

function go(pageId, word) {
    var page = pages[pageId];
    var load = '../' + page.file + '?highlight=' + encodeURIComponent(word);
    if (top.main) {
        if (!top.main.location.href.endsWith(page.file)) {
            top.main.location = load;
        }
    } else {
        if (!document.location.href.endsWith(page.file)) {
            var search = document.getElementById('search').value;
            document.location = load + '&search=' + encodeURIComponent(search);
        }
    }
}

function listMultipleWords(keywords) {
    var count = new Array();
    var weight = new Array();
    for (var i = 0; i < pages.length; i++) {
        count[i] = 0;
        weight[i] = 0.0;
    }
    for (var i = 0; i < keywords.length; i++) {
        var value = keywords[i].toLowerCase();
        if (value.length <= 1) {
            continue;
        }
        var r = ref[value.substring(0, 1)];
        if (r == undefined) {
            continue;
        }
        var words = r.split(';');
        for (var j = 0; j < words.length; j++) {
            var wordRef = words[j];
            if (wordRef.toLowerCase().indexOf(value) == 0) {
                var word = wordRef.split("=")[0].toLowerCase();
                piList = wordRef.split("=")[1].split(",");
                var w = 1;
                for (var k = 0; k < piList.length; k++) {
                    var pi = piList[k];
                    if (pi.charAt(0) == 't') {
                        pi = pi.substring(1);
                        w = 10000;
                    } else if (pi.charAt(0) == 'h') {
                        pi = pi.substring(1);
                        w = 100;
                    } else if (pi.charAt(0) == 'r') {
                        pi = pi.substring(1);
                        w = 1;
                    }
                    if (w > 0) {
                        if (word != value) {
                            // if it's only the start of the word,
                            // reduce the weight
                            w /= 10.0;
                        }
                        // higher weight for longer words
                        w += w * word.length / 10.0;
                        count[pi]++;
                        weight[pi] += w;
                    }
                }
            }
        }
    }
    var x = 0;
    var table = document.getElementById('result');
    var piList = new Array();
    var piWeight = new Array();
    for (var i = 0; i < pages.length; i++) {
        var w = weight[i];
        if (w > 0) {
            piList[x] = '' + i;
            piWeight[x] = w * count[i];
            x++;
        }
    }
    // sort
    for (var i = 1, j; i < x; i++) {
        var tw = piWeight[i];
        var ti = piList[i];
        for (j = i - 1; j >= 0 && (piWeight[j] < tw); j--) {
            piWeight[j + 1] = piWeight[j];
            piList[j + 1] = piList[j];
        }
        piWeight[j + 1] = tw;
        piList[j + 1] = ti;
    }
    addReferences(0, piList, keywords);
    if (piList.length == 0) {
        noResults(table, 'No results found');
    }
}

function addReferences(x, piList, word) {
    var table = document.getElementById('result');
    for (var j = 0; j < piList.length; j++) {
        var pi = piList[j];
        if (pi.charAt(0) == 't') {
            pi = pi.substring(1);
        } else if (pi.charAt(0) == 'h') {
            pi = pi.substring(1);
        } else if (pi.charAt(0) == 'r') {
            pi = pi.substring(1);
        }
        var tr = table.insertRow(x++);
        var td = document.createElement('td');
        var tdClass = document.createAttribute('class');
        tdClass.nodeValue = 'searchLink';
        td.setAttributeNode(tdClass);
        var ah = document.createElement('a');
        var href = document.createAttribute('href');
        var thisLink = 'javascript:go(' + pi + ', "' + word + '")';
        if (firstLink == null) {
            firstLink = pi;
            firstLinkWord = word;
        }
        href.nodeValue = thisLink;
        ah.setAttributeNode(href);
        var page = pages[pi];
        var link = document.createTextNode(page.title);
        ah.appendChild(link);
        td.appendChild(ah);
        tr.appendChild(td);
    }
    return x;
}

function trim(s) {
    while (s.charAt(0) == ' ' && s.length > 0) {
        s = s.substring(1);
    }
    while (s.charAt(s.length - 1) == ' ' && s.length > 0) {
        s = s.substring(0, s.length - 1);
    }
    return s;
}

function replaceOtherChars(s) {
    var x = "";
    for (var i = 0; i < s.length; i++) {
        var c = s.charAt(i);
        if ("\t\r\n\"'.,:;!&/\\?%@`[]{}()+-=<>|*^~#$".indexOf(c) >= 0) {
            c = " ";
        }
        x += c;
    }
    return x;
}

function noResults(table, message) {
    var tr = table.insertRow(0);
    var td = document.createElement('td');
    var tdClass = document.createAttribute('class');
    tdClass.nodeValue = 'searchKeyword';
    td.setAttributeNode(tdClass);
    var text = document.createTextNode(message);
    td.appendChild(text);
    tr.appendChild(td);
}

