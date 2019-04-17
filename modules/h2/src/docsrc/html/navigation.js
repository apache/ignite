/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 *  * Initial Developer: H2 Group
 */

function scroll() {
    var scroll = document.documentElement.scrollTop;
    if (!scroll) {
        scroll = document.body.scrollTop;
    }
    var c = 255 - Math.min(scroll / 4, 64);
    var goTop = document.getElementById('goTop');
    goTop.style.color = 'rgb(' + c + ',' + c + ',' + c + ')';
}

function loadFrameset() {
    var a = location.search.split('&');
    var page = decodeURIComponent(a[0].substr(1));
    var frame = a[1];
    if(page && frame){
        var s = "top." + frame + ".location.replace('" + page + "')";
        eval(s);
    }
    return;
}

function frameMe(frame) {
    if(location.host.indexOf('h2database') < 0) {
        // allow translation
        return;
    }
    var frameset = "frame.html"; // name of the frameset page
    if(frame == null) {
        frame = 'main';
    }
    page = new String(self.document.location);
    var pos = page.lastIndexOf("/") + 1;
    var file = page.substr(pos);
    file = encodeURIComponent(file);
    if(window.name != frame) {
        var s = frameset + "?" + file + "&" + frame;
        top.location.replace(s);
    } else {
        highlightFrame();
    }
    return;
}

function addHighlight(page, word, count) {
    if(count > 0) {
        if(top.main.document.location.href.indexOf(page) > 0 && top.main.document.body && top.main.document.body.innerHTML) {
            highlight();
        } else {
            window.setTimeout('addHighlight("'+page+'","'+word+'",'+(count-1)+')', 10);
        }
    }
}

function highlightFrame() {
    var url = new String(top.main.location.href);
    if(url.indexOf('?highlight=') < 0) {
        return;
    } else {
        var page = url.split('?highlight=');
        var word = decodeURIComponent(page[1]);
        top.main.document.body.innerHTML = highlightSearchTerms(top.main.document.body, word);
        top.main.location = '#firstFound';
        // window.setTimeout('goFirstFound()', 1);
    }
}

function highlight() {
    var url = new String(document.location.href);
    if(url.indexOf('?highlight=') < 0) {
        return;
    } else {
        var page = url.split('highlight=')[1].split('&')[0];
        var search = decodeURIComponent(url.split('search=')[1].split('#')[0]);
        var word = decodeURIComponent(page);
        document.body.innerHTML = highlightSearchTerms(document.body, word);
        document.location = '#firstFound';
        document.getElementById('search').value = search;
        listWords(search, '');
    }
}

function goFirstFound() {
    top.main.location = '#firstFound';
/*
    var page = new String(parent.main.location);
    alert('first: ' + page);
    page = page.split('#')[0];
    paramSplit = page.split('?');
    page = paramSplit[0];
    page += '#firstFound';
    if(paramSplit.length > 0) {
        page += '?' + paramSplit[1];
    }
    top.main.location = page;
*/
}

function highlightSearchTerms(body, searchText) {
    matchColor = "ffff00,00ffff,00ff00,ff8080,ff0080".split(',');
    highlightEndTag = "</span>";
    searchArray = searchText.split(",");
    if (!body || typeof(body.innerHTML) == "undefined") {
        return false;
    }
    var bodyText = body.innerHTML;
    for (var i = 0; i < searchArray.length; i++) {
        var color = matchColor[i % matchColor.length];
        highlightStartTag = "<span ";
        if(i==0) {
            highlightStartTag += "id=firstFound ";
        }
        highlightStartTag += "style='color:#000000; background-color:#"+color+";'>";
        bodyText = doHighlight(bodyText, searchArray[i], highlightStartTag, highlightEndTag);
    }
    return bodyText;
}

function doHighlight(bodyText, searchTerm, highlightStartTag, highlightEndTag) {
    if(searchTerm == undefined || searchTerm=="" || searchTerm.length < 3) {
        return bodyText;
    }
    var newText = "";
    var i = -1;
    var lcSearchTerm = searchTerm.toLowerCase();
    var lcBodyText = bodyText.toLowerCase();
    while (bodyText.length > 0) {
        i = lcBodyText.indexOf(lcSearchTerm, i+1);
        if (i < 0) {
            newText += bodyText;
            bodyText = "";
        } else {
            // skip anything inside an HTML tag
            if (bodyText.lastIndexOf(">", i) >= bodyText.lastIndexOf("<", i)) {
                // skip anything inside a <script> block
                if (lcBodyText.lastIndexOf("/script>", i) >= lcBodyText.lastIndexOf("<script", i)) {
                    newText += bodyText.substring(0, i) + highlightStartTag + bodyText.substr(i, searchTerm.length) + highlightEndTag;
                    bodyText = bodyText.substr(i + searchTerm.length);
                    lcBodyText = bodyText.toLowerCase();
                    i = -1;
                }
            }
        }
    }
    return newText;
}

var drag = false;
var dragSize = 0;
var dragStart = 0;

function mouseDown(e) {
    dragStart = e.clientX || e.pageX;
    dragSize = parseInt(document.getElementById('searchMenu').style.width);
    drag = true;
    return false;
}

function mouseUp(e) {
    drag = false;
    return false;
}

function mouseMove(e) {
    if (drag) {
        var e = e || window.event;
        var x = e.clientX || e.pageX;
        dragSize += x - dragStart;
        dragStart = x;
        document.getElementById('searchMenu').style.width=dragSize + 'px';
        return false;
    }
    return true;
}

function switchBnf(x) {
    var bnfList = document.getElementsByName('bnf');
    for (var i = 0; i < bnfList.length; i++) {
        var bnf = bnfList[i].style;
        bnf.display = bnf.display == '' ? 'none' : '';
    }
    var railroads = document.getElementsByName('railroad');
    for (var i = 0; i < railroads.length; i++) {
        var railroad = railroads[i].style;
        railroad.display = railroad.display == '' ? 'none' : '';
    }
    if (x) {
        document.location = '#' + x.id;
    }
}
