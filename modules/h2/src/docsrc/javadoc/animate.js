/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

function on(id) {
    return switchTag(id, 'titleOff', 'detailOn');
}

function off(id) {
    return switchTag(id, '', 'detail');
}

function allDetails() {
    for (i = 0;; i++) {
        x = document.getElementById('_' + i);
        if (x == null) {
            break;
        }
        switchTag(i, 'titleOff', 'detailOn');
    }
    return false;
}

function switchTag(id, title, detail) {
    if (document.getElementById('__' + id) != null) {
        document.getElementById('__' + id).className = title;
        document.getElementById('_' + id).className = detail;
    }
    return false;
}

function openLink() {
    page = new String(self.document.location);
    var pos = page.lastIndexOf("#") + 1;
    if (pos == 0) {
        return;
    }
    var ref = page.substr(pos);
    link = decodeURIComponent(ref);
    el = document.getElementById(link);
    if (el.nodeName.toLowerCase() == 'h4') {
        // constant
        return true;
    }
    el = el.parentNode.parentNode;
    window.scrollTo(0, el.offsetTop);
    on(el.id.substr(2));
    return false;
}