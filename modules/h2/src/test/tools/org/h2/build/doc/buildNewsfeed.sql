/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Automated Build' ,
    'http://www.h2database.com/html/build.html#automated', 'H2 Database Automated Build', 'en-us', NOW(), NOW(), 'Thomas Mueller');

SELECT XMLSTARTDOC() ||
    XMLNODE('feed', XMLATTR('xmlns', 'http://www.w3.org/2005/Atom') || XMLATTR('xml:lang', C.LANGUAGE),
        XMLNODE('title', XMLATTR('type', 'text'), C.TITLE) ||
        XMLNODE('id', NULL, XMLTEXT(C.LINK)) ||
        XMLNODE('author', NULL, XMLNODE('name', NULL, C.AUTHOR)) ||
        XMLNODE('link', XMLATTR('rel', 'self') || XMLATTR('href', 'http://www.h2database.com/automated/news.xml'), NULL) ||
        XMLNODE('updated', NULL, FORMATDATETIME(C.LAST, 'yyyy-MM-dd''T''HH:mm:ss''Z''', 'en', 'GMT')) ||
        GROUP_CONCAT(
            XMLNODE('entry', NULL,
                XMLNODE('title', XMLATTR('type', 'text'), I.TITLE) ||
                XMLNODE('link', XMLATTR('rel', 'alternate') || XMLATTR('type', 'text/html') || XMLATTR('href', C.LINK), NULL) ||
                XMLNODE('id', NULL, XMLTEXT(C.LINK || '/' || I.ID)) ||
                XMLNODE('updated', NULL, FORMATDATETIME(I.ISSUED, 'yyyy-MM-dd''T''HH:mm:ss''Z''', 'en', 'GMT')) ||
                XMLNODE('content', XMLATTR('type', 'html'), XMLCDATA(I.DESC))
            )
        ORDER BY I.ID DESC SEPARATOR '')
    ) CONTENT
FROM CHANNEL C, ITEM I
WHERE I.ID > (SELECT MAX(ID) FROM ITEM)-20
