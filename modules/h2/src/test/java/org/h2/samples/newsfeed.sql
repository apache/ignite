/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

CREATE TABLE VERSION(ID INT PRIMARY KEY, VERSION VARCHAR, CREATED VARCHAR);
INSERT INTO VERSION VALUES

(147, '1.4.198', '2018-03-18'),
(146, '1.4.197', '2017-06-10'),
(145, '1.4.195', '2017-04-23'),
(144, '1.4.194', '2017-03-10'),
(143, '1.4.193', '2016-10-31'),
(142, '1.4.192', '2016-05-26'),
(141, '1.4.191', '2016-01-21'),
(140, '1.4.190', '2015-10-11'),
(139, '1.4.189', '2015-09-13'),
(138, '1.4.188', '2015-08-01'),
(137, '1.4.187', '2015-04-10'),
(136, '1.4.186', '2015-03-02'),
(135, '1.4.185', '2015-01-16'),
(134, '1.4.184', '2014-12-19'),
(133, '1.4.183', '2014-12-13'),
(132, '1.4.182', '2014-10-17'),
(131, '1.4.181', '2014-08-06'),
;

CREATE TABLE CHANNEL(TITLE VARCHAR, LINK VARCHAR, DESC VARCHAR,
    LANGUAGE VARCHAR, PUB TIMESTAMP, LAST TIMESTAMP, AUTHOR VARCHAR);

INSERT INTO CHANNEL VALUES('H2 Database Engine' ,
    'http://www.h2database.com/', 'H2 Database Engine', 'en-us', NOW(), NOW(), 'Thomas Mueller');

CREATE VIEW ITEM AS
SELECT ID, 'New version available: ' || VERSION || ' (' || CREATED || ')' TITLE,
CAST((CREATED || ' 12:00:00') AS TIMESTAMP) ISSUED,
$$A new version of H2 is available for
<a href="http://www.h2database.com">download</a>.
(You may have to click 'Refresh').
<br />
For details, see the
<a href="http://www.h2database.com/html/changelog.html">change log</a>.
<br />
For future plans, see the
<a href="http://www.h2database.com/html/roadmap.html">roadmap</a>.
$$ AS DESC FROM VERSION;

SELECT 'newsfeed-rss.xml' FILE,
    XMLSTARTDOC() ||
    XMLNODE('rss', XMLATTR('version', '2.0'),
        XMLNODE('channel', NULL,
            XMLNODE('title', NULL, C.TITLE) ||
            XMLNODE('link', NULL, C.LINK) ||
            XMLNODE('description', NULL, C.DESC) ||
            XMLNODE('language', NULL, C.LANGUAGE) ||
            XMLNODE('pubDate', NULL, FORMATDATETIME(C.PUB, 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')) ||
            XMLNODE('lastBuildDate', NULL, FORMATDATETIME(C.LAST, 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')) ||
            GROUP_CONCAT(
                XMLNODE('item', NULL,
                    XMLNODE('title', NULL, I.TITLE) ||
                    XMLNODE('link', NULL, C.LINK) ||
                    XMLNODE('description', NULL, XMLCDATA(I.TITLE))
                )
            ORDER BY I.ID DESC SEPARATOR '')
        )
    ) CONTENT
FROM CHANNEL C, ITEM I
UNION
SELECT 'newsfeed-atom.xml' FILE,
    XMLSTARTDOC() ||
    XMLNODE('feed', XMLATTR('xmlns', 'http://www.w3.org/2005/Atom') || XMLATTR('xml:lang', C.LANGUAGE),
        XMLNODE('title', XMLATTR('type', 'text'), C.TITLE) ||
        XMLNODE('id', NULL, XMLTEXT(C.LINK)) ||
        XMLNODE('author', NULL, XMLNODE('name', NULL, C.AUTHOR)) ||
        XMLNODE('link', XMLATTR('rel', 'self') || XMLATTR('href', 'http://www.h2database.com/html/newsfeed-atom.xml'), NULL) ||
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
UNION
SELECT 'newsletter.txt' FILE, I.DESC CONTENT FROM ITEM I WHERE I.ID = (SELECT MAX(ID) FROM ITEM)
UNION
SELECT 'doap-h2.rdf' FILE,
    XMLSTARTDOC() ||
$$<rdf:RDF xmlns="http://usefulinc.com/ns/doap#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xml:lang="en">
<Project rdf:about="http://h2database.com">
    <name>H2 Database Engine</name>
    <homepage rdf:resource="http://h2database.com"/>
    <programming-language>Java</programming-language>
    <category rdf:resource="http://projects.apache.org/category/database"/>
    <category rdf:resource="http://projects.apache.org/category/library"/>
    <category rdf:resource="http://projects.apache.org/category/network-server"/>
    <license rdf:resource="http://usefulinc.com/doap/licenses/mpl"/>
    <bug-database rdf:resource="https://github.com/h2database/h2database/issues"/>
    <download-page rdf:resource="http://h2database.com/html/download.html"/>
    <shortdesc xml:lang="en">H2 Database Engine</shortdesc>
    <description xml:lang="en">
    H2 is a relational database management system written in Java.
    It can be embedded in Java applications or run in the client-server mode.
    The disk footprint is about 1 MB. The main programming APIs are SQL and JDBC,
    however the database also supports using the PostgreSQL ODBC driver by acting like a PostgreSQL server.
    It is possible to create both in-memory tables, as well as disk-based tables.
    Tables can be persistent or temporary. Index types are hash table and tree for in-memory tables,
    and b-tree for disk-based tables.
    All data manipulation operations are transactional. (from Wikipedia)
    </description>
    <repository>
        <SVNRepository>
            <browse rdf:resource="https://github.com/h2database/h2database"/>
            <location rdf:resource="https://github.com/h2database/h2database"/>
        </SVNRepository>
    </repository>
    <mailing-list rdf:resource="http://groups.google.com/group/h2-database"/>
$$ ||
    GROUP_CONCAT(
        XMLNODE('release', NULL,
            XMLNODE('Version', NULL,
                XMLNODE('name', NULL, 'H2 ' || V.VERSION) ||
                XMLNODE('created', NULL, V.CREATED) ||
                XMLNODE('revision', NULL, V.VERSION)
            )
        )
        ORDER BY V.ID DESC SEPARATOR '') ||
'    </Project>
</rdf:RDF>' CONTENT
FROM VERSION V
