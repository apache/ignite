-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

help abc;
> ID SECTION TOPIC SYNTAX TEXT
> -- ------- ----- ------ ----
> rows: 0

HELP ABCDE EF_GH;
> ID SECTION TOPIC SYNTAX TEXT
> -- ------- ----- ------ ----
> rows: 0

HELP HELP;
> ID SECTION          TOPIC SYNTAX                  TEXT
> -- ---------------- ----- ----------------------- ----------------------------------------------------
> 67 Commands (Other) HELP  HELP [ anything [...] ] Displays the help pages of SQL commands or keywords.
> rows: 1

HELP he lp;
> ID SECTION          TOPIC SYNTAX                  TEXT
> -- ---------------- ----- ----------------------- ----------------------------------------------------
> 67 Commands (Other) HELP  HELP [ anything [...] ] Displays the help pages of SQL commands or keywords.
> rows: 1
