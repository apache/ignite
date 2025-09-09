<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

SqlNodeList WithCreateTableOptionList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    [
        <WITH> { s = span(); }
        (
            <QUOTED_IDENTIFIER>
            {
                return IgniteSqlCreateTableOption.parseOptionList(
                    SqlParserUtil.stripQuotes(token.image, DQ, DQ, DQDQ, quotedCasing),
                    getPos().withQuoting(true)
                );
            }
        |
            CreateTableOption(list)
            (
                <COMMA> { s.add(this); } CreateTableOption(list)
            )*
            {
                return new SqlNodeList(list, s.end(this));
            }
        )
    ]
    { return null; }
}

SqlLiteral CreateTableOptionKey() :
{
}
{
    <TEMPLATE> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.TEMPLATE, getPos()); }
|
    <BACKUPS> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.BACKUPS, getPos()); }
|
    <AFFINITY_KEY> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.AFFINITY_KEY, getPos()); }
|
    <ATOMICITY> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.ATOMICITY, getPos()); }
|
    <WRITE_SYNCHRONIZATION_MODE> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.WRITE_SYNCHRONIZATION_MODE, getPos()); }
|
    <CACHE_GROUP> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.CACHE_GROUP, getPos()); }
|
    <CACHE_NAME> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.CACHE_NAME, getPos()); }
|
    <DATA_REGION> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.DATA_REGION, getPos()); }
|
    <KEY_TYPE> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.KEY_TYPE, getPos()); }
|
    <VALUE_TYPE> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.VALUE_TYPE, getPos()); }
|
    <ENCRYPTED> { return SqlLiteral.createSymbol(IgniteSqlCreateTableOptionEnum.ENCRYPTED, getPos()); }
}

void CreateTableOption(List<SqlNode> list) :
{
    final Span s;
    final SqlLiteral key;
    final SqlNode val;
}
{
    key = CreateTableOptionKey() { s = span(); }
    <EQ>
    (
        val = Literal()
    |
        val = SimpleIdentifier()
    ) {
        list.add(new IgniteSqlCreateTableOption(key, val, s.end(this)));
    }
}

SqlDataTypeSpec DataTypeEx() :
{
    final SqlDataTypeSpec dt;
}
{
    (
        dt = DataType()
    |
        dt = IntervalType()
    )
    {
        return dt;
    }
}

SqlDataTypeSpec IntervalType() :
{
    final Span s;
    final SqlIntervalQualifier intervalQualifier;
}
{
    <INTERVAL> { s = span(); } intervalQualifier = IntervalQualifier() {
        return new SqlDataTypeSpec(new IgniteSqlIntervalTypeNameSpec(intervalQualifier, s.end(this)), s.pos());
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
    final SqlNode dflt;
    SqlIdentifier id = null;
}
{
    id = SimpleIdentifier() type = DataTypeEx() nullable = NullableOptDefaultTrue()
    (
        <DEFAULT_> { s.add(this); } dflt = Literal() {
            strategy = ColumnStrategy.DEFAULT;
        }
    |
        {
            dflt = null;
            strategy = nullable ? ColumnStrategy.NULLABLE
                : ColumnStrategy.NOT_NULLABLE;
        }
    )
    [
        <PRIMARY> { s.add(this); } <KEY> {
            columnList = SqlNodeList.of(id);
            list.add(SqlDdlNodes.primary(s.end(columnList), null, columnList));
        }
    ]
    {
        list.add(
            SqlDdlNodes.column(s.add(id).end(this), id,
                type.withNullable(nullable), dflt, strategy));
    }
|
    [ <CONSTRAINT> { s.add(this); } id = SimpleIdentifier() ]
    <PRIMARY> { s.add(this); } <KEY>
    columnList = ParenthesizedSimpleIdentifierList() {
        list.add(SqlDdlNodes.primary(s.end(columnList), id, columnList));
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList columnList;
    final SqlNodeList optionList;
    final SqlNode query;
}
{
    {
        if (replace)
            throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.unsupportedClause("REPLACE"));
    }

    <TABLE>
    ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    (
        LOOKAHEAD(3)
        columnList = TableElementList()
        optionList = WithCreateTableOptionList()
        { query = null; }
    |
        (
            columnList = ParenthesizedSimpleIdentifierList()
        |
            { columnList = null; }
        )
        optionList = WithCreateTableOptionList()
        <AS> { s.add(this); } query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return new IgniteSqlCreateTable(s.end(this), ifNotExists, id, columnList, query, optionList);
    }
}

SqlNode IndexedColumn() :
{
    final Span s;
    SqlNode col;
}
{
    col = SimpleIdentifier()
    (
        <ASC>
    |   <DESC> {
            col = SqlStdOperatorTable.DESC.createCall(getPos(), col);
        }
    )?
    {
        return col;
    }
}

SqlNodeList IndexedColumnList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode col = null;
}
{
    <LPAREN> { s = span(); }
    col = IndexedColumn() { list.add(col); }
    (
        <COMMA> col = IndexedColumn() { list.add(col); }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlCreate SqlCreateIndex(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier tblId;
    final SqlNodeList columnList;
    SqlIdentifier idxId = null;
    SqlNumericLiteral parallel = null;
    SqlNumericLiteral inlineSize = null;
}
{
    {
        if (replace)
            throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.unsupportedClause("REPLACE"));
    }

    <INDEX>
    ifNotExists = IfNotExistsOpt()
    [ idxId = SimpleIdentifier() ]
    <ON>
    tblId = CompoundIdentifier()
    columnList = IndexedColumnList()
    (
        <PARALLEL> <UNSIGNED_INTEGER_LITERAL> {
            if (parallel != null)
                throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.optionAlreadyDefined("PARALLEL"));

            parallel = SqlLiteral.createExactNumeric(token.image, getPos());
        }
    |
        <INLINE_SIZE> <UNSIGNED_INTEGER_LITERAL> {
            if (inlineSize != null)
                throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.optionAlreadyDefined("INLINE_SIZE"));

            inlineSize = SqlLiteral.createExactNumeric(token.image, getPos());
        }
    )*
    {
        return new IgniteSqlCreateIndex(s.end(this), ifNotExists, idxId, tblId, columnList, parallel, inlineSize);
    }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropIndex(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <INDEX> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return new IgniteSqlDropIndex(s.end(this), ifExists, id);
    }
}

void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
{
    final SqlDataTypeSpec dt;
}
{
    <INFIX_CAST> {
        checkNonQueryExpression(exprContext);
    }
    dt = DataTypeEx() {
        list.add(
            new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
                s.pos()));
        list.add(dt);
    }
}

SqlNodeList ColumnWithTypeList() :
{
    final Span s;
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode col;
}
{
    <LPAREN> { s = span(); }
    col = ColumnWithType() { list.add(col); }
    (
        <COMMA> col = ColumnWithType() { list.add(col); }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNode ColumnWithType() :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    type = DataTypeEx()
    [
        <NOT> <NULL> {
            nullable = false;
        }
    ]
    {
        return SqlDdlNodes.column(s.add(id).end(this), id, type.withNullable(nullable), null, null);
    }
}

SqlNodeList ColumnWithTypeOrList() :
{
    SqlNode col;
    SqlNodeList list;
}
{
    col = ColumnWithType() { return new SqlNodeList(Collections.singletonList(col), col.getParserPosition()); }
|
    list = ColumnWithTypeList() { return list; }
}

SqlNode SqlAlterTable() :
{
    final Span s;
    final boolean ifExists;
    final SqlIdentifier id;
    boolean colIgnoreErr;
    SqlNode col;
    SqlNodeList cols;
}
{
    <ALTER> { s = span(); }
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier()
    (
        <LOGGING> { return new IgniteSqlAlterTable(s.end(this), ifExists, id, true); }
    |
        <NOLOGGING>  { return new IgniteSqlAlterTable(s.end(this), ifExists, id, false); }
    |
        <ADD> [<COLUMN>] colIgnoreErr = IfNotExistsOpt() cols = ColumnWithTypeOrList() {
            return new IgniteSqlAlterTableAddColumn(s.end(this), ifExists, id, colIgnoreErr, cols);
        }
    |
        <DROP> [<COLUMN>] colIgnoreErr = IfExistsOpt() cols = SimpleIdentifierOrList() {
            return new IgniteSqlAlterTableDropColumn(s.end(this), ifExists, id, colIgnoreErr, cols);
        }
    )
}

SqlCreate SqlCreateUser(Span s, boolean replace) :
{
    final SqlIdentifier user;
    final SqlNode password;
}
{
    {
        if (replace)
            throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.unsupportedClause("REPLACE"));
    }

    <USER> user = SimpleIdentifier()
    <WITH> <PASSWORD> password = StringLiteral() {
        return new IgniteSqlCreateUser(s.end(this), user, SqlLiteral.unchain(password));
    }
}

SqlNode SqlAlterUser() :
{
    final Span s;
    final SqlIdentifier user;
    final SqlNode password;
}
{
    <ALTER> { s = span(); } <USER> user = SimpleIdentifier()
    <WITH> <PASSWORD> password = StringLiteral() {
        return new IgniteSqlAlterUser(s.end(this), user, SqlLiteral.unchain(password));
    }
}

SqlDrop SqlDropUser(Span s, boolean replace) :
{
    final SqlIdentifier user;
}
{
    <USER> user = SimpleIdentifier() {
        return new IgniteSqlDropUser(s.end(this), user);
    }
}

<DEFAULT, DQID, BTID> TOKEN :
{
    < NEGATE: "!" >
|   < TILDE: "~" >
}

SqlNumericLiteral SignedIntegerLiteral() :
{
    final Span s;
}
{
    <PLUS> <UNSIGNED_INTEGER_LITERAL> {
        return SqlLiteral.createExactNumeric(token.image, getPos());
    }
|
    <MINUS> { s = span(); } <UNSIGNED_INTEGER_LITERAL> {
        return SqlLiteral.createNegative(SqlLiteral.createExactNumeric(token.image, getPos()), s.end(this));
    }
|
    <UNSIGNED_INTEGER_LITERAL> {
        return SqlLiteral.createExactNumeric(token.image, getPos());
    }
}

SqlCharStringLiteral UuidLiteral():
{
    final Span s;
    final String rawUuuid;
}
{
    <QUOTED_STRING> {
        String rawUuid = SqlParserUtil.parseString(token.image);
        try {
            java.util.UUID.fromString(rawUuid);
            return SqlLiteral.createCharString(rawUuid, getPos());
        }
        catch (Exception e) {
            throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.illegalUuid(rawUuid));
        }
    }
}

SqlCharStringLiteral IgniteUuidLiteral():
{
    final Span s;
    final String rawUuuid;
}
{
    <QUOTED_STRING> {
        String rawUuid = SqlParserUtil.parseString(token.image);
        try {
            IgniteUuid.fromString(rawUuid);
            return SqlLiteral.createCharString(rawUuid, getPos());
        }
        catch (Exception e) {
            throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.illegalIgniteUuid(rawUuid));
        }
    }
}

SqlNode SqlKillScanQuery():
{
    final Span s;
    final SqlCharStringLiteral originNodeId;
    final SqlCharStringLiteral cacheName;
    final SqlNumericLiteral queryId;
}
{
    <KILL> { s = span(); } <SCAN>
    originNodeId = UuidLiteral()
    <QUOTED_STRING> {
        cacheName = SqlLiteral.createCharString(SqlParserUtil.parseString(token.image), getPos());
    }
    queryId = SignedIntegerLiteral() {
        return IgniteSqlKill.createScanQueryKill(s.end(this), originNodeId, cacheName, queryId);
    }
}

SqlNode SqlKillContinuousQuery():
{
    final Span s;
    final SqlCharStringLiteral originNodeId;
    final SqlCharStringLiteral routineId;
}
{
    <KILL> { s = span(); } <CONTINUOUS>
    originNodeId = UuidLiteral()
    routineId = UuidLiteral() {
        return IgniteSqlKill.createContinuousQueryKill(s.end(this), originNodeId, routineId);
    }
}

SqlNode SqlKillTransaction():
{
    final Span s;
    final SqlCharStringLiteral xid;
}
{
    <KILL> { s = span(); } <TRANSACTION>
    xid = IgniteUuidLiteral() {
        return IgniteSqlKill.createTransactionKill(s.end(this), xid);
    }
}

SqlNode SqlKillService():
{
    final Span s;
    final SqlCharStringLiteral srvName;
}
{
    <KILL> { s = span(); } <SERVICE>
    <QUOTED_STRING> {
        srvName = SqlLiteral.createCharString(SqlParserUtil.parseString(token.image), getPos());
        return IgniteSqlKill.createServiceKill(s.end(this), srvName);
    }
}

SqlNode SqlKillComputeTask():
{
    final Span s;
    final SqlCharStringLiteral sesId;
}
{
    <KILL> { s = span(); } <COMPUTE>
    sesId = IgniteUuidLiteral() {
        return IgniteSqlKill.createComputeTaskKill(s.end(this), sesId);
    }
}

boolean IsAsyncOpt() :
{
}
{
    <ASYNC> { return true; } | { return false; }
}

SqlNode SqlKillQuery():
{
    final Span s;
    final boolean isAsync;
}
{
    <KILL> { s = span(); } <QUERY>
    isAsync = IsAsyncOpt()
    <QUOTED_STRING> {
        String rawQueryId = SqlParserUtil.parseString(token.image);
        SqlCharStringLiteral queryIdLiteral = SqlLiteral.createCharString(rawQueryId, getPos());
        Pair<UUID, Long> id = IgniteSqlKill.parseGlobalQueryId(rawQueryId);
        if (id == null) {
            throw SqlUtil.newContextException(getPos(), IgniteResource.INSTANCE.illegalGlobalQueryId(rawQueryId));
        }
        return IgniteSqlKill.createQueryKill(s.end(this), queryIdLiteral, id.getKey(), id.getValue(), isAsync);
    }
}

SqlNode SqlCommitTransaction():
{
    final Span s;
}
{
    <COMMIT> { s = span(); } (<TRANSACTION>)? {
        return new IgniteSqlCommit(s.end(this));
    }
}

SqlNode SqlRollbackTransaction():
{
    final Span s;
}
{
    <ROLLBACK> { s = span(); } (<TRANSACTION>)? {
        return new IgniteSqlRollback(s.end(this));
    }
}

IgniteSqlStatisticsTable StatisticsTable():
{
    final Span s = Span.of();
    final SqlIdentifier id;
    final SqlNodeList columnList;
}
{
    id = CompoundIdentifier()
    (
        columnList = ParenthesizedSimpleIdentifierList()
    |
        { columnList = null; }
    )
    {
        return new IgniteSqlStatisticsTable(id, columnList, s.end(this));
    }
}

SqlNodeList StatisticsTables():
{
    final Span s = Span.of();
    List<SqlNode> tbls = new ArrayList<SqlNode>();
    SqlNode tbl;
}
{
    tbl = StatisticsTable() { tbls.add(tbl); }
    (
        <COMMA> tbl = StatisticsTable() { tbls.add(tbl); }
    )*
    {
        return new SqlNodeList(tbls, s.end(this));
    }
}

SqlNodeList WithStatisticsAnalyzeOptionList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    [
        <WITH> { s = span(); }
        (
            StatisticsAnalyzeOption(list)
            (
                <COMMA> { s.add(this); } StatisticsAnalyzeOption(list)
            )*
            {
                return new SqlNodeList(list, s.end(this));
            }
        |
            <QUOTED_IDENTIFIER>
            {
                return IgniteSqlStatisticsAnalyzeOption.parseOptionList(
                    SqlParserUtil.stripQuotes(token.image, DQ, DQ, DQDQ, quotedCasing),
                    getPos().withQuoting(true)
                );
            }
        )
    ]
    { return null; }
}

SqlLiteral StatisticsAnalyzeOptionKey() :
{
}
{
    <DISTINCT> { return SqlLiteral.createSymbol(IgniteSqlStatisticsAnalyzeOptionEnum.DISTINCT, getPos()); }
|
    <TOTAL> { return SqlLiteral.createSymbol(IgniteSqlStatisticsAnalyzeOptionEnum.TOTAL, getPos()); }
|
    <SIZE> { return SqlLiteral.createSymbol(IgniteSqlStatisticsAnalyzeOptionEnum.SIZE, getPos()); }
|
    <NULLS> { return SqlLiteral.createSymbol(IgniteSqlStatisticsAnalyzeOptionEnum.NULLS, getPos()); }
|
    <MAX_CHANGED_PARTITION_ROWS_PERCENT> { return SqlLiteral.createSymbol(IgniteSqlStatisticsAnalyzeOptionEnum.MAX_CHANGED_PARTITION_ROWS_PERCENT, getPos()); }
}

void StatisticsAnalyzeOption(List<SqlNode> list) :
{
    final Span s;
    final SqlLiteral key;
    final SqlNode val;
}
{
    key = StatisticsAnalyzeOptionKey() { s = span(); }
    <EQ>
    (
        val = Literal()
    |
        val = SimpleIdentifier()
    ) {
        list.add(new IgniteSqlStatisticsAnalyzeOption(key, val, s.end(this)));
    }
}

SqlNode SqlStatisticsDrop():
{
    final Span s;
    SqlNodeList tablesList;
}
{
    <DROP> <STATISTICS> { s = span(); }
    tablesList = StatisticsTables()
    {
        return new IgniteSqlStatisticsDrop(tablesList, s.end(this));
    }
}

SqlNode SqlStatisticsRefresh():
{
    final Span s;
    SqlNodeList tablesList;
}
{
    <REFRESH> <STATISTICS> { s = span(); }
    tablesList = StatisticsTables()
    {
        return new IgniteSqlStatisticsRefresh(tablesList, s.end(this));
    }
}

SqlNode SqlStatisticsAnalyze():
{
    final Span s;
    SqlNodeList tablesList;
    SqlNodeList optionsList;
}
{
    <ANALYZE> { s = span(); }
    tablesList = StatisticsTables()
    optionsList = WithStatisticsAnalyzeOptionList()
    {
        return new IgniteSqlStatisticsAnalyze(tablesList, optionsList, s.end(this));
    }
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), replace, id, null, query);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}
