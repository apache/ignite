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

SqlNodeList CreateTableOptionList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s = Span.of();
}
{
    CreateTableOption(list)
    (
        <COMMA> { s.add(this); } CreateTableOption(list)
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
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
    id = SimpleIdentifier() type = DataType() nullable = NullableOptDefaultTrue()
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
}
{
    <TABLE>
    ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    columnList = TableElementList()
    (
        <WITH> { s.add(this); } optionList = CreateTableOptionList()
    |
        { optionList = null; }
    )
    {
        return new IgniteSqlCreateTable(s.end(this), ifNotExists, id, columnList, optionList);
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
    final SqlIdentifier idxId;
    final SqlIdentifier tblId;
    final SqlNodeList columnList;
    SqlNumericLiteral parallel = null;
    SqlNumericLiteral inlineSize = null;
}
{
    <INDEX>
    ifNotExists = IfNotExistsOpt()
    idxId = SimpleIdentifier()
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
    dt = DataType() {
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
    type = DataType()
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
    <USER> user = SimpleIdentifier()
    <WITH> <PASSWORD> password = StringLiteral() {
        return new IgniteSqlCreateUser(s.end(this), user, password);
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
        return new IgniteSqlAlterUser(s.end(this), user, password);
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
