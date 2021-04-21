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
