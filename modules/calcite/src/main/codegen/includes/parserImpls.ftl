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

SqlNodeList ColumnList() :
{
    final Span s;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ColumnWithType(list)
    (
        <COMMA> ColumnWithType(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void ColumnWithType(List<SqlNode> list) :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    final Span s = Span.of();
}
{
    id = CompoundIdentifier()
    type = DataType()
    [
        <NOT> <NULL> {
            nullable = false;
        }
    ]
    {
        list.add(SqlDdlNodes.column(s.add(id).end(this), id,
            type.withNullable(nullable), null, null));
    }
}

SqlNodeList CreateTableOptionList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s = Span.of();
}
{
    CreateTableOption(list)
    (
        <COMMA> CreateTableOption(list)
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}

IgniteSqlCreateTableOptionEnum CreateTableOptionEnumOpt() :
{
}
{
    <TEMPLATE> { return IgniteSqlCreateTableOptionEnum.AFFINITY_KEY; }
|
    <BACKUPS> { return IgniteSqlCreateTableOptionEnum.BACKUPS; }
|
    <AFFINITY_KEY> { return IgniteSqlCreateTableOptionEnum.AFFINITY_KEY; }
}

void CreateTableOption(List<SqlNode> list) :
{
    final Span s;
    final IgniteSqlCreateTableOptionEnum key;
    final SqlNode val;
}
{
    key = CreateTableOptionEnumOpt() { s = span(); }
    <EQ>
    (
        val = Literal()
    |
        val = TokenAsStringLiteral()
    ) {
        list.add(new IgniteSqlCreateTableOption(
            key, (SqlLiteral)val, s.end(this)
        ));
    }
}

SqlNode TokenAsStringLiteral() :
{
}
{
    {
        getNextToken();
        return SqlLiteral.createCharString(token.image, getPos());
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
    columnList = ColumnList()
    (
        <WITH> optionList = CreateTableOptionList()
    |
        { optionList = null; }
    )
    {
        return new IgniteSqlCreateTable(s.end(this), ifNotExists, id, columnList, optionList);
    }
}
