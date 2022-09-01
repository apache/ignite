/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.inject.internal.util.ImmutableMap;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillComputeTask;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillContinuousQuery;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillQuery;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillScanQuery;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillService;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillTransaction;
import org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyze;
import org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOption;
import org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOptionEnum;
import org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOptionEnum.DISTINCT;
import static org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOptionEnum.MAX_CHANGED_PARTITION_ROWS_PERCENT;
import static org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOptionEnum.NULLS;
import static org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOptionEnum.SIZE;
import static org.apache.ignite.internal.processors.query.calcite.sql.stat.IgniteSqlStatisticsAnalyzeOptionEnum.TOTAL;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test suite to verify parsing of the custom (DDL and others) command.
 */
public class SqlCustomParserTest extends GridCommonAbstractTest {
    /**
     * Very simple case where only table name and a few columns are presented.
     */
    @Test
    public void createTableSimpleCase() throws SqlParseException {
        String query = "create table my_table(id int, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));
    }

    /**
     * Parsing of CREATE TABLE statement with quoted identifiers.
     */
    @Test
    public void createTableQuotedIdentifiers() throws SqlParseException {
        String query = "create table \"My_Table\"(\"Id\" int, \"Val\" varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("My_Table")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(columnWithName("Id")));
        assertThat(createTable.columnList(), hasItem(columnWithName("Val")));
    }

    /**
     * Parsing of CREATE TABLE statement with IF NOT EXISTS.
     */
    @Test
    public void createTableIfNotExists() throws SqlParseException {
        String query = "create table if not exists my_table(id int, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(true));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint
     * is a shortcut within a column definition.
     */
    @Test
    public void createTableWithPkCase1() throws SqlParseException {
        String query = "create table my_table(id int primary key, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
            "PK constraint with name \"ID\"", SqlKeyConstraint.class,
            constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                .matches(constraint.getOperandList().get(1))
                && constraint.getOperandList().get(0) == null
                && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint
     * is set explicitly and has no name.
     */
    @Test
    public void createTableWithPkCase2() throws SqlParseException {
        String query = "create table my_table(id int, val varchar, primary key(id))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
            "PK constraint without name containing column \"ID\"", SqlKeyConstraint.class,
            constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                .matches(constraint.getOperandList().get(1))
                && constraint.getOperandList().get(0) == null
                && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint
     * is set explicitly and has a name.
     */
    @Test
    public void createTableWithPkCase3() throws SqlParseException {
        String query = "create table my_table(id int, val varchar, constraint pk_key primary key(id))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
            "PK constraint with name \"PK_KEY\" containing column \"ID\"", SqlKeyConstraint.class,
            constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                .matches(constraint.getOperandList().get(1))
                && "PK_KEY".equals(((SqlIdentifier)constraint.getOperandList().get(0)).names.get(0))
                && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint
     * consists of several columns.
     */
    @Test
    public void createTableWithPkCase4() throws SqlParseException {
        String query = "create table my_table(id1 int, id2 int, val varchar, primary key(id1, id2))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThat(createTable.name().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
            "PK constraint with two columns", SqlKeyConstraint.class,
            constraint -> hasItem(ofTypeMatching("identifier \"ID1\"", SqlIdentifier.class, id -> "ID1".equals(id.names.get(0))))
                .matches(constraint.getOperandList().get(1))
                && hasItem(ofTypeMatching("identifier \"ID2\"", SqlIdentifier.class, id -> "ID2".equals(id.names.get(0))))
                .matches(constraint.getOperandList().get(1))
                && constraint.getOperandList().get(0) == null
                && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified table options.
     */
    @Test
    public void createTableWithOptions() throws SqlParseException {
        String query = "create table my_table(id int) with" +
            " template=\"my_template\"," +
            " backups=2," +
            " affinity_key=my_aff," +
            " atomicity=atomic," +
            " write_synchronization_mode=transactional," +
            " cache_group=my_cache_group," +
            " cache_name=my_cache_name," +
            " data_region=my_data_region," +
            " key_type=my_key_type," +
            " value_type=my_value_type," +
            " encrypted=true";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        assertThatStringOptionPresent(createTable.createOptionList().getList(), "TEMPLATE", "my_template");
        assertThatIntegerOptionPresent(createTable.createOptionList().getList(), "BACKUPS", 2);
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "AFFINITY_KEY", "MY_AFF");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "ATOMICITY", "ATOMIC");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "WRITE_SYNCHRONIZATION_MODE", "TRANSACTIONAL");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "CACHE_GROUP", "MY_CACHE_GROUP");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "CACHE_NAME", "MY_CACHE_NAME");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "DATA_REGION", "MY_DATA_REGION");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "KEY_TYPE", "MY_KEY_TYPE");
        assertThatStringOptionPresent(createTable.createOptionList().getList(), "VALUE_TYPE", "MY_VALUE_TYPE");
        assertThatBooleanOptionPresent(createTable.createOptionList().getList(), "ENCRYPTED", true);
    }

    /**
     * Parsing of CREATE TABLE with specified table options (double quoted).
     */
    @Test
    public void createTableWithOptionsQuoted() throws SqlParseException {
        String query = "create table my_table(id int) with \"" +
            " template=my_template," +
            " backups=2," +
            " affinity_key=My_Aff," +
            " Atomicity=atomic," +
            " Write_Synchronization_mode=transactional," +
            " cache_group = my_cache_group," +
            " cache_name=my_cache_name ," +
            " data_region= my_data_region," +
            " key_type=my_key_type," +
            " value_type=my_value_type," +
            " encrypted=true" +
            "\"";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable)node;

        List<SqlNode> opts = createTable.createOptionList().getList();
        assertThatStringOptionPresent(opts, "TEMPLATE", "my_template");
        assertThatStringOptionPresent(opts, "BACKUPS", "2");
        assertThatStringOptionPresent(opts, "AFFINITY_KEY", "My_Aff");
        assertThatStringOptionPresent(opts, "ATOMICITY", "atomic");
        assertThatStringOptionPresent(opts, "WRITE_SYNCHRONIZATION_MODE", "transactional");
        assertThatStringOptionPresent(opts, "CACHE_GROUP", "my_cache_group");
        assertThatStringOptionPresent(opts, "CACHE_NAME", "my_cache_name");
        assertThatStringOptionPresent(opts, "DATA_REGION", "my_data_region");
        assertThatStringOptionPresent(opts, "KEY_TYPE", "my_key_type");
        assertThatStringOptionPresent(opts, "VALUE_TYPE", "my_value_type");
        assertThatStringOptionPresent(opts, "ENCRYPTED", "true");

        assertParserThrows("create table my_table(id int) with \"unknown_key=val\"", SqlParseException.class);
        assertParserThrows("create table my_table(id int) with \"template\"", SqlParseException.class);
        assertParserThrows("create table my_table(id int) with \"template=t=t\"", SqlParseException.class);
    }

    /**
     * Parsing of CREATE TABLE AS SELECT.
     */
    @Test
    public void createTableAsSelect() throws SqlParseException {
        IgniteSqlCreateTable createTable = parse("create table my_table(id) as select 1");

        assertEquals(1, createTable.columnList().size());
        assertNotNull(createTable.query());

        createTable = parse("create table my_table(id, name) as select 1, 'a'");

        assertEquals(2, createTable.columnList().size());
        assertNotNull(createTable.query());

        createTable = parse("create table my_table as select 1, 'a'");

        assertNull(createTable.columnList());
        assertNull(createTable.createOptionList());
        assertNotNull(createTable.query());

        createTable = parse("create table my_table with cache_name=a, cache_group=b as select 1");

        assertNull(createTable.columnList());
        assertNotNull(createTable.createOptionList());
        assertNotNull(createTable.query());

        assertParserThrows("create table my_table(id int) as select 1", SqlParseException.class);
        assertParserThrows("create table my_table(id.a) as select 1", SqlParseException.class);
        assertParserThrows("create table my_table(id) as select 1 with cache_name=a", SqlParseException.class);
    }

    /**
     * Create index with list of indexed columns.
     */
    @Test
    public void createIndexColumns() throws SqlParseException {
        String qry = "create index my_index on my_table(id, val1 asc, val2 desc)";

        IgniteSqlCreateIndex createIdx = parse(qry);

        assertThat(createIdx.indexName().names, is(ImmutableList.of("MY_INDEX")));
        assertThat(createIdx.tableName().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createIdx.ifNotExists, is(false));
        assertThat(createIdx.columnList(), hasItem(indexedColumn("ID", false)));
        assertThat(createIdx.columnList(), hasItem(indexedColumn("VAL1", false)));
        assertThat(createIdx.columnList(), hasItem(indexedColumn("VAL2", true)));
    }

    /**
     * Create index on table with schema.
     */
    @Test
    public void createIndexOnTableWithSchema() throws SqlParseException {
        String qry = "create index my_index on my_schema.my_table(id)";

        IgniteSqlCreateIndex createIdx = parse(qry);

        assertThat(createIdx.indexName().names, is(ImmutableList.of("MY_INDEX")));
        assertThat(createIdx.tableName().names, is(ImmutableList.of("MY_SCHEMA", "MY_TABLE")));
    }

    /**
     * Create index on table with schema.
     */
    @Test
    public void createIndexEmptyName() throws SqlParseException {
        String qry = "create index on my_table(id)";

        IgniteSqlCreateIndex createIdx = parse(qry);

        assertThat(createIdx.indexName(), nullValue());
        assertThat(createIdx.tableName().names, is(ImmutableList.of("MY_TABLE")));
    }

    /**
     * Create index with "if not exists" clause"
     */
    @Test
    public void createIndexIfNotExists() throws SqlParseException {
        String qry = "create index if not exists my_index on my_table(id)";

        IgniteSqlCreateIndex createIdx = parse(qry);

        assertThat(createIdx.indexName().names, is(ImmutableList.of("MY_INDEX")));
        assertThat(createIdx.tableName().names, is(ImmutableList.of("MY_TABLE")));
        assertThat(createIdx.ifNotExists, is(true));
    }

    /**
     * Create index with parallel and inline_size options"
     */
    @Test
    public void createIndexWithOptions() throws SqlParseException {
        String qry = "create index my_index on my_table(id) parallel 10 inline_size 20";

        IgniteSqlCreateIndex createIdx = parse(qry);

        assertThat(createIdx.indexName().names, is(ImmutableList.of("MY_INDEX")));
        assertThat(createIdx.tableName().names, is(ImmutableList.of("MY_TABLE")));
        assertEquals(10, createIdx.parallel().intValue(true));
        assertEquals(20, createIdx.inlineSize().intValue(true));

        qry = "create index my_index on my_table(id) parallel 10";

        createIdx = parse(qry);

        assertEquals(10, createIdx.parallel().intValue(true));
        assertNull(createIdx.inlineSize());

        qry = "create index my_index on my_table(id) inline_size 20";

        createIdx = parse(qry);

        assertNull(createIdx.parallel());
        assertEquals(20, createIdx.inlineSize().intValue(true));

        qry = "create index my_index on my_table(id) inline_size 20 parallel 10";

        createIdx = parse(qry);

        assertEquals(20, createIdx.inlineSize().intValue(true));
        assertEquals(10, createIdx.parallel().intValue(true));
    }

    /**
     * Create index with malformed statements.
     */
    @Test
    public void createIndexMalformed() {
        assertParserThrows("create index my_index on my_table(id) parallel 10 inline_size 20 parallel 10",
            SqlValidatorException.class, "Option 'PARALLEL' has already been defined");

        assertParserThrows("create index my_index on my_table(id) inline_size -1", SqlParseException.class);

        assertParserThrows("create index my_index on my_table(id) inline_size = 1", SqlParseException.class);

        assertParserThrows("create index my_index on my_table(id) inline_size", SqlParseException.class);

        assertParserThrows("create index if exists my_index on my_table(id)", SqlParseException.class);

        assertParserThrows("create index my_index on my_table(id asc desc)", SqlParseException.class);

        assertParserThrows("create index my_index on my_table(id nulls first)", SqlParseException.class);

        assertParserThrows("create index my_scheme.my_index on my_table(id)", SqlParseException.class);

        assertParserThrows("create index my_index on my_table(id.id2)", SqlParseException.class);
    }

    /**
     * Alter table with LOGGING/NOLOGING clause.
     */
    @Test
    public void alterTableLoggingNologging() throws SqlParseException {
        IgniteSqlAlterTable alterTbl = parse("alter table my_table logging");

        assertThat(alterTbl.name().names, is(ImmutableList.of("MY_TABLE")));
        assertEquals(false, alterTbl.ifExists());
        assertEquals(true, alterTbl.logging());

        alterTbl = parse("alter table if exists my_table nologging");

        assertThat(alterTbl.name().names, is(ImmutableList.of("MY_TABLE")));
        assertEquals(true, alterTbl.ifExists());
        assertEquals(false, alterTbl.logging());
    }

    /**
     * Alter table with schema.
     */
    @Test
    public void alterTableWithSchema() throws SqlParseException {
        IgniteSqlAlterTable alterTbl1 = parse("alter table my_schema.my_table logging");
        assertThat(alterTbl1.name().names, is(ImmutableList.of("MY_SCHEMA", "MY_TABLE")));

        IgniteSqlAlterTableAddColumn alterTbl2 = parse("alter table my_schema.my_table add column a int");

        assertThat(alterTbl2.name().names, is(ImmutableList.of("MY_SCHEMA", "MY_TABLE")));

        IgniteSqlAlterTableDropColumn alterTbl3 = parse("alter table my_schema.my_table drop column a");

        assertThat(alterTbl3.name().names, is(ImmutableList.of("MY_SCHEMA", "MY_TABLE")));
    }

    /**
     * Alter table add column.
     */
    @Test
    public void alterTableAddColumn() throws SqlParseException {
        IgniteSqlAlterTableAddColumn alterTbl;

        alterTbl = parse("alter table my_table add column a int");

        assertThat(alterTbl.name().names, is(ImmutableList.of("MY_TABLE")));
        assertEquals(false, alterTbl.ifNotExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnWithName("A")));

        alterTbl = parse("alter table my_table add column if not exists a int");

        assertEquals(true, alterTbl.ifNotExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnWithName("A")));

        alterTbl = parse("alter table my_table add a int");

        assertEquals(false, alterTbl.ifNotExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnWithName("A")));

        alterTbl = parse("alter table my_table add if not exists a int");

        assertEquals(true, alterTbl.ifNotExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnWithName("A")));

        alterTbl = parse("alter table my_table add column (a int)");

        assertEquals(false, alterTbl.ifNotExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnWithName("A")));

        alterTbl = parse("alter table my_table add column if not exists (a int)");

        assertEquals(true, alterTbl.ifNotExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnWithName("A")));

        alterTbl = parse("alter table my_table add column (a int, \"b\" varchar, c date not null)");

        assertEquals(false, alterTbl.ifNotExistsColumn());
        assertEquals(3, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(columnDeclaration("A", "INTEGER", true)));
        assertThat(alterTbl.columns(), hasItem(columnDeclaration("b", "VARCHAR", true)));
        assertThat(alterTbl.columns(), hasItem(columnDeclaration("C", "DATE", false)));
    }

    /**
     * Alter table drop column.
     */
    @Test
    public void alterTableDropColumn() throws SqlParseException {
        IgniteSqlAlterTableDropColumn alterTbl;

        alterTbl = parse("alter table my_table drop column a");

        assertThat(alterTbl.name().names, is(ImmutableList.of("MY_TABLE")));
        assertEquals(false, alterTbl.ifExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));

        alterTbl = parse("alter table my_table drop column if exists a");

        assertEquals(true, alterTbl.ifExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));

        alterTbl = parse("alter table my_table drop a");

        assertEquals(false, alterTbl.ifExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));

        alterTbl = parse("alter table my_table drop if exists a");

        assertEquals(true, alterTbl.ifExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));

        alterTbl = parse("alter table my_table drop column (a)");

        assertEquals(false, alterTbl.ifExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));

        alterTbl = parse("alter table my_table drop column if exists (a)");

        assertEquals(true, alterTbl.ifExistsColumn());
        assertEquals(1, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));

        alterTbl = parse("alter table my_table drop column (a, \"b\", c)");

        assertEquals(false, alterTbl.ifExistsColumn());
        assertEquals(3, alterTbl.columns().size());
        assertThat(alterTbl.columns(), hasItem(identifierWithName("A")));
        assertThat(alterTbl.columns(), hasItem(identifierWithName("b")));
        assertThat(alterTbl.columns(), hasItem(identifierWithName("C")));
    }

    /**
     * Malformed alter table statements.
     */
    @Test
    public void alterTableMalformed() {
        assertParserThrows("alter table my_table logging nologging", SqlParseException.class);

        assertParserThrows("alter table my_table logging add column a int", SqlParseException.class);

        assertParserThrows("alter table if not exists my_table logging", SqlParseException.class);

        assertParserThrows("alter table my_table add column if exists (a int)", SqlParseException.class);

        assertParserThrows("alter table my_table add column (a)", SqlParseException.class);

        assertParserThrows("alter table my_table drop column if not exists (a)", SqlParseException.class);

        assertParserThrows("alter table my_table drop column (a int)", SqlParseException.class);

        assertParserThrows("alter table my_table add column (a.b int)", SqlParseException.class);

        assertParserThrows("alter table my_table drop column (a.b)", SqlParseException.class);
    }

    /**
     * Test parsing of CREATE USER command
     */
    @Test
    public void createUser() throws Exception {
        IgniteSqlCreateUser createUser;

        createUser = parse("create user test with password 'asd'");

        assertEquals("TEST", createUser.user().getSimple());
        assertEquals("asd", createUser.password());

        createUser = parse("create user \"test\" with password 'asd'");

        assertEquals("test", createUser.user().getSimple());
        assertEquals("asd", createUser.password());

        assertParserThrows("create user test", SqlParseException.class);
        assertParserThrows("create user test with password", SqlParseException.class);
        assertParserThrows("create user test with password asd", SqlParseException.class);
        assertParserThrows("create user 'test' with password 'asd'", SqlParseException.class);
    }

    /**
     * Test parsing of ALTER USER command
     */
    @Test
    public void alterUser() throws Exception {
        IgniteSqlAlterUser alterUser;

        alterUser = parse("alter user test with password 'asd'");

        assertEquals("TEST", alterUser.user().getSimple());
        assertEquals("asd", alterUser.password());

        alterUser = parse("alter user \"test\" with password 'asd'");

        assertEquals("test", alterUser.user().getSimple());
        assertEquals("asd", alterUser.password());

        assertParserThrows("alter user test", SqlParseException.class);
        assertParserThrows("alter user test with password", SqlParseException.class);
        assertParserThrows("alter user test with password asd", SqlParseException.class);
        assertParserThrows("alter user 'test' with password 'asd'", SqlParseException.class);
    }

    /**
     * Test parsing of DROP USER command
     */
    @Test
    public void dropUser() throws Exception {
        IgniteSqlDropUser dropUser;

        dropUser = parse("drop user test");

        assertEquals("TEST", dropUser.user().getSimple());

        dropUser = parse("drop user \"test\"");

        assertEquals("test", dropUser.user().getSimple());

        assertParserThrows("drop user test with password 'asd'", SqlParseException.class);
    }

    /**
     * Test kill scan query parsing.
     */
    @Test
    public void killScan() throws Exception {
        IgniteSqlKill killScan;

        UUID nodeId = UUID.randomUUID();
        killScan = parse("kill scan '" + nodeId + "' 'cache-name' 100500");

        assertTrue(killScan instanceof IgniteSqlKillScanQuery);
        assertEquals("cache-name", stringValue(((IgniteSqlKillScanQuery)killScan).cacheName()));
        assertEquals(nodeId.toString(), stringValue(((IgniteSqlKillScanQuery)killScan).nodeId()));
        assertEquals(100500L, ((IgniteSqlKillScanQuery)killScan).queryId().longValue(true));

        assertParserThrows("kill scan", SqlParseException.class);
        assertParserThrows("kill scan '1234' 'test' 10", SqlParseException.class);
        assertParserThrows("kill scan '" + UUID.randomUUID() + "' 'test'", SqlParseException.class);
        assertParserThrows("kill scan '" + UUID.randomUUID() + "' 'test' 1.0", SqlParseException.class);
    }

    /**
     * Test kill service query parsing.
     */
    @Test
    public void killService() throws Exception {
        IgniteSqlKill killService;

        killService = parse("kill service 'my-service'");
        assertTrue(killService instanceof IgniteSqlKillService);
        assertEquals("my-service", stringValue(((IgniteSqlKillService)killService).serviceName()));

        assertParserThrows("kill service 'my-service' 'test'", SqlParseException.class);
        assertParserThrows("kill service 10000", SqlParseException.class);
        assertParserThrows("kill service", SqlParseException.class);
    }

    /**
     * Test kill transaction query parsing.
     */
    @Test
    public void killTransaction() throws Exception {
        IgniteSqlKill killTx;

        String txId = IgniteUuid.randomUuid().toString();
        killTx = parse("kill transaction '" + txId + "'");
        assertTrue(killTx instanceof IgniteSqlKillTransaction);
        assertEquals(txId, stringValue(((IgniteSqlKillTransaction)killTx).xid()));

        assertParserThrows("kill transaction '1233415' '1'", SqlParseException.class);
        assertParserThrows("kill transaction 10000", SqlParseException.class);
        assertParserThrows("kill transaction", SqlParseException.class);
    }

    /**
     * Test kill compute task query parsing.
     */
    @Test
    public void killComputeTask() throws Exception {
        IgniteSqlKill killTask;

        IgniteUuid sesId = IgniteUuid.randomUuid();
        killTask = parse("kill compute '" + sesId + "'");
        assertTrue(killTask instanceof IgniteSqlKillComputeTask);
        assertEquals(sesId.toString(), stringValue(((IgniteSqlKillComputeTask)killTask).sessionId()));

        assertParserThrows("kill compute '1233415'", SqlParseException.class);
        assertParserThrows("kill compute 10000", SqlParseException.class);
        assertParserThrows("kill compute", SqlParseException.class);
    }

    /**
     * Test kill continuous query parsing.
     */
    @Test
    public void killContinuousQuery() throws Exception {
        IgniteSqlKill killTask;

        UUID routineId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();

        killTask = parse("kill continuous '" + nodeId + "' '" + routineId + "'");
        assertTrue(killTask instanceof IgniteSqlKillContinuousQuery);
        assertEquals(nodeId.toString(), stringValue(((IgniteSqlKillContinuousQuery)killTask).nodeId()));
        assertEquals(routineId.toString(), stringValue(((IgniteSqlKillContinuousQuery)killTask).routineId()));

        assertParserThrows("kill continuous '1233415'", SqlParseException.class);
        assertParserThrows("kill continuous '1233415' 1000", SqlParseException.class);
        assertParserThrows("kill continuous '123' '123'", SqlParseException.class);
        assertParserThrows("kill continuous", SqlParseException.class);
    }

    /**
     * Test kill continuous query parsing.
     */
    @Test
    public void killSqlQuery() throws Exception {
        IgniteSqlKill killTask;

        UUID nodeId = UUID.randomUUID();
        long queryId = ThreadLocalRandom.current().nextLong();

        killTask = parse("kill query '" + nodeId + "_" + queryId + "'");
        assertTrue(killTask instanceof IgniteSqlKillQuery);
        assertEquals(nodeId, ((IgniteSqlKillQuery)killTask).nodeId());
        assertEquals(queryId, ((IgniteSqlKillQuery)killTask).queryId());
        assertFalse(((IgniteSqlKillQuery)killTask).isAsync());

        killTask = parse("kill query async '" + nodeId + "_" + queryId + "'");
        assertTrue(killTask instanceof IgniteSqlKillQuery);
        assertEquals(nodeId, ((IgniteSqlKillQuery)killTask).nodeId());
        assertEquals(queryId, ((IgniteSqlKillQuery)killTask).queryId());
        assertTrue(((IgniteSqlKillQuery)killTask).isAsync());

        assertParserThrows("kill query '1233415'", SqlParseException.class);
        assertParserThrows("kill query '" + UUID.randomUUID() + "_a1233415'", SqlParseException.class);
        assertParserThrows("kill query '123' '123'", SqlParseException.class);
        assertParserThrows("kill query", SqlParseException.class);
    }

    /**
     * Test parsing COMMIT and ROLLBACK statements.
     */
    @Test
    public void testCommitRollback() throws Exception {
        assertTrue(parse("commit transaction") instanceof IgniteSqlCommit);
        assertTrue(parse("commit") instanceof IgniteSqlCommit);
        assertTrue(parse("rollback transaction") instanceof IgniteSqlRollback);
        assertTrue(parse("rollback") instanceof IgniteSqlRollback);

        assertParserThrows("commit transaction 123", SqlParseException.class);
        assertParserThrows("commit 123", SqlParseException.class);
        assertParserThrows("rollback transaction 123", SqlParseException.class);
        assertParserThrows("rollback 123", SqlParseException.class);
    }

    /**
     * Test parsing statistics command.
     */
    @Test
    public void testStatisticsCommands() throws Exception {
        checkStatisticsCommand("ANALYZE tbl1", "ANALYZE", "TBL1");
        checkStatisticsCommand("ANALYZE tbl1, tbl2", "ANALYZE", "TBL1,TBL2");
        checkStatisticsCommand("REFRESH STATISTICS tbl1, tbl2", "REFRESH STATISTICS", "TBL1,TBL2");
        checkStatisticsCommand("ANALYZE tbl1(a), tbl2", "ANALYZE", "TBL1(A),TBL2");
        checkStatisticsCommand("ANALYZE tbl1(a), tbl2(b,c), tbl3", "ANALYZE", "TBL1(A),TBL2(B,C),TBL3");
        checkStatisticsCommand("REFRESH STATISTICS tbl1(a), tbl2(b,c), tbl3", "REFRESH STATISTICS", "TBL1(A),TBL2(B,C),TBL3");
        checkStatisticsCommand("DROP STATISTICS tbl1(a), tbl2(b,c), tbl3", "DROP STATISTICS", "TBL1(A),TBL2(B,C),TBL3");
    }

    /**
     * Test parsing analyze command's options.
     */
    @Test
    public void testStatisticsAnalyzeOptions() throws Exception {
        checkStatisticsCommand(
            "ANALYZE schema.tbl1(a,b), tlbl2(c) WITH \"MAX_CHANGED_PARTITION_ROWS_PERCENT=1,DISTINCT=5,NULLS=6,TOTAL=7,SIZE=8\"",
            "ANALYZE",
            "SCHEMA.TBL1(A,B),TLBL2(C)",
            ImmutableMap.of(MAX_CHANGED_PARTITION_ROWS_PERCENT, "1", DISTINCT, "5", NULLS, "6", TOTAL, "7", SIZE, "8")
        );

        checkStatisticsCommand(
            "ANALYZE schema.tbl1(a,b), tlbl2(c) WITH MAX_CHANGED_PARTITION_ROWS_PERCENT=1,DISTINCT=5,NULLS=6,TOTAL=7,SIZE=8",
            "ANALYZE",
            "SCHEMA.TBL1(A,B),TLBL2(C)",
            ImmutableMap.of(MAX_CHANGED_PARTITION_ROWS_PERCENT, "1", DISTINCT, "5", NULLS, "6", TOTAL, "7", SIZE, "8")
        );

        checkStatisticsCommand(
            "ANALYZE schema.tbl1(a,b), tlbl2(c)",
            "ANALYZE",
            "SCHEMA.TBL1(A,B),TLBL2(C)",
            ImmutableMap.of()
        );
    }

    /** */
    private void checkStatisticsCommand(
        String sql,
        String expOperator,
        String expTables
    ) throws Exception {
        checkStatisticsCommand(sql, expOperator, expTables, null);
    }

    /** */
    private void checkStatisticsCommand(
        String sql,
        String expOperator,
        String expTables,
        @Nullable Map<IgniteSqlStatisticsAnalyzeOptionEnum, String> expOptions
    ) throws Exception {
        SqlNode res = parse(sql);

        assertTrue(res instanceof IgniteSqlStatisticsCommand);

        assertEquals(expOperator, ((SqlCall)res).getOperator().getName());

        assertEquals(
            expTables,
            ((IgniteSqlStatisticsCommand)res).tables().stream().map(t -> {
                StringBuilder sb = new StringBuilder(t.name().toString());

                if (!F.isEmpty(t.columns()))
                    sb.append(t.columns().stream().map(SqlNode::toString).collect(Collectors.joining(",", "(", ")")));

                return sb.toString();
            }).collect(Collectors.joining(","))
        );

        if (res instanceof IgniteSqlStatisticsAnalyze && !F.isEmpty(expOptions)) {
            List<IgniteSqlStatisticsAnalyzeOption> options = ((IgniteSqlStatisticsAnalyze)res).options();

            assertThat(
                options.stream().collect(Collectors.toMap(IgniteSqlStatisticsAnalyzeOption::key,
                    v -> v.value().toString())),
                equalTo(expOptions)
            );
        }
    }

    /** */
    private static String stringValue(SqlLiteral literal) {
        return literal != null ? literal.getValueAs(String.class) : null;
    }

    /** */
    private void assertParserThrows(String sql, Class<? extends Exception> cls) {
        assertParserThrows(sql, cls, "");
    }

    /** */
    private void assertParserThrows(String sql, Class<? extends Exception> cls, String msg) {
        GridTestUtils.assertThrowsAnyCause(log, () -> parse(sql), cls, msg);
    }

    /**
     * Parses a given statement and returns a resulting AST.
     *
     * @param stmt Statement to parse.
     * @return An AST.
     */
    private static <T extends SqlNode> T parse(String stmt) throws SqlParseException {
        SqlParser parser = SqlParser.create(stmt, SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY));

        return (T)parser.parseStmt();
    }

    /**
     * Shortcut to verify that there is an option with a particular string value.
     *
     * @param optionList Option list from parsed AST.
     * @param option An option key of interest.
     * @param expVal Expected value.
     */
    private static void assertThatStringOptionPresent(List<SqlNode> optionList, String option, String expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
            "option " + option + "=" + expVal, IgniteSqlCreateTableOption.class,
            opt -> opt.key().name().equals(option) && opt.value() instanceof SqlIdentifier
                && Objects.equals(expVal, ((SqlIdentifier)opt.value()).names.get(0)))));
    }

    /**
     * Shortcut to verify that there is an option with a particular boolean value.
     *
     * @param optionList Option list from parsed AST.
     * @param option An option key of interest.
     * @param expVal Expected value.
     */
    private static void assertThatBooleanOptionPresent(List<SqlNode> optionList, String option, boolean expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
            "option" + option + "=" + expVal, IgniteSqlCreateTableOption.class,
            opt -> opt.key().name().equals(option) && opt.value() instanceof SqlLiteral
                && Objects.equals(expVal, ((SqlLiteral)opt.value()).booleanValue()))));
    }

    /**
     * Shortcut to verify that there is an option with a particular integer value.
     *
     * @param optionList Option list from parsed AST.
     * @param option An option key of interest.
     * @param expVal Expected value.
     */
    private static void assertThatIntegerOptionPresent(List<SqlNode> optionList, String option, int expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
            "option " + option + "=" + expVal, IgniteSqlCreateTableOption.class,
            opt -> opt.key().name().equals(option) && opt.value() instanceof SqlNumericLiteral
                && ((SqlNumericLiteral)opt.value()).isInteger()
                && Objects.equals(expVal, ((SqlLiteral)opt.value()).intValue(true)))));
    }

    /**
     * Matcher to verify name in the column declaration.
     *
     * @param name Expected name.
     * @return {@code true} in case name in the column declaration equals to the expected one.
     */
    private static <T extends SqlColumnDeclaration> Matcher<T> columnWithName(String name) {
        return new CustomMatcher<T>("column with name=" + name) {
            @Override public boolean matches(Object item) {
                return item instanceof SqlColumnDeclaration
                    && ((SqlColumnDeclaration)item).name.names.get(0).equals(name);
            }
        };
    }

    /**
     * Matcher to verify identifier.
     */
    private static <T extends SqlIdentifier> Matcher<T> identifierWithName(String name) {
        return new CustomMatcher<T>("identifier with name=" + name) {
            @Override public boolean matches(Object item) {
                return item instanceof SqlIdentifier
                    && ((SqlIdentifier)item).names.get(0).equals(name);
            }
        };
    }

    /**
     * Matcher to verify column declaration.
     */
    private static <T extends SqlColumnDeclaration> Matcher<T> columnDeclaration(String name, String type,
        boolean nullable) {
        return new CustomMatcher<T>("column declaration [name=" + name + ", type=" + type + ']') {
            @Override public boolean matches(Object item) {
                return item instanceof SqlColumnDeclaration
                    && ((SqlColumnDeclaration)item).name.names.get(0).equals(name)
                    && ((SqlColumnDeclaration)item).dataType.getTypeName().names.get(0).equals(type)
                    && ((SqlColumnDeclaration)item).dataType.getNullable() == nullable;
            }
        };
    }

    /**
     * Matcher to verify that an object of the expected type and matches the given predicat.
     *
     * @param desc Description for this matcher.
     * @param cls Expected class to verify the object is instance of.
     * @param pred Addition check that would be applied to the object.
     * @return {@code true} in case the object if instance of the given class and matches the predicat.
     */
    private static <T> Matcher<T> ofTypeMatching(String desc, Class<T> cls, Predicate<T> pred) {
        return new CustomMatcher<T>(desc) {
            @Override public boolean matches(Object item) {
                return item != null && cls.isAssignableFrom(item.getClass()) && pred.test((T)item);
            }
        };
    }

    /**
     * Matcher to verify name and direction of indexed column.
     *
     * @param name Expected name.
     * @param desc Descending order.
     * @return {@code true} in case name and order of the indexed column equal to expected values.
     */
    private static <T extends SqlColumnDeclaration> Matcher<T> indexedColumn(String name, boolean desc) {
        return new CustomMatcher<T>("column with name=" + name) {
            @Override public boolean matches(Object item) {
                SqlNode node = (SqlNode)item;

                if (desc) {
                    if (node.getKind() != SqlKind.DESCENDING)
                        return false;

                    SqlIdentifier ident = ((SqlIdentifier)((SqlCall)node).getOperandList().get(0));

                    return ident.names.get(0).equals(name);
                }
                else
                    return item instanceof SqlIdentifier && ((SqlIdentifier)node).names.get(0).equals(name);
            }
        };
    }
}
