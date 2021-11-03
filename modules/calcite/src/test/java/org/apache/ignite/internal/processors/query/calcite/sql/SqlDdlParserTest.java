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

import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the DDL command.
 */
public class SqlDdlParserTest {
    /**
     * Very simple case where only table name and a few columns are presented.
     */
    @Test
    public void createTableSimpleCase() throws SqlParseException {
        String query = "create table my_table(id int, val varchar)";
        
        SqlNode node = parse(query);
        
        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
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
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("My_Table")));
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
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(true));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));
    }
    
    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is a shortcut within a column definition.
     */
    @Test
    public void createTableWithPkCase1() throws SqlParseException {
        String query = "create table my_table(id int primary key, val varchar)";
        
        SqlNode node = parse(query);
        
        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }
    
    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is set explicitly and has no name.
     */
    @Test
    public void createTableWithPkCase2() throws SqlParseException {
        String query = "create table my_table(id int, val varchar, primary key(id))";
        
        SqlNode node = parse(query);
        
        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint without name containing column \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }
    
    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is set explicitly and has a name.
     */
    @Test
    public void createTableWithPkCase3() throws SqlParseException {
        String query = "create table my_table(id int, val varchar, constraint pk_key primary key(id))";
        
        SqlNode node = parse(query);
        
        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"PK_KEY\" containing column \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && "PK_KEY".equals(((SqlIdentifier) constraint.getOperandList().get(0)).names.get(0))
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }
    
    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint consists of several columns.
     */
    @Test
    public void createTableWithPkCase4() throws SqlParseException {
        String query = "create table my_table(id1 int, id2 int, val varchar, primary key(id1, id2))";
        
        SqlNode node = parse(query);
        
        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
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
        String query = "create table my_table(id int) with"
                + " template=\"my_template\","
                + " backups=2,"
                + " affinity_key=my_aff,"
                + " atomicity=atomic,"
                + " write_synchronization_mode=transactional,"
                + " cache_group=my_cache_group,"
                + " cache_name=my_cache_name,"
                + " data_region=my_data_region,"
                + " key_type=my_key_type,"
                + " value_type=my_value_type,"
                + " encrypted=true";
        
        SqlNode node = parse(query);
        
        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
        
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
     * Parses a given statement and returns a resulting AST.
     *
     * @param stmt Statement to parse.
     * @return An AST.
     */
    private static SqlNode parse(String stmt) throws SqlParseException {
        SqlParser parser = SqlParser.create(stmt, SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY));
        
        return parser.parseStmt();
    }
    
    /**
     * Shortcut to verify that there is an option with a particular string value.
     *
     * @param optionList Option list from parsed AST.
     * @param option     An option key of interest.
     * @param expVal     Expected value.
     */
    private static void assertThatStringOptionPresent(List<SqlNode> optionList, String option, String expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
                "option " + option + "=" + expVal, IgniteSqlCreateTableOption.class,
                opt -> opt.key().name().equals(option) && opt.value() instanceof SqlIdentifier
                        && Objects.equals(expVal, ((SqlIdentifier) opt.value()).names.get(0)))));
    }
    
    /**
     * Shortcut to verify that there is an option with a particular boolean value.
     *
     * @param optionList Option list from parsed AST.
     * @param option     An option key of interest.
     * @param expVal     Expected value.
     */
    private static void assertThatBooleanOptionPresent(List<SqlNode> optionList, String option, boolean expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
                "option" + option + "=" + expVal, IgniteSqlCreateTableOption.class,
                opt -> opt.key().name().equals(option) && opt.value() instanceof SqlLiteral
                        && Objects.equals(expVal, ((SqlLiteral) opt.value()).booleanValue()))));
    }
    
    /**
     * Shortcut to verify that there is an option with a particular integer value.
     *
     * @param optionList Option list from parsed AST.
     * @param option     An option key of interest.
     * @param expVal     Expected value.
     */
    private static void assertThatIntegerOptionPresent(List<SqlNode> optionList, String option, int expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
                "option" + option + "=" + expVal, IgniteSqlCreateTableOption.class,
                opt -> opt.key().name().equals(option) && opt.value() instanceof SqlNumericLiteral
                        && ((SqlNumericLiteral) opt.value()).isInteger()
                        && Objects.equals(expVal, ((SqlLiteral) opt.value()).intValue(true)))));
    }
    
    /**
     * Matcher to verify name in the column declaration.
     *
     * @param name Expected name.
     * @return {@code true} in case name in the column declaration equals to the expected one.
     */
    private static <T extends SqlColumnDeclaration> Matcher<T> columnWithName(String name) {
        return new CustomMatcher<T>("column with name=" + name) {
            @Override
            public boolean matches(Object item) {
                return item instanceof SqlColumnDeclaration
                        && ((SqlColumnDeclaration) item).name.names.get(0).equals(name);
            }
        };
    }
    
    /**
     * Matcher to verify that an object of the expected type and matches the given predicat.
     *
     * @param desc Description for this matcher.
     * @param cls  Expected class to verify the object is instance of.
     * @param pred Addition check that would be applied to the object.
     * @return {@code true} in case the object if instance of the given class and matches the predicat.
     */
    private static <T> Matcher<T> ofTypeMatching(String desc, Class<T> cls, Predicate<T> pred) {
        return new CustomMatcher<T>(desc) {
            @Override
            public boolean matches(Object item) {
                return item != null && cls.isAssignableFrom(item.getClass()) && pred.test((T) item);
            }
        };
    }
}
