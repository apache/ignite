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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.AFFINITY_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.ATOMICITY;
import static org.apache.ignite.internal.sql.SqlKeyword.BACKUPS;
import static org.apache.ignite.internal.sql.SqlKeyword.BIGINT;
import static org.apache.ignite.internal.sql.SqlKeyword.BIT;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOL;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOLEAN;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_GROUP;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_NAME;
import static org.apache.ignite.internal.sql.SqlKeyword.CHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.CHARACTER;
import static org.apache.ignite.internal.sql.SqlKeyword.DATA_REGION;
import static org.apache.ignite.internal.sql.SqlKeyword.DATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.DEC;
import static org.apache.ignite.internal.sql.SqlKeyword.DECIMAL;
import static org.apache.ignite.internal.sql.SqlKeyword.DOUBLE;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT4;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT8;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INT;
import static org.apache.ignite.internal.sql.SqlKeyword.INT2;
import static org.apache.ignite.internal.sql.SqlKeyword.INT4;
import static org.apache.ignite.internal.sql.SqlKeyword.INT8;
import static org.apache.ignite.internal.sql.SqlKeyword.INTEGER;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.LONGVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.MEDIUMINT;
import static org.apache.ignite.internal.sql.SqlKeyword.NCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NO_WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.NO_WRAP_VALUE;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMBER;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMERIC;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.PRECISION;
import static org.apache.ignite.internal.sql.SqlKeyword.PRIMARY;
import static org.apache.ignite.internal.sql.SqlKeyword.REAL;
import static org.apache.ignite.internal.sql.SqlKeyword.SIGNED;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLDATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLINT;
import static org.apache.ignite.internal.sql.SqlKeyword.TEMPLATE;
import static org.apache.ignite.internal.sql.SqlKeyword.TIME;
import static org.apache.ignite.internal.sql.SqlKeyword.TIMESTAMP;
import static org.apache.ignite.internal.sql.SqlKeyword.TINYINT;
import static org.apache.ignite.internal.sql.SqlKeyword.UUID;
import static org.apache.ignite.internal.sql.SqlKeyword.VAL_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR_CASESENSITIVE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_VALUE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRITE_SYNC_MODE;
import static org.apache.ignite.internal.sql.SqlKeyword.YEAR;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfNotExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseOptionalBoolValue;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatches;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * CREATE INDEX command.
 */
public class SqlCreateTableCommand implements SqlCommand {

    public static final int BACKUPS_NOT_SET = -1;

    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** IF NOT EXISTS flag. */
    private boolean ifNotExists;

    /** Columns. */
    @GridToStringInclude
    private Collection<SqlColumn> cols;

    /** Column names. */
    private Set<String> colNames;

    /** Primary key column names. */
    @GridToStringInclude
    private Collection<String> pkColNames;

    /** Cache name upon which new cache configuration for this table must be based. */
    private String templateName;

    /** Name of new cache associated with this table. */
    private String cacheName;

    /** Group to put new cache into. */
    private String cacheGrp;

    /** Atomicity mode for new cache. */
    private CacheAtomicityMode atomicityMode;

    /** Write sync mode. */
    private CacheWriteSynchronizationMode writeSyncMode;

    /** Backups number for new cache. */
    private Integer backups;

    /** Name of the column that represents affinity key. */
    private String affinityKey;

    /** Forcefully turn single column PK into an Object. */
    private Boolean wrapKey;

    /** Forcefully turn single column value into an Object. */
    private Boolean wrapVal;

    /** Name of cache key type. */
    private String keyTypeName;

    /** Name of cache value type. */
    private String valTypeName;

    /** Data region. */
    private String dataRegionName;

    /**
     * @return Cache name upon which new cache configuration for this table must be based.
     */
    public String templateName() {
        return templateName;
    }

    /**
     * @param templateName Cache name upon which new cache configuration for this table must be based.
     */
    public void templateName(String templateName) {
        this.templateName = templateName;
    }

    /**
     * @return Name of new cache associated with this table.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Name of new cache associated with this table.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Name of cache key type.
     */
    public String keyTypeName() {
        return keyTypeName;
    }

    /**
     * @param keyTypeName Name of cache key type.
     */
    public void keyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;
    }

    /**
     * @return Name of cache value type.
     */
    public String valueTypeName() {
        return valTypeName;
    }

    /**
     * @param valTypeName Name of cache value type.
     */
    public void valueTypeName(String valTypeName) {
        this.valTypeName = valTypeName;
    }

    /**
     * @return Group to put new cache into.
     */
    public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * @param cacheGrp Group to put new cache into.
     */
    public void cacheGroup(String cacheGrp) {
        this.cacheGrp = cacheGrp;
    }

    /**
     * @return Atomicity mode for new cache.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Atomicity mode for new cache.
     */
    public void atomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     * @return Write sync mode for new cache.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSyncMode;
    }

    /**
     * @param writeSyncMode Write sync mode for new cache.
     */
    public void writeSynchronizationMode(CacheWriteSynchronizationMode writeSyncMode) {
        this.writeSyncMode = writeSyncMode;
    }

    /**
     * @return Backups number for new cache.
     */
    public int backups() {
        return backups == null ? BACKUPS_NOT_SET : backups;
    }

    /**
     * @param backups Backups number for new cache.
     */
    public void backups(Integer backups) {
        this.backups = backups;
    }

    /**
     * @return Name of the column that represents affinity key.
     */
    public String affinityKey() {
        return affinityKey;
    }

    /**
     * @param affinityKey Name of the column that represents affinity key.
     */
    public void affinityKey(String affinityKey) {
        this.affinityKey = affinityKey;
    }

    /**
     * @return Forcefully turn single column PK into an Object.
     */
    public Boolean wrapKey() {
        return wrapKey;
    }

    /**
     * @param wrapKey Forcefully turn single column PK into an Object.
     */
    public void wrapKey(boolean wrapKey) {
        this.wrapKey = wrapKey;
    }

    /**
     * @return Forcefully turn single column value into an Object.
     */
    public Boolean wrapValue() {
        return wrapVal;
    }

    /**
     * @return Data region name.
     */
    public String dataRegionName() {
        return dataRegionName;
    }

    /**
     * @param dataRegionName Data region name.
     */
    public void dataRegionName(String dataRegionName) {
        this.dataRegionName = dataRegionName;
    }

    /**
     * @param wrapVal Forcefully turn single column value into an Object..
     */
    public void wrapValue(boolean wrapVal) {
        this.wrapVal = wrapVal;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return IF NOT EXISTS flag.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @return Columns.
     */
    public Collection<SqlColumn> columns() {
        return cols != null ? cols : Collections.<SqlColumn>emptySet();
    }

    /**
     * @return PK column names.
     */
    public Collection<String> primaryKeyColumnNames() {
        return pkColNames != null ? pkColNames : Collections.<String>emptySet();
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifNotExists = parseIfNotExists(lex);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex, IF);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnAndConstraintList(lex);

        parseParametersSection(lex);

        return this;
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumnAndConstraintList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        while (true) {
            parseColumnOrConstraint(lex);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumnOrConstraint(SqlLexer lex) {
        SqlLexerToken next = lex.lookAhead();

        if (next.tokenType() == SqlLexerTokenType.EOF)
            throw errorUnexpectedToken(lex, PRIMARY, "[column definition]");

        if (matchesKeyword(next, PRIMARY))
            parsePrimaryKeyConstraint(lex);
        else
            parseColumn(lex);
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);

        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            SqlColumn col = null;

            switch (lex.token()) {
                case BIT:
                case BOOL:
                case BOOLEAN:
                    col = new SqlColumn(name, SqlColumnType.BOOLEAN);

                    break;

                case TINYINT:
                    col = new SqlColumn(name, SqlColumnType.BYTE);

                    break;

                case INT2:
                case SMALLINT:
                case YEAR:
                    col = new SqlColumn(name, SqlColumnType.SHORT);

                    break;

                case INT:
                case INT4:
                case INTEGER:
                case MEDIUMINT:
                case SIGNED:
                    col = new SqlColumn(name, SqlColumnType.INT);

                    break;

                case BIGINT:
                case INT8:
                    col = new SqlColumn(name, SqlColumnType.LONG);

                    break;

                case FLOAT4:
                case REAL:
                    col = new SqlColumn(name, SqlColumnType.FLOAT);

                    break;

                case DOUBLE: {
                    SqlLexerToken next = lex.lookAhead();

                    if (matchesKeyword(next, PRECISION))
                        lex.shift();

                    col = new SqlColumn(name, SqlColumnType.DOUBLE);

                    break;
                }

                case FLOAT:
                case FLOAT8:
                    col = new SqlColumn(name, SqlColumnType.DOUBLE);

                    break;

                case DEC:
                case DECIMAL:
                case NUMBER:
                case NUMERIC: {
                    skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

                    int scale = parseInt(lex);
                    int precision = 0;

                    if (!skipCommaOrRightParenthesis(lex)) {
                        precision = parseInt(lex);

                        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
                    }

                    col = new SqlColumn(name, SqlColumnType.DECIMAL, scale, precision);

                    break;
                }

                case CHAR:
                case CHARACTER:
                case NCHAR: {
                    int precision = parseStringPrecision(lex);

                    col = new SqlColumn(name, SqlColumnType.CHAR, precision);

                    break;
                }

                case LONGVARCHAR:
                case NVARCHAR:
                case NVARCHAR2:
                case VARCHAR:
                case VARCHAR2:
                case VARCHAR_CASESENSITIVE: {
                    int precision = parseStringPrecision(lex);

                    col = new SqlColumn(name, SqlColumnType.VARCHAR, precision);

                    break;
                }

                case DATE:
                    col = new SqlColumn(name, SqlColumnType.DATE);

                    break;

                case TIME:
                    col = new SqlColumn(name, SqlColumnType.TIME);

                    break;

                case DATETIME:
                case SMALLDATETIME:
                case TIMESTAMP:
                    col = new SqlColumn(name, SqlColumnType.TIMESTAMP);

                    break;

                case UUID:
                    col = new SqlColumn(name, SqlColumnType.UUID);

                    break;
            }

            if (col != null) {
                addColumn(lex, col);

                SqlLexerToken next = lex.lookAhead();

                if (matchesKeyword(next, PRIMARY)) {
                    if (pkColNames != null)
                        throw error(lex, "PRIMARY KEY is already defined.");

                    pkColNames = new HashSet<>();

                    pkColNames.add(col.name());

                    lex.shift();

                    skipIfMatchesKeyword(lex, KEY);
                }

                return;
            }
        }

        throw errorUnexpectedToken(lex, "[column_type]");
    }

    /**
     * @param col Column.
     */
    private void addColumn(SqlLexer lex, SqlColumn col) {
        if (cols == null) {
            cols = new LinkedList<>();
            colNames = new HashSet<>();
        }

        if (!colNames.add(col.name()))
            throw error(lex, "Column already defined: " + col.name());

        cols.add(col);
    }

    /**
     * @param lex Lexer.
     */
    private void parsePrimaryKeyConstraint(SqlLexer lex) {
        if (pkColNames != null)
            throw error(lex, "PRIMARY KEY is already defined.");

        pkColNames = new HashSet<>();

        skipIfMatchesKeyword(lex, PRIMARY);
        skipIfMatchesKeyword(lex, KEY);

        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        while (true) {
            String pkColName = parseIdentifier(lex);

            if (!pkColNames.add(pkColName))
                throw error(lex, "Duplicate PK column name: " + pkColName);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
    }

    /**
     * Parse precision for CHAR and VARCHAR types.
     *
     * @param lex Lexer.
     * @return Precision.
     */
    private static int parseStringPrecision(SqlLexer lex) {
        SqlLexerToken next = lex.lookAhead();

        int res = Integer.MAX_VALUE;

        if (next.tokenType() == SqlLexerTokenType.PARENTHESIS_LEFT) {
            lex.shift();

            res = parseInt(lex);

            skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
        }

        return res;
    }

    /**
     * @param lex Lexer.
     */
    private void parseParametersSection(SqlLexer lex) {
        while (lex.tokenType() != SqlLexerTokenType.EOF) {
            if (!parseTemplate(lex) &&
                !parseBackups(lex) &&
                !parseAtomicity(lex) &&
                !parseWriteSyncMode(lex) &&
                !parseCacheGroup(lex) &&
                !parseAffinityKey(lex) &&
                !parseCacheName(lex) &&
                !parseDataRegion(lex) &&
                !parseKeyType(lex) &&
                !parseValueType(lex) &&
                !parseWrapKey(lex) &&
                !parseWrapValue(lex))
                throw errorUnexpectedToken(lex.currentToken());
        }
    }

    /** FIXME */
    private boolean parseTemplate(SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, TEMPLATE, "template name" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {

                    templateName(value);
                }
            }
        );
    }
    /** FIXME */
    private boolean parseBackups(final SqlLexer lex) {
        return SqlParserUtils.parseOptionalIntValue(lex, BACKUPS,

            new SqlParserUtils.Setter<Integer>() {

                @Override public void apply(Integer backupsNum, boolean isQuoted) {

                    if (backupsNum == null)
                        return;

                    if (backupsNum < 0)
                        throw error(lex.currentToken(), "Number of backups should be positive [val=" + backupsNum + "]");

                    backups(backupsNum);
                }
            });
    }

    /** FIXME */
    private boolean parseAtomicity(SqlLexer lex) {
        return SqlParserUtils.parseOptionalEnumValue(lex, ATOMICITY,

            CacheAtomicityMode.class, new SqlParserUtils.Setter<CacheAtomicityMode>() {

                @Override public void apply(CacheAtomicityMode mode, boolean isQuoted) {

                    atomicityMode(mode);
                }
            });
    }

    /** FIXME */
    private boolean parseWriteSyncMode(SqlLexer lex) {
        return SqlParserUtils.parseOptionalEnumValue(lex, WRITE_SYNC_MODE,

            CacheWriteSynchronizationMode.class, new SqlParserUtils.Setter<CacheWriteSynchronizationMode>() {

                @Override public void apply(CacheWriteSynchronizationMode mode, boolean isQuoted) {

                    writeSynchronizationMode(mode);
                }
            });
    }

    /** FIXME */
    private boolean parseCacheGroup(SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, CACHE_GROUP, "cache group name" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {

                    cacheGroup(value);
                }
            }
        );
    }

    /** FIXME */
    private boolean parseAffinityKey(final SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, AFFINITY_KEY, "affinity key" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {
                    value = value.trim();

                    SqlColumn affCol = null;

                    for (SqlColumn col : columns()) {
                        if (value.equalsIgnoreCase(col.name())) {
                            if (affCol != null)
                                throw error(lex.currentToken(), "Ambiguous affinity column name, use single quotes" +
                                    "for case sensitivity");

                            affCol = col;
                        }
                    }

                    if (affCol == null)
                        throw error(lex.currentToken(), "Affinity key column with given name not found");

                    if (!pkColNames.contains(affCol.name()))
                        throw error(lex.currentToken(), "Affinity key column should be a primary key column");

                    affinityKey(affCol.name());
                }
            }
        );
    }

    /** FIXME */
    private boolean parseCacheName(SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, CACHE_NAME, "cache name" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {

                    cacheName(value);
                }
            }
        );
    }

    /** FIXME */
    private boolean parseDataRegion(SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, DATA_REGION, "data region" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {

                    dataRegionName(value);
                }
            }
        );
    }

    /** FIXME */
    private boolean parseKeyType(SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, KEY_TYPE, "key type" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {

                    keyTypeName(value);
                }
            }
        );
    }

    /** FIXME */
    private boolean parseValueType(SqlLexer lex) {
        return SqlParserUtils.parseOptionalStringParam(lex, VAL_TYPE, "value type" ,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String value, boolean isQuoted) {

                    valueTypeName(value);
                }
            }
        );
    }

    /** FIXME */
    private boolean parseWrapKey(SqlLexer lex) {
        return parseOptionalBoolValue(lex, WRAP_KEY, NO_WRAP_KEY, new SqlParserUtils.Setter<Boolean>() {

                @Override public void apply(Boolean value, boolean isQuoted) {

                    wrapKey(value);
                }
            });
    }

    /** FIXME */
    private boolean parseWrapValue(SqlLexer lex) {
        return parseOptionalBoolValue(lex, WRAP_VALUE, NO_WRAP_VALUE, new SqlParserUtils.Setter<Boolean>() {

            @Override public void apply(Boolean value, boolean isQuoted) {

                wrapValue(value);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateTableCommand.class, this);
    }

    /**
     * Check that param with mandatory value has it specified.
     * @param name Param name.
     * @param val Param value to check.
     */
    private static void ensureNonEmptyVal(SqlLexerToken token, String name, String val) {
        if (F.isEmpty(val))
            throw error(token, "Parameter value cannot be empty: [name=\"" + name + "\"]");
    }
}
