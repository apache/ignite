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
import org.apache.ignite.internal.sql.SqlEnumParserUtils;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlEnumParserUtils.tryParseBoolean;
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
import static org.apache.ignite.internal.sql.SqlKeyword.WRITE_SYNCHRONIZATION_MODE;
import static org.apache.ignite.internal.sql.SqlKeyword.YEAR;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfNotExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipToken;

/**
 * CREATE INDEX command.
 */
public class SqlCreateTableCommand implements SqlCommand {

    /** Schema name. */
    @GridToStringInclude
    private String schemaName;

    /** Table name. */
    @GridToStringInclude
    private String tblName;

    /** IF NOT EXISTS flag. */
    @GridToStringInclude
    private boolean ifNotExists;

    /** Columns. */
    @GridToStringInclude
    private Map<String, SqlColumn> cols;

    /** Primary key column names. */
    @GridToStringInclude
    private Set<String> pkColNames;

    /** Cache name upon which new cache configuration for this table must be based. */
    @GridToStringInclude
    private String templateName;

    /** Name of new cache associated with this table. */
    @GridToStringInclude
    private String cacheName;

    /** Group to put new cache into. */
    @GridToStringInclude
    private String cacheGrp;

    /** Atomicity mode for new cache. */
    @GridToStringInclude
    private CacheAtomicityMode atomicityMode;

    /** Write sync mode. */
    @GridToStringInclude
    private CacheWriteSynchronizationMode writeSyncMode;

    /** Backups number for new cache. */
    @GridToStringInclude
    private Integer backups;

    /** Name of the column that represents affinity key. */
    @GridToStringInclude
    private String affinityKey;

    /** Forcefully turn single column PK into an Object. */
    @GridToStringInclude
    private Boolean wrapKey;

    /** Forcefully turn single column value into an Object. */
    @GridToStringInclude
    private Boolean wrapVal;

    /** Name of cache key type. */
    @GridToStringInclude
    private String keyTypeName;

    /** Name of cache value type. */
    @GridToStringInclude
    private String valTypeName;

    /** Data region. */
    @GridToStringInclude
    private String dataRegionName;

    /** FIXME */
    private Set<String> parsedParams = new HashSet<>();

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
    public Integer backups() {
        return backups;
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
    public Map<String, SqlColumn> columns() {
        return cols != null ? cols : Collections.<String, SqlColumn>emptyMap();
    }

    /**
     * @return PK column names.
     */
    public Set<String> primaryKeyColumnNames() {
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

            if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.PARENTHESIS_RIGHT)
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

        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.KEYWORD) {
            SqlColumn col = null;

            switch (lex.token()) {
                case BIT:
                case BOOL:
                case BOOLEAN:
                    col = new SqlColumn(name, SqlColumnType.BOOLEAN, 0, 0, true);

                    break;

                case TINYINT:
                    col = new SqlColumn(name, SqlColumnType.BYTE, 0, 0, false);

                    break;

                case INT2:
                case SMALLINT:
                case YEAR:
                    col = new SqlColumn(name, SqlColumnType.SHORT, 0, 0, false);

                    break;

                case INT:
                case INT4:
                case INTEGER:
                case MEDIUMINT:
                case SIGNED:
                    col = new SqlColumn(name, SqlColumnType.INT, 0, 0, false);

                    break;

                case BIGINT:
                case INT8:
                    col = new SqlColumn(name, SqlColumnType.LONG, 0, 0, false);

                    break;

                case FLOAT4:
                case REAL:
                    col = new SqlColumn(name, SqlColumnType.FLOAT, 0, 0, false);

                    break;

                case DOUBLE: {
                    SqlLexerToken next = lex.lookAhead();

                    if (matchesKeyword(next, PRECISION))
                        lex.shift();

                    col = new SqlColumn(name, SqlColumnType.DOUBLE, 0, 0, false);

                    break;
                }

                case FLOAT:
                case FLOAT8:
                    col = new SqlColumn(name, SqlColumnType.DOUBLE, 0, 0, false);

                    break;

                case DEC:
                case DECIMAL:
                case NUMBER:
                case NUMERIC: {
                    skipToken(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

                    int scale = parseInt(lex);
                    int precision = 0;

                    if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.COMMA) {
                        precision = parseInt(lex);

                        skipToken(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
                    }

                    col = new SqlColumn(name, SqlColumnType.DECIMAL, scale, precision, true);

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
                    col = new SqlColumn(name, SqlColumnType.DATE, 0, 0, true);

                    break;

                case TIME:
                    col = new SqlColumn(name, SqlColumnType.TIME, 0, 0, true);

                    break;

                case DATETIME:
                case SMALLDATETIME:
                case TIMESTAMP:
                    col = new SqlColumn(name, SqlColumnType.TIMESTAMP, 0, 0, true);

                    break;

                case UUID:
                    col = new SqlColumn(name, SqlColumnType.UUID, 0, 0, true);

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

                    skipKeyword(lex, KEY);
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
        if (cols == null)
            cols = new LinkedHashMap<>();

        if (cols.containsKey(col.name()))
            throw error(lex, "Column already defined: " + col.name());

        cols.put(col.name(), col);
    }

    /**
     * @param lex Lexer.
     */
    private void parsePrimaryKeyConstraint(SqlLexer lex) {
        if (pkColNames != null)
            throw error(lex, "PRIMARY KEY is already defined.");

        pkColNames = new HashSet<>();

        skipKeyword(lex, PRIMARY);
        skipKeyword(lex, KEY);

        skipToken(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        while (true) {
            String pkColName = parseIdentifier(lex);

            if (!pkColNames.add(pkColName))
                throw error(lex, "Duplicate PK column name: " + pkColName);

            if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.PARENTHESIS_RIGHT)
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

            skipToken(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
        }

        return res;
    }

    /**
     * @param lex Lexer.
     */
    private void parseParametersSection(SqlLexer lex) {

        while (lex.lookAhead().tokenType() != SqlLexerTokenType.EOF) {

            if (!tryParseTemplate(lex) &&
                !tryParseBackups(lex) &&
                !tryParseAtomicity(lex) &&
                !tryParseWriteSyncMode(lex) &&
                !tryParseCacheGroup(lex) &&
                !tryParseAffinityKey(lex) &&
                !tryParseCacheName(lex) &&
                !tryParseDataRegion(lex) &&
                !tryParseKeyType(lex) &&
                !tryParseValueType(lex) &&
                !tryParseWrapKey(lex) &&
                !tryParseWrapValue(lex))

                throw errorUnexpectedToken(lex.lookAhead());
        }
    }

    /** FIXME */
    private boolean tryParseTemplate(final SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, TEMPLATE, "template name" , parsedParams, false, true,

            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    templateName(isDflt ? null : val);
                }
            }
        );
    }
    /** FIXME */
    private boolean tryParseBackups(final SqlLexer lex) {

        return SqlParserUtils.tryParseIntParam(lex, BACKUPS, parsedParams, true,

            new SqlParserUtils.Setter<Integer>() {

                @Override public void apply(Integer backupsNum, boolean isDflt, boolean isQuoted) {

                    if (isDflt)
                        backups(null);
                    else {
                        if (backupsNum < 0)
                            throw error(lex.currentToken(), "Number of backups should be positive [val=" + backupsNum + "]");

                        backups(backupsNum);
                    }
                }
            });
    }

    /** FIXME */
    private boolean tryParseAtomicity(final SqlLexer lex) {

        return SqlEnumParserUtils.tryParseEnumParam(lex, ATOMICITY, CacheAtomicityMode.class, parsedParams, true,

            new SqlParserUtils.Setter<CacheAtomicityMode>() {

                @Override public void apply(CacheAtomicityMode mode, boolean isDflt, boolean isQuoted) {

                    atomicityMode(isDflt ? null : mode);
                }
            });
    }

    /** FIXME */
    private boolean tryParseWriteSyncMode(final SqlLexer lex) {

        return SqlEnumParserUtils.tryParseEnumParam(lex, WRITE_SYNCHRONIZATION_MODE, CacheWriteSynchronizationMode.class,

            parsedParams, true, new SqlParserUtils.Setter<CacheWriteSynchronizationMode>() {

                @Override public void apply(CacheWriteSynchronizationMode mode, boolean isDflt, boolean isQuoted) {

                    writeSynchronizationMode(isDflt ? null : mode);
                }
            });
    }

    /** FIXME */
    private boolean tryParseCacheGroup(final SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, CACHE_GROUP, "cache group name" , parsedParams,

            false, false, new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    assert !isDflt;

                    cacheGroup(val);
                }
            }
        );
    }

    /** FIXME */
    private boolean tryParseAffinityKey(final SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, AFFINITY_KEY, "affinity key", parsedParams,

            false, false, new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    assert !isDflt;

                    SqlColumn affCol = null;

                    for (SqlColumn col : columns().values()) {
                        if (col.name().equalsIgnoreCase(val)) {
                            if (affCol != null)
                                throw error(lex.currentToken(),
                                    "Ambiguous affinity column name, use single quotes for case sensitivity");

                            affCol = col;
                        }
                    }

                    if (affCol == null)
                        throw error(lex.currentToken(), "Affinity key column with given name not found");

                    if (!pkColNames.contains(affCol.name()))
                        throw error(lex.currentToken(), "Affinity key column must be one of key columns: " + affCol.name());

                    affinityKey(affCol.name());
                }
            }
        );
    }

    /** FIXME */
    private boolean tryParseCacheName(SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, CACHE_NAME, "cache name" , parsedParams,
            false, false,
            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    assert !isDflt;

                    cacheName(val);
                }
            }
        );
    }

    /** FIXME */
    private boolean tryParseDataRegion(final SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, DATA_REGION, "data region" , parsedParams,
            false, true,
            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    dataRegionName(isDflt ? null : val);
                }
            }
        );
    }

    /** FIXME */
    private boolean tryParseKeyType(final SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, KEY_TYPE, "key type" , parsedParams,
            true, false,
            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    assert !isDflt;

                    keyTypeName(val);
                }
            }
        );
    }

    /** FIXME */
    private boolean tryParseValueType(final SqlLexer lex) {

        return SqlParserUtils.tryParseStringParam(lex, VAL_TYPE, "value type" , parsedParams,
            true, false,
            new SqlParserUtils.Setter<String>() {

                @Override public void apply(String val, boolean isDflt, boolean isQuoted) {

                    assert !isDflt;

                    valueTypeName(val);
                }
            }
        );
    }

    /** FIXME */
    private boolean tryParseWrapKey(final SqlLexer lex) {

        return tryParseBoolean(lex, WRAP_KEY, NO_WRAP_KEY, parsedParams,true,
            new SqlParserUtils.Setter<Boolean>() {

                @Override public void apply(Boolean val, boolean isDflt, boolean isQuoted) {

                    wrapKey(val);
                }
            });
    }

    /** FIXME */
    private boolean tryParseWrapValue(final SqlLexer lex) {

        return tryParseBoolean(lex, WRAP_VALUE, NO_WRAP_VALUE, parsedParams, true,
            new SqlParserUtils.Setter<Boolean>() {

            @Override public void apply(Boolean val, boolean isDflt, boolean isQuoted) {

                wrapValue(val);
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
    private static void ensureNonEmptyVal(SqlLexerToken tok, String name, String val) {
        if (F.isEmpty(val))
            throw error(tok, "Parameter value cannot be empty: [name=\"" + name + "\"]");
    }
}
