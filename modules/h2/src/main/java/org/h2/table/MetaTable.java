/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.h2.command.Command;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Domain;
import org.h2.engine.FunctionAlias;
import org.h2.engine.FunctionAlias.JavaMethod;
import org.h2.engine.QueryStatisticsData;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MetaIndex;
import org.h2.message.DbException;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.MVTableEngine.Store;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.InDoubtTransaction;
import org.h2.store.PageStore;
import org.h2.tools.Csv;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringIgnoreCase;

/**
 * This class is responsible to build the database meta data pseudo tables.
 */
public class MetaTable extends Table {

    /**
     * The approximate number of rows of a meta table.
     */
    public static final long ROW_COUNT_APPROXIMATION = 1000;

    private static final String CHARACTER_SET_NAME = "Unicode";

    private static final int TABLES = 0;
    private static final int COLUMNS = 1;
    private static final int INDEXES = 2;
    private static final int TABLE_TYPES = 3;
    private static final int TYPE_INFO = 4;
    private static final int CATALOGS = 5;
    private static final int SETTINGS = 6;
    private static final int HELP = 7;
    private static final int SEQUENCES = 8;
    private static final int USERS = 9;
    private static final int ROLES = 10;
    private static final int RIGHTS = 11;
    private static final int FUNCTION_ALIASES = 12;
    private static final int SCHEMATA = 13;
    private static final int TABLE_PRIVILEGES = 14;
    private static final int COLUMN_PRIVILEGES = 15;
    private static final int COLLATIONS = 16;
    private static final int VIEWS = 17;
    private static final int IN_DOUBT = 18;
    private static final int CROSS_REFERENCES = 19;
    private static final int CONSTRAINTS = 20;
    private static final int FUNCTION_COLUMNS = 21;
    private static final int CONSTANTS = 22;
    private static final int DOMAINS = 23;
    private static final int TRIGGERS = 24;
    private static final int SESSIONS = 25;
    private static final int LOCKS = 26;
    private static final int SESSION_STATE = 27;
    private static final int QUERY_STATISTICS = 28;
    private static final int SYNONYMS = 29;
    private static final int TABLE_CONSTRAINTS = 30;
    private static final int KEY_COLUMN_USAGE = 31;
    private static final int REFERENTIAL_CONSTRAINTS = 32;
    private static final int META_TABLE_TYPE_COUNT = REFERENTIAL_CONSTRAINTS + 1;

    private final int type;
    private final int indexColumn;
    private final MetaIndex metaIndex;

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    public MetaTable(Schema schema, int id, int type) {
        // tableName will be set later
        super(schema, id, null, true, true);
        this.type = type;
        Column[] cols;
        String indexColumnName = null;
        switch (type) {
        case TABLES:
            setMetaTableName("TABLES");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    // extensions
                    "STORAGE_TYPE",
                    "SQL",
                    "REMARKS",
                    "LAST_MODIFICATION BIGINT",
                    "ID INT",
                    "TYPE_NAME",
                    "TABLE_CLASS",
                    "ROW_COUNT_ESTIMATE BIGINT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMNS:
            setMetaTableName("COLUMNS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE INT",
                    "CHARACTER_MAXIMUM_LENGTH INT",
                    "CHARACTER_OCTET_LENGTH INT",
                    "NUMERIC_PRECISION INT",
                    "NUMERIC_PRECISION_RADIX INT",
                    "NUMERIC_SCALE INT",
                    "DATETIME_PRECISION INT",
                    "INTERVAL_TYPE",
                    "INTERVAL_PRECISION INT",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    // extensions
                    "TYPE_NAME",
                    "NULLABLE INT",
                    "IS_COMPUTED BIT",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "SEQUENCE_NAME",
                    "REMARKS",
                    "SOURCE_DATA_TYPE SMALLINT",
                    "COLUMN_TYPE",
                    "COLUMN_ON_UPDATE",
                    "IS_VISIBLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case INDEXES:
            setMetaTableName("INDEXES");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "NON_UNIQUE BIT",
                    "INDEX_NAME",
                    "ORDINAL_POSITION SMALLINT",
                    "COLUMN_NAME",
                    "CARDINALITY INT",
                    "PRIMARY_KEY BIT",
                    "INDEX_TYPE_NAME",
                    "IS_GENERATED BIT",
                    "INDEX_TYPE SMALLINT",
                    "ASC_OR_DESC",
                    "PAGES INT",
                    "FILTER_CONDITION",
                    "REMARKS",
                    "SQL",
                    "ID INT",
                    "SORT_TYPE INT",
                    "CONSTRAINT_NAME",
                    "INDEX_CLASS",
                    "AFFINITY BIT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case TABLE_TYPES:
            setMetaTableName("TABLE_TYPES");
            cols = createColumns("TYPE");
            break;
        case TYPE_INFO:
            setMetaTableName("TYPE_INFO");
            cols = createColumns(
                "TYPE_NAME",
                "DATA_TYPE INT",
                "PRECISION INT",
                "PREFIX",
                "SUFFIX",
                "PARAMS",
                "AUTO_INCREMENT BIT",
                "MINIMUM_SCALE SMALLINT",
                "MAXIMUM_SCALE SMALLINT",
                "RADIX INT",
                "POS INT",
                "CASE_SENSITIVE BIT",
                "NULLABLE SMALLINT",
                "SEARCHABLE SMALLINT"
            );
            break;
        case CATALOGS:
            setMetaTableName("CATALOGS");
            cols = createColumns("CATALOG_NAME");
            break;
        case SETTINGS:
            setMetaTableName("SETTINGS");
            cols = createColumns("NAME", "VALUE");
            break;
        case HELP:
            setMetaTableName("HELP");
            cols = createColumns(
                    "ID INT",
                    "SECTION",
                    "TOPIC",
                    "SYNTAX",
                    "TEXT"
            );
            break;
        case SEQUENCES:
            setMetaTableName("SEQUENCES");
            cols = createColumns(
                    "SEQUENCE_CATALOG",
                    "SEQUENCE_SCHEMA",
                    "SEQUENCE_NAME",
                    "CURRENT_VALUE BIGINT",
                    "INCREMENT BIGINT",
                    "IS_GENERATED BIT",
                    "REMARKS",
                    "CACHE BIGINT",
                    "MIN_VALUE BIGINT",
                    "MAX_VALUE BIGINT",
                    "IS_CYCLE BIT",
                    "ID INT"
            );
            break;
        case USERS:
            setMetaTableName("USERS");
            cols = createColumns(
                    "NAME",
                    "ADMIN",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case ROLES:
            setMetaTableName("ROLES");
            cols = createColumns(
                    "NAME",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case RIGHTS:
            setMetaTableName("RIGHTS");
            cols = createColumns(
                    "GRANTEE",
                    "GRANTEETYPE",
                    "GRANTEDROLE",
                    "RIGHTS",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case FUNCTION_ALIASES:
            setMetaTableName("FUNCTION_ALIASES");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "COLUMN_COUNT INT",
                    "RETURNS_RESULT SMALLINT",
                    "REMARKS",
                    "ID INT",
                    "SOURCE"
            );
            break;
        case FUNCTION_COLUMNS:
            setMetaTableName("FUNCTION_COLUMNS");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "COLUMN_COUNT INT",
                    "POS INT",
                    "COLUMN_NAME",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "PRECISION INT",
                    "SCALE SMALLINT",
                    "RADIX SMALLINT",
                    "NULLABLE SMALLINT",
                    "COLUMN_TYPE SMALLINT",
                    "REMARKS",
                    "COLUMN_DEFAULT"
            );
            break;
        case SCHEMATA:
            setMetaTableName("SCHEMATA");
            cols = createColumns(
                    "CATALOG_NAME",
                    "SCHEMA_NAME",
                    "SCHEMA_OWNER",
                    "DEFAULT_CHARACTER_SET_NAME",
                    "DEFAULT_COLLATION_NAME",
                    "IS_DEFAULT BIT",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case TABLE_PRIVILEGES:
            setMetaTableName("TABLE_PRIVILEGES");
            cols = createColumns(
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMN_PRIVILEGES:
            setMetaTableName("COLUMN_PRIVILEGES");
            cols = createColumns(
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLLATIONS:
            setMetaTableName("COLLATIONS");
            cols = createColumns(
                    "NAME",
                    "KEY"
            );
            break;
        case VIEWS:
            setMetaTableName("VIEWS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "VIEW_DEFINITION",
                    "CHECK_OPTION",
                    "IS_UPDATABLE",
                    "STATUS",
                    "REMARKS",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case IN_DOUBT:
            setMetaTableName("IN_DOUBT");
            cols = createColumns(
                    "TRANSACTION",
                    "STATE"
            );
            break;
        case CROSS_REFERENCES:
            setMetaTableName("CROSS_REFERENCES");
            cols = createColumns(
                    "PKTABLE_CATALOG",
                    "PKTABLE_SCHEMA",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CATALOG",
                    "FKTABLE_SCHEMA",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "ORDINAL_POSITION SMALLINT",
                    "UPDATE_RULE SMALLINT",
                    "DELETE_RULE SMALLINT",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY SMALLINT"
            );
            indexColumnName = "PKTABLE_NAME";
            break;
        case CONSTRAINTS:
            setMetaTableName("CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CONSTRAINT_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "UNIQUE_INDEX_NAME",
                    "CHECK_EXPRESSION",
                    "COLUMN_LIST",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case CONSTANTS:
            setMetaTableName("CONSTANTS");
            cols = createColumns(
                    "CONSTANT_CATALOG",
                    "CONSTANT_SCHEMA",
                    "CONSTANT_NAME",
                    "DATA_TYPE INT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case DOMAINS:
            setMetaTableName("DOMAINS");
            cols = createColumns(
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE INT",
                    "PRECISION INT",
                    "SCALE INT",
                    "TYPE_NAME",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case TRIGGERS:
            setMetaTableName("TRIGGERS");
            cols = createColumns(
                    "TRIGGER_CATALOG",
                    "TRIGGER_SCHEMA",
                    "TRIGGER_NAME",
                    "TRIGGER_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "BEFORE BIT",
                    "JAVA_CLASS",
                    "QUEUE_SIZE INT",
                    "NO_WAIT BIT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case SESSIONS: {
            setMetaTableName("SESSIONS");
            cols = createColumns(
                    "ID INT",
                    "USER_NAME",
                    "SESSION_START TIMESTAMP WITH TIME ZONE",
                    "STATEMENT",
                    "STATEMENT_START TIMESTAMP WITH TIME ZONE",
                    "CONTAINS_UNCOMMITTED BIT",
                    "STATE",
                    "BLOCKER_ID INT"
            );
            break;
        }
        case LOCKS: {
            setMetaTableName("LOCKS");
            cols = createColumns(
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "SESSION_ID INT",
                    "LOCK_TYPE"
            );
            break;
        }
        case SESSION_STATE: {
            setMetaTableName("SESSION_STATE");
            cols = createColumns(
                    "KEY",
                    "SQL"
            );
            break;
        }
        case QUERY_STATISTICS: {
            setMetaTableName("QUERY_STATISTICS");
            cols = createColumns(
                    "SQL_STATEMENT",
                    "EXECUTION_COUNT INT",
                    "MIN_EXECUTION_TIME DOUBLE",
                    "MAX_EXECUTION_TIME DOUBLE",
                    "CUMULATIVE_EXECUTION_TIME DOUBLE",
                    "AVERAGE_EXECUTION_TIME DOUBLE",
                    "STD_DEV_EXECUTION_TIME DOUBLE",
                    "MIN_ROW_COUNT INT",
                    "MAX_ROW_COUNT INT",
                    "CUMULATIVE_ROW_COUNT LONG",
                    "AVERAGE_ROW_COUNT DOUBLE",
                    "STD_DEV_ROW_COUNT DOUBLE"
            );
            break;
        }
        case SYNONYMS: {
            setMetaTableName("SYNONYMS");
            cols = createColumns(
                    "SYNONYM_CATALOG",
                    "SYNONYM_SCHEMA",
                    "SYNONYM_NAME",
                    "SYNONYM_FOR",
                    "SYNONYM_FOR_SCHEMA",
                    "TYPE_NAME",
                    "STATUS",
                    "REMARKS",
                    "ID INT"
            );
            indexColumnName = "SYNONYM_NAME";
            break;
        }
        case TABLE_CONSTRAINTS: {
            setMetaTableName("TABLE_CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CONSTRAINT_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "IS_DEFERRABLE",
                    "INITIALLY_DEFERRED"
            );
            indexColumnName = "TABLE_NAME";
            break;
        }
        case KEY_COLUMN_USAGE: {
            setMetaTableName("KEY_COLUMN_USAGE");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "POSITION_IN_UNIQUE_CONSTRAINT INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        }
        case REFERENTIAL_CONSTRAINTS: {
            setMetaTableName("REFERENTIAL_CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "UNIQUE_CONSTRAINT_CATALOG",
                    "UNIQUE_CONSTRAINT_SCHEMA",
                    "UNIQUE_CONSTRAINT_NAME",
                    "MATCH_OPTION",
                    "UPDATE_RULE",
                    "DELETE_RULE"
            );
            break;
        }
        default:
            throw DbException.throwInternalError("type="+type);
        }
        setColumns(cols);

        if (indexColumnName == null) {
            indexColumn = -1;
            metaIndex = null;
        } else {
            indexColumn = getColumn(database.sysIdentifier(indexColumnName)).getColumnId();
            IndexColumn[] indexCols = IndexColumn.wrap(
                    new Column[] { cols[indexColumn] });
            metaIndex = new MetaIndex(this, indexCols, false);
        }
    }

    private void setMetaTableName(String upperName) {
        setObjectName(database.sysIdentifier(upperName));
    }

    private Column[] createColumns(String... names) {
        Column[] cols = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            String nameType = names[i];
            int idx = nameType.indexOf(' ');
            int dataType;
            String name;
            if (idx < 0) {
                dataType = database.getMode().lowerCaseIdentifiers ?
                        Value.STRING_IGNORECASE : Value.STRING;
                name = nameType;
            } else {
                dataType = DataType.getTypeByName(nameType.substring(idx + 1), database.getMode()).type;
                name = nameType.substring(0, idx);
            }
            cols[i] = new Column(database.sysIdentifier(name), dataType);
        }
        return cols;
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
            IndexColumn[] cols, IndexType indexType, boolean create,
            String indexComment) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        // nothing to do
        return false;
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    private String identifier(String s) {
        if (database.getMode().lowerCaseIdentifiers) {
            s = s == null ? null : StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    private ArrayList<Table> getAllTables(Session session) {
        ArrayList<Table> tables = database.getAllTablesAndViews(true);
        ArrayList<Table> tempTables = session.getLocalTempTables();
        tables.addAll(tempTables);
        return tables;
    }

    private ArrayList<Table> getTablesByName(Session session, String tableName) {
        ArrayList<Table> tables = database.getTableOrViewByName(tableName);
        for (Table temp : session.getLocalTempTables()) {
            if (temp.getName().equals(tableName)) {
                tables.add(temp);
            }
        }
        return tables;
    }

    private boolean checkIndex(Session session, String value, Value indexFrom,
            Value indexTo) {
        if (value == null || (indexFrom == null && indexTo == null)) {
            return true;
        }
        Database db = session.getDatabase();
        Value v;
        if (database.getMode().lowerCaseIdentifiers) {
            v = ValueStringIgnoreCase.get(value);
        } else {
            v = ValueString.get(value);
        }
        if (indexFrom != null && db.compare(v, indexFrom) < 0) {
            return false;
        }
        if (indexTo != null && db.compare(v, indexTo) > 0) {
            return false;
        }
        return true;
    }

    private static String replaceNullWithEmpty(String s) {
        return s == null ? "" : s;
    }

    private boolean hideTable(Table table, Session session) {
        return table.isHidden() && session != database.getSystemSession();
    }

    /**
     * Generate the data for the given metadata table using the given first and
     * last row filters.
     *
     * @param session the session
     * @param first the first row to return
     * @param last the last row to return
     * @return the generated rows
     */
    public ArrayList<Row> generateRows(Session session, SearchRow first,
            SearchRow last) {
        Value indexFrom = null, indexTo = null;

        if (indexColumn >= 0) {
            if (first != null) {
                indexFrom = first.getValue(indexColumn);
            }
            if (last != null) {
                indexTo = last.getValue(indexColumn);
            }
        }

        ArrayList<Row> rows = Utils.newSmallArrayList();
        String catalog = database.getShortName();
        boolean admin = session.getUser().isAdmin();
        switch (type) {
        case TABLES: {
            for (Table table : getAllTables(session)) {
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (hideTable(table, session)) {
                    continue;
                }
                String storageType;
                if (table.isTemporary()) {
                    if (table.isGlobalTemporary()) {
                        storageType = "GLOBAL TEMPORARY";
                    } else {
                        storageType = "LOCAL TEMPORARY";
                    }
                } else {
                    storageType = table.isPersistIndexes() ?
                            "CACHED" : "MEMORY";
                }
                String sql = table.getCreateSQL();
                if (!admin) {
                    if (sql != null && sql.contains(DbException.HIDE_SQL)) {
                        // hide the password of linked tables
                        sql = "-";
                    }
                }
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        tableName,
                        // TABLE_TYPE
                        table.getTableType().toString(),
                        // STORAGE_TYPE
                        storageType,
                        // SQL
                        sql,
                        // REMARKS
                        replaceNullWithEmpty(table.getComment()),
                        // LAST_MODIFICATION
                        ValueLong.get(table.getMaxDataModificationId()),
                        // ID
                        ValueInt.get(table.getId()),
                        // TYPE_NAME
                        null,
                        // TABLE_CLASS
                        table.getClass().getName(),
                        // ROW_COUNT_ESTIMATE
                        ValueLong.get(table.getRowCountApproximation())
                );
            }
            break;
        }
        case COLUMNS: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexFrom.equals(indexTo)) {
                String tableName = indexFrom.getString();
                if (tableName == null) {
                    break;
                }
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (hideTable(table, session)) {
                    continue;
                }
                Column[] cols = table.getColumns();
                String collation = database.getCompareMode().getName();
                for (int j = 0; j < cols.length; j++) {
                    Column c = cols[j];
                    Domain domain = c.getDomain();
                    DataType dataType = c.getDataType();
                    ValueInt precision = ValueInt.get(c.getPrecisionAsInt());
                    ValueInt scale = ValueInt.get(c.getType().getScale());
                    Sequence sequence = c.getSequence();
                    boolean hasDateTimePrecision;
                    int type = dataType.type;
                    switch (type) {
                    case Value.TIME:
                    case Value.DATE:
                    case Value.TIMESTAMP:
                    case Value.TIMESTAMP_TZ:
                    case Value.INTERVAL_SECOND:
                    case Value.INTERVAL_DAY_TO_SECOND:
                    case Value.INTERVAL_HOUR_TO_SECOND:
                    case Value.INTERVAL_MINUTE_TO_SECOND:
                        hasDateTimePrecision = true;
                        break;
                    default:
                        hasDateTimePrecision = false;
                    }
                    boolean isInterval = DataType.isIntervalType(type);
                    String createSQLWithoutName = c.getCreateSQLWithoutName();
                    add(rows,
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            table.getSchema().getName(),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            c.getName(),
                            // ORDINAL_POSITION
                            ValueInt.get(j + 1),
                            // DOMAIN_CATALOG
                            domain != null ? catalog : null,
                            // DOMAIN_SCHEMA
                            domain != null ? database.getMainSchema().getName() : null,
                            // DOMAIN_NAME
                            domain != null ? domain.getName() : null,
                            // COLUMN_DEFAULT
                            c.getDefaultSQL(),
                            // IS_NULLABLE
                            c.isNullable() ? "YES" : "NO",
                            // DATA_TYPE
                            ValueInt.get(dataType.sqlType),
                            // CHARACTER_MAXIMUM_LENGTH
                            precision,
                            // CHARACTER_OCTET_LENGTH
                            precision,
                            // NUMERIC_PRECISION
                            precision,
                            // NUMERIC_PRECISION_RADIX
                            ValueInt.get(10),
                            // NUMERIC_SCALE
                            scale,
                            // DATETIME_PRECISION
                            hasDateTimePrecision ? scale : null,
                            // INTERVAL_TYPE
                            isInterval ? createSQLWithoutName.substring(9) : null,
                            // INTERVAL_PRECISION
                            isInterval ? precision : null,
                            // CHARACTER_SET_NAME
                            CHARACTER_SET_NAME,
                            // COLLATION_NAME
                            collation,
                            // TYPE_NAME
                            identifier(isInterval ? "INTERVAL" : dataType.name),
                            // NULLABLE
                            ValueInt.get(c.isNullable()
                                    ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls),
                            // IS_COMPUTED
                            ValueBoolean.get(c.getComputed()),
                            // SELECTIVITY
                            ValueInt.get(c.getSelectivity()),
                            // CHECK_CONSTRAINT
                            c.getCheckConstraintSQL(session, c.getName()),
                            // SEQUENCE_NAME
                            sequence == null ? null : sequence.getName(),
                            // REMARKS
                            replaceNullWithEmpty(c.getComment()),
                            // SOURCE_DATA_TYPE
                            // SMALLINT
                            null,
                            // COLUMN_TYPE
                            createSQLWithoutName,
                            // COLUMN_ON_UPDATE
                            c.getOnUpdateSQL(),
                            // IS_VISIBLE
                            ValueBoolean.get(c.getVisible())
                    );
                }
            }
            break;
        }
        case INDEXES: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexFrom.equals(indexTo)) {
                String tableName = indexFrom.getString();
                if (tableName == null) {
                    break;
                }
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (hideTable(table, session)) {
                    continue;
                }
                ArrayList<Index> indexes = table.getIndexes();
                ArrayList<Constraint> constraints = table.getConstraints();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (index.getCreateSQL() == null) {
                        continue;
                    }
                    String constraintName = null;
                    for (int k = 0; constraints != null && k < constraints.size(); k++) {
                        Constraint constraint = constraints.get(k);
                        if (constraint.usesIndex(index)) {
                            if (index.getIndexType().isPrimaryKey()) {
                                if (constraint.getConstraintType() == Constraint.Type.PRIMARY_KEY) {
                                    constraintName = constraint.getName();
                                }
                            } else {
                                constraintName = constraint.getName();
                            }
                        }
                    }
                    IndexColumn[] cols = index.getIndexColumns();
                    String indexClass = index.getClass().getName();
                    for (int k = 0; k < cols.length; k++) {
                        IndexColumn idxCol = cols[k];
                        Column column = idxCol.column;
                        add(rows,
                                // TABLE_CATALOG
                                catalog,
                                // TABLE_SCHEMA
                                table.getSchema().getName(),
                                // TABLE_NAME
                                tableName,
                                // NON_UNIQUE
                                ValueBoolean.get(!index.getIndexType().isUnique()),
                                // INDEX_NAME
                                index.getName(),
                                // ORDINAL_POSITION
                                ValueShort.get((short) (k + 1)),
                                // COLUMN_NAME
                                column.getName(),
                                // CARDINALITY
                                ValueInt.get(0),
                                // PRIMARY_KEY
                                ValueBoolean.get(index.getIndexType().isPrimaryKey()),
                                // INDEX_TYPE_NAME
                                index.getIndexType().getSQL(),
                                // IS_GENERATED
                                ValueBoolean.get(index.getIndexType().getBelongsToConstraint()),
                                // INDEX_TYPE
                                ValueShort.get(DatabaseMetaData.tableIndexOther),
                                // ASC_OR_DESC
                                (idxCol.sortType & SortOrder.DESCENDING) != 0 ? "D" : "A",
                                // PAGES
                                ValueInt.get(0),
                                // FILTER_CONDITION
                                "",
                                // REMARKS
                                replaceNullWithEmpty(index.getComment()),
                                // SQL
                                index.getCreateSQL(),
                                // ID
                                ValueInt.get(index.getId()),
                                // SORT_TYPE
                                ValueInt.get(idxCol.sortType),
                                // CONSTRAINT_NAME
                                constraintName,
                                // INDEX_CLASS
                                indexClass,
                                // AFFINITY
                                ValueBoolean.get(index.getIndexType().isAffinity())
                            );
                    }
                }
            }
            break;
        }
        case TABLE_TYPES: {
            add(rows, TableType.TABLE.toString());
            add(rows, TableType.TABLE_LINK.toString());
            add(rows, TableType.SYSTEM_TABLE.toString());
            add(rows, TableType.VIEW.toString());
            add(rows, TableType.EXTERNAL_TABLE_ENGINE.toString());
            break;
        }
        case TYPE_INFO: {
            for (DataType t : DataType.getTypes()) {
                if (t.hidden || t.sqlType == Value.NULL) {
                    continue;
                }
                add(rows,
                        // TYPE_NAME
                        t.name,
                        // DATA_TYPE
                        ValueInt.get(t.sqlType),
                        // PRECISION
                        ValueInt.get(MathUtils.convertLongToInt(t.maxPrecision)),
                        // PREFIX
                        t.prefix,
                        // SUFFIX
                        t.suffix,
                        // PARAMS
                        t.params,
                        // AUTO_INCREMENT
                        ValueBoolean.get(t.autoIncrement),
                        // MINIMUM_SCALE
                        ValueShort.get((short) t.minScale),
                        // MAXIMUM_SCALE
                        ValueShort.get((short) t.maxScale),
                        // RADIX
                        t.decimal ? ValueInt.get(10) : null,
                        // POS
                        ValueInt.get(t.sqlTypePos),
                        // CASE_SENSITIVE
                        ValueBoolean.get(t.caseSensitive),
                        // NULLABLE
                        ValueShort.get((short) DatabaseMetaData.typeNullable),
                        // SEARCHABLE
                        ValueShort.get((short) DatabaseMetaData.typeSearchable)
                );
            }
            break;
        }
        case CATALOGS: {
            add(rows, catalog);
            break;
        }
        case SETTINGS: {
            for (Setting s : database.getAllSettings()) {
                String value = s.getStringValue();
                if (value == null) {
                    value = Integer.toString(s.getIntValue());
                }
                add(rows,
                        identifier(s.getName()),
                        value
                );
            }
            add(rows, "info.BUILD_ID", "" + Constants.BUILD_ID);
            add(rows, "info.VERSION_MAJOR", "" + Constants.VERSION_MAJOR);
            add(rows, "info.VERSION_MINOR", "" + Constants.VERSION_MINOR);
            add(rows, "info.VERSION", Constants.getFullVersion());
            if (admin) {
                String[] settings = {
                        "java.runtime.version", "java.vm.name",
                        "java.vendor", "os.name", "os.arch", "os.version",
                        "sun.os.patch.level", "file.separator",
                        "path.separator", "line.separator", "user.country",
                        "user.language", "user.variant", "file.encoding"                };
                for (String s : settings) {
                    add(rows, "property." + s, Utils.getProperty(s, ""));
                }
            }
            add(rows, "EXCLUSIVE", database.getExclusiveSession() == null ?
                    "FALSE" : "TRUE");
            add(rows, "MODE", database.getMode().getName());
            add(rows, "MULTI_THREADED", database.isMultiThreaded() ? "1" : "0");
            add(rows, "QUERY_TIMEOUT", Integer.toString(session.getQueryTimeout()));
            add(rows, "RETENTION_TIME", Integer.toString(database.getRetentionTime()));
            add(rows, "LOG", Integer.toString(database.getLogMode()));
            // database settings
            HashMap<String, String> s = database.getSettings().getSettings();
            ArrayList<String> settingNames = new ArrayList<>(s.size());
            settingNames.addAll(s.keySet());
            Collections.sort(settingNames);
            for (String k : settingNames) {
                add(rows, k, s.get(k));
            }
            if (database.isPersistent()) {
                PageStore pageStore = database.getPageStore();
                if (pageStore != null) {
                    add(rows, "info.FILE_WRITE_TOTAL",
                            Long.toString(pageStore.getWriteCountTotal()));
                    add(rows, "info.FILE_WRITE",
                            Long.toString(pageStore.getWriteCount()));
                    add(rows, "info.FILE_READ",
                            Long.toString(pageStore.getReadCount()));
                    add(rows, "info.PAGE_COUNT",
                            Integer.toString(pageStore.getPageCount()));
                    add(rows, "info.PAGE_SIZE",
                            Integer.toString(pageStore.getPageSize()));
                    add(rows, "info.CACHE_MAX_SIZE",
                            Integer.toString(pageStore.getCache().getMaxMemory()));
                    add(rows, "info.CACHE_SIZE",
                            Integer.toString(pageStore.getCache().getMemory()));
                }
                Store store = database.getStore();
                if (store != null) {
                    MVStore mvStore = store.getMvStore();
                    FileStore fs = mvStore.getFileStore();
                    add(rows, "info.FILE_WRITE",
                            Long.toString(fs.getWriteCount()));
                    add(rows, "info.FILE_READ",
                            Long.toString(fs.getReadCount()));
                    add(rows, "info.UPDATE_FAILURE_PERCENT",
                            String.format(Locale.ENGLISH, "%.2f%%", 100 * mvStore.getUpdateFailureRatio()));
                    long size;
                    try {
                        size = fs.getFile().size();
                    } catch (IOException e) {
                        throw DbException.convertIOException(e, "Can not get size");
                    }
                    int pageSize = 4 * 1024;
                    long pageCount = size / pageSize;
                    add(rows, "info.PAGE_COUNT",
                            Long.toString(pageCount));
                    add(rows, "info.PAGE_SIZE",
                            Integer.toString(mvStore.getPageSplitSize()));
                    add(rows, "info.CACHE_MAX_SIZE",
                            Integer.toString(mvStore.getCacheSize()));
                    add(rows, "info.CACHE_SIZE",
                            Integer.toString(mvStore.getCacheSizeUsed()));
                }
            }
            break;
        }
        case HELP: {
            String resource = "/org/h2/res/help.csv";
            try {
                byte[] data = Utils.getResource(resource);
                Reader reader = new InputStreamReader(
                        new ByteArrayInputStream(data));
                Csv csv = new Csv();
                csv.setLineCommentCharacter('#');
                ResultSet rs = csv.read(reader, null);
                for (int i = 0; rs.next(); i++) {
                    add(rows,
                        // ID
                        ValueInt.get(i),
                        // SECTION
                        rs.getString(1).trim(),
                        // TOPIC
                        rs.getString(2).trim(),
                        // SYNTAX
                        rs.getString(3).trim(),
                        // TEXT
                        rs.getString(4).trim()
                    );
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            }
            break;
        }
        case SEQUENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.SEQUENCE)) {
                Sequence s = (Sequence) obj;
                add(rows,
                        // SEQUENCE_CATALOG
                        catalog,
                        // SEQUENCE_SCHEMA
                        s.getSchema().getName(),
                        // SEQUENCE_NAME
                        s.getName(),
                        // CURRENT_VALUE
                        ValueLong.get(s.getCurrentValue()),
                        // INCREMENT
                        ValueLong.get(s.getIncrement()),
                        // IS_GENERATED
                        ValueBoolean.get(s.getBelongsToTable()),
                        // REMARKS
                        replaceNullWithEmpty(s.getComment()),
                        // CACHE
                        ValueLong.get(s.getCacheSize()),
                        // MIN_VALUE
                        ValueLong.get(s.getMinValue()),
                        // MAX_VALUE
                        ValueLong.get(s.getMaxValue()),
                        // IS_CYCLE
                        ValueBoolean.get(s.getCycle()),
                        // ID
                        ValueInt.get(s.getId())
                    );
            }
            break;
        }
        case USERS: {
            for (User u : database.getAllUsers()) {
                if (admin || session.getUser() == u) {
                    add(rows,
                            // NAME
                            identifier(u.getName()),
                            // ADMIN
                            String.valueOf(u.isAdmin()),
                            // REMARKS
                            replaceNullWithEmpty(u.getComment()),
                            // ID
                            ValueInt.get(u.getId())
                    );
                }
            }
            break;
        }
        case ROLES: {
            for (Role r : database.getAllRoles()) {
                if (admin || session.getUser().isRoleGranted(r)) {
                    add(rows,
                            // NAME
                            identifier(r.getName()),
                            // REMARKS
                            replaceNullWithEmpty(r.getComment()),
                            // ID
                            ValueInt.get(r.getId())
                    );
                }
            }
            break;
        }
        case RIGHTS: {
            if (admin) {
                for (Right r : database.getAllRights()) {
                    Role role = r.getGrantedRole();
                    DbObject grantee = r.getGrantee();
                    String rightType = grantee.getType() == DbObject.USER ? "USER" : "ROLE";
                    if (role == null) {
                        DbObject object = r.getGrantedObject();
                        Schema schema = null;
                        Table table = null;
                        if (object != null) {
                            if (object instanceof Schema) {
                                schema = (Schema) object;
                            } else if (object instanceof Table) {
                                table = (Table) object;
                                schema = table.getSchema();
                            }
                        }
                        String tableName = (table != null) ? table.getName() : "";
                        String schemaName = (schema != null) ? schema.getName() : "";
                        if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                            continue;
                        }
                        add(rows,
                                // GRANTEE
                                identifier(grantee.getName()),
                                // GRANTEETYPE
                                rightType,
                                // GRANTEDROLE
                                "",
                                // RIGHTS
                                r.getRights(),
                                // TABLE_SCHEMA
                                schemaName,
                                // TABLE_NAME
                                tableName,
                                // ID
                                ValueInt.get(r.getId())
                        );
                    } else {
                        add(rows,
                                // GRANTEE
                                identifier(grantee.getName()),
                                // GRANTEETYPE
                                rightType,
                                // GRANTEDROLE
                                identifier(role.getName()),
                                // RIGHTS
                                "",
                                // TABLE_SCHEMA
                                "",
                                // TABLE_NAME
                                "",
                                // ID
                                ValueInt.get(r.getId())
                        );
                    }
                }
            }
            break;
        }
        case FUNCTION_ALIASES: {
            for (SchemaObject aliasAsSchemaObject :
                    database.getAllSchemaObjects(DbObject.FUNCTION_ALIAS)) {
                FunctionAlias alias = (FunctionAlias) aliasAsSchemaObject;
                JavaMethod[] methods;
                try {
                    methods = alias.getJavaMethods();
                } catch (DbException e) {
                    methods = new JavaMethod[0];
                }
                for (FunctionAlias.JavaMethod method : methods) {
                    add(rows,
                            // ALIAS_CATALOG
                            catalog,
                            // ALIAS_SCHEMA
                            alias.getSchema().getName(),
                            // ALIAS_NAME
                            alias.getName(),
                            // JAVA_CLASS
                            alias.getJavaClassName(),
                            // JAVA_METHOD
                            alias.getJavaMethodName(),
                            // DATA_TYPE
                            ValueInt.get(DataType.convertTypeToSQLType(method.getDataType())),
                            // TYPE_NAME
                            DataType.getDataType(method.getDataType()).name,
                            // COLUMN_COUNT
                            ValueInt.get(method.getParameterCount()),
                            // RETURNS_RESULT
                            ValueShort.get(method.getDataType() == Value.NULL
                                    ? (short) DatabaseMetaData.procedureNoResult
                                    : (short) DatabaseMetaData.procedureReturnsResult),
                            // REMARKS
                            replaceNullWithEmpty(alias.getComment()),
                            // ID
                            ValueInt.get(alias.getId()),
                            // SOURCE
                            alias.getSource()
                            // when adding more columns, see also below
                    );
                }
            }
            for (UserAggregate agg : database.getAllAggregates()) {
                add(rows,
                        // ALIAS_CATALOG
                        catalog,
                        // ALIAS_SCHEMA
                        database.getMainSchema().getName(),
                        // ALIAS_NAME
                        agg.getName(),
                        // JAVA_CLASS
                        agg.getJavaClassName(),
                        // JAVA_METHOD
                        "",
                        // DATA_TYPE
                        ValueInt.get(Types.NULL),
                        // TYPE_NAME
                        DataType.getDataType(Value.NULL).name,
                        // COLUMN_COUNT
                        ValueInt.get(1),
                        // RETURNS_RESULT
                        ValueShort.get((short) DatabaseMetaData.procedureReturnsResult),
                        // REMARKS
                        replaceNullWithEmpty(agg.getComment()),
                        // ID
                        ValueInt.get(agg.getId()),
                        // SOURCE
                        ""
                        // when adding more columns, see also below
                );
            }
            break;
        }
        case FUNCTION_COLUMNS: {
            for (SchemaObject aliasAsSchemaObject :
                    database.getAllSchemaObjects(DbObject.FUNCTION_ALIAS)) {
                FunctionAlias alias = (FunctionAlias) aliasAsSchemaObject;
                JavaMethod[] methods;
                try {
                    methods = alias.getJavaMethods();
                } catch (DbException e) {
                    methods = new JavaMethod[0];
                }
                for (FunctionAlias.JavaMethod method : methods) {
                    // Add return column index 0
                    if (method.getDataType() != Value.NULL) {
                        DataType dt = DataType.getDataType(method.getDataType());
                        add(rows,
                                // ALIAS_CATALOG
                                catalog,
                                // ALIAS_SCHEMA
                                alias.getSchema().getName(),
                                // ALIAS_NAME
                                alias.getName(),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                ValueInt.get(method.getParameterCount()),
                                // POS
                                ValueInt.get(0),
                                // COLUMN_NAME
                                "P0",
                                // DATA_TYPE
                                ValueInt.get(DataType.convertTypeToSQLType(method.getDataType())),
                                // TYPE_NAME
                                dt.name,
                                // PRECISION
                                ValueInt.get(MathUtils.convertLongToInt(dt.defaultPrecision)),
                                // SCALE
                                ValueShort.get((short) dt.defaultScale),
                                // RADIX
                                ValueShort.get((short) 10),
                                // NULLABLE
                                ValueShort.get((short) DatabaseMetaData.columnNullableUnknown),
                                // COLUMN_TYPE
                                ValueShort.get((short) DatabaseMetaData.procedureColumnReturn),
                                // REMARKS
                                "",
                                // COLUMN_DEFAULT
                                null
                        );
                    }
                    Class<?>[] columnList = method.getColumnClasses();
                    for (int k = 0; k < columnList.length; k++) {
                        if (method.hasConnectionParam() && k == 0) {
                            continue;
                        }
                        Class<?> clazz = columnList[k];
                        int dataType = DataType.getTypeFromClass(clazz);
                        DataType dt = DataType.getDataType(dataType);
                        add(rows,
                                // ALIAS_CATALOG
                                catalog,
                                // ALIAS_SCHEMA
                                alias.getSchema().getName(),
                                // ALIAS_NAME
                                alias.getName(),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                ValueInt.get(method.getParameterCount()),
                                // POS
                                ValueInt.get(k + (method.hasConnectionParam() ? 0 : 1)),
                                // COLUMN_NAME
                                "P" + (k + 1),
                                // DATA_TYPE
                                ValueInt.get(DataType.convertTypeToSQLType(dt.type)),
                                // TYPE_NAME
                                dt.name,
                                // PRECISION
                                ValueInt.get(MathUtils.convertLongToInt(dt.defaultPrecision)),
                                // SCALE
                                ValueShort.get((short) dt.defaultScale),
                                // RADIX
                                ValueShort.get((short) 10),
                                // NULLABLE
                                ValueShort.get(clazz.isPrimitive()
                                        ? (short) DatabaseMetaData.columnNoNulls
                                        : (short) DatabaseMetaData.columnNullable),
                                // COLUMN_TYPE
                                ValueShort.get((short) DatabaseMetaData.procedureColumnIn),
                                // REMARKS
                                "",
                                // COLUMN_DEFAULT
                                null
                        );
                    }
                }
            }
            break;
        }
        case SCHEMATA: {
            String collation = database.getCompareMode().getName();
            for (Schema schema : database.getAllSchemas()) {
                add(rows,
                        // CATALOG_NAME
                        catalog,
                        // SCHEMA_NAME
                        schema.getName(),
                        // SCHEMA_OWNER
                        identifier(schema.getOwner().getName()),
                        // DEFAULT_CHARACTER_SET_NAME
                        CHARACTER_SET_NAME,
                        // DEFAULT_COLLATION_NAME
                        collation,
                        // IS_DEFAULT
                        ValueBoolean.get(schema.getId() == Constants.MAIN_SCHEMA_ID),
                        // REMARKS
                        replaceNullWithEmpty(schema.getComment()),
                        // ID
                        ValueInt.get(schema.getId())
                );
            }
            break;
        }
        case TABLE_PRIVILEGES: {
            for (Right r : database.getAllRights()) {
                DbObject object = r.getGrantedObject();
                if (!(object instanceof Table)) {
                    continue;
                }
                Table table = (Table) object;
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                addPrivileges(rows, r.getGrantee(), catalog, table, null,
                        r.getRightMask());
            }
            break;
        }
        case COLUMN_PRIVILEGES: {
            for (Right r : database.getAllRights()) {
                DbObject object = r.getGrantedObject();
                if (!(object instanceof Table)) {
                    continue;
                }
                Table table = (Table) object;
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                DbObject grantee = r.getGrantee();
                int mask = r.getRightMask();
                for (Column column : table.getColumns()) {
                    addPrivileges(rows, grantee, catalog, table,
                            column.getName(), mask);
                }
            }
            break;
        }
        case COLLATIONS: {
            for (Locale l : Collator.getAvailableLocales()) {
                add(rows,
                        // NAME
                        CompareMode.getName(l),
                        // KEY
                        l.toString()
                );
            }
            break;
        }
        case VIEWS: {
            for (Table table : getAllTables(session)) {
                if (table.getTableType() != TableType.VIEW) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                TableView view = (TableView) table;
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        tableName,
                        // VIEW_DEFINITION
                        table.getCreateSQL(),
                        // CHECK_OPTION
                        "NONE",
                        // IS_UPDATABLE
                        "NO",
                        // STATUS
                        view.isInvalid() ? "INVALID" : "VALID",
                        // REMARKS
                        replaceNullWithEmpty(view.getComment()),
                        // ID
                        ValueInt.get(view.getId())
                );
            }
            break;
        }
        case IN_DOUBT: {
            ArrayList<InDoubtTransaction> prepared = database.getInDoubtTransactions();
            if (prepared != null && admin) {
                for (InDoubtTransaction prep : prepared) {
                    add(rows,
                            // TRANSACTION
                            prep.getTransactionName(),
                            // STATE
                            prep.getState()
                    );
                }
            }
            break;
        }
        case CROSS_REFERENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                if (constraint.getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential ref = (ConstraintReferential) constraint;
                IndexColumn[] cols = ref.getColumns();
                IndexColumn[] refCols = ref.getRefColumns();
                Table tab = ref.getTable();
                Table refTab = ref.getRefTable();
                String tableName = refTab.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                ValueShort update = ValueShort.get(getRefAction(ref.getUpdateAction()));
                ValueShort delete = ValueShort.get(getRefAction(ref.getDeleteAction()));
                for (int j = 0; j < cols.length; j++) {
                    add(rows,
                            // PKTABLE_CATALOG
                            catalog,
                            // PKTABLE_SCHEMA
                            refTab.getSchema().getName(),
                            // PKTABLE_NAME
                            refTab.getName(),
                            // PKCOLUMN_NAME
                            refCols[j].column.getName(),
                            // FKTABLE_CATALOG
                            catalog,
                            // FKTABLE_SCHEMA
                            tab.getSchema().getName(),
                            // FKTABLE_NAME
                            tab.getName(),
                            // FKCOLUMN_NAME
                            cols[j].column.getName(),
                            // ORDINAL_POSITION
                            ValueShort.get((short) (j + 1)),
                            // UPDATE_RULE
                            update,
                            // DELETE_RULE
                            delete,
                            // FK_NAME
                            ref.getName(),
                            // PK_NAME
                            ref.getUniqueIndex().getName(),
                            // DEFERRABILITY
                            ValueShort.get((short) DatabaseMetaData.importedKeyNotDeferrable)
                    );
                }
            }
            break;
        }
        case CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                Constraint.Type constraintType = constraint.getConstraintType();
                String checkExpression = null;
                IndexColumn[] indexColumns = null;
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                Index index = constraint.getUniqueIndex();
                String uniqueIndexName = null;
                if (index != null) {
                    uniqueIndexName = index.getName();
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (constraintType == Constraint.Type.CHECK) {
                    checkExpression = ((ConstraintCheck) constraint).getExpression().getSQL(true);
                } else if (constraintType == Constraint.Type.UNIQUE ||
                        constraintType == Constraint.Type.PRIMARY_KEY) {
                    indexColumns = ((ConstraintUnique) constraint).getColumns();
                } else if (constraintType == Constraint.Type.REFERENTIAL) {
                    indexColumns = ((ConstraintReferential) constraint).getColumns();
                }
                String columnList = null;
                if (indexColumns != null) {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0, length = indexColumns.length; i < length; i++) {
                        if (i > 0) {
                            builder.append(',');
                        }
                        builder.append(indexColumns[i].column.getName());
                    }
                    columnList = builder.toString();
                }
                add(rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        constraint.getSchema().getName(),
                        // CONSTRAINT_NAME
                        constraint.getName(),
                        // CONSTRAINT_TYPE
                        constraintType == Constraint.Type.PRIMARY_KEY ?
                                constraintType.getSqlName() : constraintType.name(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        tableName,
                        // UNIQUE_INDEX_NAME
                        uniqueIndexName,
                        // CHECK_EXPRESSION
                        checkExpression,
                        // COLUMN_LIST
                        columnList,
                        // REMARKS
                        replaceNullWithEmpty(constraint.getComment()),
                        // SQL
                        constraint.getCreateSQL(),
                        // ID
                        ValueInt.get(constraint.getId())
                    );
            }
            break;
        }
        case CONSTANTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.CONSTANT)) {
                Constant constant = (Constant) obj;
                ValueExpression expr = constant.getValue();
                add(rows,
                        // CONSTANT_CATALOG
                        catalog,
                        // CONSTANT_SCHEMA
                        constant.getSchema().getName(),
                        // CONSTANT_NAME
                        constant.getName(),
                        // DATA_TYPE
                        ValueInt.get(DataType.convertTypeToSQLType(expr.getType().getValueType())),
                        // REMARKS
                        replaceNullWithEmpty(constant.getComment()),
                        // SQL
                        expr.getSQL(true),
                        // ID
                        ValueInt.get(constant.getId())
                    );
            }
            break;
        }
        case DOMAINS: {
            for (Domain dt : database.getAllDomains()) {
                Column col = dt.getColumn();
                add(rows,
                        // DOMAIN_CATALOG
                        catalog,
                        // DOMAIN_SCHEMA
                        database.getMainSchema().getName(),
                        // DOMAIN_NAME
                        dt.getName(),
                        // COLUMN_DEFAULT
                        col.getDefaultSQL(),
                        // IS_NULLABLE
                        col.isNullable() ? "YES" : "NO",
                        // DATA_TYPE
                        ValueInt.get(col.getDataType().sqlType),
                        // PRECISION
                        ValueInt.get(col.getPrecisionAsInt()),
                        // SCALE
                        ValueInt.get(col.getType().getScale()),
                        // TYPE_NAME
                        col.getDataType().name,
                        // SELECTIVITY INT
                        ValueInt.get(col.getSelectivity()),
                        // CHECK_CONSTRAINT
                        col.getCheckConstraintSQL(session, "VALUE"),
                        // REMARKS
                        replaceNullWithEmpty(dt.getComment()),
                        // SQL
                        dt.getCreateSQL(),
                        // ID
                        ValueInt.get(dt.getId())
                );
            }
            break;
        }
        case TRIGGERS: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.TRIGGER)) {
                TriggerObject trigger = (TriggerObject) obj;
                Table table = trigger.getTable();
                add(rows,
                        // TRIGGER_CATALOG
                        catalog,
                        // TRIGGER_SCHEMA
                        trigger.getSchema().getName(),
                        // TRIGGER_NAME
                        trigger.getName(),
                        // TRIGGER_TYPE
                        trigger.getTypeNameList(new StringBuilder()).toString(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        table.getName(),
                        // BEFORE
                        ValueBoolean.get(trigger.isBefore()),
                        // JAVA_CLASS
                        trigger.getTriggerClassName(),
                        // QUEUE_SIZE
                        ValueInt.get(trigger.getQueueSize()),
                        // NO_WAIT
                        ValueBoolean.get(trigger.isNoWait()),
                        // REMARKS
                        replaceNullWithEmpty(trigger.getComment()),
                        // SQL
                        trigger.getCreateSQL(),
                        // ID
                        ValueInt.get(trigger.getId())
                );
            }
            break;
        }
        case SESSIONS: {
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    Command command = s.getCurrentCommand();
                    int blockingSessionId = s.getBlockingSessionId();
                    add(rows,
                            // ID
                            ValueInt.get(s.getId()),
                            // USER_NAME
                            s.getUser().getName(),
                            // SESSION_START
                            DateTimeUtils.timestampTimeZoneFromMillis(s.getSessionStart()),
                            // STATEMENT
                            command == null ? null : command.toString(),
                            // STATEMENT_START
                            command == null ? null : s.getCurrentCommandStart(),
                            // CONTAINS_UNCOMMITTED
                            ValueBoolean.get(s.containsUncommitted()),
                            // STATE
                            String.valueOf(s.getState()),
                            // BLOCKER_ID
                            blockingSessionId == 0 ? null : ValueInt.get(blockingSessionId)
                    );
                }
            }
            break;
        }
        case LOCKS: {
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    for (Table table : s.getLocks()) {
                        add(rows,
                                // TABLE_SCHEMA
                                table.getSchema().getName(),
                                // TABLE_NAME
                                table.getName(),
                                // SESSION_ID
                                ValueInt.get(s.getId()),
                                // LOCK_TYPE
                                table.isLockedExclusivelyBy(s) ? "WRITE" : "READ"
                        );
                    }
                }
            }
            break;
        }
        case SESSION_STATE: {
            for (String name : session.getVariableNames()) {
                Value v = session.getVariable(name);
                StringBuilder builder = new StringBuilder().append("SET @").append(name).append(' ');
                v.getSQL(builder);
                add(rows,
                        // KEY
                        "@" + name,
                        builder.toString()
                );
            }
            for (Table table : session.getLocalTempTables()) {
                add(rows,
                        // KEY
                        "TABLE " + table.getName(),
                        // SQL
                        table.getCreateSQL()
                );
            }
            String[] path = session.getSchemaSearchPath();
            if (path != null && path.length > 0) {
                StringBuilder builder = new StringBuilder("SET SCHEMA_SEARCH_PATH ");
                for (int i = 0, l = path.length; i < l; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    StringUtils.quoteIdentifier(builder, path[i]);
                }
                add(rows,
                        // KEY
                        "SCHEMA_SEARCH_PATH",
                        // SQL
                        builder.toString()
                );
            }
            String schema = session.getCurrentSchemaName();
            if (schema != null) {
                add(rows,
                        // KEY
                        "SCHEMA",
                        // SQL
                        StringUtils.quoteIdentifier(new StringBuilder("SET SCHEMA "), schema).toString()
                );
            }
            break;
        }
        case QUERY_STATISTICS: {
            QueryStatisticsData control = database.getQueryStatisticsData();
            if (control != null) {
                for (QueryStatisticsData.QueryEntry entry : control.getQueries()) {
                    add(rows,
                            // SQL_STATEMENT
                            entry.sqlStatement,
                            // EXECUTION_COUNT
                            ValueInt.get(entry.count),
                            // MIN_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeMinNanos / 1_000_000d),
                            // MAX_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeMaxNanos / 1_000_000d),
                            // CUMULATIVE_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeCumulativeNanos / 1_000_000d),
                            // AVERAGE_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeMeanNanos / 1_000_000d),
                            // STD_DEV_EXECUTION_TIME
                            ValueDouble.get(entry.getExecutionTimeStandardDeviation() / 1_000_000d),
                            // MIN_ROW_COUNT
                            ValueInt.get(entry.rowCountMin),
                            // MAX_ROW_COUNT
                            ValueInt.get(entry.rowCountMax),
                            // CUMULATIVE_ROW_COUNT
                            ValueLong.get(entry.rowCountCumulative),
                            // AVERAGE_ROW_COUNT
                            ValueDouble.get(entry.rowCountMean),
                            // STD_DEV_ROW_COUNT
                            ValueDouble.get(entry.getRowCountStandardDeviation())
                    );
                }
            }
            break;
        }
        case SYNONYMS: {
            for (TableSynonym synonym : database.getAllSynonyms()) {
                add(rows,
                        // SYNONYM_CATALOG
                        catalog,
                        // SYNONYM_SCHEMA
                        synonym.getSchema().getName(),
                        // SYNONYM_NAME
                        synonym.getName(),
                        // SYNONYM_FOR
                        synonym.getSynonymForName(),
                        // SYNONYM_FOR_SCHEMA
                        synonym.getSynonymForSchema().getName(),
                        // TYPE NAME
                        "SYNONYM",
                        // STATUS
                        "VALID",
                        // REMARKS
                        replaceNullWithEmpty(synonym.getComment()),
                        // ID
                        ValueInt.get(synonym.getId())
                );
            }
            break;
        }
        case TABLE_CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                Constraint.Type constraintType = constraint.getConstraintType();
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                add(rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        constraint.getSchema().getName(),
                        // CONSTRAINT_NAME
                        constraint.getName(),
                        // CONSTRAINT_TYPE
                        constraintType.getSqlName(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        tableName,
                        // IS_DEFERRABLE
                        "NO",
                        // INITIALLY_DEFERRED
                        "NO"
                );
            }
            break;
        }
        case KEY_COLUMN_USAGE: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                Constraint.Type constraintType = constraint.getConstraintType();
                IndexColumn[] indexColumns = null;
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (constraintType == Constraint.Type.UNIQUE ||
                        constraintType == Constraint.Type.PRIMARY_KEY) {
                    indexColumns = ((ConstraintUnique) constraint).getColumns();
                } else if (constraintType == Constraint.Type.REFERENTIAL) {
                    indexColumns = ((ConstraintReferential) constraint).getColumns();
                }
                if (indexColumns == null) {
                    continue;
                }
                ConstraintUnique referenced;
                if (constraintType == Constraint.Type.REFERENTIAL) {
                    referenced = lookupUniqueForReferential((ConstraintReferential) constraint);
                } else {
                    referenced = null;
                }
                for (int i = 0; i < indexColumns.length; i++) {
                    IndexColumn indexColumn = indexColumns[i];
                    ValueInt ordinalPosition = ValueInt.get(i + 1);
                    ValueInt positionInUniqueConstraint;
                    if (constraintType == Constraint.Type.REFERENTIAL) {
                        positionInUniqueConstraint = ordinalPosition;
                        if (referenced != null) {
                            Column c = ((ConstraintReferential) constraint).getRefColumns()[i].column;
                            IndexColumn[] refColumns = referenced.getColumns();
                            for (int j = 0; j < refColumns.length; j++) {
                                if (refColumns[j].column.equals(c)) {
                                    positionInUniqueConstraint = ValueInt.get(j + 1);
                                    break;
                                }
                            }
                        }
                    } else {
                        positionInUniqueConstraint = null;
                    }
                    add(rows,
                            // CONSTRAINT_CATALOG
                            catalog,
                            // CONSTRAINT_SCHEMA
                            constraint.getSchema().getName(),
                            // CONSTRAINT_NAME
                            constraint.getName(),
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            table.getSchema().getName(),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            indexColumn.columnName,
                            // ORDINAL_POSITION
                            ordinalPosition,
                            // POSITION_IN_UNIQUE_CONSTRAINT
                            positionInUniqueConstraint
                    );
                }
            }
            break;
        }
        case REFERENTIAL_CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                if (((Constraint) obj).getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential constraint = (ConstraintReferential) obj;
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                // Should be referenced unique constraint, but H2 uses indexes instead.
                // So try to find matching unique constraint first and there is no such
                // constraint use index name to return something.
                SchemaObject unique = lookupUniqueForReferential(constraint);
                if (unique == null) {
                    unique = constraint.getUniqueIndex();
                }
                add(rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        constraint.getSchema().getName(),
                        // CONSTRAINT_NAME
                        constraint.getName(),
                        // UNIQUE_CONSTRAINT_CATALOG
                        catalog,
                        // UNIQUE_CONSTRAINT_SCHEMA
                        unique.getSchema().getName(),
                        // UNIQUE_CONSTRAINT_NAME
                        unique.getName(),
                        // MATCH_OPTION
                        "NONE",
                        // UPDATE_RULE
                        constraint.getUpdateAction().getSqlName(),
                        // DELETE_RULE
                        constraint.getDeleteAction().getSqlName()
                );
            }
            break;
        }
        default:
            DbException.throwInternalError("type="+type);
        }
        return rows;
    }

    private static short getRefAction(ConstraintActionType action) {
        switch (action) {
        case CASCADE:
            return DatabaseMetaData.importedKeyCascade;
        case RESTRICT:
            return DatabaseMetaData.importedKeyRestrict;
        case SET_DEFAULT:
            return DatabaseMetaData.importedKeySetDefault;
        case SET_NULL:
            return DatabaseMetaData.importedKeySetNull;
        default:
            throw DbException.throwInternalError("action="+action);
        }
    }

    private static ConstraintUnique lookupUniqueForReferential(ConstraintReferential referential) {
        Table table = referential.getRefTable();
        for (Constraint c : table.getConstraints()) {
            if (c.getConstraintType() == Constraint.Type.UNIQUE) {
                ConstraintUnique unique = (ConstraintUnique) c;
                if (unique.getReferencedColumns(table).equals(referential.getReferencedColumns(table))) {
                    return unique;
                }
            }
        }
        return null;
    }

    @Override
    public void removeRow(Session session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void addRow(Session session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void unlock(Session s) {
        // nothing to do
    }

    private void addPrivileges(ArrayList<Row> rows, DbObject grantee,
            String catalog, Table table, String column, int rightMask) {
        if ((rightMask & Right.SELECT) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "SELECT");
        }
        if ((rightMask & Right.INSERT) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "INSERT");
        }
        if ((rightMask & Right.UPDATE) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "UPDATE");
        }
        if ((rightMask & Right.DELETE) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "DELETE");
        }
    }

    private void addPrivilege(ArrayList<Row> rows, DbObject grantee,
            String catalog, Table table, String column, String right) {
        String isGrantable = "NO";
        if (grantee.getType() == DbObject.USER) {
            User user = (User) grantee;
            if (user.isAdmin()) {
                // the right is grantable if the grantee is an admin
                isGrantable = "YES";
            }
        }
        if (column == null) {
            add(rows,
                    // GRANTOR
                    null,
                    // GRANTEE
                    identifier(grantee.getName()),
                    // TABLE_CATALOG
                    catalog,
                    // TABLE_SCHEMA
                    table.getSchema().getName(),
                    // TABLE_NAME
                    table.getName(),
                    // PRIVILEGE_TYPE
                    right,
                    // IS_GRANTABLE
                    isGrantable
            );
        } else {
            add(rows,
                    // GRANTOR
                    null,
                    // GRANTEE
                    identifier(grantee.getName()),
                    // TABLE_CATALOG
                    catalog,
                    // TABLE_SCHEMA
                    table.getSchema().getName(),
                    // TABLE_NAME
                    table.getName(),
                    // COLUMN_NAME
                    column,
                    // PRIVILEGE_TYPE
                    right,
                    // IS_GRANTABLE
                    isGrantable
            );
        }
    }

    private void add(ArrayList<Row> rows, Object... stringsOrValues) {
        Value[] values = new Value[stringsOrValues.length];
        for (int i = 0; i < stringsOrValues.length; i++) {
            Object s = stringsOrValues[i];
            Value v = s == null ? ValueNull.INSTANCE : s instanceof String ? ValueString.get((String) s) : (Value) s;
            values[i] = columns[i].convert(v);
        }
        Row row = database.createRow(values, 1);
        row.setKey(rows.size());
        rows.add(row);
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public long getRowCount(Session session) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public boolean canDrop() {
        return false;
    }

    @Override
    public TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    @Override
    public Index getScanIndex(Session session) {
        return new MetaIndex(this, IndexColumn.wrap(columns), true);
    }

    @Override
    public ArrayList<Index> getIndexes() {
        ArrayList<Index> list = new ArrayList<>(2);
        if (metaIndex == null) {
            return list;
        }
        list.add(new MetaIndex(this, IndexColumn.wrap(columns), true));
        // TODO re-use the index
        list.add(metaIndex);
        return list;
    }

    @Override
    public long getMaxDataModificationId() {
        switch (type) {
        case SETTINGS:
        case IN_DOUBT:
        case SESSIONS:
        case LOCKS:
        case SESSION_STATE:
            return Long.MAX_VALUE;
        }
        return database.getModificationDataId();
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    /**
     * Get the number of meta table types. Supported meta table
     * types are 0 .. this value - 1.
     *
     * @return the number of meta table types
     */
    public static int getMetaTableTypeCount() {
        return META_TABLE_TYPE_COUNT;
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean canReference() {
        return false;
    }

}
