/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.sql.Timestamp;
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
import org.h2.engine.FunctionAlias;
import org.h2.engine.FunctionAlias.JavaMethod;
import org.h2.engine.QueryStatisticsData;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.engine.UserDataType;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MetaIndex;
import org.h2.index.MultiVersionIndex;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.mvstore.FileStore;
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
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;
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
            setObjectName("TABLES");
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
            setObjectName("COLUMNS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE INT",
                    "CHARACTER_MAXIMUM_LENGTH INT",
                    "CHARACTER_OCTET_LENGTH INT",
                    "NUMERIC_PRECISION INT",
                    "NUMERIC_PRECISION_RADIX INT",
                    "NUMERIC_SCALE INT",
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
                    "COLUMN_ON_UPDATE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case INDEXES:
            setObjectName("INDEXES");
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
            setObjectName("TABLE_TYPES");
            cols = createColumns("TYPE");
            break;
        case TYPE_INFO:
            setObjectName("TYPE_INFO");
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
            setObjectName("CATALOGS");
            cols = createColumns("CATALOG_NAME");
            break;
        case SETTINGS:
            setObjectName("SETTINGS");
            cols = createColumns("NAME", "VALUE");
            break;
        case HELP:
            setObjectName("HELP");
            cols = createColumns(
                    "ID INT",
                    "SECTION",
                    "TOPIC",
                    "SYNTAX",
                    "TEXT"
            );
            break;
        case SEQUENCES:
            setObjectName("SEQUENCES");
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
            setObjectName("USERS");
            cols = createColumns(
                    "NAME",
                    "ADMIN",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case ROLES:
            setObjectName("ROLES");
            cols = createColumns(
                    "NAME",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case RIGHTS:
            setObjectName("RIGHTS");
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
            setObjectName("FUNCTION_ALIASES");
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
            setObjectName("FUNCTION_COLUMNS");
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
            setObjectName("SCHEMATA");
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
            setObjectName("TABLE_PRIVILEGES");
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
            setObjectName("COLUMN_PRIVILEGES");
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
            setObjectName("COLLATIONS");
            cols = createColumns(
                    "NAME",
                    "KEY"
            );
            break;
        case VIEWS:
            setObjectName("VIEWS");
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
            setObjectName("IN_DOUBT");
            cols = createColumns(
                    "TRANSACTION",
                    "STATE"
            );
            break;
        case CROSS_REFERENCES:
            setObjectName("CROSS_REFERENCES");
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
            setObjectName("CONSTRAINTS");
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
            setObjectName("CONSTANTS");
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
            setObjectName("DOMAINS");
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
            setObjectName("TRIGGERS");
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
            setObjectName("SESSIONS");
            cols = createColumns(
                    "ID INT",
                    "USER_NAME",
                    "SESSION_START",
                    "STATEMENT",
                    "STATEMENT_START",
                    "CONTAINS_UNCOMMITTED"
            );
            break;
        }
        case LOCKS: {
            setObjectName("LOCKS");
            cols = createColumns(
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "SESSION_ID INT",
                    "LOCK_TYPE"
            );
            break;
        }
        case SESSION_STATE: {
            setObjectName("SESSION_STATE");
            cols = createColumns(
                    "KEY",
                    "SQL"
            );
            break;
        }
        case QUERY_STATISTICS: {
            setObjectName("QUERY_STATISTICS");
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
            setObjectName("SYNONYMS");
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
            setObjectName("TABLE_CONSTRAINTS");
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
            setObjectName("KEY_COLUMN_USAGE");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION",
                    "POSITION_IN_UNIQUE_CONSTRAINT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        }
        case REFERENTIAL_CONSTRAINTS: {
            setObjectName("REFERENTIAL_CONSTRAINTS");
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
            indexColumn = getColumn(indexColumnName).getColumnId();
            IndexColumn[] indexCols = IndexColumn.wrap(
                    new Column[] { cols[indexColumn] });
            metaIndex = new MetaIndex(this, indexCols, false);
        }
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
            cols[i] = new Column(name, dataType);
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
        if (database.getMode().lowerCaseIdentifiers) {
            tableName = StringUtils.toUpperEnglish(tableName);
        }
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

        ArrayList<Row> rows = New.arrayList();
        String catalog = identifier(database.getShortName());
        boolean admin = session.getUser().isAdmin();
        switch (type) {
        case TABLES: {
            for (Table table : getAllTables(session)) {
                String tableName = identifier(table.getName());
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
                    if (sql != null && sql.contains(JdbcSQLException.HIDE_SQL)) {
                        // hide the password of linked tables
                        sql = "-";
                    }
                }
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
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
                        "" + table.getMaxDataModificationId(),
                        // ID
                        "" + table.getId(),
                        // TYPE_NAME
                        null,
                        // TABLE_CLASS
                        table.getClass().getName(),
                        // ROW_COUNT_ESTIMATE
                        "" + table.getRowCountApproximation()
                );
            }
            break;
        }
        case COLUMNS: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexFrom.equals(indexTo)) {
                String tableName = identifier(indexFrom.getString());
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = identifier(table.getName());
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
                    Sequence sequence = c.getSequence();
                    add(rows,
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            identifier(table.getSchema().getName()),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            identifier(c.getName()),
                            // ORDINAL_POSITION
                            String.valueOf(j + 1),
                            // COLUMN_DEFAULT
                            c.getDefaultSQL(),
                            // IS_NULLABLE
                            c.isNullable() ? "YES" : "NO",
                            // DATA_TYPE
                            "" + DataType.convertTypeToSQLType(c.getType()),
                            // CHARACTER_MAXIMUM_LENGTH
                            "" + c.getPrecisionAsInt(),
                            // CHARACTER_OCTET_LENGTH
                            "" + c.getPrecisionAsInt(),
                            // NUMERIC_PRECISION
                            "" + c.getPrecisionAsInt(),
                            // NUMERIC_PRECISION_RADIX
                            "10",
                            // NUMERIC_SCALE
                            "" + c.getScale(),
                            // CHARACTER_SET_NAME
                            CHARACTER_SET_NAME,
                            // COLLATION_NAME
                            collation,
                            // TYPE_NAME
                            identifier(DataType.getDataType(c.getType()).name),
                            // NULLABLE
                            "" + (c.isNullable() ?
                                    DatabaseMetaData.columnNullable :
                                    DatabaseMetaData.columnNoNulls) ,
                            // IS_COMPUTED
                            "" + (c.getComputed() ? "TRUE" : "FALSE"),
                            // SELECTIVITY
                            "" + (c.getSelectivity()),
                            // CHECK_CONSTRAINT
                            c.getCheckConstraintSQL(session, c.getName()),
                            // SEQUENCE_NAME
                            sequence == null ? null : sequence.getName(),
                            // REMARKS
                            replaceNullWithEmpty(c.getComment()),
                            // SOURCE_DATA_TYPE
                            null,
                            // COLUMN_TYPE
                            c.getCreateSQLWithoutName(),
                            // COLUMN_ON_UPDATE
                            c.getOnUpdateSQL()
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
                String tableName = identifier(indexFrom.getString());
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = identifier(table.getName());
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
                    String indexClass;
                    if (index instanceof MultiVersionIndex) {
                        indexClass = ((MultiVersionIndex) index).
                                getBaseIndex().getClass().getName();
                    } else {
                        indexClass = index.getClass().getName();
                    }
                    for (int k = 0; k < cols.length; k++) {
                        IndexColumn idxCol = cols[k];
                        Column column = idxCol.column;
                        add(rows,
                                // TABLE_CATALOG
                                catalog,
                                // TABLE_SCHEMA
                                identifier(table.getSchema().getName()),
                                // TABLE_NAME
                                tableName,
                                // NON_UNIQUE
                                index.getIndexType().isUnique() ?
                                        "FALSE" : "TRUE",
                                // INDEX_NAME
                                identifier(index.getName()),
                                // ORDINAL_POSITION
                                "" + (k+1),
                                // COLUMN_NAME
                                identifier(column.getName()),
                                // CARDINALITY
                                "0",
                                // PRIMARY_KEY
                                index.getIndexType().isPrimaryKey() ?
                                        "TRUE" : "FALSE",
                                // INDEX_TYPE_NAME
                                index.getIndexType().getSQL(),
                                // IS_GENERATED
                                index.getIndexType().getBelongsToConstraint() ?
                                        "TRUE" : "FALSE",
                                // INDEX_TYPE
                                "" + DatabaseMetaData.tableIndexOther,
                                // ASC_OR_DESC
                                (idxCol.sortType & SortOrder.DESCENDING) != 0 ?
                                        "D" : "A",
                                // PAGES
                                "0",
                                // FILTER_CONDITION
                                "",
                                // REMARKS
                                replaceNullWithEmpty(index.getComment()),
                                // SQL
                                index.getCreateSQL(),
                                // ID
                                "" + index.getId(),
                                // SORT_TYPE
                                "" + idxCol.sortType,
                                // CONSTRAINT_NAME
                                constraintName,
                                // INDEX_CLASS
                                indexClass,
                                // AFFINITY
                                index.getIndexType().isAffinity() ?
                                        "TRUE" : "FALSE"
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
        case CATALOGS: {
            add(rows, catalog);
            break;
        }
        case SETTINGS: {
            for (Setting s : database.getAllSettings()) {
                String value = s.getStringValue();
                if (value == null) {
                    value = "" + s.getIntValue();
                }
                add(rows,
                        identifier(s.getName()),
                        value
                );
            }
            add(rows, "info.BUILD_ID", "" + Constants.BUILD_ID);
            add(rows, "info.VERSION_MAJOR", "" + Constants.VERSION_MAJOR);
            add(rows, "info.VERSION_MINOR", "" + Constants.VERSION_MINOR);
            add(rows, "info.VERSION", "" + Constants.getFullVersion());
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
            add(rows, "MVCC", database.isMultiVersion() ? "TRUE" : "FALSE");
            add(rows, "QUERY_TIMEOUT", "" + session.getQueryTimeout());
            add(rows, "RETENTION_TIME", "" + database.getRetentionTime());
            add(rows, "LOG", "" + database.getLogMode());
            // database settings
            ArrayList<String> settingNames = New.arrayList();
            HashMap<String, String> s = database.getSettings().getSettings();
            settingNames.addAll(s.keySet());
            Collections.sort(settingNames);
            for (String k : settingNames) {
                add(rows, k, s.get(k));
            }
            if (database.isPersistent()) {
                PageStore store = database.getPageStore();
                if (store != null) {
                    add(rows, "info.FILE_WRITE_TOTAL",
                            "" + store.getWriteCountTotal());
                    add(rows, "info.FILE_WRITE",
                            "" + store.getWriteCount());
                    add(rows, "info.FILE_READ",
                            "" + store.getReadCount());
                    add(rows, "info.PAGE_COUNT",
                            "" + store.getPageCount());
                    add(rows, "info.PAGE_SIZE",
                            "" + store.getPageSize());
                    add(rows, "info.CACHE_MAX_SIZE",
                            "" + store.getCache().getMaxMemory());
                    add(rows, "info.CACHE_SIZE",
                            "" + store.getCache().getMemory());
                }
                Store mvStore = database.getMvStore();
                if (mvStore != null) {
                    FileStore fs = mvStore.getStore().getFileStore();
                    add(rows, "info.FILE_WRITE", "" +
                            fs.getWriteCount());
                    add(rows, "info.FILE_READ", "" +
                            fs.getReadCount());
                    long size;
                    try {
                        size = fs.getFile().size();
                    } catch (IOException e) {
                        throw DbException.convertIOException(e, "Can not get size");
                    }
                    int pageSize = 4 * 1024;
                    long pageCount = size / pageSize;
                    add(rows, "info.PAGE_COUNT", "" +
                            pageCount);
                    add(rows, "info.PAGE_SIZE", "" +
                            pageSize);
                    add(rows, "info.CACHE_MAX_SIZE", "" +
                            mvStore.getStore().getCacheSize());
                    add(rows, "info.CACHE_SIZE", "" +
                            mvStore.getStore().getCacheSizeUsed());
                }
            }
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
                        String.valueOf(t.sqlType),
                        // PRECISION
                        String.valueOf(MathUtils.convertLongToInt(t.maxPrecision)),
                        // PREFIX
                        t.prefix,
                        // SUFFIX
                        t.suffix,
                        // PARAMS
                        t.params,
                        // AUTO_INCREMENT
                        String.valueOf(t.autoIncrement),
                        // MINIMUM_SCALE
                        String.valueOf(t.minScale),
                        // MAXIMUM_SCALE
                        String.valueOf(t.maxScale),
                        // RADIX
                        t.decimal ? "10" : null,
                        // POS
                        String.valueOf(t.sqlTypePos),
                        // CASE_SENSITIVE
                        String.valueOf(t.caseSensitive),
                        // NULLABLE
                        "" + DatabaseMetaData.typeNullable,
                        // SEARCHABLE
                        "" + DatabaseMetaData.typeSearchable
                );
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
                        String.valueOf(i),
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
                        identifier(s.getSchema().getName()),
                        // SEQUENCE_NAME
                        identifier(s.getName()),
                        // CURRENT_VALUE
                        String.valueOf(s.getCurrentValue()),
                        // INCREMENT
                        String.valueOf(s.getIncrement()),
                        // IS_GENERATED
                        s.getBelongsToTable() ? "TRUE" : "FALSE",
                        // REMARKS
                        replaceNullWithEmpty(s.getComment()),
                        // CACHE
                        String.valueOf(s.getCacheSize()),
                        // MIN_VALUE
                        String.valueOf(s.getMinValue()),
                        // MAX_VALUE
                        String.valueOf(s.getMaxValue()),
                        // IS_CYCLE
                        s.getCycle() ? "TRUE" : "FALSE",
                        // ID
                        "" + s.getId()
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
                            "" + u.getId()
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
                            "" + r.getId()
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
                    String rightType = grantee.getType() == DbObject.USER ?
                            "USER" : "ROLE";
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
                        String tableName = (table != null) ? identifier(table.getName()) : "";
                        String schemaName = (schema != null) ? identifier(schema.getName()) : "";
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
                                "" + r.getId()
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
                                "" + r.getId()
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
                    int returnsResult = method.getDataType() == Value.NULL ?
                            DatabaseMetaData.procedureNoResult :
                            DatabaseMetaData.procedureReturnsResult;
                    add(rows,
                            // ALIAS_CATALOG
                            catalog,
                            // ALIAS_SCHEMA
                            alias.getSchema().getName(),
                            // ALIAS_NAME
                            identifier(alias.getName()),
                            // JAVA_CLASS
                            alias.getJavaClassName(),
                            // JAVA_METHOD
                            alias.getJavaMethodName(),
                            // DATA_TYPE
                            "" + DataType.convertTypeToSQLType(method.getDataType()),
                            // TYPE_NAME
                            DataType.getDataType(method.getDataType()).name,
                            // COLUMN_COUNT INT
                            "" + method.getParameterCount(),
                            // RETURNS_RESULT SMALLINT
                            "" + returnsResult,
                            // REMARKS
                            replaceNullWithEmpty(alias.getComment()),
                            // ID
                            "" + alias.getId(),
                            // SOURCE
                            alias.getSource()
                            // when adding more columns, see also below
                    );
                }
            }
            for (UserAggregate agg : database.getAllAggregates()) {
                int returnsResult = DatabaseMetaData.procedureReturnsResult;
                add(rows,
                        // ALIAS_CATALOG
                        catalog,
                        // ALIAS_SCHEMA
                        Constants.SCHEMA_MAIN,
                        // ALIAS_NAME
                        identifier(agg.getName()),
                        // JAVA_CLASS
                        agg.getJavaClassName(),
                        // JAVA_METHOD
                        "",
                        // DATA_TYPE
                        "" + DataType.convertTypeToSQLType(Value.NULL),
                        // TYPE_NAME
                        DataType.getDataType(Value.NULL).name,
                        // COLUMN_COUNT INT
                        "1",
                        // RETURNS_RESULT SMALLINT
                        "" + returnsResult,
                        // REMARKS
                        replaceNullWithEmpty(agg.getComment()),
                        // ID
                        "" + agg.getId(),
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
                                identifier(alias.getName()),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                "" + method.getParameterCount(),
                                // POS INT
                                "0",
                                // COLUMN_NAME
                                "P0",
                                // DATA_TYPE
                                "" + DataType.convertTypeToSQLType(method.getDataType()),
                                // TYPE_NAME
                                dt.name,
                                // PRECISION INT
                                "" + MathUtils.convertLongToInt(dt.defaultPrecision),
                                // SCALE
                                "" + dt.defaultScale,
                                // RADIX
                                "10",
                                // NULLABLE SMALLINT
                                "" + DatabaseMetaData.columnNullableUnknown,
                                // COLUMN_TYPE
                                "" + DatabaseMetaData.procedureColumnReturn,
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
                        int nullable = clazz.isPrimitive() ? DatabaseMetaData.columnNoNulls
                                : DatabaseMetaData.columnNullable;
                        add(rows,
                                // ALIAS_CATALOG
                                catalog,
                                // ALIAS_SCHEMA
                                alias.getSchema().getName(),
                                // ALIAS_NAME
                                identifier(alias.getName()),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                "" + method.getParameterCount(),
                                // POS INT
                                "" + (k + (method.hasConnectionParam() ? 0 : 1)),
                                // COLUMN_NAME
                                "P" + (k + 1),
                                // DATA_TYPE
                                "" + DataType.convertTypeToSQLType(dt.type),
                                // TYPE_NAME
                                dt.name,
                                // PRECISION INT
                                "" + MathUtils.convertLongToInt(dt.defaultPrecision),
                                // SCALE
                                "" + dt.defaultScale,
                                // RADIX
                                "10",
                                // NULLABLE SMALLINT
                                "" + nullable,
                                // COLUMN_TYPE
                                "" + DatabaseMetaData.procedureColumnIn,
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
                        identifier(schema.getName()),
                        // SCHEMA_OWNER
                        identifier(schema.getOwner().getName()),
                        // DEFAULT_CHARACTER_SET_NAME
                        CHARACTER_SET_NAME,
                        // DEFAULT_COLLATION_NAME
                        collation,
                        // IS_DEFAULT
                        Constants.SCHEMA_MAIN.equals(
                                schema.getName()) ? "TRUE" : "FALSE",
                        // REMARKS
                        replaceNullWithEmpty(schema.getComment()),
                        // ID
                        "" + schema.getId()
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
                String tableName = identifier(table.getName());
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
                String tableName = identifier(table.getName());
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
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                TableView view = (TableView) table;
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
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
                        "" + view.getId()
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
                String tableName = identifier(refTab.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                int update = getRefAction(ref.getUpdateAction());
                int delete = getRefAction(ref.getDeleteAction());
                for (int j = 0; j < cols.length; j++) {
                    add(rows,
                            // PKTABLE_CATALOG
                            catalog,
                            // PKTABLE_SCHEMA
                            identifier(refTab.getSchema().getName()),
                            // PKTABLE_NAME
                            identifier(refTab.getName()),
                            // PKCOLUMN_NAME
                            identifier(refCols[j].column.getName()),
                            // FKTABLE_CATALOG
                            catalog,
                            // FKTABLE_SCHEMA
                            identifier(tab.getSchema().getName()),
                            // FKTABLE_NAME
                            identifier(tab.getName()),
                            // FKCOLUMN_NAME
                            identifier(cols[j].column.getName()),
                            // ORDINAL_POSITION
                            String.valueOf(j + 1),
                            // UPDATE_RULE SMALLINT
                            String.valueOf(update),
                            // DELETE_RULE SMALLINT
                            String.valueOf(delete),
                            // FK_NAME
                            identifier(ref.getName()),
                            // PK_NAME
                            identifier(ref.getUniqueIndex().getName()),
                            // DEFERRABILITY
                            "" + DatabaseMetaData.importedKeyNotDeferrable
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
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (constraintType == Constraint.Type.CHECK) {
                    checkExpression = ((ConstraintCheck) constraint).getExpression().getSQL();
                } else if (constraintType == Constraint.Type.UNIQUE ||
                        constraintType == Constraint.Type.PRIMARY_KEY) {
                    indexColumns = ((ConstraintUnique) constraint).getColumns();
                } else if (constraintType == Constraint.Type.REFERENTIAL) {
                    indexColumns = ((ConstraintReferential) constraint).getColumns();
                }
                String columnList = null;
                if (indexColumns != null) {
                    StatementBuilder buff = new StatementBuilder();
                    for (IndexColumn col : indexColumns) {
                        buff.appendExceptFirst(",");
                        buff.append(col.column.getName());
                    }
                    columnList = buff.toString();
                }
                add(rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        identifier(constraint.getSchema().getName()),
                        // CONSTRAINT_NAME
                        identifier(constraint.getName()),
                        // CONSTRAINT_TYPE
                        constraintType.toString(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
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
                        "" + constraint.getId()
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
                        identifier(constant.getSchema().getName()),
                        // CONSTANT_NAME
                        identifier(constant.getName()),
                        // CONSTANT_TYPE
                        "" + DataType.convertTypeToSQLType(expr.getType()),
                        // REMARKS
                        replaceNullWithEmpty(constant.getComment()),
                        // SQL
                        expr.getSQL(),
                        // ID
                        "" + constant.getId()
                    );
            }
            break;
        }
        case DOMAINS: {
            for (UserDataType dt : database.getAllUserDataTypes()) {
                Column col = dt.getColumn();
                add(rows,
                        // DOMAIN_CATALOG
                        catalog,
                        // DOMAIN_SCHEMA
                        Constants.SCHEMA_MAIN,
                        // DOMAIN_NAME
                        identifier(dt.getName()),
                        // COLUMN_DEFAULT
                        col.getDefaultSQL(),
                        // IS_NULLABLE
                        col.isNullable() ? "YES" : "NO",
                        // DATA_TYPE
                        "" + col.getDataType().sqlType,
                        // PRECISION INT
                        "" + col.getPrecisionAsInt(),
                        // SCALE INT
                        "" + col.getScale(),
                        // TYPE_NAME
                        col.getDataType().name,
                        // SELECTIVITY INT
                        "" + col.getSelectivity(),
                        // CHECK_CONSTRAINT
                        "" + col.getCheckConstraintSQL(session, "VALUE"),
                        // REMARKS
                        replaceNullWithEmpty(dt.getComment()),
                        // SQL
                        "" + dt.getCreateSQL(),
                        // ID
                        "" + dt.getId()
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
                        identifier(trigger.getSchema().getName()),
                        // TRIGGER_NAME
                        identifier(trigger.getName()),
                        // TRIGGER_TYPE
                        trigger.getTypeNameList(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        identifier(table.getName()),
                        // BEFORE BIT
                        "" + trigger.isBefore(),
                        // JAVA_CLASS
                        trigger.getTriggerClassName(),
                        // QUEUE_SIZE INT
                        "" + trigger.getQueueSize(),
                        // NO_WAIT BIT
                        "" + trigger.isNoWait(),
                        // REMARKS
                        replaceNullWithEmpty(trigger.getComment()),
                        // SQL
                        trigger.getCreateSQL(),
                        // ID
                        "" + trigger.getId()
                );
            }
            break;
        }
        case SESSIONS: {
            long now = System.currentTimeMillis();
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    Command command = s.getCurrentCommand();
                    long start = s.getCurrentCommandStart();
                    if (start == 0) {
                        start = now;
                    }
                    add(rows,
                            // ID
                            "" + s.getId(),
                            // USER_NAME
                            s.getUser().getName(),
                            // SESSION_START
                            new Timestamp(s.getSessionStart()).toString(),
                            // STATEMENT
                            command == null ? null : command.toString(),
                            // STATEMENT_START
                            new Timestamp(start).toString(),
                            // CONTAINS_UNCOMMITTED
                            "" + s.containsUncommitted()
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
                                "" + s.getId(),
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
                add(rows,
                        // KEY
                        "@" + name,
                        // SQL
                        "SET @" + name + " " + v.getSQL()
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
                StatementBuilder buff = new StatementBuilder(
                        "SET SCHEMA_SEARCH_PATH ");
                for (String p : path) {
                    buff.appendExceptFirst(", ");
                    buff.append(StringUtils.quoteIdentifier(p));
                }
                add(rows,
                        // KEY
                        "SCHEMA_SEARCH_PATH",
                        // SQL
                        buff.toString()
                );
            }
            String schema = session.getCurrentSchemaName();
            if (schema != null) {
                add(rows,
                        // KEY
                        "SCHEMA",
                        // SQL
                        "SET SCHEMA " + StringUtils.quoteIdentifier(schema)
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
                            "" + entry.count,
                            // MIN_EXECUTION_TIME
                            "" + entry.executionTimeMinNanos / 1000d / 1000,
                            // MAX_EXECUTION_TIME
                            "" + entry.executionTimeMaxNanos / 1000d / 1000,
                            // CUMULATIVE_EXECUTION_TIME
                            "" + entry.executionTimeCumulativeNanos / 1000d / 1000,
                            // AVERAGE_EXECUTION_TIME
                            "" + entry.executionTimeMeanNanos / 1000d / 1000,
                            // STD_DEV_EXECUTION_TIME
                            "" + entry.getExecutionTimeStandardDeviation() / 1000d / 1000,
                            // MIN_ROW_COUNT
                            "" + entry.rowCountMin,
                            // MAX_ROW_COUNT
                            "" + entry.rowCountMax,
                            // CUMULATIVE_ROW_COUNT
                            "" + entry.rowCountCumulative,
                            // AVERAGE_ROW_COUNT
                            "" + entry.rowCountMean,
                            // STD_DEV_ROW_COUNT
                            "" + entry.getRowCountStandardDeviation()
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
                        identifier(synonym.getSchema().getName()),
                        // SYNONYM_NAME
                        identifier(synonym.getName()),
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
                        "" + synonym.getId()
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
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                add(rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        identifier(constraint.getSchema().getName()),
                        // CONSTRAINT_NAME
                        identifier(constraint.getName()),
                        // CONSTRAINT_TYPE
                        constraintType.getSqlName(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
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
                String tableName = identifier(table.getName());
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
                    String ordinalPosition = Integer.toString(i + 1);
                    String positionInUniqueConstraint;
                    if (constraintType == Constraint.Type.REFERENTIAL) {
                        positionInUniqueConstraint = ordinalPosition;
                        if (referenced != null) {
                            Column c = ((ConstraintReferential) constraint).getRefColumns()[i].column;
                            IndexColumn[] refColumns = referenced.getColumns();
                            for (int j = 0; j < refColumns.length; j++) {
                                if (refColumns[j].column.equals(c)) {
                                    positionInUniqueConstraint = Integer.toString(j + 1);
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
                            identifier(constraint.getSchema().getName()),
                            // CONSTRAINT_NAME
                            identifier(constraint.getName()),
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            identifier(table.getSchema().getName()),
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
                        identifier(constraint.getSchema().getName()),
                        // CONSTRAINT_NAME
                        identifier(constraint.getName()),
                        // UNIQUE_CONSTRAINT_CATALOG
                        catalog,
                        // UNIQUE_CONSTRAINT_SCHEMA
                        identifier(unique.getSchema().getName()),
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

    private static int getRefAction(ConstraintActionType action) {
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
                    identifier(table.getSchema().getName()),
                    // TABLE_NAME
                    identifier(table.getName()),
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
                    identifier(table.getSchema().getName()),
                    // TABLE_NAME
                    identifier(table.getName()),
                    // COLUMN_NAME
                    identifier(column),
                    // PRIVILEGE_TYPE
                    right,
                    // IS_GRANTABLE
                    isGrantable
            );
        }
    }

    private void add(ArrayList<Row> rows, String... strings) {
        Value[] values = new Value[strings.length];
        for (int i = 0; i < strings.length; i++) {
            String s = strings[i];
            Value v = (s == null) ? (Value) ValueNull.INSTANCE : ValueString.get(s);
            Column col = columns[i];
            v = col.convert(v);
            values[i] = v;
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
        ArrayList<Index> list = New.arrayList();
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
