/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.ResultSetMetaData;
import java.util.Arrays;
import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.SequenceValue;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueEnum;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;

/**
 * This class represents a column in a table.
 */
public class Column {

    /**
     * The name of the rowid pseudo column.
     */
    public static final String ROWID = "_ROWID_";

    /**
     * This column is not nullable.
     */
    public static final int NOT_NULLABLE =
            ResultSetMetaData.columnNoNulls;

    /**
     * This column is nullable.
     */
    public static final int NULLABLE =
            ResultSetMetaData.columnNullable;

    /**
     * It is not know whether this column is nullable.
     */
    public static final int NULLABLE_UNKNOWN =
            ResultSetMetaData.columnNullableUnknown;

    private final int type;
    private long precision;
    private int scale;
    private String[] enumerators;
    private int displaySize;
    private Table table;
    private String name;
    private int columnId;
    private boolean nullable = true;
    private Expression defaultExpression;
    private Expression onUpdateExpression;
    private Expression checkConstraint;
    private String checkConstraintSQL;
    private String originalSQL;
    private boolean autoIncrement;
    private long start;
    private long increment;
    private boolean convertNullToDefault;
    private Sequence sequence;
    private boolean isComputed;
    private TableFilter computeTableFilter;
    private int selectivity;
    private SingleColumnResolver resolver;
    private String comment;
    private boolean primaryKey;
    private boolean visible = true;

    public Column(String name, int type) {
        this(name, type, -1, -1, -1, null);
    }

    public Column(String name, int type, long precision, int scale,
            int displaySize) {
        this(name, type, precision, scale, displaySize, null);
    }

    public Column(String name, int type, long precision, int scale,
            int displaySize, String[] enumerators) {
        this.name = name;
        this.type = type;
        if (precision == -1 && scale == -1 && displaySize == -1 && type != Value.UNKNOWN) {
            DataType dt = DataType.getDataType(type);
            precision = dt.defaultPrecision;
            scale = dt.defaultScale;
            displaySize = dt.defaultDisplaySize;
        }
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
        this.enumerators = enumerators;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Column)) {
            return false;
        }
        Column other = (Column) o;
        if (table == null || other.table == null ||
                name == null || other.name == null) {
            return false;
        }
        if (table != other.table) {
            return false;
        }
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        if (table == null || name == null) {
            return 0;
        }
        return table.getId() ^ name.hashCode();
    }

    public boolean isEnumerated() {
        return type == Value.ENUM;
    }

    public Column getClone() {
        Column newColumn = new Column(name, type, precision, scale, displaySize, enumerators);
        newColumn.copy(this);
        return newColumn;
    }

    /**
     * Convert a value to this column's type.
     *
     * @param v the value
     * @return the value
     */
    public Value convert(Value v) {
        return convert(v, null);
    }

    /**
     * Convert a value to this column's type using the given {@link Mode}.
     * <p>
     * Use this method in case the conversion is Mode-dependent.
     *
     * @param v the value
     * @param mode the database {@link Mode} to use
     * @return the value
     */
    public Value convert(Value v, Mode mode) {
        try {
            return v.convertTo(type, MathUtils.convertLongToInt(precision), mode, this, getEnumerators());
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                String target = (table == null ? "" : table.getName() + ": ") +
                        getCreateSQL();
                throw DbException.get(
                        ErrorCode.DATA_CONVERSION_ERROR_1, e,
                        v.getSQL() + " (" + target + ")");
            }
            throw e;
        }
    }

    boolean getComputed() {
        return isComputed;
    }

    /**
     * Compute the value of this computed column.
     *
     * @param session the session
     * @param row the row
     * @return the value
     */
    synchronized Value computeValue(Session session, Row row) {
        computeTableFilter.setSession(session);
        computeTableFilter.set(row);
        return defaultExpression.getValue(session);
    }

    /**
     * Set the default value in the form of a computed expression of other
     * columns.
     *
     * @param expression the computed expression
     */
    public void setComputedExpression(Expression expression) {
        this.isComputed = true;
        this.defaultExpression = expression;
    }

    /**
     * Set the table and column id.
     *
     * @param table the table
     * @param columnId the column index
     */
    public void setTable(Table table, int columnId) {
        this.table = table;
        this.columnId = columnId;
    }

    public Table getTable() {
        return table;
    }

    /**
     * Set the default expression.
     *
     * @param session the session
     * @param defaultExpression the default expression
     */
    public void setDefaultExpression(Session session,
            Expression defaultExpression) {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = ValueExpression.get(
                        defaultExpression.getValue(session));
            }
        }
        this.defaultExpression = defaultExpression;
    }

    /**
     * Set the on update expression.
     *
     * @param session the session
     * @param onUpdateExpression the on update expression
     */
    public void setOnUpdateExpression(Session session, Expression onUpdateExpression) {
        // also to test that no column names are used
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
            if (onUpdateExpression.isConstant()) {
                onUpdateExpression = ValueExpression.get(onUpdateExpression.getValue(session));
            }
        }
        this.onUpdateExpression = onUpdateExpression;
    }

    public int getColumnId() {
        return columnId;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(name);
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public long getPrecision() {
        return precision;
    }

    public void setPrecision(long p) {
        precision = p;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    public int getScale() {
        return scale;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    public String[] getEnumerators() {
        return enumerators;
    }

    public void setEnumerators(String[] enumerators) {
        this.enumerators = enumerators;
    }

    public boolean getVisible() {
        return visible;
    }

    public void setVisible(boolean b) {
        visible = b;
    }

    /**
     * Validate the value, convert it if required, and update the sequence value
     * if required. If the value is null, the default value (NULL if no default
     * is set) is returned. Check constraints are validated as well.
     *
     * @param session the session
     * @param value the value or null
     * @return the new or converted value
     */
    public Value validateConvertUpdateSequence(Session session, Value value) {
        // take a local copy of defaultExpression to avoid holding the lock
        // while calling getValue
        final Expression localDefaultExpression;
        synchronized (this) {
            localDefaultExpression = defaultExpression;
        }
        if (value == null) {
            if (localDefaultExpression == null) {
                value = ValueNull.INSTANCE;
            } else {
                value = localDefaultExpression.getValue(session).convertTo(type);
                session.getGeneratedKeys().add(this);
                if (primaryKey) {
                    session.setLastIdentity(value);
                }
            }
        }
        Mode mode = session.getDatabase().getMode();
        if (value == ValueNull.INSTANCE) {
            if (convertNullToDefault) {
                value = localDefaultExpression.getValue(session).convertTo(type);
                session.getGeneratedKeys().add(this);
            }
            if (value == ValueNull.INSTANCE && !nullable) {
                if (mode.convertInsertNullToZero) {
                    DataType dt = DataType.getDataType(type);
                    if (dt.decimal) {
                        value = ValueInt.get(0).convertTo(type);
                    } else if (dt.type == Value.TIMESTAMP) {
                        value = ValueTimestamp.fromMillis(session.getTransactionStart());
                    } else if (dt.type == Value.TIMESTAMP_TZ) {
                        long ms = session.getTransactionStart();
                        value = ValueTimestampTimeZone.fromDateValueAndNanos(
                                DateTimeUtils.dateValueFromDate(ms),
                                DateTimeUtils.nanosFromDate(ms), (short) 0);
                    } else if (dt.type == Value.TIME) {
                        value = ValueTime.fromNanos(0);
                    } else if (dt.type == Value.DATE) {
                        value = ValueDate.fromMillis(session.getTransactionStart());
                    } else {
                        value = ValueString.get("").convertTo(type);
                    }
                } else {
                    throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, name);
                }
            }
        }
        if (checkConstraint != null) {
            resolver.setValue(value);
            Value v;
            synchronized (this) {
                v = checkConstraint.getValue(session);
            }
            // Both TRUE and NULL are ok
            if (v != ValueNull.INSTANCE && !v.getBoolean()) {
                throw DbException.get(
                        ErrorCode.CHECK_CONSTRAINT_VIOLATED_1,
                        checkConstraint.getSQL());
            }
        }
        value = value.convertScale(mode.convertOnlyToSmallerScale, scale);
        if (precision > 0) {
            if (!value.checkPrecision(precision)) {
                String s = value.getTraceSQL();
                if (s.length() > 127) {
                    s = s.substring(0, 128) + "...";
                }
                throw DbException.get(ErrorCode.VALUE_TOO_LONG_2,
                        getCreateSQL(), s + " (" + value.getPrecision() + ")");
            }
        }
        if (isEnumerated() && value != ValueNull.INSTANCE) {
            if (!ValueEnum.isValid(enumerators, value)) {
                String s = value.getTraceSQL();
                if (s.length() > 127) {
                    s = s.substring(0, 128) + "...";
                }
                throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED,
                        getCreateSQL(), s);
            }

            value = ValueEnum.get(enumerators, value.getInt());
        }
        updateSequenceIfRequired(session, value);
        return value;
    }

    private void updateSequenceIfRequired(Session session, Value value) {
        if (sequence != null) {
            long current = sequence.getCurrentValue();
            long inc = sequence.getIncrement();
            long now = value.getLong();
            boolean update = false;
            if (inc > 0 && now > current) {
                update = true;
            } else if (inc < 0 && now < current) {
                update = true;
            }
            if (update) {
                sequence.modify(now + inc, null, null, null);
                session.setLastIdentity(ValueLong.get(now));
                sequence.flush(session);
            }
        }
    }

    /**
     * Convert the auto-increment flag to a sequence that is linked with this
     * table.
     *
     * @param session the session
     * @param schema the schema where the sequence should be generated
     * @param id the object id
     * @param temporary true if the sequence is temporary and does not need to
     *            be stored
     */
    public void convertAutoIncrementToSequence(Session session, Schema schema,
            int id, boolean temporary) {
        if (!autoIncrement) {
            DbException.throwInternalError();
        }
        if ("IDENTITY".equals(originalSQL)) {
            originalSQL = "BIGINT";
        } else if ("SERIAL".equals(originalSQL)) {
            originalSQL = "INT";
        }
        String sequenceName;
        do {
            ValueUuid uuid = ValueUuid.getNewRandom();
            String s = uuid.getString();
            s = StringUtils.toUpperEnglish(s.replace('-', '_'));
            sequenceName = "SYSTEM_SEQUENCE_" + s;
        } while (schema.findSequence(sequenceName) != null);
        Sequence seq = new Sequence(schema, id, sequenceName, start, increment);
        seq.setTemporary(temporary);
        session.getDatabase().addSchemaObject(session, seq);
        setAutoIncrement(false, 0, 0);
        SequenceValue seqValue = new SequenceValue(seq);
        setDefaultExpression(session, seqValue);
        setSequence(seq);
    }

    /**
     * Prepare all expressions of this column.
     *
     * @param session the session
     */
    public void prepareExpression(Session session) {
        if (defaultExpression != null || onUpdateExpression != null) {
            computeTableFilter = new TableFilter(session, table, null, false, null, 0, null);
            if (defaultExpression != null) {
                defaultExpression.mapColumns(computeTableFilter, 0);
                defaultExpression = defaultExpression.optimize(session);
            }
            if (onUpdateExpression != null) {
                onUpdateExpression.mapColumns(computeTableFilter, 0);
                onUpdateExpression = onUpdateExpression.optimize(session);
            }
        }
    }

    public String getCreateSQLWithoutName() {
        return getCreateSQL(false);
    }

    public String getCreateSQL() {
        return getCreateSQL(true);
    }

    private String getCreateSQL(boolean includeName) {
        StringBuilder buff = new StringBuilder();
        if (includeName && name != null) {
            buff.append(Parser.quoteIdentifier(name)).append(' ');
        }
        if (originalSQL != null) {
            buff.append(originalSQL);
        } else {
            buff.append(DataType.getDataType(type).name);
            switch (type) {
            case Value.DECIMAL:
                buff.append('(').append(precision).append(", ").append(scale).append(')');
                break;
            case Value.ENUM:
                buff.append('(');
                for (int i = 0; i < enumerators.length; i++) {
                    buff.append('\'').append(enumerators[i]).append('\'');
                    if(i < enumerators.length - 1) {
                        buff.append(',');
                    }
                }
                buff.append(')');
                break;
            case Value.BYTES:
            case Value.STRING:
            case Value.STRING_IGNORECASE:
            case Value.STRING_FIXED:
                if (precision < Integer.MAX_VALUE) {
                    buff.append('(').append(precision).append(')');
                }
                break;
            default:
            }
        }

        if (!visible) {
            buff.append(" INVISIBLE ");
        }

        if (defaultExpression != null) {
            String sql = defaultExpression.getSQL();
            if (sql != null) {
                if (isComputed) {
                    buff.append(" AS ").append(sql);
                } else if (defaultExpression != null) {
                    buff.append(" DEFAULT ").append(sql);
                }
            }
        }
        if (onUpdateExpression != null) {
            String sql = onUpdateExpression.getSQL();
            if (sql != null) {
                buff.append(" ON UPDATE ").append(sql);
            }
        }
        if (!nullable) {
            buff.append(" NOT NULL");
        }
        if (convertNullToDefault) {
            buff.append(" NULL_TO_DEFAULT");
        }
        if (sequence != null) {
            buff.append(" SEQUENCE ").append(sequence.getSQL());
        }
        if (selectivity != 0) {
            buff.append(" SELECTIVITY ").append(selectivity);
        }
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (checkConstraint != null) {
            buff.append(" CHECK ").append(checkConstraintSQL);
        }
        return buff.toString();
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setOriginalSQL(String original) {
        originalSQL = original;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    public Expression getOnUpdateExpression() {
        return onUpdateExpression;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    /**
     * Set the autoincrement flag and related properties of this column.
     *
     * @param autoInc the new autoincrement flag
     * @param start the sequence start value
     * @param increment the sequence increment
     */
    public void setAutoIncrement(boolean autoInc, long start, long increment) {
        this.autoIncrement = autoInc;
        this.start = start;
        this.increment = increment;
        this.nullable = false;
        if (autoInc) {
            convertNullToDefault = true;
        }
    }

    public void setConvertNullToDefault(boolean convert) {
        this.convertNullToDefault = convert;
    }

    /**
     * Rename the column. This method will only set the column name to the new
     * value.
     *
     * @param newName the new column name
     */
    public void rename(String newName) {
        this.name = newName;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public Sequence getSequence() {
        return sequence;
    }

    /**
     * Get the selectivity of the column. Selectivity 100 means values are
     * unique, 10 means every distinct value appears 10 times on average.
     *
     * @return the selectivity
     */
    public int getSelectivity() {
        return selectivity == 0 ? Constants.SELECTIVITY_DEFAULT : selectivity;
    }

    /**
     * Set the new selectivity of a column.
     *
     * @param selectivity the new value
     */
    public void setSelectivity(int selectivity) {
        selectivity = selectivity < 0 ? 0 : (selectivity > 100 ? 100 : selectivity);
        this.selectivity = selectivity;
    }

    /**
     * Add a check constraint expression to this column. An existing check
     * constraint constraint is added using AND.
     *
     * @param session the session
     * @param expr the (additional) constraint
     */
    public void addCheckConstraint(Session session, Expression expr) {
        if (expr == null) {
            return;
        }
        resolver = new SingleColumnResolver(this);
        synchronized (this) {
            String oldName = name;
            if (name == null) {
                name = "VALUE";
            }
            expr.mapColumns(resolver, 0);
            name = oldName;
        }
        expr = expr.optimize(session);
        resolver.setValue(ValueNull.INSTANCE);
        // check if the column is mapped
        synchronized (this) {
            expr.getValue(session);
        }
        if (checkConstraint == null) {
            checkConstraint = expr;
        } else {
            checkConstraint = new ConditionAndOr(ConditionAndOr.AND, checkConstraint, expr);
        }
        checkConstraintSQL = getCheckConstraintSQL(session, name);
    }

    /**
     * Remove the check constraint if there is one.
     */
    public void removeCheckConstraint() {
        checkConstraint = null;
        checkConstraintSQL = null;
    }

    /**
     * Get the check constraint expression for this column if set.
     *
     * @param session the session
     * @param asColumnName the column name to use
     * @return the constraint expression
     */
    public Expression getCheckConstraint(Session session, String asColumnName) {
        if (checkConstraint == null) {
            return null;
        }
        Parser parser = new Parser(session);
        String sql;
        synchronized (this) {
            String oldName = name;
            name = asColumnName;
            sql = checkConstraint.getSQL();
            name = oldName;
        }
        return parser.parseExpression(sql);
    }

    String getDefaultSQL() {
        return defaultExpression == null ? null : defaultExpression.getSQL();
    }

    String getOnUpdateSQL() {
        return onUpdateExpression == null ? null : onUpdateExpression.getSQL();
    }

    int getPrecisionAsInt() {
        return MathUtils.convertLongToInt(precision);
    }

    DataType getDataType() {
        return DataType.getDataType(type);
    }

    /**
     * Get the check constraint SQL snippet.
     *
     * @param session the session
     * @param asColumnName the column name to use
     * @return the SQL snippet
     */
    String getCheckConstraintSQL(Session session, String asColumnName) {
        Expression constraint = getCheckConstraint(session, asColumnName);
        return constraint == null ? "" : constraint.getSQL();
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * Visit the default expression, the check constraint, and the sequence (if
     * any).
     *
     * @param visitor the visitor
     * @return true if every visited expression returned true, or if there are
     *         no expressions
     */
    boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.GET_DEPENDENCIES) {
            if (sequence != null) {
                visitor.getDependencies().add(sequence);
            }
        }
        if (defaultExpression != null && !defaultExpression.isEverything(visitor)) {
            return false;
        }
        if (checkConstraint != null && !checkConstraint.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Check whether the new column is of the same type and not more restricted
     * than this column.
     *
     * @param newColumn the new (target) column
     * @return true if the new column is compatible
     */
    public boolean isWideningConversion(Column newColumn) {
        if (type != newColumn.type) {
            return false;
        }
        if (precision > newColumn.precision) {
            return false;
        }
        if (scale != newColumn.scale) {
            return false;
        }
        if (nullable && !newColumn.nullable) {
            return false;
        }
        if (convertNullToDefault != newColumn.convertNullToDefault) {
            return false;
        }
        if (primaryKey != newColumn.primaryKey) {
            return false;
        }
        if (autoIncrement || newColumn.autoIncrement) {
            return false;
        }
        if (checkConstraint != null || newColumn.checkConstraint != null) {
            return false;
        }
        if (convertNullToDefault || newColumn.convertNullToDefault) {
            return false;
        }
        if (defaultExpression != null || newColumn.defaultExpression != null) {
            return false;
        }
        if (isComputed || newColumn.isComputed) {
            return false;
        }
        if (onUpdateExpression != null || newColumn.onUpdateExpression != null) {
            return false;
        }
        return true;
    }

    /**
     * Copy the data of the source column into the current column.
     *
     * @param source the source column
     */
    public void copy(Column source) {
        checkConstraint = source.checkConstraint;
        checkConstraintSQL = source.checkConstraintSQL;
        displaySize = source.displaySize;
        name = source.name;
        precision = source.precision;
        enumerators = source.enumerators == null ? null :
            Arrays.copyOf(source.enumerators, source.enumerators.length);
        scale = source.scale;
        // table is not set
        // columnId is not set
        nullable = source.nullable;
        defaultExpression = source.defaultExpression;
        onUpdateExpression = source.onUpdateExpression;
        originalSQL = source.originalSQL;
        // autoIncrement, start, increment is not set
        convertNullToDefault = source.convertNullToDefault;
        sequence = source.sequence;
        comment = source.comment;
        computeTableFilter = source.computeTableFilter;
        isComputed = source.isComputed;
        selectivity = source.selectivity;
        primaryKey = source.primaryKey;
        visible = source.visible;
    }

}
