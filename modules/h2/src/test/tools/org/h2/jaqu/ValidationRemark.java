/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import org.h2.jaqu.TableDefinition.FieldDefinition;
import org.h2.jaqu.TableInspector.ColumnInspector;
import org.h2.util.StringUtils;

/**
 * A validation remark is a result of running a model validation. Each remark
 * has a level, associated component (schema, table, column, index), and a
 * message.
 */
public class ValidationRemark {

    /**
     * The validation message level.
     */
    public static enum Level {
        CONSIDER, WARN, ERROR;
    }

    private final Level level;
    private final String table;
    private final String fieldType;
    private final String fieldName;
    private final String message;

    private ValidationRemark(Level level, String table, String type,
            String message) {
        this.level = level;
        this.table = table;
        this.fieldType = type;
        this.fieldName = "";
        this.message = message;
    }

    private ValidationRemark(Level level, String table, FieldDefinition field,
            String message) {
        this.level = level;
        this.table = table;
        this.fieldType = field.dataType;
        this.fieldName = field.columnName;
        this.message = message;
    }

    private ValidationRemark(Level level, String table, ColumnInspector col,
            String message) {
        this.level = level;
        this.table = table;
        this.fieldType = col.type;
        this.fieldName = col.name;
        this.message = message;
    }

    public static ValidationRemark consider(String table, String type,
            String message) {
        return new ValidationRemark(Level.CONSIDER, table, type, message);
    }

    public static ValidationRemark consider(String table, ColumnInspector col,
            String message) {
        return new ValidationRemark(Level.CONSIDER, table, col, message);
    }

    public static ValidationRemark warn(String table, ColumnInspector col,
            String message) {
        return new ValidationRemark(Level.WARN, table, col, message);
    }

    public static ValidationRemark warn(String table, String type,
            String message) {
        return new ValidationRemark(Level.WARN, table, type, message);
    }

    public static ValidationRemark error(String table, ColumnInspector col,
            String message) {
        return new ValidationRemark(Level.ERROR, table, col, message);
    }

    public static ValidationRemark error(String table, String type,
            String message) {
        return new ValidationRemark(Level.ERROR, table, type, message);
    }

    public static ValidationRemark error(String table, FieldDefinition field,
            String message) {
        return new ValidationRemark(Level.ERROR, table, field, message);
    }

    public ValidationRemark throwError(boolean throwOnError) {
        if (throwOnError && isError()) {
            throw new RuntimeException(toString());
        }
        return this;
    }

    public boolean isError() {
        return level.equals(Level.ERROR);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.pad(level.name(), 9, " ", true));
        sb.append(StringUtils.pad(table, 25, " ", true));
        sb.append(StringUtils.pad(fieldName, 20, " ", true));
        sb.append(' ');
        sb.append(message);
        return sb.toString();
    }

    public String toCSVString() {
        StringBuilder sb = new StringBuilder();
        sb.append(level.name()).append(',');
        sb.append(table).append(',');
        sb.append(fieldType).append(',');
        sb.append(fieldName).append(',');
        sb.append(message);
        return sb.toString();
    }

}
