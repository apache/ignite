/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
package org.h2.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import org.h2.engine.Session;
import org.h2.expression.Expression;

/**
 * A factory for column names.
 */
public class ColumnNamer {

    private static final String DEFAULT_COLUMN_NAME = "DEFAULT";

    private final ColumnNamerConfiguration configuration;
    private final Session session;
    private final Set<String> existingColumnNames = new HashSet<>();

    public ColumnNamer(Session session) {
        this.session = session;
        if (this.session != null && this.session.getColumnNamerConfiguration() != null) {
            // use original from session
            this.configuration = this.session.getColumnNamerConfiguration();
        } else {
            // detached namer, create new
            this.configuration = ColumnNamerConfiguration.getDefault();
            if (session != null) {
                session.setColumnNamerConfiguration(this.configuration);
            }
        }
    }

    /**
     * Create a standardized column name that isn't null and doesn't have a CR/LF in it.
     * @param columnExp the column expression
     * @param indexOfColumn index of column in below array
     * @param columnNameOverides array of overriding column names
     * @return the new column name
     */
    public String getColumnName(Expression columnExp, int indexOfColumn, String[] columnNameOverides) {
        String columnNameOverride = null;
        if (columnNameOverides != null && columnNameOverides.length > indexOfColumn) {
            columnNameOverride = columnNameOverides[indexOfColumn];
        }
        return getColumnName(columnExp, indexOfColumn, columnNameOverride);
    }

    /**
     * Create a standardized column name that isn't null and doesn't have a CR/LF in it.
     * @param columnExp the column expression
     * @param indexOfColumn index of column in below array
     * @param columnNameOverride single overriding column name
     * @return the new column name
     */
    public String getColumnName(Expression columnExp, int indexOfColumn, String columnNameOverride) {
        // try a name from the column name override
        String columnName = null;
        if (columnNameOverride != null) {
            columnName = columnNameOverride;
            if (!isAllowableColumnName(columnName)) {
                columnName = fixColumnName(columnName);
            }
            if (!isAllowableColumnName(columnName)) {
                columnName = null;
            }
        }
        // try a name from the column alias
        if (columnName == null && columnExp.getAlias() != null && !DEFAULT_COLUMN_NAME.equals(columnExp.getAlias())) {
            columnName = columnExp.getAlias();
            if (!isAllowableColumnName(columnName)) {
                columnName = fixColumnName(columnName);
            }
            if (!isAllowableColumnName(columnName)) {
                columnName = null;
            }
        }
        // try a name derived from the column expression SQL
        if (columnName == null && columnExp.getColumnName() != null
                && !DEFAULT_COLUMN_NAME.equals(columnExp.getColumnName())) {
            columnName = columnExp.getColumnName();
            if (!isAllowableColumnName(columnName)) {
                columnName = fixColumnName(columnName);
            }
            if (!isAllowableColumnName(columnName)) {
                columnName = null;
            }
        }
        // try a name derived from the column expression plan SQL
        if (columnName == null && columnExp.getSQL() != null && !DEFAULT_COLUMN_NAME.equals(columnExp.getSQL())) {
            columnName = columnExp.getSQL();
            if (!isAllowableColumnName(columnName)) {
                columnName = fixColumnName(columnName);
            }
            if (!isAllowableColumnName(columnName)) {
                columnName = null;
            }
        }
        // go with a innocuous default name pattern
        if (columnName == null) {
            columnName = configuration.getDefaultColumnNamePattern().replace("$$", "" + (indexOfColumn + 1));
        }
        if (existingColumnNames.contains(columnName) && configuration.isGenerateUniqueColumnNames()) {
            columnName = generateUniqueName(columnName);
        }
        existingColumnNames.add(columnName);
        return columnName;
    }

    private String generateUniqueName(String columnName) {
        String newColumnName = columnName;
        int loopCount = 2;
        while (existingColumnNames.contains(newColumnName)) {
            String loopCountString = "_" + loopCount;
            newColumnName = columnName.substring(0,
                    Math.min(columnName.length(), configuration.getMaxIdentiferLength() - loopCountString.length()))
                    + loopCountString;
            loopCount++;
        }
        return newColumnName;
    }

    private boolean isAllowableColumnName(String proposedName) {

        // check null
        if (proposedName == null) {
            return false;
        }
        // check size limits
        if (proposedName.length() > configuration.getMaxIdentiferLength() || proposedName.length() == 0) {
            return false;
        }
        Matcher match = configuration.getCompiledRegularExpressionMatchAllowed().matcher(proposedName);
        return match.matches();
    }

    private String fixColumnName(String proposedName) {
        Matcher match = configuration.getCompiledRegularExpressionMatchDisallowed().matcher(proposedName);
        proposedName = match.replaceAll("");

        // check size limits - then truncate
        if (proposedName.length() > configuration.getMaxIdentiferLength()) {
            proposedName = proposedName.substring(0, configuration.getMaxIdentiferLength());
        }

        return proposedName;
    }

    public ColumnNamerConfiguration getConfiguration() {
        return configuration;
    }

}
