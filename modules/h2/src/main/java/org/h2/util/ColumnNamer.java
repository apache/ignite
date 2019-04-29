/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
package org.h2.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.h2.engine.Session;
import org.h2.expression.Expression;

/**
 * A factory for column names.
 */
public class ColumnNamer {

    private static final String DEFAULT_COLUMN_NAME = "DEFAULT";

    private final ColumnNamerConfiguration configuration;
    private final Set<String> existingColumnNames = new HashSet<>();

    public ColumnNamer(Session session) {
        if (session != null && session.getColumnNamerConfiguration() != null) {
            // use original from session
            this.configuration = session.getColumnNamerConfiguration();
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
        String columnName = getColumnName(columnNameOverride, null);
        if (columnName == null) {
            // try a name from the column alias
            columnName = getColumnName(columnExp.getAlias(), DEFAULT_COLUMN_NAME);
            if (columnName == null) {
                // try a name derived from the column expression SQL
                columnName = getColumnName(columnExp.getColumnName(), DEFAULT_COLUMN_NAME);
                if (columnName == null) {
                    // try a name derived from the column expression plan SQL
                    columnName = getColumnName(columnExp.getSQL(false), DEFAULT_COLUMN_NAME);
                    // go with a innocuous default name pattern
                    if (columnName == null) {
                        columnName = configuration.getDefaultColumnNamePattern()
                                .replace("$$", Integer.toString(indexOfColumn + 1));
                    }
                }
            }
        }
        if (existingColumnNames.contains(columnName) && configuration.isGenerateUniqueColumnNames()) {
            columnName = generateUniqueName(columnName);
        }
        existingColumnNames.add(columnName);
        return columnName;
    }

    private String getColumnName(String proposedName, String disallowedName) {
        String columnName = null;
        if (proposedName != null && !proposedName.equals(disallowedName)) {
            if (isAllowableColumnName(proposedName)) {
                columnName = proposedName;
            } else {
                proposedName = fixColumnName(proposedName);
                if (isAllowableColumnName(proposedName)) {
                    columnName = proposedName;
                }
            }
        }
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
        int length = proposedName.length();
        if (length > configuration.getMaxIdentiferLength() || length == 0) {
            return false;
        }
        Pattern allowed = configuration.getCompiledRegularExpressionMatchAllowed();
        return allowed == null || allowed.matcher(proposedName).matches();
    }

    private String fixColumnName(String proposedName) {
        Pattern disallowed = configuration.getCompiledRegularExpressionMatchDisallowed();
        if (disallowed == null) {
            proposedName = StringUtils.replaceAll(proposedName, "\u0000", "");
        } else {
            proposedName = disallowed.matcher(proposedName).replaceAll("");
        }

        // check size limits - then truncate
        int length = proposedName.length(), maxLength = configuration.getMaxIdentiferLength();
        if (length > maxLength) {
            proposedName = proposedName.substring(0, maxLength);
        }

        return proposedName;
    }

    public ColumnNamerConfiguration getConfiguration() {
        return configuration;
    }

}
