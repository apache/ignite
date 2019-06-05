/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
package org.h2.util;

import java.util.regex.Pattern;
import org.h2.engine.Mode.ModeEnum;
import static org.h2.engine.Mode.ModeEnum.*;
import org.h2.message.DbException;

/**
 * The configuration for the allowed column names.
 */
public class ColumnNamerConfiguration {

    private static final String DEFAULT_COMMAND = "DEFAULT";
    private static final String REGULAR_EXPRESSION_MATCH_DISALLOWED = "REGULAR_EXPRESSION_MATCH_DISALLOWED = ";
    private static final String REGULAR_EXPRESSION_MATCH_ALLOWED = "REGULAR_EXPRESSION_MATCH_ALLOWED = ";
    private static final String DEFAULT_COLUMN_NAME_PATTERN = "DEFAULT_COLUMN_NAME_PATTERN = ";
    private static final String MAX_IDENTIFIER_LENGTH = "MAX_IDENTIFIER_LENGTH = ";
    private static final String EMULATE_COMMAND = "EMULATE = ";
    private static final String GENERATE_UNIQUE_COLUMN_NAMES = "GENERATE_UNIQUE_COLUMN_NAMES = ";

    private int maxIdentiferLength;
    private String regularExpressionMatchAllowed;
    private String regularExpressionMatchDisallowed;
    private String defaultColumnNamePattern;
    private boolean generateUniqueColumnNames;
    private Pattern compiledRegularExpressionMatchAllowed;
    private Pattern compiledRegularExpressionMatchDisallowed;

    public ColumnNamerConfiguration(int maxIdentiferLength, String regularExpressionMatchAllowed,
            String regularExpressionMatchDisallowed, String defaultColumnNamePattern,
            boolean generateUniqueColumnNames) {

        this.maxIdentiferLength = maxIdentiferLength;
        this.regularExpressionMatchAllowed = regularExpressionMatchAllowed;
        this.regularExpressionMatchDisallowed = regularExpressionMatchDisallowed;
        this.defaultColumnNamePattern = defaultColumnNamePattern;
        this.generateUniqueColumnNames = generateUniqueColumnNames;

        compiledRegularExpressionMatchAllowed = Pattern.compile(regularExpressionMatchAllowed);
        compiledRegularExpressionMatchDisallowed = Pattern.compile(regularExpressionMatchDisallowed);
    }

    public int getMaxIdentiferLength() {
        return maxIdentiferLength;
    }

    public void setMaxIdentiferLength(int maxIdentiferLength) {
        this.maxIdentiferLength = Math.max(30, maxIdentiferLength);
        if (maxIdentiferLength != getMaxIdentiferLength()) {
            throw DbException.getInvalidValueException("Illegal value (<30) in SET COLUMN_NAME_RULES",
                    "MAX_IDENTIFIER_LENGTH=" + maxIdentiferLength);
        }
    }

    public String getRegularExpressionMatchAllowed() {
        return regularExpressionMatchAllowed;
    }

    public void setRegularExpressionMatchAllowed(String regularExpressionMatchAllowed) {
        this.regularExpressionMatchAllowed = regularExpressionMatchAllowed;
    }

    public String getRegularExpressionMatchDisallowed() {
        return regularExpressionMatchDisallowed;
    }

    public void setRegularExpressionMatchDisallowed(String regularExpressionMatchDisallowed) {
        this.regularExpressionMatchDisallowed = regularExpressionMatchDisallowed;
    }

    public String getDefaultColumnNamePattern() {
        return defaultColumnNamePattern;
    }

    public void setDefaultColumnNamePattern(String defaultColumnNamePattern) {
        this.defaultColumnNamePattern = defaultColumnNamePattern;
    }

    public Pattern getCompiledRegularExpressionMatchAllowed() {
        return compiledRegularExpressionMatchAllowed;
    }

    public void setCompiledRegularExpressionMatchAllowed(Pattern compiledRegularExpressionMatchAllowed) {
        this.compiledRegularExpressionMatchAllowed = compiledRegularExpressionMatchAllowed;
    }

    public Pattern getCompiledRegularExpressionMatchDisallowed() {
        return compiledRegularExpressionMatchDisallowed;
    }

    public void setCompiledRegularExpressionMatchDisallowed(Pattern compiledRegularExpressionMatchDisallowed) {
        this.compiledRegularExpressionMatchDisallowed = compiledRegularExpressionMatchDisallowed;
    }

    /**
     * Configure the column namer.
     *
     * @param stringValue the configuration
     */
    public void configure(String stringValue) {
        try {
            if (stringValue.equalsIgnoreCase(DEFAULT_COMMAND)) {
                configure(REGULAR);
            } else if (stringValue.startsWith(EMULATE_COMMAND)) {
                configure(ModeEnum.valueOf(unquoteString(stringValue.substring(EMULATE_COMMAND.length()))));
            } else if (stringValue.startsWith(MAX_IDENTIFIER_LENGTH)) {
                int maxLength = Integer.parseInt(stringValue.substring(MAX_IDENTIFIER_LENGTH.length()));
                setMaxIdentiferLength(maxLength);
            } else if (stringValue.startsWith(GENERATE_UNIQUE_COLUMN_NAMES)) {
                setGenerateUniqueColumnNames(
                        Integer.parseInt(stringValue.substring(GENERATE_UNIQUE_COLUMN_NAMES.length())) == 1);
            } else if (stringValue.startsWith(DEFAULT_COLUMN_NAME_PATTERN)) {
                setDefaultColumnNamePattern(
                        unquoteString(stringValue.substring(DEFAULT_COLUMN_NAME_PATTERN.length())));
            } else if (stringValue.startsWith(REGULAR_EXPRESSION_MATCH_ALLOWED)) {
                setRegularExpressionMatchAllowed(
                        unquoteString(stringValue.substring(REGULAR_EXPRESSION_MATCH_ALLOWED.length())));
            } else if (stringValue.startsWith(REGULAR_EXPRESSION_MATCH_DISALLOWED)) {
                setRegularExpressionMatchDisallowed(
                        unquoteString(stringValue.substring(REGULAR_EXPRESSION_MATCH_DISALLOWED.length())));
            } else {
                throw DbException.getInvalidValueException("SET COLUMN_NAME_RULES: unknown id:" + stringValue,
                        stringValue);
            }
            recompilePatterns();
        }
        // Including NumberFormatException|PatternSyntaxException
        catch (RuntimeException e) {
            throw DbException.getInvalidValueException("SET COLUMN_NAME_RULES:" + e.getMessage(), stringValue);

        }
    }

    private void recompilePatterns() {
        try {
            // recompile RE patterns
            setCompiledRegularExpressionMatchAllowed(Pattern.compile(getRegularExpressionMatchAllowed()));
            setCompiledRegularExpressionMatchDisallowed(Pattern.compile(getRegularExpressionMatchDisallowed()));
        } catch (Exception e) {
            configure(REGULAR);
            throw e;
        }
    }

    public static ColumnNamerConfiguration getDefault() {
        return new ColumnNamerConfiguration(Integer.MAX_VALUE, "(?m)(?s).+", "(?m)(?s)[\\x00]", "_UNNAMED_$$", false);
    }

    private static String unquoteString(String s) {
        if (s.startsWith("'") && s.endsWith("'")) {
            s = s.substring(1, s.length() - 1);
            return s;
        }
        return s;
    }

    public boolean isGenerateUniqueColumnNames() {
        return generateUniqueColumnNames;
    }

    public void setGenerateUniqueColumnNames(boolean generateUniqueColumnNames) {
        this.generateUniqueColumnNames = generateUniqueColumnNames;
    }

    /**
     * Configure the rules.
     *
     * @param modeEnum the mode
     */
    public void configure(ModeEnum modeEnum) {
        switch (modeEnum) {
        case Oracle:
            // Nonquoted identifiers can contain only alphanumeric characters
            // from your database character set and the underscore (_), dollar
            // sign ($), and pound sign (#).
            setMaxIdentiferLength(128);
            setRegularExpressionMatchAllowed("(?m)(?s)\"?[A-Za-z0-9_\\$#]+\"?");
            setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_\"\\$#]");
            setDefaultColumnNamePattern("_UNNAMED_$$");
            setGenerateUniqueColumnNames(false);
            break;

        case MSSQLServer:
            // https://docs.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server
            setMaxIdentiferLength(128);
            // allows [] around names
            setRegularExpressionMatchAllowed("(?m)(?s)[A-Za-z0-9_\\[\\]]+");
            setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_\\[\\]]");
            setDefaultColumnNamePattern("_UNNAMED_$$");
            setGenerateUniqueColumnNames(false);
            break;

        case PostgreSQL:
            // this default can be changed to 128 by postgres config
            setMaxIdentiferLength(63);
            setRegularExpressionMatchAllowed("(?m)(?s)[A-Za-z0-9_\\$]+");
            setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_\\$]");
            setDefaultColumnNamePattern("_UNNAMED_$$");
            setGenerateUniqueColumnNames(false);
            break;

        case MySQL:
            // https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
            setMaxIdentiferLength(64);
            setRegularExpressionMatchAllowed("(?m)(?s)`?[A-Za-z0-9_`\\$]+`?");
            setRegularExpressionMatchDisallowed("(?m)(?s)[^A-Za-z0-9_`\\$]");
            setDefaultColumnNamePattern("_UNNAMED_$$");
            setGenerateUniqueColumnNames(false);
            break;

        case REGULAR:
        case DB2:
        case Derby:
        case HSQLDB:
        case Ignite:
            default:
            setMaxIdentiferLength(Integer.MAX_VALUE);
            setRegularExpressionMatchAllowed("(?m)(?s).+");
            setRegularExpressionMatchDisallowed("(?m)(?s)[\\x00]");
            setDefaultColumnNamePattern("_UNNAMED_$$");
            setGenerateUniqueColumnNames(false);
            break;
        }
        recompilePatterns();
    }

}
