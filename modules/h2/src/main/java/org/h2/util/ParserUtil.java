/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

public class ParserUtil {

    /**
     * A keyword.
     */
    public static final int KEYWORD = 1;

    /**
     * An identifier (table name, column name,...).
     */
    public static final int IDENTIFIER = 2;

    /**
     * The token "ALL".
     */
    public static final int ALL = IDENTIFIER + 1;

    /**
     * The token "ARRAY".
     */
    public static final int ARRAY = ALL + 1;

    /**
     * The token "CASE".
     */
    public static final int CASE = ARRAY + 1;

    /**
     * The token "CHECK".
     */
    public static final int CHECK = CASE + 1;

    /**
     * The token "CONSTRAINT".
     */
    public static final int CONSTRAINT = CHECK + 1;

    /**
     * The token "CROSS".
     */
    public static final int CROSS = CONSTRAINT + 1;

    /**
     * The token "CURRENT_DATE".
     */
    public static final int CURRENT_DATE = CROSS + 1;

    /**
     * The token "CURRENT_TIME".
     */
    public static final int CURRENT_TIME = CURRENT_DATE + 1;

    /**
     * The token "CURRENT_TIMESTAMP".
     */
    public static final int CURRENT_TIMESTAMP = CURRENT_TIME + 1;

    /**
     * The token "CURRENT_USER".
     */
    public static final int CURRENT_USER = CURRENT_TIMESTAMP + 1;

    /**
     * The token "DISTINCT".
     */
    public static final int DISTINCT = CURRENT_USER + 1;

    /**
     * The token "EXCEPT".
     */
    public static final int EXCEPT = DISTINCT + 1;

    /**
     * The token "EXISTS".
     */
    public static final int EXISTS = EXCEPT + 1;

    /**
     * The token "FALSE".
     */
    public static final int FALSE = EXISTS + 1;

    /**
     * The token "FETCH".
     */
    public static final int FETCH = FALSE + 1;

    /**
     * The token "FOR".
     */
    public static final int FOR = FETCH + 1;

    /**
     * The token "FOREIGN".
     */
    public static final int FOREIGN = FOR + 1;

    /**
     * The token "FROM".
     */
    public static final int FROM = FOREIGN + 1;

    /**
     * The token "FULL".
     */
    public static final int FULL = FROM + 1;

    /**
     * The token "GROUP".
     */
    public static final int GROUP = FULL + 1;

    /**
     * The token "HAVING".
     */
    public static final int HAVING = GROUP + 1;

    /**
     * The token "IF".
     */
    public static final int IF = HAVING + 1;

    /**
     * The token "INNER".
     */
    public static final int INNER = IF + 1;

    /**
     * The token "INTERSECT".
     */
    public static final int INTERSECT = INNER + 1;

    /**
     * The token "INTERSECTS".
     */
    public static final int INTERSECTS = INTERSECT + 1;

    /**
     * The token "INTERVAL".
     */
    public static final int INTERVAL = INTERSECTS + 1;

    /**
     * The token "IS".
     */
    public static final int IS = INTERVAL + 1;

    /**
     * The token "JOIN".
     */
    public static final int JOIN = IS + 1;

    /**
     * The token "LIKE".
     */
    public static final int LIKE = JOIN + 1;

    /**
     * The token "LIMIT".
     */
    public static final int LIMIT = LIKE + 1;

    /**
     * The token "LOCALTIME".
     */
    public static final int LOCALTIME = LIMIT + 1;

    /**
     * The token "LOCALTIMESTAMP".
     */
    public static final int LOCALTIMESTAMP = LOCALTIME + 1;

    /**
     * The token "MINUS".
     */
    public static final int MINUS = LOCALTIMESTAMP + 1;

    /**
     * The token "NATURAL".
     */
    public static final int NATURAL = MINUS + 1;

    /**
     * The token "NOT".
     */
    public static final int NOT = NATURAL + 1;

    /**
     * The token "NULL".
     */
    public static final int NULL = NOT + 1;

    /**
     * The token "OFFSET".
     */
    public static final int OFFSET = NULL + 1;

    /**
     * The token "ON".
     */
    public static final int ON = OFFSET + 1;

    /**
     * The token "ORDER".
     */
    public static final int ORDER = ON + 1;

    /**
     * The token "PRIMARY".
     */
    public static final int PRIMARY = ORDER + 1;

    /**
     * The token "QUALIFY".
     */
    public static final int QUALIFY = PRIMARY + 1;

    /**
     * The token "ROW".
     */
    public static final int ROW = QUALIFY + 1;

    /**
     * The token "_ROWID_".
     */
    public static final int _ROWID_ = ROW + 1;

    /**
     * The token "ROWNUM".
     */
    public static final int ROWNUM = _ROWID_ + 1;

    /**
     * The token "SELECT".
     */
    public static final int SELECT = ROWNUM + 1;

    /**
     * The token "TABLE".
     */
    public static final int TABLE = SELECT + 1;

    /**
     * The token "TRUE".
     */
    public static final int TRUE = TABLE + 1;

    /**
     * The token "UNION".
     */
    public static final int UNION = TRUE + 1;

    /**
     * The token "UNIQUE".
     */
    public static final int UNIQUE = UNION + 1;

    /**
     * The token "VALUES".
     */
    public static final int VALUES = UNIQUE + 1;

    /**
     * The token "WHERE".
     */
    public static final int WHERE = VALUES + 1;

    /**
     * The token "WINDOW".
     */
    public static final int WINDOW = WHERE + 1;

    /**
     * The token "WITH".
     */
    public static final int WITH = WINDOW + 1;

    private static final int UPPER_OR_OTHER_LETTER =
            1 << Character.UPPERCASE_LETTER
            | 1 << Character.MODIFIER_LETTER
            | 1 << Character.OTHER_LETTER;

    private static final int UPPER_OR_OTHER_LETTER_OR_DIGIT =
            UPPER_OR_OTHER_LETTER
            | 1 << Character.DECIMAL_DIGIT_NUMBER;

    private static final int LOWER_OR_OTHER_LETTER =
            1 << Character.LOWERCASE_LETTER
            | 1 << Character.MODIFIER_LETTER
            | 1 << Character.OTHER_LETTER;

    private static final int LOWER_OR_OTHER_LETTER_OR_DIGIT =
            LOWER_OR_OTHER_LETTER
            | 1 << Character.DECIMAL_DIGIT_NUMBER;

    private static final int LETTER =
            1 << Character.UPPERCASE_LETTER
            | 1 << Character.LOWERCASE_LETTER
            | 1 << Character.TITLECASE_LETTER
            | 1 << Character.MODIFIER_LETTER
            | 1 << Character.OTHER_LETTER;

    private static final int LETTER_OR_DIGIT =
            LETTER
            | 1 << Character.DECIMAL_DIGIT_NUMBER;

    private ParserUtil() {
        // utility class
    }

    /**
     * Checks if this string is a SQL keyword.
     *
     * @param s the token to check
     * @param ignoreCase true if case should be ignored, false if only upper case
     *            tokens are detected as keywords
     * @return true if it is a keyword
     */
    public static boolean isKeyword(String s, boolean ignoreCase) {
        int length = s.length();
        if (length == 0) {
            return false;
        }
        return getSaveTokenType(s, ignoreCase, 0, length, false) != IDENTIFIER;
    }

    /**
     * Is this a simple identifier (in the JDBC specification sense).
     *
     * @param s identifier to check
     * @param databaseToUpper whether unquoted identifiers are converted to upper case
     * @param databaseToLower whether unquoted identifiers are converted to lower case
     * @return is specified identifier may be used without quotes
     * @throws NullPointerException if s is {@code null}
     */
    public static boolean isSimpleIdentifier(String s, boolean databaseToUpper, boolean databaseToLower) {
        int length = s.length();
        if (length == 0) {
            return false;
        }
        int startFlags, partFlags;
        if (databaseToUpper) {
            if (databaseToLower) {
                throw new IllegalArgumentException("databaseToUpper && databaseToLower");
            } else {
                startFlags = UPPER_OR_OTHER_LETTER;
                partFlags = UPPER_OR_OTHER_LETTER_OR_DIGIT;
            }
        } else {
            if (databaseToLower) {
                startFlags = LOWER_OR_OTHER_LETTER;
                partFlags = LOWER_OR_OTHER_LETTER_OR_DIGIT;
            } else {
                startFlags = LETTER;
                partFlags = LETTER_OR_DIGIT;
            }
        }
        char c = s.charAt(0);
        if ((startFlags >>> Character.getType(c) & 1) == 0 && c != '_') {
            return false;
        }
        for (int i = 1; i < length; i++) {
            c = s.charAt(i);
            if ((partFlags >>> Character.getType(c) & 1) == 0 && c != '_') {
                return false;
            }
        }
        return getSaveTokenType(s, !databaseToUpper, 0, length, true) == IDENTIFIER;
    }

    /**
     * Get the token type.
     *
     * @param s the string with token
     * @param ignoreCase true if case should be ignored, false if only upper case
     *            tokens are detected as keywords
     * @param start start index of token
     * @param end index of token, exclusive; must be greater than start index
     * @param additionalKeywords whether TOP, INTERSECTS, and "current data /
     *                           time" functions are keywords
     * @return the token type
     */
    public static int getSaveTokenType(String s, boolean ignoreCase, int start, int end, boolean additionalKeywords) {
        /*
         * JdbcDatabaseMetaData.getSQLKeywords() and tests should be updated when new
         * non-SQL:2003 keywords are introduced here.
         */
        char c = s.charAt(start);
        if (ignoreCase) {
            // Convert a-z to A-Z and 0x7f to _ (need special handling).
            c &= 0xffdf;
        }
        switch (c) {
        case 'A':
            if (eq("ALL", s, ignoreCase, start, end)) {
                return ALL;
            } else if (eq("ARRAY", s, ignoreCase, start, end)) {
                return ARRAY;
            }
            if (additionalKeywords) {
                if (eq("AND", s, ignoreCase, start, end) || eq("AS", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'B':
            if (additionalKeywords) {
                if (eq("BETWEEN", s, ignoreCase, start, end) || eq("BOTH", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'C':
            if (eq("CASE", s, ignoreCase, start, end)) {
                return CASE;
            } else if (eq("CHECK", s, ignoreCase, start, end)) {
                return CHECK;
            } else if (eq("CONSTRAINT", s, ignoreCase, start, end)) {
                return CONSTRAINT;
            } else if (eq("CROSS", s, ignoreCase, start, end)) {
                return CROSS;
            } else if (eq("CURRENT_DATE", s, ignoreCase, start, end)) {
                return CURRENT_DATE;
            } else if (eq("CURRENT_TIME", s, ignoreCase, start, end)) {
                return CURRENT_TIME;
            } else if (eq("CURRENT_TIMESTAMP", s, ignoreCase, start, end)) {
                return CURRENT_TIMESTAMP;
            } else if (eq("CURRENT_USER", s, ignoreCase, start, end)) {
                return CURRENT_USER;
            }
            return IDENTIFIER;
        case 'D':
            if (eq("DISTINCT", s, ignoreCase, start, end)) {
                return DISTINCT;
            }
            return IDENTIFIER;
        case 'E':
            if (eq("EXCEPT", s, ignoreCase, start, end)) {
                return EXCEPT;
            } else if (eq("EXISTS", s, ignoreCase, start, end)) {
                return EXISTS;
            }
            return IDENTIFIER;
        case 'F':
            if (eq("FETCH", s, ignoreCase, start, end)) {
                return FETCH;
            } else if (eq("FROM", s, ignoreCase, start, end)) {
                return FROM;
            } else if (eq("FOR", s, ignoreCase, start, end)) {
                return FOR;
            } else if (eq("FOREIGN", s, ignoreCase, start, end)) {
                return FOREIGN;
            } else if (eq("FULL", s, ignoreCase, start, end)) {
                return FULL;
            } else if (eq("FALSE", s, ignoreCase, start, end)) {
                return FALSE;
            }
            if (additionalKeywords) {
                if (eq("FILTER", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'G':
            if (eq("GROUP", s, ignoreCase, start, end)) {
                return GROUP;
            }
            if (additionalKeywords) {
                if (eq("GROUPS", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'H':
            if (eq("HAVING", s, ignoreCase, start, end)) {
                return HAVING;
            }
            return IDENTIFIER;
        case 'I':
            if (eq("IF", s, ignoreCase, start, end)) {
                return IF;
            } else if (eq("INNER", s, ignoreCase, start, end)) {
                return INNER;
            } else if (eq("INTERSECT", s, ignoreCase, start, end)) {
                return INTERSECT;
            } else if (eq("INTERSECTS", s, ignoreCase, start, end)) {
                return INTERSECTS;
            } else if (eq("INTERVAL", s, ignoreCase, start, end)) {
                return INTERVAL;
            } else if (eq("IS", s, ignoreCase, start, end)) {
                return IS;
            }
            if (additionalKeywords) {
                if (eq("ILIKE", s, ignoreCase, start, end) || eq("IN", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'J':
            if (eq("JOIN", s, ignoreCase, start, end)) {
                return JOIN;
            }
            return IDENTIFIER;
        case 'L':
            if (eq("LIMIT", s, ignoreCase, start, end)) {
                return LIMIT;
            } else if (eq("LIKE", s, ignoreCase, start, end)) {
                return LIKE;
            } else if (eq("LOCALTIME", s, ignoreCase, start, end)) {
                return LOCALTIME;
            } else if (eq("LOCALTIMESTAMP", s, ignoreCase, start, end)) {
                return LOCALTIMESTAMP;
            }
            if (additionalKeywords) {
                if (eq("LEADING", s, ignoreCase, start, end) || eq("LEFT", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'M':
            if (eq("MINUS", s, ignoreCase, start, end)) {
                return MINUS;
            }
            return IDENTIFIER;
        case 'N':
            if (eq("NOT", s, ignoreCase, start, end)) {
                return NOT;
            } else if (eq("NATURAL", s, ignoreCase, start, end)) {
                return NATURAL;
            } else if (eq("NULL", s, ignoreCase, start, end)) {
                return NULL;
            }
            return IDENTIFIER;
        case 'O':
            if (eq("OFFSET", s, ignoreCase, start, end)) {
                return OFFSET;
            } else if (eq("ON", s, ignoreCase, start, end)) {
                return ON;
            } else if (eq("ORDER", s, ignoreCase, start, end)) {
                return ORDER;
            }
            if (additionalKeywords) {
                if (eq("OR", s, ignoreCase, start, end) || eq("OVER", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'P':
            if (eq("PRIMARY", s, ignoreCase, start, end)) {
                return PRIMARY;
            }
            if (additionalKeywords) {
                if (eq("PARTITION", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'Q':
            if (eq("QUALIFY", s, ignoreCase, start, end)) {
                return QUALIFY;
            }
            return IDENTIFIER;
        case 'R':
            if (eq("ROW", s, ignoreCase, start, end)) {
                return ROW;
            } else if (eq("ROWNUM", s, ignoreCase, start, end)) {
                return ROWNUM;
            }
            if (additionalKeywords) {
                if (eq("RANGE", s, ignoreCase, start, end) || eq("REGEXP", s, ignoreCase, start, end)
                        || eq("ROWS", s, ignoreCase, start, end) || eq("RIGHT", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'S':
            if (eq("SELECT", s, ignoreCase, start, end)) {
                return SELECT;
            }
            if (additionalKeywords) {
                if (eq("SYSDATE", s, ignoreCase, start, end) || eq("SYSTIME", s, ignoreCase, start, end)
                        || eq("SYSTIMESTAMP", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'T':
            if (eq("TABLE", s, ignoreCase, start, end)) {
                return TABLE;
            } else if (eq("TRUE", s, ignoreCase, start, end)) {
                return TRUE;
            }
            if (additionalKeywords) {
                if (eq("TODAY", s, ignoreCase, start, end) || eq("TOP", s, ignoreCase, start, end)
                        || eq("TRAILING", s, ignoreCase, start, end)) {
                    return KEYWORD;
                }
            }
            return IDENTIFIER;
        case 'U':
            if (eq("UNIQUE", s, ignoreCase, start, end)) {
                return UNIQUE;
            } else if (eq("UNION", s, ignoreCase, start, end)) {
                return UNION;
            }
            return IDENTIFIER;
        case 'V':
            if (eq("VALUES", s, ignoreCase, start, end)) {
                return VALUES;
            }
            return IDENTIFIER;
        case 'W':
            if (eq("WHERE", s, ignoreCase, start, end)) {
                return WHERE;
            } else if (eq("WINDOW", s, ignoreCase, start, end)) {
                return WINDOW;
            } else if (eq("WITH", s, ignoreCase, start, end)) {
                return WITH;
            }
            return IDENTIFIER;
        case '_':
            // Cannot use eq() because 0x7f can be converted to '_' (0x5f)
            if (end - start == 7 && "_ROWID_".regionMatches(ignoreCase, 0, s, start, 7)) {
                return _ROWID_;
            }
            //$FALL-THROUGH$
        default:
            return IDENTIFIER;
        }
    }

    private static boolean eq(String expected, String s, boolean ignoreCase, int start, int end) {
        int len = expected.length();
        // First letter was already checked
        return end - start == len && expected.regionMatches(ignoreCase, 1, s, start + 1, len - 1);
    }

}
