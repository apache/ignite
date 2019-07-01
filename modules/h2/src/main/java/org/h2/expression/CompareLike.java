/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Pattern matching comparison expression: WHERE NAME LIKE ?
 */
public class CompareLike extends Condition {

    private static final int MATCH = 0, ONE = 1, ANY = 2;

    private final CompareMode compareMode;
    private final String defaultEscape;
    private Expression left;
    private Expression right;
    private Expression escape;

    private boolean isInit;

    private char[] patternChars;
    private String patternString;
    /** one of MATCH / ONE / ANY */
    private int[] patternTypes;
    private int patternLength;

    private final boolean regexp;
    private Pattern patternRegexp;

    private boolean ignoreCase;
    private boolean fastCompare;
    private boolean invalidPattern;
    /** indicates that we can shortcut the comparison and use startsWith */
    private boolean shortcutToStartsWith;
    /** indicates that we can shortcut the comparison and use endsWith */
    private boolean shortcutToEndsWith;
    /** indicates that we can shortcut the comparison and use contains */
    private boolean shortcutToContains;

    public CompareLike(Database db, Expression left, Expression right,
            Expression escape, boolean regexp) {
        this(db.getCompareMode(), db.getSettings().defaultEscape, left, right,
                escape, regexp);
    }

    public CompareLike(CompareMode compareMode, String defaultEscape,
            Expression left, Expression right, Expression escape, boolean regexp) {
        this.compareMode = compareMode;
        this.defaultEscape = defaultEscape;
        this.regexp = regexp;
        this.left = left;
        this.right = right;
        this.escape = escape;
    }

    private static Character getEscapeChar(String s) {
        return s == null || s.length() == 0 ? null : s.charAt(0);
    }

    @Override
    public String getSQL() {
        String sql;
        if (regexp) {
            sql = left.getSQL() + " REGEXP " + right.getSQL();
        } else {
            sql = left.getSQL() + " LIKE " + right.getSQL();
            if (escape != null) {
                sql += " ESCAPE " + escape.getSQL();
            }
        }
        return "(" + sql + ")";
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        if (left.getType() == Value.STRING_IGNORECASE) {
            ignoreCase = true;
        }
        if (left.isValueSet()) {
            Value l = left.getValue(session);
            if (l == ValueNull.INSTANCE) {
                // NULL LIKE something > NULL
                return ValueExpression.getNull();
            }
        }
        if (escape != null) {
            escape = escape.optimize(session);
        }
        if (right.isValueSet() && (escape == null || escape.isValueSet())) {
            if (left.isValueSet()) {
                return ValueExpression.get(getValue(session));
            }
            Value r = right.getValue(session);
            if (r == ValueNull.INSTANCE) {
                // something LIKE NULL > NULL
                return ValueExpression.getNull();
            }
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                return ValueExpression.getNull();
            }
            String p = r.getString();
            initPattern(p, getEscapeChar(e));
            if (invalidPattern) {
                return ValueExpression.getNull();
            }
            if ("%".equals(p)) {
                // optimization for X LIKE '%': convert to X IS NOT NULL
                return new Comparison(session,
                        Comparison.IS_NOT_NULL, left, null).optimize(session);
            }
            if (isFullMatch()) {
                // optimization for X LIKE 'Hello': convert to X = 'Hello'
                Value value = ValueString.get(patternString);
                Expression expr = ValueExpression.get(value);
                return new Comparison(session,
                        Comparison.EQUAL, left, expr).optimize(session);
            }
            isInit = true;
        }
        return this;
    }

    private Character getEscapeChar(Value e) {
        if (e == null) {
            return getEscapeChar(defaultEscape);
        }
        String es = e.getString();
        Character esc;
        if (es == null) {
            esc = getEscapeChar(defaultEscape);
        } else if (es.length() == 0) {
            esc = null;
        } else if (es.length() > 1) {
            throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, es);
        } else {
            esc = es.charAt(0);
        }
        return esc;
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (regexp) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        // parameters are always evaluatable, but
        // we need to check if the value is set
        // (at prepare time)
        // otherwise we would need to prepare at execute time,
        // which may be slower (possibly not in this case)
        if (!right.isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
            return;
        }
        if (escape != null &&
                !escape.isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
            return;
        }
        String p = right.getValue(session).getString();
        if (!isInit) {
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                // should already be optimized
                DbException.throwInternalError();
            }
            initPattern(p, getEscapeChar(e));
        }
        if (invalidPattern) {
            return;
        }
        if (patternLength <= 0 || patternTypes[0] != MATCH) {
            // can't use an index
            return;
        }
        int dataType = l.getColumn().getType();
        if (dataType != Value.STRING && dataType != Value.STRING_IGNORECASE &&
                dataType != Value.STRING_FIXED) {
            // column is not a varchar - can't use the index
            return;
        }
        // Get the MATCH prefix and see if we can create an index condition from
        // that.
        int maxMatch = 0;
        StringBuilder buff = new StringBuilder();
        while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
            buff.append(patternChars[maxMatch++]);
        }
        String begin = buff.toString();
        if (maxMatch == patternLength) {
            filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL, l,
                    ValueExpression.get(ValueString.get(begin))));
        } else {
            // TODO check if this is correct according to Unicode rules
            // (code points)
            String end;
            if (begin.length() > 0) {
                filter.addIndexCondition(IndexCondition.get(
                        Comparison.BIGGER_EQUAL, l,
                        ValueExpression.get(ValueString.get(begin))));
                char next = begin.charAt(begin.length() - 1);
                // search the 'next' unicode character (or at least a character
                // that is higher)
                for (int i = 1; i < 2000; i++) {
                    end = begin.substring(0, begin.length() - 1) + (char) (next + i);
                    if (compareMode.compareString(begin, end, ignoreCase) == -1) {
                        filter.addIndexCondition(IndexCondition.get(
                                Comparison.SMALLER, l,
                                ValueExpression.get(ValueString.get(end))));
                        break;
                    }
                }
            }
        }
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        if (!isInit) {
            Value r = right.getValue(session);
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            String p = r.getString();
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            initPattern(p, getEscapeChar(e));
        }
        if (invalidPattern) {
            return ValueNull.INSTANCE;
        }
        String value = l.getString();
        boolean result;
        if (regexp) {
            result = patternRegexp.matcher(value).find();
        } else if (shortcutToStartsWith) {
            result = value.regionMatches(ignoreCase, 0, patternString, 0, patternLength - 1);
        } else if (shortcutToEndsWith) {
            result = value.regionMatches(ignoreCase, value.length() -
                    patternLength + 1, patternString, 1, patternLength - 1);
        } else if (shortcutToContains) {
            String p = patternString.substring(1, patternString.length() - 1);
            if (ignoreCase) {
                result = containsIgnoreCase(value, p);
            } else {
                result = value.contains(p);
            }
        } else {
            result = compareAt(value, 0, 0, value.length(), patternChars, patternTypes);
        }
        return ValueBoolean.get(result);
    }

    private static boolean containsIgnoreCase(String src, String what) {
        final int length = what.length();
        if (length == 0) {
            // Empty string is contained
            return true;
        }

        final char firstLo = Character.toLowerCase(what.charAt(0));
        final char firstUp = Character.toUpperCase(what.charAt(0));

        for (int i = src.length() - length; i >= 0; i--) {
            // Quick check before calling the more expensive regionMatches()
            final char ch = src.charAt(i);
            if (ch != firstLo && ch != firstUp) {
                continue;
            }
            if (src.regionMatches(true, i, what, 0, length)) {
                return true;
            }
        }

        return false;
    }

    private boolean compareAt(String s, int pi, int si, int sLen,
            char[] pattern, int[] types) {
        for (; pi < patternLength; pi++) {
            switch (types[pi]) {
            case MATCH:
                if ((si >= sLen) || !compare(pattern, s, pi, si++)) {
                    return false;
                }
                break;
            case ONE:
                if (si++ >= sLen) {
                    return false;
                }
                break;
            case ANY:
                if (++pi >= patternLength) {
                    return true;
                }
                while (si < sLen) {
                    if (compare(pattern, s, pi, si) &&
                            compareAt(s, pi, si, sLen, pattern, types)) {
                        return true;
                    }
                    si++;
                }
                return false;
            default:
                DbException.throwInternalError("" + types[pi]);
            }
        }
        return si == sLen;
    }

    private boolean compare(char[] pattern, String s, int pi, int si) {
        return pattern[pi] == s.charAt(si) ||
                (!fastCompare && compareMode.equalsChars(patternString, pi, s,
                        si, ignoreCase));
    }

    /**
     * Test if the value matches the pattern.
     *
     * @param testPattern the pattern
     * @param value the value
     * @param escapeChar the escape character
     * @return true if the value matches
     */
    public boolean test(String testPattern, String value, char escapeChar) {
        initPattern(testPattern, escapeChar);
        if (invalidPattern) {
            return false;
        }
        return compareAt(value, 0, 0, value.length(), patternChars, patternTypes);
    }

    private void initPattern(String p, Character escapeChar) {
        if (compareMode.getName().equals(CompareMode.OFF) && !ignoreCase) {
            fastCompare = true;
        }
        if (regexp) {
            patternString = p;
            try {
                if (ignoreCase) {
                    patternRegexp = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
                } else {
                    patternRegexp = Pattern.compile(p);
                }
            } catch (PatternSyntaxException e) {
                throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, p);
            }
            return;
        }
        patternLength = 0;
        if (p == null) {
            patternTypes = null;
            patternChars = null;
            return;
        }
        int len = p.length();
        patternChars = new char[len];
        patternTypes = new int[len];
        boolean lastAny = false;
        for (int i = 0; i < len; i++) {
            char c = p.charAt(i);
            int type;
            if (escapeChar != null && escapeChar == c) {
                if (i >= len - 1) {
                    invalidPattern = true;
                    return;
                }
                c = p.charAt(++i);
                type = MATCH;
                lastAny = false;
            } else if (c == '%') {
                if (lastAny) {
                    continue;
                }
                type = ANY;
                lastAny = true;
            } else if (c == '_') {
                type = ONE;
            } else {
                type = MATCH;
                lastAny = false;
            }
            patternTypes[patternLength] = type;
            patternChars[patternLength++] = c;
        }
        for (int i = 0; i < patternLength - 1; i++) {
            if ((patternTypes[i] == ANY) && (patternTypes[i + 1] == ONE)) {
                patternTypes[i] = ONE;
                patternTypes[i + 1] = ANY;
            }
        }
        patternString = new String(patternChars, 0, patternLength);

        // Clear optimizations
        shortcutToStartsWith = false;
        shortcutToEndsWith = false;
        shortcutToContains = false;

        // optimizes the common case of LIKE 'foo%'
        if (compareMode.getName().equals(CompareMode.OFF) && patternLength > 1) {
            int maxMatch = 0;
            while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
                maxMatch++;
            }
            if (maxMatch == patternLength - 1 && patternTypes[patternLength - 1] == ANY) {
                shortcutToStartsWith = true;
                return;
            }
        }
        // optimizes the common case of LIKE '%foo'
        if (compareMode.getName().equals(CompareMode.OFF) && patternLength > 1) {
            if (patternTypes[0] == ANY) {
                int maxMatch = 1;
                while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
                    maxMatch++;
                }
                if (maxMatch == patternLength) {
                    shortcutToEndsWith = true;
                    return;
                }
            }
        }
        // optimizes the common case of LIKE '%foo%'
        if (compareMode.getName().equals(CompareMode.OFF) && patternLength > 2) {
            if (patternTypes[0] == ANY) {
                int maxMatch = 1;
                while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
                    maxMatch++;
                }
                if (maxMatch == patternLength - 1 && patternTypes[patternLength - 1] == ANY) {
                    shortcutToContains = true;
                }
            }
        }
    }

    private boolean isFullMatch() {
        if (patternTypes == null) {
            return false;
        }
        for (int type : patternTypes) {
            if (type != MATCH) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
        if (escape != null) {
            escape.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
        if (escape != null) {
            escape.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public void updateAggregate(Session session) {
        left.updateAggregate(session);
        right.updateAggregate(session);
        if (escape != null) {
            escape.updateAggregate(session);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor)
                && (escape == null || escape.isEverything(visitor));
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost() + 3;
    }

}
