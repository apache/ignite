/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.Types;
import java.util.ArrayList;
import org.h2.util.New;

/**
 * Represents an expression.
 */
public class Expression {

    private String sql;
    private final TestSynth config;
    private final Command command;

    private Expression(TestSynth config, Command command) {
        this.config = config;
        this.command = command;
        sql = "";
    }

    /**
     * Create a random select list.
     *
     * @param config the configuration
     * @param command the command
     * @return the select list
     */
    static String[] getRandomSelectList(TestSynth config, Command command) {
        if (config.random().getBoolean(30)) {
            return new String[] { "*" };
        }
        ArrayList<String> exp = New.arrayList();
        String sql = "";
        if (config.random().getBoolean(10)) {
            sql += "DISTINCT ";
        }
        int len = config.random().getLog(8) + 1;
        for (int i = 0; i < len; i++) {
            sql += getRandomExpression(config, command).getSQL();
            sql += " AS A" + i + " ";
            exp.add(sql);
            sql = "";
        }
        return exp.toArray(new String[0]);
    }

    /**
     * Generate a random condition.
     *
     * @param config the configuration
     * @param command the command
     * @return the random condition expression
     */
    static Expression getRandomCondition(TestSynth config, Command command) {
        Expression condition = new Expression(config, command);
        if (config.random().getBoolean(50)) {
            condition.create();
        }
        return condition;
    }

    private static Expression getRandomExpression(TestSynth config,
            Command command) {
        Expression expression = new Expression(config, command);
        String alias = command.getRandomTableAlias();
        Column column = command.getTable(alias).getRandomConditionColumn();
        if (column == null) {
            expression.createValue();
        } else {
            expression.createExpression(alias, column);
        }
        return expression;
    }

    private void createValue() {
        Value v = Column.getRandomColumn(config).getRandomValue();
        sql = v.getSQL();
    }

    /**
     * Generate a random join condition.
     *
     * @param config the configuration
     * @param command the command
     * @param alias the alias name
     * @return the join condition
     */
    static Expression getRandomJoinOn(TestSynth config, Command command,
            String alias) {
        Expression expression = new Expression(config, command);
        expression.createJoinComparison(alias);
        return expression;
    }

    /**
     * Generate a random sort order list.
     *
     * @param config the configuration
     * @param command the command
     * @return the ORDER BY list
     */
    static String getRandomOrder(TestSynth config, Command command) {
        int len = config.random().getLog(6);
        String sql = "";
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sql += ", ";
            }
            int max = command.selectList.length;
            int idx = config.random().getInt(max);
            // sql += getRandomExpression(command).getSQL();
            // if (max > 1 && config.random().getBoolean(50)) {
            sql += "A" + idx;
            // } else {
            // sql += String.valueOf(idx + 1);
            // }
            if (config.random().getBoolean(50)) {
                if (config.random().getBoolean(10)) {
                    sql += " ASC";
                } else {
                    sql += " DESC";
                }
            }
        }
        return sql;
    }

    /**
     * Get the SQL snippet of this expression.
     *
     * @return the SQL snippet
     */
    String getSQL() {
        return sql.trim().length() == 0 ? null : sql.trim();
    }

    private boolean is(int percent) {
        return config.random().getBoolean(percent);
    }

    private String oneOf(String[] list) {
        int i = config.random().getInt(list.length);
        if (!sql.endsWith(" ")) {
            sql += " ";
        }
        sql += list[i] + " ";
        return list[i];
    }

    private static String getColumnName(String alias, Column column) {
        if (alias == null) {
            return column.getName();
        }
        return alias + "." + column.getName();
    }

    private void createJoinComparison(String alias) {
        int len = config.random().getLog(5) + 1;
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sql += "AND ";
            }
            Column column = command.getTable(alias).getRandomConditionColumn();
            if (column == null) {
                sql += "1=1";
                return;
            }
            sql += getColumnName(alias, column);
            sql += "=";
            String a2;
            do {
                a2 = command.getRandomTableAlias();
            } while (a2.equals(alias));
            Table t2 = command.getTable(a2);
            Column c2 = t2.getRandomColumnOfType(column.getType());
            if (c2 == null) {
                sql += column.getRandomValue().getSQL();
            } else {
                sql += getColumnName(a2, c2);
            }
            sql += " ";
        }
    }

    private void create() {
        createComparison();
        while (is(50)) {
            oneOf(new String[] { "AND", "OR" });
            createComparison();
        }
    }

    // private void createSubquery() {
    // // String alias = command.getRandomTableAlias();
    // // Table t1 = command.getTable(alias);
    // Database db = command.getDatabase();
    // Table t2 = db.getRandomTable();
    // String a2 = command.getNextTableAlias();
    // sql += "SELECT * FROM " + t2.getName() + " " + a2 + " WHERE ";
    // command.addSubqueryTable(a2, t2);
    // createComparison();
    // command.removeSubqueryTable(a2);
    // }

    private void createComparison() {
        if (is(5)) {
            sql += " NOT( ";
            createComparisonSub();
            sql += ")";
        } else {
            createComparisonSub();
        }
    }

    private void createComparisonSub() {
        /*
         * if (is(10)) { sql += " EXISTS("; createSubquery(); sql += ")";
         * return; } else
         */
        if (is(10)) {
            sql += "(";
            create();
            sql += ")";
            return;
        }
        String alias = command.getRandomTableAlias();
        Column column = command.getTable(alias).getRandomConditionColumn();
        if (column == null) {
            if (is(50)) {
                sql += "1=1";
            } else {
                sql += "1=0";
            }
            return;
        }
        boolean columnFirst = is(90);
        if (columnFirst) {
            sql += getColumnName(alias, column);
        } else {
            Value v = column.getRandomValue();
            sql += v.getSQL();
        }
        if (is(10)) {
            oneOf(new String[] { "IS NULL", "IS NOT NULL" });
        } else if (is(10)) {
            oneOf(new String[] { "BETWEEN", "NOT BETWEEN" });
            Value v = column.getRandomValue();
            sql += v.getSQL();
            sql += " AND ";
            v = column.getRandomValue();
            sql += v.getSQL();
            // } else if (is(10)) {
            // // oneOf(new String[] { "IN", "NOT IN" });
            // sql += " IN ";
            // sql += "(";
            // int len = config.random().getInt(8) + 1;
            // for (int i = 0; i < len; i++) {
            // if (i > 0) {
            // sql += ", ";
            // }
            // sql += column.getRandomValueNotNull().getSQL();
            // }
            // sql += ")";
        } else {
            if (column.getType() == Types.VARCHAR) {
                oneOf(new String[] { "=", "=", "=", "<", ">",
                        "<=", ">=", "<>", "LIKE", "NOT LIKE" });
            } else {
                oneOf(new String[] { "=", "=", "=", "<", ">",
                        "<=", ">=", "<>" });
            }
            if (columnFirst) {
                Value v = column.getRandomValue();
                sql += v.getSQL();
            } else {
                sql += getColumnName(alias, column);
            }
        }
    }

    private void createExpression(String alias, Column type) {
        boolean op = is(20);
        // no null values if there is an operation
        boolean allowNull = !op;
        // boolean allowNull =true;

        createTerm(alias, type, true);
        if (op) {
            switch (type.getType()) {
            case Types.INTEGER:
                if (config.is(TestSynth.POSTGRESQL)) {
                    oneOf(new String[] { "+", "-", "/" });
                } else {
                    oneOf(new String[] { "+", "-", "*", "/" });
                }
                createTerm(alias, type, allowNull);
                break;
            case Types.DECIMAL:
                oneOf(new String[] { "+", "-", "*" });
                createTerm(alias, type, allowNull);
                break;
            case Types.VARCHAR:
                sql += " || ";
                createTerm(alias, type, allowNull);
                break;
            case Types.BLOB:
            case Types.CLOB:
            case Types.DATE:
                break;
            default:
            }
        }
    }

    private void createTerm(String alias, Column type, boolean allowNull) {
        int dt = type.getType();
        if (is(5) && (dt == Types.INTEGER) || (dt == Types.DECIMAL)) {
            sql += " - ";
            allowNull = false;
        }
        if (is(10)) {
            sql += "(";
            createTerm(alias, type, allowNull);
            sql += ")";
            return;
        }
        if (is(20)) {
            // if (is(10)) {
            // sql += "CAST(";
            // // TODO cast
            // Column c = Column.getRandomColumn(config);
            // createTerm(alias, c, allowNull);
            // sql += " AS ";
            // sql += type.getTypeName();
            // sql += ")";
            // return;
            // }
            switch (dt) {
            // case Types.INTEGER:
            // String function = oneOf(new String[] { "LENGTH" /*, "MOD" */ });
            // sql += "(";
            // createTerm(alias, type, allowNull);
            // sql += ")";
            // break;
            case Types.VARCHAR:
                oneOf(new String[] { "LOWER", "UPPER" });
                sql += "(";
                createTerm(alias, type, allowNull);
                sql += ")";
                break;
            default:
                createTerm(alias, type, allowNull);
            }
            return;
        }
        if (is(60)) {
            String a2 = command.getRandomTableAlias();
            Column column = command.getTable(a2).getRandomColumnOfType(dt);
            if (column != null) {
                sql += getColumnName(a2, column);
                return;
            }
        }

        Value v = Value.getRandom(config, dt, 20, 2, allowNull);
        sql += v.getSQL();
    }

    @Override
    public String toString() {
        throw new AssertionError();
    }

}
