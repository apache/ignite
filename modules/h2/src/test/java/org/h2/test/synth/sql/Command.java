/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.SQLException;
import java.util.HashMap;
import org.h2.util.StatementBuilder;

/**
 * Represents a statement.
 */
class Command {
    private static final int CONNECT = 0, RESET = 1, DISCONNECT = 2,
            CREATE_TABLE = 3, INSERT = 4, DROP_TABLE = 5, SELECT = 6,
            DELETE = 7, UPDATE = 8, COMMIT = 9, ROLLBACK = 10,
            AUTOCOMMIT_ON = 11, AUTOCOMMIT_OFF = 12,
            CREATE_INDEX = 13, DROP_INDEX = 14, END = 15;

    /**
     * The select list.
     */
    String[] selectList;

    private TestSynth config;
    private final int type;
    private Table table;
    private HashMap<String, Table> tables;
    private Index index;
    private Column[] columns;
    private Value[] values;
    private String condition;
    // private int nextAlias;
    private String order;
    private final String join = "";
    private Result result;

    private Command(TestSynth config, int type) {
        this.config = config;
        this.type = type;
    }

    private Command(TestSynth config, int type, Table table) {
        this.config = config;
        this.type = type;
        this.table = table;
    }

    private Command(TestSynth config, int type, Table table, String alias) {
        this.config = config;
        this.type = type;
        this.table = table;
        this.tables = new HashMap<>();
        this.tables.put(alias, table);
    }

    private Command(TestSynth config, int type, Index index) {
        this.config = config;
        this.type = type;
        this.index = index;
    }

    Command(int type, String alias, Table table) {
        this.type = type;
        if (alias == null) {
            alias = table.getName();
        }
        addSubqueryTable(alias, table);
        this.table = table;
    }

//    static Command getDropTable(TestSynth config, Table table) {
//        return new Command(config, Command.DROP_TABLE, table);
//    }

//    Command getCommit(TestSynth config) {
//        return new Command(config, Command.COMMIT);
//    }

//    Command getRollback(TestSynth config) {
//        return new Command(config, Command.ROLLBACK);
//    }

//    Command getSetAutoCommit(TestSynth config, boolean auto) {
//        int type = auto ? Command.AUTOCOMMIT_ON : Command.AUTOCOMMIT_OFF;
//        return new Command(config, type);
//    }

    /**
     * Create a connect command.
     *
     * @param config the configuration
     * @return the command
     */
    static Command getConnect(TestSynth config) {
        return new Command(config, CONNECT);
    }

    /**
     * Create a reset command.
     *
     * @param config the configuration
     * @return the command
     */
    static Command getReset(TestSynth config) {
        return new Command(config, RESET);
    }

    /**
     * Create a disconnect command.
     *
     * @param config the configuration
     * @return the command
     */
    static Command getDisconnect(TestSynth config) {
        return new Command(config, DISCONNECT);
    }

    /**
     * Create an end command.
     *
     * @param config the configuration
     * @return the command
     */
    static Command getEnd(TestSynth config) {
        return new Command(config, END);
    }

    /**
     * Create a create table command.
     *
     * @param config the configuration
     * @param table the table
     * @return the command
     */
    static Command getCreateTable(TestSynth config, Table table) {
        return new Command(config, CREATE_TABLE, table);
    }

    /**
     * Create a create index command.
     *
     * @param config the configuration
     * @param index the index
     * @return the command
     */
    static Command getCreateIndex(TestSynth config, Index index) {
        return new Command(config, CREATE_INDEX, index);
    }

    /**
     * Create a random select command.
     *
     * @param config the configuration
     * @param table the table
     * @return the command
     */
    static Command getRandomSelect(TestSynth config, Table table) {
        Command command = new Command(config, Command.SELECT, table, "M");
        command.selectList = Expression.getRandomSelectList(config, command);
        // TODO group by, having, joins
        command.condition = Expression.getRandomCondition(config, command).getSQL();
        command.order = Expression.getRandomOrder(config, command);
        return command;
    }

//    static Command getRandomSelectJoin(TestSynth config, Table table) {
//        Command command = new Command(config, Command.SELECT, table, "M");
//        int len = config.random().getLog(5) + 1;
//        String globalJoinCondition = "";
//        for (int i = 0; i < len; i++) {
//            Table t2 = config.randomTable();
//            String alias = "J" + i;
//            command.addSubqueryTable(alias, t2);
//            Expression joinOn =
//                Expression.getRandomJoinOn(config, command, alias);
//            if (config.random().getBoolean(50)) {
//                // regular join
//                if (globalJoinCondition.length() > 0) {
//                    globalJoinCondition += " AND ";
//
//                }
//                globalJoinCondition += " (" + joinOn.getSQL() + ") ";
//                command.addJoin(", " + t2.getName() + " " + alias);
//            } else {
//                String join = " JOIN " + t2.getName() +
//                    " " + alias + " ON " + joinOn.getSQL();
//                if (config.random().getBoolean(20)) {
//                    command.addJoin(" LEFT OUTER" + join);
//                } else {
//                    command.addJoin(" INNER" + join);
//                }
//            }
//        }
//        command.selectList =
//            Expression.getRandomSelectList(config, command);
//        // TODO group by, having
//        String cond = Expression.getRandomCondition(config, command).getSQL();
//        if (globalJoinCondition.length() > 0) {
//            if (cond != null) {
//                cond = "(" + globalJoinCondition + " ) AND (" + cond + ")";
//            } else {
//                cond = globalJoinCondition;
//            }
//        }
//        command.condition = cond;
//        command.order = Expression.getRandomOrder(config, command);
//        return command;
//    }

    /**
     * Create a random delete command.
     *
     * @param config the configuration
     * @param table the table
     * @return the command
     */
    static Command getRandomDelete(TestSynth config, Table table) {
        Command command = new Command(config, Command.DELETE, table);
        command.condition = Expression.getRandomCondition(config, command).getSQL();
        return command;
    }

    /**
     * Create a random update command.
     *
     * @param config the configuration
     * @param table the table
     * @return the command
     */
    static Command getRandomUpdate(TestSynth config, Table table) {
        Command command = new Command(config, Command.UPDATE, table);
        command.prepareUpdate();
        return command;
    }

    /**
     * Create a random insert command.
     *
     * @param config the configuration
     * @param table the table
     * @return the command
     */
    static Command getRandomInsert(TestSynth config, Table table) {
        Command command = new Command(config, Command.INSERT, table);
        command.prepareInsert();
        return command;
    }

    /**
     * Add a subquery table to the command.
     *
     * @param alias the table alias
     * @param t the table
     */
    void addSubqueryTable(String alias, Table t) {
        tables.put(alias, t);
    }

//    void removeSubqueryTable(String alias) {
//        tables.remove(alias);
//    }

    private void prepareInsert() {
        Column[] c;
        if (config.random().getBoolean(70)) {
            c = table.getColumns();
        } else {
            int len = config.random().getInt(table.getColumnCount() - 1) + 1;
            c = columns = table.getRandomColumns(len);
        }
        values = new Value[c.length];
        for (int i = 0; i < c.length; i++) {
            values[i] = c[i].getRandomValue();
        }
    }

    private void prepareUpdate() {
        int len = config.random().getLog(table.getColumnCount() - 1) + 1;
        Column[] c = columns = table.getRandomColumns(len);
        values = new Value[c.length];
        for (int i = 0; i < c.length; i++) {
            values[i] = c[i].getRandomValue();
        }
        condition = Expression.getRandomCondition(config, this).getSQL();
    }

    private Result select(DbInterface db) throws SQLException {
        StatementBuilder buff = new StatementBuilder("SELECT ");
        for (String s : selectList) {
            buff.appendExceptFirst(", ");
            buff.append(s);
        }
        buff.append("  FROM ").append(table.getName()).append(" M").
            append(' ').append(join);
        if (condition != null) {
            buff.append("  WHERE ").append(condition);
        }
        if (order.trim().length() > 0) {
            buff.append("  ORDER BY ").append(order);
        }
        return db.select(buff.toString());
    }

    /**
     * Run the command against the specified database.
     *
     * @param db the database
     * @return the result
     */
    Result run(DbInterface db) throws Exception {
        try {
            switch (type) {
            case CONNECT:
                db.connect();
                result = new Result("connect");
                break;
            case RESET:
                db.reset();
                result = new Result("reset");
                break;
            case DISCONNECT:
                db.disconnect();
                result = new Result("disconnect");
                break;
            case END:
                db.end();
                result = new Result("disconnect");
                break;
            case CREATE_TABLE:
                db.createTable(table);
                result = new Result("createTable");
                break;
            case DROP_TABLE:
                db.dropTable(table);
                result = new Result("dropTable");
                break;
            case CREATE_INDEX:
                db.createIndex(index);
                result = new Result("createIndex");
                break;
            case DROP_INDEX:
                db.dropIndex(index);
                result = new Result("dropIndex");
                break;
            case INSERT:
                result = db.insert(table, columns, values);
                break;
            case SELECT:
                result = select(db);
                break;
            case DELETE:
                result = db.delete(table, condition);
                break;
            case UPDATE:
                result = db.update(table, columns, values, condition);
                break;
            case AUTOCOMMIT_ON:
                db.setAutoCommit(true);
                result = new Result("setAutoCommit true");
                break;
            case AUTOCOMMIT_OFF:
                db.setAutoCommit(false);
                result = new Result("setAutoCommit false");
                break;
            case COMMIT:
                db.commit();
                result = new Result("commit");
                break;
            case ROLLBACK:
                db.rollback();
                result = new Result("rollback");
                break;
            default:
                throw new AssertionError("type=" + type);
            }
        } catch (SQLException e) {
            result = new Result("", e);
        }
        return result;
    }

//    public String getNextTableAlias() {
//        return "S" + nextAlias++;
//    }

    /**
     * Get a random table alias name.
     *
     * @return the alias name
     */
    String getRandomTableAlias() {
        if (tables == null) {
            return null;
        }
        Object[] list = tables.keySet().toArray();
        int i = config.random().getInt(list.length);
        return (String) list[i];
    }

    /**
     * Get the table with the specified alias.
     *
     * @param alias the alias or null if there is only one table
     * @return the table
     */
    Table getTable(String alias) {
        if (alias == null) {
            return table;
        }
        return tables.get(alias);
    }

//    public void addJoin(String string) {
//        join += string;
//    }

//    static Command getSelectAll(TestSynth config, Table table) {
//        Command command = new Command(config, Command.SELECT, table, "M");
//        command.selectList = new String[] { "*" };
//        command.order = "";
//        return command;
//    }

}
