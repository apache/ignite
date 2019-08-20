/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.util.ArrayList;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.MathUtils;
import org.h2.util.New;

/**
 * A test that generates random SQL statements against a number of databases
 * and compares the results.
 */
public class TestSynth extends TestBase {

    //  TODO hsqldb: call 1||null should return 1 but returns null
    //  TODO hsqldb: call mod(1) should return invalid parameter count

    /**
     * A H2 database connection.
     */
    static final int H2 = 0;

    /**
     * An in-memory H2 database connection.
     */
    static final int H2_MEM = 1;

    /**
     * An HSQLDB database connection.
     */
    static final int HSQLDB = 2;

    /**
     * A MySQL database connection.
     */
    static final int MYSQL = 3;

    /**
     * A PostgreSQL database connection.
     */
    static final int POSTGRESQL = 4;

    private final DbState dbState = new DbState(this);
    private ArrayList<DbInterface> databases;
    private ArrayList<Command> commands;
    private final RandomGen random = new RandomGen();
    private boolean showError, showLog;
    private boolean stopImmediately;
    private int mode;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * Check whether this database is of the specified type.
     *
     * @param isType the database type
     * @return true if it is
     */
    boolean is(int isType) {
        return mode == isType;
    }

    /**
     * Get the random number generator.
     *
     * @return the random number generator
     */
    RandomGen random() {
        return random;
    }

    /**
     * Get a random identifier.
     *
     * @return the random identifier
     */
    String randomIdentifier() {
        int len = random.getLog(8) + 2;
        while (true) {
            return random.randomString(len);
        }
    }

    private void add(Command command) throws Exception {
        command.run(dbState);
        commands.add(command);
    }

    private void addRandomCommands() throws Exception {
        switch (random.getInt(20)) {
        case 0: {
            add(Command.getDisconnect(this));
            add(Command.getConnect(this));
            break;
        }
        case 1: {
            Table table = Table.newRandomTable(this);
            add(Command.getCreateTable(this, table));
            break;
        }
        case 2: {
            Table table = randomTable();
            add(Command.getCreateIndex(this, table.newRandomIndex()));
            break;
        }
        case 3:
        case 4:
        case 5: {
            Table table = randomTable();
            add(Command.getRandomInsert(this, table));
            break;
        }
        case 6:
        case 7:
        case 8: {
            Table table = randomTable();
            add(Command.getRandomUpdate(this, table));
            break;
        }
        case 9:
        case 10: {
            Table table = randomTable();
            add(Command.getRandomDelete(this, table));
            break;
        }
        default: {
            Table table = randomTable();
            add(Command.getRandomSelect(this, table));
        }
        }
    }

    private void testRun(int seed) throws Exception {
        random.setSeed(seed);
        commands = New.arrayList();
        add(Command.getConnect(this));
        add(Command.getReset(this));

        for (int i = 0; i < 1; i++) {
            Table table = Table.newRandomTable(this);
            add(Command.getCreateTable(this, table));
            add(Command.getCreateIndex(this, table.newRandomIndex()));
        }
        for (int i = 0; i < 2000; i++) {
            addRandomCommands();
        }
        // for (int i = 0; i < 20; i++) {
        // Table table = randomTable();
        // add(Command.getRandomInsert(this, table));
        // }
        // for (int i = 0; i < 100; i++) {
        // Table table = randomTable();
        // add(Command.getRandomSelect(this, table));
        // }
        // for (int i = 0; i < 10; i++) {
        // Table table = randomTable();
        // add(Command.getRandomUpdate(this, table));
        // }
        // for (int i = 0; i < 30; i++) {
        // Table table = randomTable();
        // add(Command.getRandomSelect(this, table));
        // }
        // for (int i = 0; i < 50; i++) {
        // Table table = randomTable();
        // add(Command.getRandomDelete(this, table));
        // }
        // for (int i = 0; i < 10; i++) {
        // Table table = randomTable();
        // add(Command.getRandomSelect(this, table));
        // }
        // while(true) {
        // Table table = randomTable();
        // if(table == null) {
        // break;
        // }
        // add(Command.getDropTable(this, table));
        // }
        add(Command.getDisconnect(this));
        add(Command.getEnd(this));

        for (int i = 0; i < commands.size(); i++) {
            Command command = commands.get(i);
            boolean stop = process(seed, i, command);
            if (stop) {
                break;
            }
        }
    }

    private boolean process(int seed, int id, Command command) throws Exception {
        try {

            ArrayList<Result> results = New.arrayList();
            for (int i = 0; i < databases.size(); i++) {
                DbInterface db = databases.get(i);
                Result result = command.run(db);
                results.add(result);
                if (showError && i == 0) {
                    // result.log();
                }
            }
            compareResults(results);

        } catch (Error e) {
            if (showError) {
                TestBase.logError("synth", e);
            }
            System.out.println("new TestSynth().init(test).testCase(" + seed +
                    "); // id=" + id + " " + e.toString());
            if (stopImmediately) {
                System.exit(0);
            }
            return true;
        }
        return false;
    }

    private void compareResults(ArrayList<Result> results) {
        Result original = results.get(0);
        for (int i = 1; i < results.size(); i++) {
            Result copy = results.get(i);
            if (original.compareTo(copy) != 0) {
                if (showError) {
                    throw new AssertionError(
                            "Results don't match: original (0): \r\n" +
                                    original + "\r\n" + "other:\r\n" + copy);
                }
                throw new AssertionError("Results don't match");
            }
        }
    }

    /**
     * Get a random table.
     *
     * @return the table
     */
    Table randomTable() {
        return dbState.randomTable();
    }

    /**
     * Print this message if the log is enabled.
     *
     * @param id the id
     * @param s the message
     */
    void log(int id, String s) {
        if (showLog && id == 0) {
            System.out.println(s);
        }
    }

    int getMode() {
        return mode;
    }

    private void addDatabase(String className, String url, String user,
            String password, boolean useSentinel) {
        DbConnection db = new DbConnection(this, className, url, user,
                password, databases.size(), useSentinel);
        databases.add(db);
    }

    @Override
    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        deleteDb("synth/synth");
        databases = New.arrayList();

        // mode = HSQLDB;
        // addDatabase("org.hsqldb.jdbcDriver", "jdbc:hsqldb:test", "sa", "" );
        // addDatabase("org.h2.Driver", "jdbc:h2:synth;mode=hsqldb", "sa", "");

        // mode = POSTGRESQL;
        // addDatabase("org.postgresql.Driver", "jdbc:postgresql:test", "sa",
        // "sa");
        // addDatabase("org.h2.Driver", "jdbc:h2:synth;mode=postgresql", "sa",
        // "");

        mode = H2_MEM;
        org.h2.Driver.load();
        addDatabase("org.h2.Driver", "jdbc:h2:mem:synth", "sa", "", true);
        addDatabase("org.h2.Driver", "jdbc:h2:" +
                getBaseDir() + "/synth/synth", "sa", "", false);

        // addDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test",
        // "sa", "");
        // addDatabase("org.h2.Driver", "jdbc:h2:synth;mode=mysql", "sa", "");

        // addDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test",
        // "sa", "");
        // addDatabase("org.ldbc.jdbc.jdbcDriver",
        // "jdbc:ldbc:mysql://localhost/test", "sa", "");
        // addDatabase("org.h2.Driver", "jdbc:h2:memFS:synth", "sa", "");

        // MySQL: NOT is bound to column: NOT ID = 1 means (NOT ID) = 1 instead
        // of NOT (ID=1)
        for (int i = 0; i < databases.size(); i++) {
            DbConnection conn = (DbConnection) databases.get(i);
            System.out.println(i + " = " + conn.toString());
        }
        showError = true;
        showLog = false;

        // stopImmediately = true;
        // showLog = true;
        // testRun(110600); // id=27 java.lang.Error: results don't match:
        // original (0):
        // System.exit(0);

        return this;
    }

    private void testCase(int seed) throws Exception {
        deleteDb("synth/synth");
        try {
            printTime("TestSynth " + seed);
            testRun(seed);
        } catch (Error e) {
            TestBase.logError("error", e);
            System.exit(0);
        }
    }

    @Override
    public void test() throws Exception {
        while (true) {
            int seed = MathUtils.randomInt(Integer.MAX_VALUE);
            testCase(seed);
        }
    }

}
