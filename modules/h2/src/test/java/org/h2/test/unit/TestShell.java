/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import org.h2.test.TestBase;
import org.h2.tools.Shell;
import org.h2.util.Task;

/**
 * Test the shell tool.
 */
public class TestShell extends TestBase {

    /**
     * The output stream of the tool.
     */
    PrintStream toolOut;

    /**
     * The input stream of the tool.
     */
    InputStream toolIn;

    private LineNumberReader lineReader;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        Shell shell = new Shell();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        shell.setOut(new PrintStream(buff));
        shell.runTool("-url", "jdbc:h2:mem:", "-driver", "org.h2.Driver",
                "-user", "sa", "-password", "sa", "-properties", "null",
                "-sql", "select 'Hello ' || 'World' as hi");
        String s = new String(buff.toByteArray());
        assertContains(s, "HI");
        assertContains(s, "Hello World");
        assertContains(s, "(1 row, ");

        shell = new Shell();
        buff = new ByteArrayOutputStream();
        shell.setOut(new PrintStream(buff));
        shell.runTool("-help");
        s = new String(buff.toByteArray());
        assertContains(s,
                "Interactive command line tool to access a database using JDBC.");

        test(true);
        test(false);
    }

    private void test(final boolean commandLineArgs) throws IOException {
        PipedInputStream testIn = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(testIn);
        toolOut = new PrintStream(out, true);
        out = new PipedOutputStream();
        PrintStream testOut = new PrintStream(out, true);
        toolIn = new PipedInputStream(out);
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                try {
                    Shell shell = new Shell();
                    shell.setIn(toolIn);
                    shell.setOut(toolOut);
                    shell.setErr(toolOut);
                    if (commandLineArgs) {
                        shell.runTool("-url", "jdbc:h2:mem:",
                                "-user", "sa", "-password", "sa");
                    } else {
                        shell.runTool();
                    }
                } finally {
                    toolOut.close();
                }
            }
        };
        task.execute();
        InputStreamReader reader = new InputStreamReader(testIn);
        lineReader = new LineNumberReader(reader);
        read("");
        read("Welcome to H2 Shell");
        read("Exit with");
        if (!commandLineArgs) {
            read("[Enter]");
            testOut.println("jdbc:h2:mem:");
            read("URL");
            testOut.println("");
            read("Driver");
            testOut.println("sa");
            read("User");
            testOut.println("sa");
            read("Password");
        }
        read("Commands are case insensitive");
        read("help or ?");
        read("list");
        read("maxwidth");
        read("autocommit");
        read("history");
        read("quit or exit");
        read("");
        testOut.println("history");
        read("sql> No history");
        testOut.println("1");
        read("sql> Not found");
        testOut.println("select 1 a;");
        read("sql> A");
        read("1");
        read("(1 row,");
        testOut.println("history");
        read("sql> #1: select 1 a");
        read("To re-run a statement, type the number and press and enter");
        testOut.println("1");
        read("sql> select 1 a");
        read("A");
        read("1");
        read("(1 row,");

        testOut.println("select 'x' || space(1000) large, 'y' small;");
        read("sql> LARGE");
        read("x");
        read("(data is partially truncated)");
        read("(1 row,");

        testOut.println("select x, 's' s from system_range(0, 10001);");
        read("sql> X    | S");
        for (int i = 0; i < 10000; i++) {
            read((i + "     ").substring(0, 4) + " | s");
        }
        for (int i = 10000; i <= 10001; i++) {
            read((i + "     ").substring(0, 5) + " | s");
        }
        read("(10002 rows,");

        testOut.println("select error;");
        read("sql> Error:");
        if (read("").startsWith("Column \"ERROR\" not found")) {
            read("");
        }
        testOut.println("create table test(id int primary key, name varchar)\n;");
        read("sql> ...>");
        testOut.println("insert into test values(1, 'Hello');");
        read("sql>");
        testOut.println("select null n, * from test;");
        read("sql> N    | ID | NAME");
        read("null | 1  | Hello");
        read("(1 row,");

        // test history
        for (int i = 0; i < 30; i++) {
            testOut.println("select " + i + " ID from test;");
            read("sql> ID");
            read("" + i);
            read("(1 row,");
        }
        testOut.println("20");
        read("sql> select 10 ID from test");
        read("ID");
        read("10");
        read("(1 row,");

        testOut.println("maxwidth");
        read("sql> Usage: maxwidth <integer value>");
        read("Maximum column width is now 100");
        testOut.println("maxwidth 80");
        read("sql> Maximum column width is now 80");
        testOut.println("autocommit");
        read("sql> Usage: autocommit [true|false]");
        read("Autocommit is now true");
        testOut.println("autocommit false");
        read("sql> Autocommit is now false");
        testOut.println("autocommit true");
        read("sql> Autocommit is now true");
        testOut.println("list");
        read("sql> Result list mode is now on");

        testOut.println("select 1 first, 2 second;");
        read("sql> FIRST : 1");
        read("SECOND: 2");
        read("(1 row, ");

        testOut.println("select x from system_range(1, 3);");
        read("sql> X: 1");
        read("");
        read("X: 2");
        read("");
        read("X: 3");
        read("(3 rows, ");

        testOut.println("select x, 2 as y from system_range(1, 3) where 1 = 0;");
        read("sql> X");
        read("Y");
        read("(0 rows, ");

        testOut.println("list");
        read("sql> Result list mode is now off");
        testOut.println("help");
        read("sql> Commands are case insensitive");
        read("help or ?");
        read("list");
        read("maxwidth");
        read("autocommit");
        read("history");
        read("quit or exit");
        read("");
        testOut.println("exit");
        read("sql>");
        task.get();
    }

    private String read(String expectedStart) throws IOException {
        String line = lineReader.readLine();
        // System.out.println(": " + line);
        assertStartsWith(line, expectedStart);
        return line;
    }

}
