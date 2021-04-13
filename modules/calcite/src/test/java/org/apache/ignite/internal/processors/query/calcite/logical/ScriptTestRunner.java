/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.logical;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ScriptTestRunner {
    /** Hashing label. */
    private static final Pattern HASHING_PTRN = Pattern.compile("([0-9]+) values hashing to ([0-9a-fA-F]+)");

    /** Hashing label. */
    private static final Set<String> ignoredStmts = ImmutableSet.of("PRAGMA");

    /** NULL label. */
    private static final String NULL = "NULL";

    /** NULL label. */
    private static final String schemaPublic = "PUBLIC";

    /** Test script path. */
    private final Path test;

    /** Query engine. */
    private final QueryEngine engine;

    /** Logger. */
    private final IgniteLogger log;

    /** Script. */
    private Script script;

    /** Loop variables. */
    private HashMap<String, Integer> loopVars = new HashMap<>();

    /** */
    public ScriptTestRunner(Path test, QueryEngine engine, IgniteLogger log) {
        this.test = test;
        this.engine = engine;
        this.log = log;
    }

    /** */
    public void run() throws Exception {
        script = new Script(test);

        try {
            while (script.ready()) {
                try {
                    Command cmd = script.nextCommand();

                    if (cmd != null)
                        cmd.execute();
                }
                finally {
                    loopVars.clear();
                }
            }
        }
        finally {
            script.close();
        }
    }

    /** */
    private class Script implements AutoCloseable {
        /** Reader. */
        private final String fileName;

        /** Reader. */
        private final BufferedReader r;

        /** Line number. */
        private int lineNum;

        /** */
        Script(Path test) throws IOException {
            fileName = test.getFileName().toString();

            r = Files.newBufferedReader(test);
        }

        /** */
        String nextLine() throws IOException {
            while (r.ready()) {
                String s = r.readLine();

                lineNum++;

                return s.trim();
            }

            return null;
        }

        /** */
        boolean ready() throws IOException {
            return r.ready();
        }

        /** */
        public int lineNumber() {
            return lineNum;
        }

        /** */
        String positionDescription() {
            return fileName + ':' + lineNum;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            r.close();
        }

        /** */
        Command nextCommand() throws IOException {
            while (script.ready()) {
                String s = script.nextLine();

                if (F.isEmpty(s) || s.startsWith("#"))
                    continue;

                String[] tokens = s.split("\\s+");

                assert !F.isEmpty(tokens) : "Invalid command line. "
                        + script.positionDescription() + ". [cmd=" + s + ']';

                Command cmd = null;

                switch (tokens[0]) {
                    case "statement":
                        cmd = new Statement(tokens);

                        break;

                    case "query":
                        cmd = new Query(tokens);

                        break;

                    case "loop":
                        cmd = new Loop(tokens);

                        break;

                    case "endloop":
                        cmd = new EndLoop();

                        break;

                    case "mode":
                        // TODO: output_hash. output_result, debug, skip, unskip

                        break;

                    default:
                        throw new IgniteException("Unexpected command. "
                            + script.positionDescription() + ". [cmd=" + s + ']');
                }

                if (cmd != null)
                   return cmd;
            }

            return null;
        }
    }

    /** */
    private abstract class Command {
        /** */
        protected final String posDesc;

        /** */
        Command() {
            posDesc = script.positionDescription();
        }

        /** */
        abstract void execute();
    }

    /** */
    private class Loop extends Command {
        /** */
        List<Command> cmds = new ArrayList<>();

        /** */
        int begin;

        /** */
        int end;

        /** */
        String var;

        /** */
        Loop(String[] cmdTokens) throws IOException {
            try {
                var = cmdTokens[1];
                begin = Integer.parseInt(cmdTokens[2]);
                end = Integer.parseInt(cmdTokens[3]);
            }
            catch (Exception e) {
                throw new IgniteException("Unexpected loop syntax. "
                    + script.positionDescription() + ". [cmd=" + cmdTokens + ']');
            }

            while (script.ready()) {
                Command cmd = script.nextCommand();

                if (cmd instanceof EndLoop)
                    break;

                cmds.add(cmd);
            }
        }

        /** */
        @Override void execute() {
            for (int i = begin; i < end; ++i) {
                loopVars.put(var, i);

                for (Command c : cmds)
                    c.execute();
            }
        }
    }

    /** */
    private class EndLoop extends Command {
        /** {@inheritDoc} */
        @Override void execute() {
            // No-op.
        }
    }


    /** */
    private class Statement extends Command {
        /** */
        @GridToStringInclude
        List<String> queries;

        /** */
        @GridToStringInclude
        ExpectedStatementStatus expected;

        /** */
        Statement(String[] cmd) throws IOException {
            switch (cmd[1]) {
                case "ok":
                    expected = ExpectedStatementStatus.OK;

                    break;

                case "error":
                    expected = ExpectedStatementStatus.ERROR;

                    break;

                default:
                    throw new IgniteException("Statement argument should be 'ok' or 'error'. "
                        + script.positionDescription() + "[cmd=" + Arrays.toString(cmd) + ']');
            }

            queries = new ArrayList<>();

            while (script.ready()) {
                String s = script.nextLine();

                if (F.isEmpty(s))
                    break;

                queries.add(s);
            }
        }

        /** {@inheritDoc} */
        @Override void execute() {
            for (String qry : queries) {

                String[] toks = qry.split("\\s+");

                if (ignoredStmts.contains(toks[0])) {
                    log.info("Ignore: " + toString());

                    continue;
                }

                log.info("Execute: " + qry);

                try {
                    List<FieldsQueryCursor<List<?>>> curs = engine.query(null, schemaPublic, qry);

                    assert curs.size() == 1 : "Unexpected results [cursorsCount=" + curs.size() + ']';

                    try (QueryCursor<List<?>> cur = curs.get(0)) {
                        cur.getAll();
                    }

                    if (expected != ExpectedStatementStatus.OK)
                        throw new IgniteException("Error expected at: " + posDesc + ". Statement: " + toString());
                }
                catch (Throwable e) {
                    if (expected != ExpectedStatementStatus.ERROR)
                        throw new IgniteException("Error at: " + posDesc + ". Statement: " + toString(), e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Statement.class, this);
        }
    }

    /** */
    private class Query extends Command {
        @GridToStringInclude
        List<ColumnType> resTypes = new ArrayList<>();

        /** */
        @GridToStringInclude
        StringBuilder sql = new StringBuilder();

        /** */
        @GridToStringInclude
        List<List<String>> expectedRes;

        /** */
        String expectedHash;

        /** */
        int expectedRows;

        /** */
        Query(String[] cmd) throws IOException {
            String resTypesChars = cmd[1];

            for (int i = 0; i < resTypesChars.length(); i++) {
                switch (resTypesChars.charAt(i)) {
                    case 'I':
                        resTypes.add(ColumnType.I);

                        break;

                    case 'R':
                        resTypes.add(ColumnType.R);

                        break;

                    case 'T':
                        resTypes.add(ColumnType.T);

                        break;

                    default:
                        throw new IgniteException("Unknown type character '" + resTypesChars.charAt(i) + "'. "
                            + script.positionDescription() + "[cmd=" + Arrays.toString(cmd) + ']');
                }
            }

            if (F.isEmpty(resTypes)) {
                throw new IgniteException("Missing type string. "
                    + script.positionDescription() + "[cmd=" + Arrays.toString(cmd) + ']');
            }

            // Read SQL query
            while (script.ready()) {
                String s = script.nextLine();

                if (s.equals("----"))
                    break;

                sql.append(s);
            }

            // Read expected results
            String s = script.nextLine();
            Matcher m = HASHING_PTRN.matcher(s);

            if (m.matches()) {
                // Expected results are hashing
                expectedRows = Integer.parseInt(m.group(1));
                expectedHash = m.group(2);
            }
            else {
                // Read expected results tuples.
                expectedRes = new ArrayList<>();

                boolean singleValOnLine = false;

                List<String> row = new ArrayList<>();

                while (!F.isEmpty(s)) {
                    String[] vals = s.split("\\t");

                    if (!singleValOnLine && vals.length == 1 && vals.length != resTypes.size())
                        singleValOnLine = true;

                    if (vals.length != resTypes.size() && !singleValOnLine) {
                        throw new IgniteException("Invalid columns count at the result. "
                            + script.positionDescription() + " [row=\"" + s + "\", types=" + resTypes + ']');
                    }

                    try {
                        if (singleValOnLine) {
                            row.add(NULL.equals(vals[0]) ? null : vals[0]);

                            if (row.size() == resTypes.size()) {
                                expectedRes.add(row);

                                row = new ArrayList<>();
                            }
                        }
                        else {
                            for (int i = 0; i < vals.length; ++i)
                                row.add(NULL.equals(vals[i]) ? null : vals[i]);

                            expectedRes.add(row);
                            row = new ArrayList<>();
                        }
                    }
                    catch (Exception e) {
                        throw new IgniteException("Cannot parse expected results. "
                            + script.positionDescription() + "[row=\"" + s + "\", types=" + resTypes + ']', e);
                    }

                    s = script.nextLine();
                }
            }
        }

        /** {@inheritDoc} */
        @Override void execute() {
            log.info("Execute: " + sql);

            try {
                List<FieldsQueryCursor<List<?>>> curs = engine.query(null, schemaPublic, sql.toString());

                assert curs.size() == 1 : "Unexpected results [cursorsCount=" + curs.size() + ']';

                try (QueryCursor<List<?>> cur = curs.get(0)) {
                    List<List<?>> res = cur.getAll();

                    checkResult(res);
                }
            }
            catch (IgniteSQLException e) {
                throw new IgniteException("Error at: " + posDesc + ". sql: " + sql, e);
            }
        }

        /** */
        void checkResult(List<List<?>> res) {
            if (expectedHash != null)
                checkResultsHashed(res);
            else
                checkResultTuples(res);
        }

        /** */
        private void checkResultTuples(List<List<?>> res) {
            if (expectedRes.size() != res.size()) {
                throw new AssertionError("Invalid results rows count at " + posDesc +
                    ". [expected=" + expectedRes + ", actual=" + res + ']');
            }

            for (int i = 0; i < expectedRes.size(); ++i) {
                List<String> expectedRow = expectedRes.get(i);
                List<?> row = res.get(i);

                if (row.size() != expectedRow.size()) {
                    throw new AssertionError("Invalid columns count at " + posDesc +
                        ". [expected=" + expectedRes + ", actual=" + res+ ']');
                }

                for (int j = 0; j < expectedRow.size(); ++j) {
                    checkEquals("Not expected result at " + posDesc +
                        ". [row=" + i + ", col=" + j +
                        ", expected=" + expectedRow.get(j) + ", actual=" + row.get(j) + ']', expectedRow.get(j), row.get(j));
                }
            }
        }

        /** */
        private void checkEquals(String msg, String expectedStr, Object actual) {
            if (actual instanceof Number) {
                BigDecimal actDec = new BigDecimal(String.valueOf(actual));
                BigDecimal expDec = new BigDecimal(expectedStr);

                if (actDec.compareTo(expDec) != 0)
                    throw new AssertionError(msg);
            }
            else {
                if (!String.valueOf(expectedStr).equals(String.valueOf(actual)))
                    throw new AssertionError(msg);
            }
        }

        /** */
        private void checkResultsHashed(List<List<?>> res) {
            // TODO:
            throw new UnsupportedOperationException("Hashed results compare not supported");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Query.class, this);
        }
    }

    /** */
    private enum ExpectedStatementStatus {
        OK,
        ERROR
    }

    /** */
    private enum ColumnType {
        I,
        T,
        R
    }
}
