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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.IgniteUtils.byteArray2HexString;

/**
 *
 */
public class SqlScriptRunner {
    /** UTF-8 character name. */
    static final Charset UTF_8 = StandardCharsets.UTF_8;

    /** Hashing label pattern. */
    private static final Pattern HASHING_PTRN = Pattern.compile("([0-9]+) values hashing to ([0-9a-fA-F]+)");

    /** NULL label. */
    private static final String NULL = "NULL";

    /** Default schema. */
    private static final String schemaPublic = "PUBLIC";

    /** Comparator for "rowsort" sort mode. */
    private final Comparator<List<?>> ROW_COMPARATOR = (r1, r2) -> {
        int rows = r1.size();

        for (int i = 0; i < rows; ++i) {
            String s1 = toString(r1.get(i));
            String s2 = toString(r2.get(i));

            int comp = s1.compareTo(s2);
            if (comp != 0)
                return comp;
        }

        return 0;
    };

    /** Comparator for "valuesort" sort mode. */
    private static final Comparator<Object> ITM_COMPARATOR = (r1, r2) -> {
        String s1 = String.valueOf(r1);
        String s2 = String.valueOf(r2);

        return s1.compareTo(s2);
    };

    /** Test script path. */
    private final Path test;

    /** Query engine. */
    private final QueryEngine engine;

    /** Loop variables. */
    private final Map<String, Integer> loopVars = new HashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /** Script. */
    private Script script;

    /** String presentation of null's. */
    private String nullLbl = NULL;

    /** nl to bytes representation. */
    private static final byte[] NL_BYTES = "\n".getBytes();

    /** Eqivalent results store. */
    private Map<String, Collection<String>> eqResStorage = new HashMap<>();

    /** Hash algo. */
    MessageDigest messageDigest;

    /** */
    public SqlScriptRunner(Path test, QueryEngine engine, IgniteLogger log) {
        this.test = test;
        this.engine = engine;
        this.log = log;

        try {
            messageDigest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public void run() throws Exception {
        try (Script s = new Script(test)) {
            script = s;
            nullLbl = NULL;

            for (Command cmd : script) {
                try {
                    cmd.execute();
                }
                finally {
                    loopVars.clear();
                }
            }
        }
    }

    /** */
    private List<List<?>> sql(String sql) {
        if (!loopVars.isEmpty()) {
            for (Map.Entry<String, Integer> loopVar : loopVars.entrySet())
                sql = sql.replaceAll("\\$\\{" + loopVar.getKey() + "\\}", loopVar.getValue().toString());
        }

        log.info("Execute: " + sql);

        List<FieldsQueryCursor<List<?>>> curs = engine.query(null, schemaPublic, sql);

        assert curs.size() == 1 : "Unexpected results [cursorsCount=" + curs.size() + ']';

        try (QueryCursor<List<?>> cur = curs.get(0)) {
            return cur.getAll();
        }
    }

    /** */
    private String toString(Object res) {
        if (res == null)
            return nullLbl;
        else if (res instanceof byte[])
            return ByteString.toString((byte[])res, 16);
        else if (res instanceof Map)
            return mapToString((Map<?, ?>)res);
        else
            return String.valueOf(res);
    }

    /** */
    private String mapToString(Map<?, ?> map) {
        if (map == null)
            return nullLbl;

        List<String> entries = (new TreeMap<>(map)).entrySet().stream()
            .map(e -> toString(e.getKey()) + ":" + toString(e.getValue()))
            .collect(Collectors.toList());

        return "{" + String.join(", ", entries) + "}";
    }

    /** */
    private class Script implements Iterable<Command>, AutoCloseable {
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
            return nextLineWithoutTrim();
        }

        /** */
        String nextLineWithoutTrim() throws IOException {
            String s = r.readLine();

            lineNum++;

            return s;
        }

        /** */
        boolean ready() throws IOException {
            return r.ready();
        }

        /** */
        String positionDescription() {
            return '(' + fileName + ':' + lineNum + ')';
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            r.close();
        }

        /** */
        private Command nextCommand() {
            try {
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
            catch (IOException e) {
                throw new RuntimeException("Cannot read next command", e);
            }
        }

        /** */
        @NotNull @Override public Iterator<Command> iterator() {
            final Command cmd0 = nextCommand();
            return new Iterator<Command>() {
                /** */
                private Command cmd = cmd0;

                @Override public boolean hasNext() {
                    return cmd != null;
                }

                @Override public Command next() {
                    if (cmd == null)
                        throw new NoSuchElementException();

                    Command ret = cmd;

                    cmd = nextCommand();

                    return ret;
                }
            };
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

                if ("PRAGMA".equals(toks[0])) {
                    String[] pragmaParams = toks[1].split("=");

                    if ("null".equals(pragmaParams[0]))
                        nullLbl = pragmaParams[1];
                    else
                        log.info("Ignore: " + toString());

                    continue;
                }

                try {
                    sql(qry);

                    if (expected != ExpectedStatementStatus.OK)
                        throw new IgniteException("Error expected at: " + posDesc + ". Statement: " + this);
                }
                catch (Throwable e) {
                    if (expected != ExpectedStatementStatus.ERROR)
                        throw new IgniteException("Error at: " + posDesc + ". Statement: " + this, e);
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
        /** */
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

        /** Sorting algo. */
        SortType sortType = SortType.NOSORT;

        /** Equality label. */
        String eqLabel;

        /** */
        Query(String[] cmd) throws IOException {
            String resTypesChars = cmd[1];

            if (cmd.length > 2)
                sortType = SortType.valueOf(cmd[2].toUpperCase());

            if (cmd.length > 3)
                eqLabel = cmd[3].toLowerCase();

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
                        throw new IgniteException("Unknown type character '" + resTypesChars.charAt(i) + "' at: "
                            + script.positionDescription() + "[cmd=" + Arrays.toString(cmd) + ']');
                }
            }

            if (F.isEmpty(resTypes)) {
                throw new IgniteException("Missing type string at: "
                    + script.positionDescription() + "[cmd=" + Arrays.toString(cmd) + ']');
            }

            // Read SQL query
            while (script.ready()) {
                String s = script.nextLine();

                if (s.equals("----"))
                    break;

                if (sql.length() > 0)
                    sql.append(U.nl());

                sql.append(s);
            }

            // Read expected results
            String s = script.nextLineWithoutTrim();
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
                        throw new IgniteException("Invalid columns count at the result at: "
                            + script.positionDescription() + " [row=\"" + s + "\", types=" + resTypes + ']');
                    }

                    try {
                        if (singleValOnLine) {
                            row.add(nullLbl.equals(vals[0]) ? null : vals[0]);

                            if (row.size() == resTypes.size()) {
                                expectedRes.add(row);

                                row = new ArrayList<>();
                            }
                        }
                        else {
                            for (String val : vals)
                                row.add(nullLbl.equals(val) ? null : val);

                            expectedRes.add(row);
                            row = new ArrayList<>();
                        }
                    }
                    catch (Exception e) {
                        throw new IgniteException("Cannot parse expected results at: "
                            + script.positionDescription() + "[row=\"" + s + "\", types=" + resTypes + ']', e);
                    }

                    s = script.nextLineWithoutTrim();
                }
            }
        }

        /** {@inheritDoc} */
        @Override void execute() {
            try {
                List<List<?>> res = sql(sql.toString());

                checkResult(res);
            }
            catch (Throwable e) {
                throw new IgniteException("Error at: " + posDesc + ". sql: " + sql, e);
            }
        }

        /** */
        void checkResult(List<List<?>> res) {
            if (sortType == SortType.ROWSORT) {
                res.sort(ROW_COMPARATOR);

                if (expectedRes != null)
                    expectedRes.sort(ROW_COMPARATOR);
            }
            else if (sortType == SortType.VALUESORT) {
                List<Object> flattenRes = new ArrayList<>();

                res.forEach(flattenRes::addAll);

                flattenRes.sort(ITM_COMPARATOR);

                List<List<?>> resSizeAware = new ArrayList<>();
                int rowLen = resTypes.size();
                List rowRes = new ArrayList(rowLen);
                for (Object item : flattenRes) {
                    rowRes.add(item);

                    if (--rowLen == 0) {
                        resSizeAware.add(rowRes);
                        rowRes = new ArrayList(rowLen);
                        rowLen = resTypes.size();
                    }
                }

                res = resSizeAware;
            }

            if (expectedHash != null)
                checkResultsHashed(res);
            else
                checkResultTuples(res);
        }

        /** */
        private void checkResultTuples(List<List<?>> res) {
            if (expectedRes.size() != res.size()) {
                throw new AssertionError("Invalid results rows count at: " + posDesc +
                    ". [expectedRows=" + expectedRes.size() + ", actualRows=" + res.size() +
                    ", expected=" + expectedRes + ", actual=" + res + ']');
            }

            for (int i = 0; i < expectedRes.size(); ++i) {
                List<String> expectedRow = expectedRes.get(i);
                List<?> row = res.get(i);

                if (row.size() != expectedRow.size()) {
                    throw new AssertionError("Invalid columns count at: " + posDesc +
                        ". [expected=" + expectedRes + ", actual=" + res + ']');
                }

                for (int j = 0; j < expectedRow.size(); ++j) {
                    checkEquals("Not expected result at: " + posDesc +
                        ". [row=" + i + ", col=" + j +
                        ", expected=" + expectedRow.get(j) + ", actual=" + SqlScriptRunner.this.toString(row.get(j)) + ']',
                        expectedRow.get(j),
                        row.get(j)
                    );
                }
            }
        }

        /** */
        private void checkEquals(String msg, String expectedStr, Object actual) {
            if (actual == null && (expectedStr == null || nullLbl.equalsIgnoreCase(expectedStr)))
                return;

            if (actual != null ^ expectedStr != null)
                throw new AssertionError(msg);

            // Alternative values for boolean "0" and "1".
            if (actual instanceof Boolean && expectedStr.equals((Boolean)actual ? "1" : "0"))
                return;

            if (actual instanceof Number) {
                if ("NaN".equals(expectedStr) || expectedStr.endsWith("Infinity")) {
                    if (!expectedStr.equals(String.valueOf(actual)))
                        throw new AssertionError(msg);
                }
                else {
                    BigDecimal actDec = new BigDecimal(String.valueOf(actual));
                    BigDecimal expDec = new BigDecimal(expectedStr);

                    if (actDec.compareTo(expDec) != 0)
                        throw new AssertionError(msg);
                }
            }
            else if (actual instanceof Map) {
                String actualStr = mapToString((Map<? extends Comparable, ?>)actual);

                if (!expectedStr.equals(actualStr))
                    throw new AssertionError(msg);
            }
            else {
                if (!expectedStr.equals(SqlScriptRunner.this.toString(actual)) &&
                    !("(empty)".equals(expectedStr) && SqlScriptRunner.this.toString(actual).isEmpty()))
                     throw new AssertionError(msg);
            }
        }

        /** */
        private void checkResultsHashed(List<List<?>> res) {
            Objects.requireNonNull(res, "empty result set");

            messageDigest.reset();

            for (List<?> row : res) {
                for (Object col : row) {
                    messageDigest.update(SqlScriptRunner.this.toString(col).getBytes(Charset.forName(UTF_8.name())));
                    messageDigest.update(NL_BYTES);
                }
            }

            String res0 = byteArray2HexString(messageDigest.digest(), false);

            if (eqLabel != null) {
                if (res0.equals(expectedHash))
                    eqResStorage.computeIfAbsent(eqLabel, k -> new ArrayList<>()).add(sql.toString());
                else {
                    Collection<String> eq = eqResStorage.get(eqLabel);

                    if (eq != null)
                        throw new AssertionError("Results of queries need to be equal: " + eq +
                            U.nl() + " and " + U.nl() + sql);
                }
            }

            if (!res0.equals(expectedHash))
                throw new AssertionError("Unexpected hash result, error at: " + posDesc +
                    ", expected=" + expectedHash + ", calculated=" + res0 +
                    ", expectedRows=" + expectedRows + ", returnedRows=" + res.size() * res.get(0).size());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Query.class, this);
        }
    }

    /** */
    private enum ExpectedStatementStatus {
        /** */
        OK,

        /** */
        ERROR
    }

    /** */
    private enum ColumnType {
        /** */
        I,

        /** */
        T,

        /** */
        R
    }

    /** */
    private enum SortType {
        /** */
        ROWSORT,

        /** */
        VALUESORT,

        /** */
        NOSORT
    }
}
