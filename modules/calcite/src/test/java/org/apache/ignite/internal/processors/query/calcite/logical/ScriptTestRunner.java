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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ScriptTestRunner {
    /** Test script path. */
    private final Path test;

    /** Query engine. */
    private final QueryEngine engine;

    /** Logger. */
    private final IgniteLogger log;

    /** Script. */
    private Script script;

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
                String s = script.nextLine();

                if (F.isEmpty(s) || s.startsWith("#"))
                    continue;

                String[] tokens = s.split("\\s+");

                if (tokens.length < 2)
                    throw new IgniteException("Invalid command line. " + script.positionDescription() + ". [cmd=" + s + ']');

                Command cmd;
                switch (tokens[0]) {
                    case "statement":
                        cmd = new Statement(tokens);

                        break;

                    case "query":
                        cmd = new Query(tokens);

                        break;

                    default:
                        throw new IgniteException("Unexpected command. "
                            + script.positionDescription() + ". [cmd=" + s + ']');
                }

                cmd.execute();
            }
        }
        finally {
            script.close();
        }
    }

    /** */
    private static class Script implements AutoCloseable {
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
    }

    /** */
    private abstract class Command {
        /** */
        abstract void execute();
    }

    /** */
    private class Statement extends Command {
        /** */
        @GridToStringInclude
        List<String> sql;

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
                        + script.positionDescription() + "[cmd = " + Arrays.toString(cmd) + ']');
            }

            sql = new ArrayList<>();

            while (script.ready()) {
                String s = script.nextLine();

                if (F.isEmpty(s))
                    break;

                sql.add(s);
            }
        }

        /** {@inheritDoc} */
        @Override void execute() {
            // TODO
            log.info("+++ " + toString());
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
        List<List<Object>> expectedRes;

        /** */
        Query(String[] cmd) throws IOException {
            String resTypesChars = cmd[1];

            for (int i = 0; i < resTypesChars.length(); i++) {
                switch (resTypesChars.charAt(i)) {
                    case 'I':
                        resTypes.add(ColumnType.INTEGER);

                        break;

                    case 'R':
                        resTypes.add(ColumnType.DECIMAL);

                        break;

                    case 'T':
                        resTypes.add(ColumnType.STRING);

                        break;

                    default:
                        throw new IgniteException("Unknown type character '" + resTypesChars.charAt(i) + "'. "
                            + script.positionDescription() + "[cmd = " + Arrays.toString(cmd) + ']');
                }
            }

            if (F.isEmpty(resTypes))
                throw new IgniteException("Missing type string. "
                    + script.positionDescription() + "[cmd = " + Arrays.toString(cmd) + ']');

            while (script.ready()) {
                String s = script.nextLine();

                if (s.equals("----"))
                    break;

                sql.append(s);
            }
        }

        /** {@inheritDoc} */
        @Override void execute() {
            log.info("+++ " + toString());
            // TODO
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
        INTEGER,
        STRING,
        DECIMAL
    }
}
