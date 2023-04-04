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

package org.apache.ignite.util;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commands.api.CLICommandFrontend;
import org.apache.ignite.internal.commands.impl.CLICommandFrontendImpl;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.Command.EXPERIMENTAL_LABEL;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.TIME_PREFIX;
import static org.apache.ignite.internal.commands.impl.CLICommandFrontendImpl.ASCII_LOGO;

/**
 *
 */
public class CliHelpTest extends GridCommandHandlerAbstractTest {
    /** */
    @Test
    public void testHelpParity() {
        doTest(true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "false")
    public void testHelpParityWithoutExperimental() {
        doTest(false);
    }

    /** */
    private void doTest(boolean experimentalEnabled) {
        Function<IgniteLogger, CLICommandFrontend> cliFactory0 = cliFactory;

        try {
            injectTestSystemOut();

            cliFactory = CommandHandler::new;

            assertEquals(EXIT_CODE_OK, execute("--help"));

            String controlShOut = testOut.toString();

            testOut = new ByteArrayOutputStream(16 * 1024);

            injectTestSystemOut();

            cliFactory = CLICommandFrontendImpl::new;

            assertEquals(EXIT_CODE_OK, execute("--help"));

            String cliFrontendOut = testOut.toString();

            if (experimentalEnabled) {
                assertTrue(controlShOut.contains(EXPERIMENTAL_LABEL));
                assertTrue(cliFrontendOut.contains(EXPERIMENTAL_LABEL));
            }
            else {
                assertFalse(controlShOut.contains(EXPERIMENTAL_LABEL));
                assertFalse(cliFrontendOut, cliFrontendOut.contains(EXPERIMENTAL_LABEL));
            }

            diff(controlShOut, cliFrontendOut);
        }
        finally {
            cliFactory = cliFactory0;
        }
    }

    /** */
    private void diff(String controlSh, String igniteCli) {
        List<String> original = stringToList(controlSh);
        List<String> revised = stringToList(igniteCli);

        original = original.subList(0, original.size() - 2); // Removing time info.
        revised = revised.subList(ASCII_LOGO.size(), revised.size() - 2); //Removing logo and time info.

        List<DiffRow> diff = DiffRowGenerator.create()
            .ignoreWhiteSpaces(true)
            .build()
            .generateDiffRows(original, revised);

        int lines = 0;

        if (!diff.isEmpty()) {
            System.setOut(sysOut);
            System.setIn(sysIn);

            Consumer<String> printer = System.out::println;

            for (DiffRow row : diff) {
                if (row.getOldLine().startsWith(TIME_PREFIX) || row.getTag() == DiffRow.Tag.EQUAL)
                    continue;

                lines++;

                if (row.getTag() == DiffRow.Tag.CHANGE) {
                    printer.accept("- " + row.getOldLine());
                    printer.accept("+ " + row.getNewLine());
                }
                else if (row.getTag() == DiffRow.Tag.INSERT)
                    printer.accept("+ " + row.getNewLine());
                else if (row.getTag() == DiffRow.Tag.DELETE)
                    printer.accept("- " + row.getOldLine());
            }
        }

        assertTrue("Diff must be empty [lines=" + lines + ']', lines == 0);
    }

    /** */
    public static List<String> stringToList(String str) {
        return Arrays.asList(str.split(System.lineSeparator()));
    }

    /** {@inheritDoc} */
    @Override protected boolean showTestOutput() {
        return false;
    }
}
