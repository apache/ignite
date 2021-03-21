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

package org.apache.ignite.cli.ui;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.ignite.cli.AbstractCliTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static picocli.CommandLine.Help.Ansi.AUTO;

/** */
public class ProgressBarTest extends AbstractCliTest {
    /** */
    private PrintWriter out;

    /** */
    private ByteArrayOutputStream outputStream;

    /** */
    @BeforeEach
    private void setUp() {
        outputStream = new ByteArrayOutputStream();
        out = new PrintWriter(outputStream, true);
    }

    /** */
    @AfterEach
    private void tearDown() throws IOException {
        out.close();
        outputStream.close();
    }

    /** */
    @Test
    public void testScaledToTerminalWidth() throws IOException {
        var progressBar = new ProgressBar(out, 3, 80);
        progressBar.step();
        assertEquals(80, outputStream.toString().replace("\r", "").length());
        assertEquals(
            "\r|========================>                                                 | 33%",
            outputStream.toString()
        );
    }

    /** */
    @Test
    public void testRedundantStepsProgressBar() {
        var progressBar = new ProgressBar(out, 3, 80);
        progressBar.step();
        progressBar.step();
        progressBar.step();
        progressBar.step();
        assertEquals(AUTO.string(
            "\r|========================>                                                 | 33%" +
                "\r|================================================>                         | 66%" +
                "\r|==========================================================================|@|green,bold Done!|@" +
                "\r|==========================================================================|@|green,bold Done!|@"),
            outputStream.toString()
        );
    }
}
