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

package org.apache.ignite.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Example test utilities.
 */
public class ExampleTestUtils {
    /**
     * Interface for a general example main function.
     */
    @FunctionalInterface
    public interface ExampleConsumer {
        void accept(String[] args) throws Exception;
    }

    /**
     * Capture console output of the example.
     *
     * @param consumer Method which output should be captured. Ordinary main of the example.
     * @param args     Arguments.
     * @return Captured output as a string.
     */
    public static String captureConsole(ExampleConsumer consumer, String[] args) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outStream);

        PrintStream old = System.out;

        try {
            System.setOut(printStream);

            consumer.accept(args);

            System.out.flush();
        } finally {
            System.setOut(old);
        }

        return outStream.toString();
    }

    /**
     * Assert that console output of the example equals expected.
     *
     * @param consumer Method which output should be captured. Ordinary main of the example.
     * @param args     Arguments.
     * @param expected Expected console output.
     */
    public static void assertConsoleOutput(ExampleConsumer consumer, String[] args, String expected) throws Exception {
        String captured = ExampleTestUtils.captureConsole(consumer, args);

        captured = captured.replaceAll("\r", "");

        assertEquals(expected, captured);
    }

    /**
     * Assert that console output of the example equals expected.
     *
     * @param consumer Method which output should be captured. Ordinary main of the example.
     * @param args     Arguments.
     * @param expected Expected console output.
     */
    public static void assertConsoleOutputContains(ExampleConsumer consumer, String[] args,
            String... expected) throws Exception {
        String captured = ExampleTestUtils.captureConsole(consumer, args);

        captured = captured.replaceAll("\r", "");

        for (String single : expected) {
            assertTrue(captured.contains(single));
        }
    }
}
