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

import java.io.PrintWriter;

/**
 * Simple ASCII spinner. It should be used for processes with unknown duration instead of {@link ProgressBar}
 */
public class Spinner implements AutoCloseable {
    /** Writer for display spinner. */
    private final PrintWriter out;

    /** Text prefix for spinner. */
    private final String spinnerPrefixText;

    /** Spinner cycle counter. */
    private int spinnerCnt;

    /**
     * @param out Writer to display spinner.
     */
    public Spinner(PrintWriter out) {
        this(out, "");
    }

    /**
     * @param out               Writer to display spinner.
     * @param spinnerPrefixText Spinner prefix text.
     */
    public Spinner(PrintWriter out, String spinnerPrefixText) {
        this.out = out;
        this.spinnerPrefixText = spinnerPrefixText;
    }

    /**
     * Spin one more time.
     */
    public void spin() {
        out.print("\r" + spinnerPrefixText + ".".repeat(1 + spinnerCnt % 3) + " ".repeat(2 - spinnerCnt % 3));
        out.flush();

        // Reset counter to protect it from overflow.
        spinnerCnt = 1 + spinnerCnt % 3;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        out.print("\r" + spinnerPrefixText + "...");
        out.println();

        out.flush();
    }
}
