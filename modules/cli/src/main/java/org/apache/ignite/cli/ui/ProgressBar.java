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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Help.Ansi;

/**
 * Basic implementation of a progress bar.
 */
public class ProgressBar implements AutoCloseable {
    /** Logger. **/
    private final Logger log = LoggerFactory.getLogger(getClass());

    /** Out to output the progress bar UI.. */
    private final PrintWriter out;

    /** Current progress. */
    private int curr;

    /** Maximum progress bar value. */
    private int max;

    /** Target width of bar in symbols. */
    private final int targetBarWidth;

    /** Execute. */
    private ScheduledExecutorService exec;

    /**
     * Creates a new progress bar.
     *
     * @param out Output which terminal render will use
     * @param initMax Initial maximum number of steps.
     * @param terminalWidth Width of user terminal for scale progress bar length
     */
    public ProgressBar(PrintWriter out, int initMax, int terminalWidth) {
        this.out = out;

        assert initMax > 0;

        max = initMax;

        // A huge progress bar for big terminals looks ugly.
        // It's better to have just enough wide progress bar.
        targetBarWidth = Math.min(100, terminalWidth);
    }

    /**
     * Updates maximum number of steps.
     *
     * @param newMax New maximum.
     */
    public void setMax(int newMax) {
        assert newMax > 0;

        max = newMax;
    }

    /**
     * Performs a single step.
     */
    public void step() {
        if (curr < max)
            curr++;

        out.print('\r' + render());
        out.flush();
    }

    /**
     * Performs a single step every N milliseconds.
     *
     * @param interval Interval in milliseconds.
     */
    public void stepPeriodically(long interval) {
        if (exec == null) {
            exec = Executors.newSingleThreadScheduledExecutor();

            exec.scheduleAtFixedRate(this::step, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        while (curr < max) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                break;
            }

            step();
        }

        out.println();
    }

    /**
     * Renders current progress bar state to Ansi string.
     *
     * @return Ansi string with progress bar.
     */
    private String render() {
        assert curr <= max;

        var completedPart = ((double)curr / (double)max);

        // Space reserved for '||Done!'
        var reservedSpace = 7;

        if (targetBarWidth < reservedSpace) {
           log.warn("Terminal width is so small to show the progress bar");

           return "";
        }

        var numOfCompletedSymbols = (int) (completedPart * (targetBarWidth - reservedSpace));

        StringBuilder sb = new StringBuilder("|");

        sb.append("=".repeat(numOfCompletedSymbols));

        String percentage;
        int percentageLen;

        if (completedPart < 1) {
            sb.append('>').append(" ".repeat(targetBarWidth - reservedSpace - numOfCompletedSymbols));

            percentage = (int) (completedPart * 100) + "%";
            percentageLen = percentage.length();

            sb.append("|").append(" ".repeat(4 - percentageLen)).append(percentage);
        }
        else
            sb.append("=|@|green,bold Done!|@");

        return Ansi.AUTO.string(sb.toString());
    }
}
