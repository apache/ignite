/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Class for printing progress of some task.
 * Progress is printed {@code chunksNum} times.
 */
public class ProgressPrinter {
    /** */
    private static final int DEFAULT_CHUNKS_NUM = 50;

    /** */
    private static final int MIN_PROGRESS_BAR_LENGTH = 20;

    /** */
    private static final int MAX_CAPTION_LENTH = 40;

    /** */
    private final long total;

    /** */
    private long curr = 0;

    /** */
    private final int chunksNum;

    /** */
    private final String caption;

    /** */
    private final PrintStream printStream;

    /** */
    private int lastChunkLogged = -1;

    /** */
    private Long timeStarted = null;

    /**
     * Constructor.
     *
     * @param caption Caption.
     * @param total Total count of items to process.
     */
    public ProgressPrinter(PrintStream printStream, String caption, long total) {
        this(printStream, caption, total, DEFAULT_CHUNKS_NUM);
    }

    /**
     * Constructor.
     *
     * @param caption Caption.
     * @param total Total count of items to process.
     * @param chunksNum Number of progress bar chunks to print.
     */
    public ProgressPrinter(PrintStream printStream, String caption, long total, int chunksNum) {
        this.printStream = printStream;
        this.caption = caption.length() >= MAX_CAPTION_LENTH ? caption.substring(0, MAX_CAPTION_LENTH) : caption;
        this.total = total;
        this.chunksNum = chunksNum;
    }

    /**
     * Prints current progress.
     */
    public void printProgress() {
        curr++;

        if (curr > total)
            throw new RuntimeException("Current value can't be greater than total value.");

        if (timeStarted == null)
            timeStarted = System.currentTimeMillis();

        final double currRatio = (double)curr / total;

        final int currChunk = (int)(currRatio * chunksNum);

        if (currChunk > lastChunkLogged || curr == total) {
            lastChunkLogged++;

            printProgress0(curr, currRatio);
        }
    }

    /** */
    private void printProgress0(long curr, double currRatio) {
        int progressBarLen = MIN_PROGRESS_BAR_LENGTH + (MAX_CAPTION_LENTH - caption.length());

        String progressBarFmt = "\r%s %4s [%" + progressBarLen + "s] %-50s";

        int percentage = (int)(currRatio * 100);
        int progressCurrLen = (int)(currRatio * progressBarLen);
        long timeRunning = System.currentTimeMillis() - timeStarted;
        long timeEstimated = (long)(timeRunning / currRatio);

        GridStringBuilder progressBuilder = new GridStringBuilder();

        for (int i = 0; i < progressBarLen; i++)
            progressBuilder.a(i < progressCurrLen ? "=" : " ");

        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

        long dayMillis = 24 * 60 * 60 * 1000;
        long daysRunning = timeRunning / dayMillis;
        long daysEstimated = timeEstimated / dayMillis;

        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String txtProgress = String.format(
            "%s/%s (%s%s / %s%s)",
            curr,
            total,
            daysRunning > 0 ? daysRunning + " days " : "",
            timeFormat.format(new Date(timeRunning)),
            daysEstimated > 0 ? daysEstimated + " days " : "",
            timeFormat.format(new Date(timeEstimated))
        );

        String progressBar = String.format(
            progressBarFmt,
            caption + ":",
            percentage + "%",
            progressBuilder.toString(),
            txtProgress
        );

        printStream.print(progressBar);
    }
}
