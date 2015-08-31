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

package org.apache.ignite.internal.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Timer to use mostly for debugging purposes.
 */
public class GridTimer {
    /** Debug date format. */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SS");

    /** Timer name. */
    private final String name;

    /** Start time. */
    private final long start = U.currentTimeMillis();

    /** End time. */
    private long end;

    /** Max duration threshold. */
    private long threshold;

    /**
     * @param name Timer name.
     */
    public GridTimer(String name) {
        this.name = name;

        threshold = -1;
    }

    /**
     * @param name Timer name.
     * @param threshold Max duration threshold.
     */
    public GridTimer(String name, long threshold) {
        this.name = name;
        this.threshold = threshold;
    }

    /**
     * Stops this timer.
     *
     * @return Duration.
     */
    public long stop() {
        end = U.currentTimeMillis();

        if (maxedOut())
            debug("Timer maxed out [name=" + name + ", duration=" + duration() + ']');

        return end - start;
    }

    /**
     * Stops this timer.
     *
     * @return {@code True} if didn't max out.
     */
    public boolean stopx() {
        end = U.currentTimeMillis();

        if (maxedOut()) {
            debug("Timer maxed out [name=" + name + ", duration=" + duration() + ']');

            return false;
        }

        return true;
    }

    /**
     * @return {@code True} if maxed out.
     */
    public boolean maxedOut() {
        return threshold > 0 && duration() >= threshold;
    }

    /**
     * @return {@code True} if stopped.
     */
    boolean stopped() {
        return end > 0;
    }

    /**
     * @return Timer duration.
     */
    public long duration() {
        return end > 0 ? end - start : U.currentTimeMillis() - start;
    }

    /**
     * @return End time ({@code -1} if not stopped).
     */
    public long endTime() {
        return end;
    }

    /**
     * @return Start time.
     */
    public long startTime() {
        return start;
    }

    /**
     * @return Max duration threshold.
     */
    public long threshold() {
        return threshold;
    }

    /**
     * @return Timer name.
     */
    public String name() {
        return name;
    }

    /**
     * @param msg Message to debug.
     */
    private void debug(String msg) {
        System.out.println('<' + DEBUG_DATE_FMT.format(new Date(U.currentTimeMillis())) + "><DEBUG><" +
            Thread.currentThread().getName() + "> " + msg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTimer.class, this, "duration", duration());
    }
}