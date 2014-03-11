/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;

import java.text.*;
import java.util.*;

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
     * @return End time ({@code -1 if not stopped).
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
