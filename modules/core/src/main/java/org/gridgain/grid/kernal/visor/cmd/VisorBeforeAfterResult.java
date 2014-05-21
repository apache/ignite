/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.io.*;

/**
 * Task result with before after state.
 */
public class VisorBeforeAfterResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final long before;

    private final long after;

    public VisorBeforeAfterResult(long before, long after) {
        this.before = before;
        this.after = after;
    }

    /**
     * @return Before.
     */
    public long before() {
        return before;
    }

    /**
     * @return After.
     */
    public long after() {
        return after;
    }
}
