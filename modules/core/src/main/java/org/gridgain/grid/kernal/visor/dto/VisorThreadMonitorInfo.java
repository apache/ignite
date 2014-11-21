/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import java.lang.management.*;

/**
 * Data transfer object for {@link MonitorInfo}.
 */
public class VisorThreadMonitorInfo extends VisorThreadLockInfo {
    /** */
    private static final long serialVersionUID = 0L;

    /** Stack depth. */
    private final Integer stackDepth;

    /** Stack frame. */
    private final StackTraceElement stackFrame;

    /**
     * Create thread monitor info with given parameters.
     *
     * @param className Fully qualified name of the class of the lock object.
     * @param identityHashCode Identity hash code of the lock object.
     * @param stackDepth Depth in the stack trace where the object monitor was locked.
     * @param stackFrame Stack frame that locked the object monitor.
     */
    public VisorThreadMonitorInfo(String className, Integer identityHashCode, Integer stackDepth,
        StackTraceElement stackFrame) {
        super(className, identityHashCode);

        this.stackDepth = stackDepth;
        this.stackFrame = stackFrame;
    }

    /** Create data transfer object for given monitor info. */
    public static VisorThreadMonitorInfo from(MonitorInfo mi) {
        assert mi != null;

        return new VisorThreadMonitorInfo(mi.getClassName(), mi.getIdentityHashCode(), mi.getLockedStackDepth(),
            mi.getLockedStackFrame());
    }

    /**
     * @return Stack depth.
     */
    public Integer stackDepth() {
        return stackDepth;
    }

    /**
     * @return Stack frame.
     */
    public StackTraceElement stackFrame() {
        return stackFrame;
    }
}
