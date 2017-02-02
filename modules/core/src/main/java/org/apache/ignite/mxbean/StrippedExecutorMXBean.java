package org.apache.ignite.mxbean;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * MBean that provides access to information about stripped executor service.
 */
@MXBeanDescription("MBean that provides access to information about stripped executor service.")
public interface StrippedExecutorMXBean {
    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     */
    @MXBeanDescription("Starvation in striped pool.")
    public void checkStarvation();

    /**
     * @return Stripes count.
     */
    @MXBeanDescription("Stripes count.")
    public int stripes();

    /**
     * {@inheritDoc}
     */
    @MXBeanDescription("Stripes count.")
    public boolean isShutdown();

    /**
     * {@inheritDoc}
     */
    public boolean isTerminated();

    /**
     * @return Return total queue size of all stripes.
     */
    @MXBeanDescription("Total queue size of all stripes.")
    public int queueSize();

    /**
     * @return Completed tasks count.
     */
    @MXBeanDescription("Completed tasks count.")
    public long completedTasks();

    /**
     * {@inheritDoc}
     */
    public String toString();
}
