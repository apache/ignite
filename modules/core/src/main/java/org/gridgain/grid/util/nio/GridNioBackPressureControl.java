/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.apache.ignite.thread.*;

/**
 * Utility class that allows to ignore back-pressure control for threads that are processing messages.
 */
public class GridNioBackPressureControl {
    /** Thread local flag indicating that thread is processing message. */
    private static ThreadLocal<Boolean> threadProcMsg = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    /**
     * @return Flag indicating whether current thread is processing message.
     */
    public static boolean threadProcessingMessage() {
        Thread th = Thread.currentThread();

        if (th instanceof IgniteThread)
            return ((IgniteThread)th).processingMessage();

        return threadProcMsg.get();
    }

    /**
     * @param processing Flag indicating whether current thread is processing message.
     */
    public static void threadProcessingMessage(boolean processing) {
        Thread th = Thread.currentThread();

        if (th instanceof IgniteThread)
            ((IgniteThread)th).processingMessage(processing);
        else
            threadProcMsg.set(processing);
    }
}
