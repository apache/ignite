/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi;

import org.apache.ignite.*;

import java.util.concurrent.*;

/**
 * This class provides implementation of {@link ThreadFactory}  factory
 * for creating grid SPI threads.
 */
public class IgniteSpiThreadFactory implements ThreadFactory {
    /** */
    private final IgniteLogger log;

    /** */
    private final String gridName;

    /** */
    private final String threadName;

    /**
     * @param gridName Grid name, possibly {@code null} for default grid.
     * @param threadName Name for threads created by this factory.
     * @param log Grid logger.
     */
    public IgniteSpiThreadFactory(String gridName, String threadName, IgniteLogger log) {
        assert log != null;
        assert threadName != null;

        this.gridName = gridName;
        this.threadName = threadName;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(final Runnable r) {
        return new IgniteSpiThread(gridName, threadName, log) {
            /** {@inheritDoc} */
            @Override protected void body() throws InterruptedException {
                r.run();
            }
        };
    }
}
