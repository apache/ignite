/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs.mapreduce;

/**
 * Adapter for {@link IgniteFsJob} with no-op implementation of {@link #cancel()} method.
 */
public abstract class IgniteFsJobAdapter implements IgniteFsJob {
    /** {@inheritDoc} */
    @Override public void cancel() {
        // No-op.
    }
}
