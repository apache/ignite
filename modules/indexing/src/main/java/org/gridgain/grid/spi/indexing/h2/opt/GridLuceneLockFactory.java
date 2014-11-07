/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.apache.lucene.store.*;
import org.gridgain.grid.util.*;

import java.io.*;

/**
 * Lucene {@link LockFactory} implementation.
 */
public class GridLuceneLockFactory extends LockFactory {
    /** */
    @SuppressWarnings("TypeMayBeWeakened")
    private final GridConcurrentHashSet<String> locks = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override public Lock makeLock(String lockName) {
        return new LockImpl(lockName);
    }

    /** {@inheritDoc} */
    @Override public void clearLock(String lockName) throws IOException {
        locks.remove(lockName);
    }

    /**
     * {@link Lock} Implementation.
     */
    private class LockImpl extends Lock {
        /** */
        private final String lockName;

        /**
         * @param lockName Lock name.
         */
        private LockImpl(String lockName) {
            this.lockName = lockName;
        }

        /** {@inheritDoc} */
        @Override public boolean obtain() throws IOException {
            return locks.add(lockName);
        }

        /** {@inheritDoc} */
        @Override public void release() throws IOException {
            locks.remove(lockName);
        }

        /** {@inheritDoc} */
        @Override public boolean isLocked() throws IOException {
            return locks.contains(lockName);
        }
    }
}
