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

package org.apache.ignite.internal.cache.query.index;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract class for all Index implementations.
 */
public abstract class AbstractIndex implements Index {
    /** Whether index is rebuilding now. */
    private final AtomicBoolean rebuildInProgress = new AtomicBoolean(false);

    /**
     * @param val Mark or unmark index to rebuild.
     */
    public void markIndexRebuild(boolean val, boolean disableWalForIdx) {
        if (rebuildInProgress.compareAndSet(!val, val)) {
            if (val) {
                if (disableWalForIdx)
                    disableWal();
            }
            else
                enableWal();
        }
    }

    /** */
    public abstract void disableWal();

    /** */
    public abstract void enableWal();

    /**
     * @return Whether index is rebuilding now.
     */
    public boolean rebuildInProgress() {
        return rebuildInProgress.get();
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        if (clazz == null)
            return null;

        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException(
            String.format("Cannot unwrap [%s] to [%s]", getClass().getName(), clazz.getName())
        );
    }
}
