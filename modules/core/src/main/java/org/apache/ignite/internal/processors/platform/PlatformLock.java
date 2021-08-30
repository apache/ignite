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

package org.apache.ignite.internal.processors.platform;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLock;

/**
 * Platform wrapper for {@link IgniteLock}.
 */
class PlatformLock extends PlatformAbstractTarget {
    /** */
    private static final int OP_LOCK = 1;

    /** */
    private static final int OP_TRY_LOCK = 2;

    /** */
    private static final int OP_UNLOCK = 3;

    /** */
    private static final int OP_REMOVE = 4;

    /** */
    private static final int OP_IS_BROKEN = 5;

    /** */
    private static final int OP_IS_LOCKED = 6;

    /** */
    private static final int OP_IS_REMOVED = 7;

    /** Wrapped lock. */
    private final IgniteLock lock;

    /**
     * Constructor.
     * @param ctx Context.
     * @param lock Lock to wrap.
     */
    PlatformLock(PlatformContext ctx, IgniteLock lock) {
        super(ctx);

        this.lock = lock;
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_LOCK: {
                lock.lock();

                return TRUE;
            }

            case OP_TRY_LOCK: {
                boolean locked = val < 0
                        ? lock.tryLock()
                        : lock.tryLock(val, TimeUnit.MILLISECONDS);

                return locked ? TRUE : FALSE;
            }

            case OP_UNLOCK: {
                lock.unlock();

                return TRUE;
            }

            case OP_REMOVE: {
                lock.close();

                return TRUE;
            }

            case OP_IS_BROKEN: {
                return lock.isBroken() ? TRUE : FALSE;
            }

            case OP_IS_LOCKED: {
                return lock.isLocked() ? TRUE : FALSE;
            }

            case OP_IS_REMOVED: {
                return lock.removed() ? TRUE : FALSE;
            }
        }

        return super.processInLongOutLong(type, val);
    }
}
