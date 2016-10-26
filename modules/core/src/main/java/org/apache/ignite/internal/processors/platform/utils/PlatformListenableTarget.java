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

package org.apache.ignite.internal.processors.platform.utils;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Wraps listenable in a platform target.
 */
public class PlatformListenableTarget extends PlatformAbstractTarget {
    /** */
    private static final int OP_CANCEL = 1;

    /** */
    private static final int OP_IS_CANCELLED = 2;

    /** Wrapped listenable */
    private final PlatformListenable listenable;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    public PlatformListenableTarget(PlatformListenable listenable, PlatformContext platformCtx) {
        super(platformCtx);

        assert listenable != null;

        this.listenable = listenable;
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_CANCEL:
                return listenable.cancel() ? TRUE : FALSE;

            case OP_IS_CANCELLED:
                return listenable.isCancelled() ? TRUE : FALSE;
        }

        return super.processInLongOutLong(type, val);
    }
}
