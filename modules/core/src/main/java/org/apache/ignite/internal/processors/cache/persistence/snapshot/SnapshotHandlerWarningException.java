/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation warning. Warnings do not interrupt snapshot process but raise exception at the end to make the
 * operation status 'not OK' if no other error occured.
 */
public class SnapshotHandlerWarningException extends IgniteCheckedException {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Warning exclusion type. */
    private final Class<? extends SnapshotHandler<?>> exclusionHndType;

    /** Warning producer type. */
    private final Class<? extends SnapshotHandler<?>> hndType;

    /**
     * @param wrnMsg Warning message.
     * @param hndType Warning handler type.
     * @param exclHndType Exclusion handler type. Tells to exclude this warning if another warning of snapshot
     *                    handler type @{code exclHndType} is registered.
     */
    public SnapshotHandlerWarningException(String wrnMsg, Class<? extends SnapshotHandler<?>> hndType,
        @Nullable Class<? extends SnapshotHandler<?>> exclHndType) {
        super(wrnMsg);

        this.hndType = hndType;
        this.exclusionHndType = exclHndType;
    }

    /** */
    public Class<? extends SnapshotHandler<?>> exclusionHandlerType() {
        return exclusionHndType;
    }

    /** */
    public Class<? extends SnapshotHandler<?>> handlerType() {
        return hndType;
    }
}
