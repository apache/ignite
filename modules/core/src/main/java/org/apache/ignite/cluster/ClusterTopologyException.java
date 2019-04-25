/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cluster;

import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * This exception is used to indicate error with the cluster topology (e.g., crashed node, etc.).
 */
public class ClusterTopologyException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Retry ready future. */
    private transient IgniteFuture<?> readyFut;

    /**
     * Creates new topology exception with given error message.
     *
     * @param msg Error message.
     */
    public ClusterTopologyException(String msg) {
        super(msg);
    }

    /**
     * Creates new topology exception with given error message and optional
     * nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ClusterTopologyException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * @return Retry ready future.
     */
    public IgniteFuture<?> retryReadyFuture() {
        return readyFut;
    }

    /**
     * @param readyFut Retry ready future.
     */
    public void retryReadyFuture(IgniteFuture<?> readyFut) {
        this.readyFut = readyFut;
    }
}