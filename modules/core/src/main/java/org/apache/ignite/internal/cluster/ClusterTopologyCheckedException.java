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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.jetbrains.annotations.*;

/**
 * This exception is used to indicate error with grid topology (e.g., crashed node, etc.).
 */
public class ClusterTopologyCheckedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Next topology version to wait. */
    private transient IgniteInternalFuture<?> readyFut;

    /**
     * Creates new topology exception with given error message.
     *
     * @param msg Error message.
     */
    public ClusterTopologyCheckedException(String msg) {
        super(msg);
    }

    /**
     * Creates new topology exception with given error message and optional
     * nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ClusterTopologyCheckedException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * @return Retry ready future.
     */
    public IgniteInternalFuture<?> retryReadyFuture() {
        return readyFut;
    }

    /**
     * @param readyFut Retry ready future.
     */
    public void retryReadyFuture(IgniteInternalFuture<?> readyFut) {
        this.readyFut = readyFut;
    }
}
