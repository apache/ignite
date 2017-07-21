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

import org.jetbrains.annotations.Nullable;

/**
 * This exception defines illegal call on empty projection. Thrown by projection when operation
 * that requires at least one node is called on empty projection.
 */
public class ClusterGroupEmptyCheckedException extends ClusterTopologyCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with default error message.
     */
    public ClusterGroupEmptyCheckedException() {
        super("Cluster group is empty.");
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public ClusterGroupEmptyCheckedException(String msg) {
        super(msg);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ClusterGroupEmptyCheckedException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}