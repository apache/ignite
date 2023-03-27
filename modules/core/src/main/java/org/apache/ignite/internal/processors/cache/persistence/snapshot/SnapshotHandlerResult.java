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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Result of local processing on the node. In addition to the result received from the handler, it also includes
 * information about the error (if any) and the node on which this result was received.
 *
 * @param <T> Type of the local processing result.
 */
public class SnapshotHandlerResult<T> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Result of local processing. */
    private final T data;

    /** Processing error. */
    private final Exception err;

    /** Processing node. */
    private final ClusterNode node;

    /**
     * @param data Result of local processing.
     * @param err Processing error.
     * @param node Processing node.
     */
    public SnapshotHandlerResult(@Nullable T data, @Nullable Exception err, ClusterNode node) {
        this.data = data;
        this.err = err;
        this.node = node;
    }

    /** @return Result of local processing. */
    public @Nullable T data() {
        return data;
    }

    /** @return Processing error. */
    public @Nullable Exception error() {
        return err;
    }

    /** @return Processing node. */
    public ClusterNode node() {
        return node;
    }
}
