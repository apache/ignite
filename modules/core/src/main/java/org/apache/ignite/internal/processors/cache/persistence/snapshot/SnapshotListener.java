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

import java.io.File;
import java.util.UUID;

/**
 *
 */
public interface SnapshotListener {
    /**
     * @param rmtNodeId Remote node id received partition from.
     * @param part Partition file.
     * @param grpId Cache group id.
     * @param partId Partition id.
     */
    public void onPartition(UUID rmtNodeId, File part, int grpId, int partId);

    /**
     * @param rmtNodeId Remote node id ends processing request.
     */
    public void onEnd(UUID rmtNodeId);

    /**
     * @param rmtNodeId Remote node id.
     * @param t Exception which has been occurred.
     */
    public void onException(UUID rmtNodeId, Throwable t);
}
