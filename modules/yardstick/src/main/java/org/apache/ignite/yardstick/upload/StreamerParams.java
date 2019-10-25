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

package org.apache.ignite.yardstick.upload;

import org.jetbrains.annotations.Nullable;

/**
 * Holds parameters to tweak streamer in upload benchmarks.
 * If any method returns <code>null</code>, no parameter passed to streamer.
 */
public interface StreamerParams {
    /**
     * @return Per-node buffer size.
     */
    @Nullable public Integer streamerPerNodeBufferSize();

    /**
     * @return Per-node parallel operations.
     */
    @Nullable public Integer streamerPerNodeParallelOperations();

    /**
     * @return Local batch size.
     */
    @Nullable public Integer streamerLocalBatchSize();

    /**
     * @return Allow overwrite flag.
     */
    @Nullable public Boolean streamerAllowOverwrite();

    /**
     * @return Ordered flag.
     */
    public boolean streamerOrdered();
}
