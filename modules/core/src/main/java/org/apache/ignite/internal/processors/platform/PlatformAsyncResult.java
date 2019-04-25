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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Represents asynchronous operation result.
 */
public interface PlatformAsyncResult {
    /**
     * Async operation future.
     *
     * @return Future.
     */
    IgniteFuture future();

    /**
     * Async operation result writer method.
     *
     * @param writer Writer.
     * @param result Async operation result.
     */
    void write(BinaryRawWriterEx writer, Object result);
}
