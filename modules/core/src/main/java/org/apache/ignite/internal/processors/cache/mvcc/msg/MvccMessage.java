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

package org.apache.ignite.internal.processors.cache.mvcc.msg;

import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Common interface for all MVCC-related messages.
 */
public interface MvccMessage extends Message {
    /**
     * @return {@code True} if should wait for coordinator initialization.
     */
    public boolean waitForCoordinatorInit();

    /**
     * @return {@code True} if message should be processed from NIO thread.
     */
    public boolean processedFromNioThread();
}
