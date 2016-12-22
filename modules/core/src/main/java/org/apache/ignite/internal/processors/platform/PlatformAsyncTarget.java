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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Async target.
 */
public interface PlatformAsyncTarget {
    /**
     * Gets future for the current operation.
     *
     * @return current future.
     * @throws IgniteCheckedException If failed.
     */
    IgniteInternalFuture currentFuture() throws IgniteCheckedException;

    /**
     * Gets a custom future writer.
     *
     * @param opId Operation id.
     * @return A custom writer for given op id.
     */
    @Nullable PlatformFutureUtils.Writer futureWriter(int opId);
}
