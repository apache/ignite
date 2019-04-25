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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.failure.FailureProcessor;

/**
 * Provides facility to handle failures by custom user implementations,
 * which can be configured by {@link IgniteConfiguration#setFailureHandler(FailureHandler)} method.
 */
public interface FailureHandler {
    /**
     * Handles failure occurred on {@code ignite} instance.
     * Failure details is contained in {@code failureCtx}.
     * Returns {@code true} if kernal context must be invalidated by {@link FailureProcessor} after calling this method.
     *
     * @param ignite Ignite instance.
     * @param failureCtx Failure context.
     * @return Whether kernal context must be invalidated or not.
     */
    public boolean onFailure(Ignite ignite, FailureContext failureCtx);
}
