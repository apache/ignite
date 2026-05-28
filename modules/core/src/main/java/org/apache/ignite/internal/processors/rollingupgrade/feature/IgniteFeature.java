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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

/**
 * Represents a change in Ignite node behavior that is not compatible with Ignite nodes from previous versions.
 * {@link IgniteFeature} is one of the core mechanisms on which the Ignite Rolling Upgrade implementation depends.
 * It serves two main purposes:
 * <ol>
 *     <li>determining Rolling Upgrade availability between different Ignite versions</li>
 *     <li>providing the ability to deactivate Ignite functionality introduced in newer versions for the duration of a Rolling Upgrade</li>
 * </ol>
 *
 * @see IgniteFeatureSet
 */
public interface IgniteFeature {
    /**
     * <p>{@link IgniteFeature} is identified by a unique integer ID. IDs of {@link IgniteFeature} instances must:</p>
     * <ul>
     *     <li>remain unchanged between Ignite versions</li>
     *     <li>start at {@code 0} and increase monotonically</li>
     * </ul>
     *
     * @return The unique identifier of this feature.
     */
    int id();
}
