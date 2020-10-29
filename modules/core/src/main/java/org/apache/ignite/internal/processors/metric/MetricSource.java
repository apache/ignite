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

package org.apache.ignite.internal.processors.metric;

//TODO: javadoc
/**
 * Interface for all metric source.
 */
public interface MetricSource {
    /**
     * Returns metric source name.
     *
     * @return Metric source name.
     */
    public String name();

    /**
     * Enables metrics for metric source. Creates and returns {@link MetricRegistry} built during enabling.
     *
     * @return Newly created {@link MetricRegistry} instance or {@code null} if metrics are already enabled.
     */
    public MetricRegistry enable();

    /**
     * Disables metrics for metric source.
     */
    public void disable();

    /**
     * Checks whether metrics is enabled (switched on) or not (switched off) for metric source.
     *
     * @return {@code True} if metrics are enabled, otherwise - {@code false}.
     */
    public boolean enabled();
}
