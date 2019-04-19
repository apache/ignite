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

package org.apache.ignite.internal.processors.hadoop.counter;

/**
 * Hadoop counter.
 */
public interface HadoopCounter {
    /**
     * Gets name.
     *
     * @return Name of the counter.
     */
    public String name();

    /**
     * Gets counter group.
     *
     * @return Counter group's name.
     */
    public String group();

    /**
     * Merge the given counter to this counter.
     *
     * @param cntr Counter to merge into this counter.
     */
    public void merge(HadoopCounter cntr);
}