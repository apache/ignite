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

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.IgniteCheckedException;

/**
 * Partition resolver interface. Takes argument, data type and cache name, returns partition.
 * The only purpose of this methods is to allow partition pruning classes to be located in core module.
 */
public interface PartitionResolver {
    /**
     * Resolve partition.
     *
     * @param arg Argument.
     * @param dataType Data type.
     * @param cacheName Cache name.
     * @return Partition.
     * @throws IgniteCheckedException If failed.
     */
    int partition(Object arg, int dataType, String cacheName) throws IgniteCheckedException;
}
