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

package org.apache.ignite.ml.dlearn;

/**
 * D-learn partition storage is a common interface for partition storages both local and distributed. Partition storage
 * allows to save, retrieve and remove objects identified by keys from the d-learn partition.
 */
public interface DLearnPartitionStorage {
    /**
     * Saves the given value in the d-learn partition with the given key.
     *
     * @param key key
     * @param val value
     * @param <T> type of value
     */
    public <T> void put(String key, T val);

    /**
     * Retrieves value from the d-learn partition by the given key.
     *
     * @param key key
     * @param <T> type of value
     * @return value
     */
    public <T> T get(String key);

    /**
     * Removes value from the d-learn partition by the given key.
     *
     * @param key key
     */
    public void remove(String key);
}
