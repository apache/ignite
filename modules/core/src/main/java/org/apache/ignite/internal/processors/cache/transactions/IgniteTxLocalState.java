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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface IgniteTxLocalState extends IgniteTxState {
    /**
     * @param entry Entry.
     */
    public void addEntry(IgniteTxEntry entry);

    /**
     * @param txSize Transaction size.
     * @return {@code True} if transaction was successfully  started.
     */
    public boolean init(int txSize);

    /**
     * @return {@code True} if init method was called.
     */
    public boolean initialized();

    /**
     *
     */
    public void seal();

    /**
     * @return Cache partitions touched by current tx.
     */
    public Map<Integer, Set<Integer>> touchedPartitions();

    /**
     * Remembers that particular cache partition was touched by current tx.
     *
     * @param cacheId Cache id.
     * @param partId Partition id.
     */
    public void touchPartition(int cacheId, int partId);

    /**
     * @return Recovery mode flag.
     */
    public boolean recovery();
}
