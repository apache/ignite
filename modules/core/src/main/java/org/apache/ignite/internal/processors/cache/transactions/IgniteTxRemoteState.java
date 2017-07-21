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

/**
 *
 */
public interface IgniteTxRemoteState extends IgniteTxState {
    /**
     * @param key Key.
     * @param e Entry.
     */
    public void addWriteEntry(IgniteTxKey key, IgniteTxEntry e);

    /**
     * @param key Entry key.
     */
    public void clearEntry(IgniteTxKey key);

    /**
     * @param part Partition number.
     */
    public void invalidPartition(int part);
}
