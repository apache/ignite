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

package org.apache.ignite.internal.pagemem.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 *
 */
public interface IgnitePartitionCatchUpLog {
    /**
     * @param entry An entry to log.
     * @return The logged entry pointer.
     * @throws IgniteCheckedException If failed to construct the entry.
     */
    public WALPointer log(WALRecord entry) throws IgniteCheckedException;

    /**
     * @return The records iterator.
     * @throws IgniteCheckedException If failed to start iteration.
     */
    public WALIterator replay() throws IgniteCheckedException;

    /**
     * @return <tt>true</tt> if the head of the temporary WAL has beed catched up.
     */
    public boolean catched();

    /**
     * @throws IgniteCheckedException If fails.
     */
    public void clear() throws IgniteCheckedException;
}
