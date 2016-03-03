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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import java.io.OutputStream;

/**
 *
 */
public interface WriteAheadLog {
    /**
     * Starts transaction tracking.
     *
     * @param xidVer Transaction version.
     */
    public void txBegin(GridCacheVersion xidVer);

    /**
     * Ends transaction tracking. After this method returns, all tracked changes are guaranteed to
     * be flushed to the disk.
     *
     * @param xidVer Transaction version.
     * @throws IllegalArgumentException If transaction with such ID has not been started.
     */
    public void txEnd(GridCacheVersion xidVer);

    /**
     * Adds logical transaction change information.
     *
     * @param entries Transaction entries being committed.
     * @param xidVer Transaction version.
     * @throws IllegalArgumentException If transaction with such ID has not been started.
     */
    public void addTransactionEntries(Iterable<IgniteTxEntry> entries, GridCacheVersion xidVer);

    /**
     * Adds binary transaction change information. Change is considered to be completed when the
     * output stream returned from this method is closed.
     *
     * @param xidVer Transaction version.
     * @return Output stream to write changes to.
     */
    public OutputStream addBinaryEntry(GridCacheVersion xidVer);

    // TODO design iterator for reading written entries.
}
