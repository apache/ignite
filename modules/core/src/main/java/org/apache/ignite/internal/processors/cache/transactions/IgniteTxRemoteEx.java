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

import java.util.Collection;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Local transaction API.
 */
public interface IgniteTxRemoteEx extends IgniteInternalTx {
    /**
     * @return Remote thread ID.
     */
    public long remoteThreadId();

    /**
     * @param baseVer Base version.
     * @param committedVers Committed version.
     * @param rolledbackVers Rolled back version.
     * @param pendingVers Pending versions.
     */
    public void doneRemote(GridCacheVersion baseVer, Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers, Collection<GridCacheVersion> pendingVers);

    /**
     * @param e Sets write value for pessimistic transactions.
     * @return {@code True} if entry was found.
     */
    public boolean setWriteValue(IgniteTxEntry e);
}