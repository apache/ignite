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

package org.apache.ignite.internal.processors.cache.consistentcut;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * This interface marks messages that responsible for notifying nodes with Consistent Cut Version.
 */
public interface ConsistentCutVersionAware {
    /**
     * Version of the latest observable Consistent Cut on sender node.
     *
     * It is used to trigger Consistent Cut procedure on receiver.
     */
    public long latestCutVersion();

    /**
     * Version of the latest Consistent Cut that doesn't include specified committed transaction.
     *
     * It is used to notify a transaction in the check-list whether to include it to this Consistent Cut.
     */
    public default long txCutVersion() {
        return -1L;
    }

    /**
     * Transaction ID to notify with {@link #txCutVersion()}.
     *
     * @return Near transaction ID.
     */
    public default GridCacheVersion nearTxVersion() {
        return null;
    }

}
