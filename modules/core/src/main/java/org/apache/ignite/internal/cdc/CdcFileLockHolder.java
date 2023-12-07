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

package org.apache.ignite.internal.cdc;

import java.lang.management.ManagementFactory;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.FileLockHolder;

/**
 * Lock file holder for Change Data Capture application.
 *
 * @see CdcMain
 * @see CdcConsumerState
 */
public class CdcFileLockHolder extends FileLockHolder {
    /** Consumer ID. */
    private final String consumerId;

    /**
     * @param rootDir Root directory for lock file.
     * @param log Log.
     */
    public CdcFileLockHolder(String rootDir, String consumerId, IgniteLogger log) {
        super(rootDir, log);

        this.consumerId = consumerId;
    }

    /** {@inheritDoc} */
    @Override public String lockInfo() {
        return "[consumerId=" + consumerId + ", proc=" + ManagementFactory.getRuntimeMXBean().getName() + ']';
    }

    /** {@inheritDoc} */
    @Override protected String warningMessage(String lockInfo) {
        return "Failed to acquire file lock. Will try again in 1s " +
            "[proc=" + ManagementFactory.getRuntimeMXBean().getName() + ", holder=" + lockInfo +
            ", path=" + lockPath() + ']';
    }
}
