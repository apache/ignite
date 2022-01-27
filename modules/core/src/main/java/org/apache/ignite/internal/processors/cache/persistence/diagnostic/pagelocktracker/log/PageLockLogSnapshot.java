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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;

/**
 * Page lock log snapshot.
 */
public class PageLockLogSnapshot extends PageLockDump {
    /** List of log entries. */
    public final List<LogEntry> locklog;

    /**
     *
     */
    public PageLockLogSnapshot(
        String name,
        long time,
        int headIdx,
        List<LogEntry> locklog,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        super(name, time, headIdx, nextOp, nextOpStructureId, nextOpPageId);
        this.locklog = locklog;
    }
}
