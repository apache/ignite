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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;

/**
 * Snapshot restore operation handling task.
 */
public class SnapshotHandlerRestoreTask implements Supplier<Map<String, SnapshotHandlerResult<Object>>> {
    /** */
    private final IgniteEx ignite;

    /** */
    private final SnapshotFileTree sft;

    /** */
    private final Collection<String> rqGrps;

    /** */
    private final boolean check;

    /** */
    SnapshotHandlerRestoreTask(
        IgniteEx ignite,
        IgniteLogger log,
        SnapshotFileTree sft,
        Collection<String> grps,
        boolean check
    ) {
        this.ignite = ignite;
        this.sft = sft;
        this.rqGrps = grps;
        this.check = check;
    }

    /** */
    @Override public Map<String, SnapshotHandlerResult<Object>> get() {
        try {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
            SnapshotMetadata meta = snpMgr.readSnapshotMetadata(sft.meta());

            return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                new SnapshotHandlerContext(meta, rqGrps, ignite.localNode(), sft, false, check));
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }
    }
}
