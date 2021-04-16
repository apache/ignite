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

package org.apache.ignite.internal.visor.cache.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of {@link IndexForceRebuildTask}.
 */
public class IndexForceRebuildTaskRes extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches for which indexes rebuild was triggered. */
    private Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild;

    /** Caches with indexes rebuild in progress. */
    private Set<IndexRebuildStatusInfoContainer> cachesWithRebuildInProgress;

    /** Names of caches that were not found. */
    private Set<String> notFoundCacheNames;

    /**
     * Empty constructor required for Serializable.
     */
    public IndexForceRebuildTaskRes() {
        // No-op.
    }

    /** */
    public IndexForceRebuildTaskRes(
        Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild,
        Set<IndexRebuildStatusInfoContainer> cachesWithRebuildInProgress,
        Set<String> notFoundCacheNames
    ) {
        this.cachesWithStartedRebuild = cachesWithStartedRebuild;
        this.cachesWithRebuildInProgress = cachesWithRebuildInProgress;
        this.notFoundCacheNames = notFoundCacheNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, cachesWithStartedRebuild);
        U.writeCollection(out, cachesWithRebuildInProgress);
        U.writeCollection(out, notFoundCacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cachesWithStartedRebuild = U.readSet(in);
        cachesWithRebuildInProgress = U.readSet(in);
        notFoundCacheNames = U.readSet(in);
    }

    /** */
    public Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild() {
        return cachesWithStartedRebuild;
    }

    /** */
    public Set<IndexRebuildStatusInfoContainer> cachesWithRebuildInProgress() {
        return cachesWithRebuildInProgress;
    }

    /** */
    public Set<String> notFoundCacheNames() {
        return notFoundCacheNames;
    }
}
