/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class SnapshotMetadataVerificationTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Full snapshot metadata. */
    private Map<ClusterNode, List<SnapshotMetadata>> meta;

    /** Errors happened during snapshot metadata verification. */
    private Map<ClusterNode, Exception> exceptions;

    /** */
    public SnapshotMetadataVerificationTaskResult(
        Map<ClusterNode, List<SnapshotMetadata>> meta,
        Map<ClusterNode, Exception> exceptions
    ) {
        this.meta = Collections.unmodifiableMap(meta);
        this.exceptions = Collections.unmodifiableMap(exceptions);
    }

    /** */
    public SnapshotMetadataVerificationTaskResult() {
    }

    /** @return Errors happened during snapshot metadata verification. */
    public Map<ClusterNode, Exception> exceptions() {
        return Collections.unmodifiableMap(exceptions);
    }

    /** @return Full snapshot metadata. */
    public Map<ClusterNode, List<SnapshotMetadata>> meta() {
        return Collections.unmodifiableMap(meta);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, meta);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        meta = U.readMap(in);
        exceptions = U.readMap(in);
    }
}
