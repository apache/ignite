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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Input parameters for checking snapshot metadata.
 */
public class SnapshotMetadataVerificationTaskArg extends VisorDataTransferObject {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Snapshot name. */
    private String snpName;

    /** Snapshot directory path. */
    private String snpPath;

    /** Incremental snapshot index. */
    private int incIdx;

    /** Default constructor. */
    public SnapshotMetadataVerificationTaskArg() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     */
    public SnapshotMetadataVerificationTaskArg(String snpName, @Nullable String snpPath, int incIdx) {
        this.snpName = snpName;
        this.snpPath = snpPath;
        this.incIdx = incIdx;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Snapshot directory path.
     */
    public String snapshotPath() {
        return snpPath;
    }

    /**
     * @return Incremental snapshot index.
     */
    public int incrementIndex() {
        return incIdx;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, snpName);
        U.writeString(out, snpPath);
        out.writeInt(incIdx);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        snpName = U.readString(in);
        snpPath = U.readString(in);
        incIdx = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotMetadataVerificationTaskArg.class, this);
    }
}
