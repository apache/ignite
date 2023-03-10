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

package org.apache.ignite.internal.visor.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Argument for the task to manage snapshot restore operation.
 */
public class VisorSnapshotRestoreTaskArg extends VisorSnapshotCreateTaskArg {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cache group names. */
    private Collection<String> grpNames;

    /** Snapshot restore operation management action. */
    private VisorSnapshotRestoreTaskAction action;

    /** Incremental snapshot index. */
    private int incIdx;

    /** Default constructor. */
    public VisorSnapshotRestoreTaskArg() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name.
     * @param snpPath Snapshot path.
     * @param incIdx Incremental snapshot index, {@code null} if no increments should be restored.
     * @param sync Synchronous execution flag.
     * @param action Snapshot restore operation management action.
     * @param grpNames Cache group names.
     */
    public VisorSnapshotRestoreTaskArg(
        String snpName,
        String snpPath,
        @Nullable Integer incIdx,
        boolean sync,
        VisorSnapshotRestoreTaskAction action,
        @Nullable Collection<String> grpNames
    ) {
        super(snpName, snpPath, sync, false);

        this.action = action;
        this.grpNames = grpNames;
        this.incIdx = incIdx == null ? 0 : incIdx;
    }

    /** @return Cache group names. */
    public Collection<String> groupNames() {
        return grpNames;
    }

    /** @return Snapshot restore operation management action. */
    public VisorSnapshotRestoreTaskAction jobAction() {
        return action;
    }

    /** @return Incremental snapshot index. */
    public int incrementIndex() {
        return incIdx;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);
        U.writeEnum(out, action);
        U.writeCollection(out, grpNames);
        out.writeInt(incIdx);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(ver, in);
        action = U.readEnum(in, VisorSnapshotRestoreTaskAction.class);
        grpNames = U.readCollection(in);
        incIdx = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSnapshotRestoreTaskArg.class, this);
    }
}
