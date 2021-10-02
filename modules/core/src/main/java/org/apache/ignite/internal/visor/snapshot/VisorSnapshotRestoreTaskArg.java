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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Argument for the task to manage snapshot restore operation.
 */
public class VisorSnapshotRestoreTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot name. */
    private String snpName;

    /** Cache group names. */
    private Collection<String> grpNames;

    /** Snapshot restore operation management action. */
    private VisorSnapshotRestoreTaskAction action;

    /** Default constructor. */
    public VisorSnapshotRestoreTaskArg() {
        // No-op.
    }

    /**
     * @param action Snapshot restore operation management action.
     * @param snpName Snapshot name.
     * @param grpNames Cache group names.
     */
    public VisorSnapshotRestoreTaskArg(
        VisorSnapshotRestoreTaskAction action,
        String snpName,
        @Nullable Collection<String> grpNames
    ) {
        this.snpName = snpName;
        this.grpNames = grpNames;
        this.action = action;
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** @return Cache group names. */
    public Collection<String> groupNames() {
        return grpNames;
    }

    /** @return Snapshot restore operation management action. */
    public VisorSnapshotRestoreTaskAction jobAction() {
        return action;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, action);
        U.writeString(out, snpName);
        U.writeCollection(out, grpNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        action = U.readEnum(in, VisorSnapshotRestoreTaskAction.class);
        snpName = U.readString(in);
        grpNames = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSnapshotRestoreTaskArg.class, this);
    }
}
