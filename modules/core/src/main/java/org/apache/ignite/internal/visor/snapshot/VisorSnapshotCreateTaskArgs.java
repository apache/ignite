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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Argument for the task to manage snapshot restore operation.
 */
public class VisorSnapshotCreateTaskArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot name. */
    private String snpName;

    /** Snapshot operation management action. */
    private VisorSnapshotTaskAction action;

    /** Default constructor. */
    public VisorSnapshotCreateTaskArgs() {
        // No-op.
    }

    /**
     * @param action Snapshot restore operation management action.
     * @param snpName Snapshot name.
     */
    public VisorSnapshotCreateTaskArgs(
        VisorSnapshotTaskAction action,
        String snpName
    ) {
        this.snpName = snpName;
        this.action = action;
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** @return Snapshot restore operation management action. */
    public VisorSnapshotTaskAction jobAction() {
        return action;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, action);
        U.writeString(out, snpName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        action = U.readEnum(in, VisorSnapshotTaskAction.class);
        snpName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSnapshotCreateTaskArgs.class, this);
    }
}
