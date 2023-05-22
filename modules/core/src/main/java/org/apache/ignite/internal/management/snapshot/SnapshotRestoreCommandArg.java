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

package org.apache.ignite.internal.management.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 */
@ArgumentGroup(value = {"start", "status", "cancel"}, onlyOneOf = true, optional = true)
public class SnapshotRestoreCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(description = "Snapshot name. " +
        "In case incremental snapshot (--incremental) full snapshot name must be provided")
    private String snapshotName;

    /** */
    @Argument(optional = true, example = "incrementIndex", description = "Incremental snapshot index. " +
        "The command will restore snapshot and after that all its increments sequentially from 1 to the specified index")
    private int increment;

    /** */
    @Argument(optional = true, description = "Cache group names", example = "group1,...groupN")
    private String[] groups;

    /** */
    @Argument(example = "path", optional = true,
        description = "Path to the directory where the snapshot files are located. " +
            "If not specified, the default configured snapshot directory will be used")
    private String src;

    /** */
    @Argument(optional = true, description = "Run the operation synchronously, " +
        "the command will wait for the entire operation to complete. " +
        "Otherwise, it will be performed in the background, and the command will immediately return control")
    private boolean sync;

    /** */
    @Argument(optional = true,
        description = "Check snapshot data integrity before restore (slow!). Similar to the \"check\" command")
    private boolean check;

    /** */
    @Argument(optional = true,
        description = "Snapshot restore operation status (Command deprecated. Use '--snapshot status' instead)")
    private boolean status;

    /** */
    @Argument(optional = true,
        description = "Cancel snapshot restore operation (Command deprecated. Use '--snapshot cancel' instead)")
    private boolean cancel;

    /** */
    @Argument(optional = true, description = "Start snapshot restore operation (Default action)")
    private boolean start;

    /** */
    public void ensureOptions() {
        if (!sync)
            return;

        if (cancel)
            throw new IllegalArgumentException("--sync and --cancel can't be used together");

        if (status)
            throw new IllegalArgumentException("--sync and --status can't be used together");
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, snapshotName);
        out.writeInt(increment);
        U.writeArray(out, groups);
        U.writeString(out, src);
        out.writeBoolean(sync);
        out.writeBoolean(check);
        out.writeBoolean(status);
        out.writeBoolean(cancel);
        out.writeBoolean(start);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        snapshotName = U.readString(in);
        increment = in.readInt();
        groups = U.readArray(in, String.class);
        src = U.readString(in);
        sync = in.readBoolean();
        check = in.readBoolean();
        status = in.readBoolean();
        cancel = in.readBoolean();
        start = in.readBoolean();
    }

    /** */
    public boolean start() {
        return start;
    }

    /** */
    public void start(boolean start) {
        this.start = start;
        ensureOptions();
    }

    /** */
    public boolean status() {
        return status;
    }

    /** */
    public void status(boolean status) {
        this.status = status;
        ensureOptions();
    }

    /** */
    public boolean cancel() {
        return cancel;
    }

    /** */
    public void cancel(boolean cancel) {
        this.cancel = cancel;
        ensureOptions();
    }

    /** */
    public String[] groups() {
        return groups;
    }

    /** */
    public void groups(String[] groups) {
        this.groups = groups;
    }

    /** */
    public String snapshotName() {
        return snapshotName;
    }

    /** */
    public void snapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    /** */
    public int increment() {
        return increment;
    }

    /** */
    public void increment(int increment) {
        this.increment = increment;
    }

    /** */
    public String src() {
        return src;
    }

    /** */
    public void src(String src) {
        this.src = src;
    }

    /** */
    public boolean sync() {
        return sync;
    }

    /** */
    public void sync(boolean sync) {
        this.sync = sync;
        ensureOptions();
    }

    /** */
    public boolean check() {
        return check;
    }

    /** */
    public void check(boolean check) {
        this.check = check;
    }
}
