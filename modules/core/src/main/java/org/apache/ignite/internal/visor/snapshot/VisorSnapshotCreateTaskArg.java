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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Argument for the task to create snapshot.
 */
public class VisorSnapshotCreateTaskArg extends VisorSnapshotCheckTaskArg {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Synchronous execution flag. */
    private boolean sync;

    /** Incremental snapshot flag. */
    private boolean incremental;

    /** Default constructor. */
    public VisorSnapshotCreateTaskArg() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param sync Synchronous execution flag.
     * @param incremental Incremental snapshot flag.
     */
    public VisorSnapshotCreateTaskArg(String snpName, String snpPath, boolean sync, boolean incremental) {
        super(snpName, snpPath);

        this.sync = sync;
        this.incremental = incremental;
    }

    /** @return Synchronous execution flag. */
    public boolean sync() {
        return sync;
    }

    /** @return Incremental snapshot flag. */
    public boolean incremental() {
        return incremental;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        out.writeBoolean(sync);
        out.writeBoolean(incremental);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(ver, in);

        sync = in.readBoolean();
        incremental = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSnapshotCreateTaskArg.class, this);
    }
}
