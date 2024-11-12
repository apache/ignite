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
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class SnapshotCheckCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(description = "Snapshot name. " +
        "In case incremental snapshot (--incremental) full snapshot name must be provided")
    private String snapshotName;

    /** */
    @Argument(example = "path", optional = true,
        description = "Path to the directory where the snapshot files are located. " +
            "If not specified, the default configured snapshot directory will be used")
    private String src;

    /** */
    @Argument(example = "incrementIndex", optional = true,
        description = "Incremental snapshot index. " +
            "The command will check incremental snapshots sequentially from 1 to the specified index")
    private int increment;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, snapshotName);
        U.writeString(out, src);
        out.writeInt(increment);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        snapshotName = U.readString(in);
        src = U.readString(in);
        increment = in.readInt();
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
    public String src() {
        return src;
    }

    /** */
    public void src(String src) {
        this.src = src;
    }

    /** */
    public int increment() {
        return increment;
    }

    /** */
    public void increment(int increment) {
        this.increment = increment;
    }
}
