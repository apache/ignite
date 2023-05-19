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

/** */
public class SnapshotCreateCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(description = "Snapshot name. " +
        "In case incremental snapshot (--incremental) full snapshot name must be provided")
    private String snapshotName;

    /** */
    @Argument(example = "path", optional = true,
        description = "Path to the directory where the snapshot will be saved. " +
        "If not specified, the default configured snapshot directory will be used")
    private String dest;

    /** */
    @Argument(optional = true, description = "Run the operation synchronously, " +
        "the command will wait for the entire operation to complete. " +
        "Otherwise, it will be performed in the background, and the command will immediately return control")
    private boolean sync;

    /** */
    @Argument(optional = true, description = "Create an incremental snapshot for previously created full snapshot. " +
        "Full snapshot must be accessible via --dest and snapshot_name")
    private boolean incremental;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
    }
}
