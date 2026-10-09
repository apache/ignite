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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;

/** */
public class SnapshotCreateCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Positional
    @Argument(description = "Snapshot name. " +
        "In the case of incremental snapshot (--incremental) full snapshot name must be provided")
    String snapshotName;

    /** */
    @Order(1)
    @Argument(example = "path", optional = true,
        description = "Path to the directory where the snapshot will be saved. " +
        "If not specified, the default configured snapshot directory will be used")
    String dest;

    /** */
    @Order(2)
    @Argument(optional = true, description = "Run the operation synchronously, " +
        "the command will wait for the entire operation to complete. " +
        "Otherwise, it will be performed in the background, and the command will immediately return control")
    boolean sync;

    /** */
    @Order(3)
    @Argument(optional = true, description = "Create an incremental snapshot for previously created full snapshot. " +
        "Full snapshot must be accessible via --dest and snapshot_name")
    boolean incremental;

    /** */
    public String snapshotName() {
        return snapshotName;
    }

    /** */
    public void snapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    /** */
    public String dest() {
        return dest;
    }

    /** */
    public void dest(String dest) {
        this.dest = dest;
    }

    /** */
    public boolean sync() {
        return sync;
    }

    /** */
    public void sync(boolean sync) {
        this.sync = sync;
    }

    /** */
    public boolean incremental() {
        return incremental;
    }

    /** */
    public void incremental(boolean incremental) {
        this.incremental = incremental;
    }
}
