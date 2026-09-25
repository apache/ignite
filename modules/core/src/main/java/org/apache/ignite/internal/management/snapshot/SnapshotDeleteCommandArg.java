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
import org.jetbrains.annotations.Nullable;

/** */
public class SnapshotDeleteCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Positional
    @Argument(description = "Snapshot name")
    @Nullable String snapshotName;

    /** */
    @Order(1)
    @Argument(example = "path/to/snapshots", optional = true, description = "Path to snapshot location directory. If not specified " +
        "or specified a relative path, the default snapshot configuration directory will be used")
    @Nullable String src;

    /** */
    public @Nullable String snapshotName() {
        return snapshotName;
    }

    /** */
    public void snapshotName(@Nullable String snapshotName) {
        this.snapshotName = snapshotName;
    }

    /** */
    public @Nullable String src() {
        return src;
    }

    /** */
    public void src(@Nullable String src) {
        this.src = src;
    }
}
