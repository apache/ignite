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

/** Snapshot restore operation management action. */
public enum VisorSnapshotRestoreTaskAction {
    /** Start snapshot restore operation. */
    START,

    /** Cancel snapshot restore operation. */
    CANCEL,

    /** Status of the snapshot restore operation. */
    STATUS;

    /**
     * @param cmdArg Command line argument.
     * @return Snapshot restore operation management action.
     */
    public static VisorSnapshotRestoreTaskAction fromCmdArg(String cmdArg) {
        for (VisorSnapshotRestoreTaskAction val : values()) {
            if (cmdArg.equalsIgnoreCase(val.cmdName()))
                return val;
        }

        throw new IllegalArgumentException("Unexpected command line argument \"" + cmdArg + "\"");
    }

    /** @return Command line argument name. */
    public String cmdName() {
        return "--" + name().toLowerCase();
    }
}
