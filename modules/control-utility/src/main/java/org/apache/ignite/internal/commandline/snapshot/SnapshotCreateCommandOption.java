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

package org.apache.ignite.internal.commandline.snapshot;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot create command options.
 */
public enum SnapshotCreateCommandOption implements CommandArg {
    /** Synchronous execution flag. */
    SYNC("--sync", null, "Run the operation synchronously, the command will wait for the entire operation to complete. " +
        "Otherwise, it will be performed in the background, and the command will immediately return control."),

    /** Snapshot directory path. */
    DESTINATION("--dest", "path", "Path to the directory where the snapshot will be saved. If not specified, " +
        "the default configured snapshot directory will be used.");

    /** Name. */
    private final String name;

    /** Argument. */
    private final String arg;

    /** Description. */
    private final String desc;

    /**
     * @param name Name.
     * @param arg Argument.
     * @param desc Description.
     */
    SnapshotCreateCommandOption(String name, @Nullable String arg, String desc) {
        this.name = name;
        this.arg = arg;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** @return Argument. */
    public String arg() {
        return arg;
    }

    /** @return Description. */
    public String description() {
        return desc;
    }

    /** @return Argument name. */
    @Override public String toString() {
        return argName();
    }
}
