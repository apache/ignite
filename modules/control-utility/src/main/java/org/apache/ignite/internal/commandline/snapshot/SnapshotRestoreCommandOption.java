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

/**
 * Snapshot restore command options.
 */
public enum SnapshotRestoreCommandOption implements CommandArg {
    /** Cache group names. */
    GROUPS("--groups", "group1,...groupN", "Cache group names."),

    /** Synchronous execution flag. */
    SYNC(SnapshotCreateCommandOption.SYNC.argName(), SnapshotCreateCommandOption.SYNC.optionName(),
        SnapshotCreateCommandOption.SYNC.description());

    /** Argument name. */
    private final String argName;

    /** Option name. */
    private final String optionName;

    /** Option description. */
    private final String desc;

    /**
     * @param argName Argument name.
     * @param optionName Option name.
     * @param desc Option description.
     */
    SnapshotRestoreCommandOption(String argName, String optionName, String desc) {
        this.argName = argName;
        this.optionName = optionName;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return argName;
    }

    /** @return Option name. */
    public String optionName() {
        return optionName;
    }

    /** @return Option description. */
    public String description() {
        return desc;
    }

    /** @return Argument name. */
    @Override public String toString() {
        return argName();
    }
}
