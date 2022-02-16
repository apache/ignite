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
 * Snapshot create command options.
 */
public enum SnapshotCreateCommandOption implements CommandArg {
    /** Synchronous execution flag. */
    SYNC("sync", "Run the operation synchronously, the command will wait for the entire operation to complete. " +
        "Otherwise, it will be performed in the background, and the command will immediately return control.");

    /** Option name. */
    private final String name;

    /** Option description. */
    private final String desc;

    /**
     * @param name Option name.
     * @param desc Option description.
     */
    SnapshotCreateCommandOption(String name, String desc) {
        this.name = name;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return "--" + name;
    }

    /** @return Option name. */
    public String optionName() {
        return name;
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
