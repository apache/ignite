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

package org.apache.ignite.internal.commandline.defragmentation;

import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationOperation;
import org.jetbrains.annotations.Nullable;

/** */
public enum DefragmentationSubcommands {
    /** */
    SCHEDULE("schedule", VisorDefragmentationOperation.SCHEDULE),

    /** */
    STATUS("status", VisorDefragmentationOperation.STATUS),

    /** */
    CANCEL("cancel", VisorDefragmentationOperation.CANCEL);

    /** */
    private final String name;

    /** */
    private final VisorDefragmentationOperation visorOperation;

    /** */
    DefragmentationSubcommands(String name, VisorDefragmentationOperation visorOperation) {
        this.name = name;
        this.visorOperation = visorOperation;
    }

    /**
     * @param strRep String representation of subcommand.
     * @return Subcommand for its string representation.
     */
    public static @Nullable DefragmentationSubcommands of(String strRep) {
        for (DefragmentationSubcommands cmd : values()) {
            if (cmd.text().equalsIgnoreCase(strRep))
                return cmd;
        }

        return null;
    }

    /** */
    public String text() {
        return name;
    }

    /** */
    public VisorDefragmentationOperation operation() {
        return visorOperation;
    }
}
