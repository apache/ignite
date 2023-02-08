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

package org.apache.ignite.internal.commandline.cdc;

import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.util.typedef.F;

/**
 * CDC sub commands.
 */
public enum CdcSubcommands {
    /** Sub-command to delete lost segment links. */
    DELETE_LOST_SEGMENT_LINKS(new DeleteLostSegmentLinksCommand()),

    /** Sub-command to flush caches. */
    SYNC_CACHES(new FlushCachesCommand());

    /** Sub-command. */
    private final AbstractCommand<?> cmd;

    /** @param cmd Sub-command. */
    CdcSubcommands(AbstractCommand<?> cmd) {
        this.cmd = cmd;
    }

    /**
     * @param name Command name (case insensitive).
     * @return Command for the specified name.
     */
    public static CdcSubcommands of(String name) {
        CdcSubcommands[] cmds = values();

        for (CdcSubcommands cmd : cmds) {
            if (cmd.subCommand().name().equalsIgnoreCase(name))
                return cmd;
        }

        throw new IllegalArgumentException(
            "Invalid argument: " + name + ". One of " + F.asList(cmds) + " is expected.");
    }

    /** @return Sub-command. */
    public AbstractCommand<?> subCommand() {
        return cmd;
    }

    /** @return Sub-command name. */
    @Override public String toString() {
        return cmd.name();
    }
}
