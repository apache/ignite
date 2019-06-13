/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.ru;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.jetbrains.annotations.Nullable;

/**
 * Set of rolling upgrade commands.
 */
public enum RollingUpgradeSubCommands {
    /**
     * Enable rolling upgrade.
     */
    ENABLE("on", RollingUpgradeCommandArg.class),

    /**
     * Disable rolling upgrade.
     */
    DISABLE("off"),

    /**
     * Rolling Upgrade status.
     */
    STATUS("status");

    /** Enumerated values. */
    private static final RollingUpgradeSubCommands[] VALS = values();

    /** Name. */
    private final String name;

    /** Enum class with argument list for command. */
    private final Class<? extends Enum<? extends CommandArg>> cmdArgs;

    /**
     * @param name Name.
     */
    RollingUpgradeSubCommands(String name) {
        this.name = name;
        cmdArgs = null;
    }

    /**
     * @param name Name.
     * @param cmdArgs Arguments.
     */
    RollingUpgradeSubCommands(String name, Class<? extends Enum<? extends CommandArg>> cmdArgs) {
        this.name = name;
        this.cmdArgs = cmdArgs;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static RollingUpgradeSubCommands of(String text) {
        for (RollingUpgradeSubCommands cmd : values()) {
            if (cmd.text().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /**
     * @return Name.
     */
    public String text() {
        return name;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static RollingUpgradeSubCommands fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
