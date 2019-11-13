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
package org.apache.ignite.internal.commandline.management;

import org.jetbrains.annotations.Nullable;

/**
 * Set of management commands.
 */
public enum ManagementCommandList {
    /**
     * Enable management.
     */
    ENABLE("on"),

    /**
     * Disable management.
     */
    DISABLE("off"),

    /**
     * Management status.
     */
    URI("uri"),

    /**
     * Management status.
     */
    STATUS("status"),

    /**
     * Prints out help for the management command.
     */
    HELP("help");

    /** Enumerated values. */
    private static final ManagementCommandList[] VALS = values();

    /** Name. */
    private final String name;

    /**
     * @param name Name.
     */
    ManagementCommandList(String name) {
        this.name = name;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static ManagementCommandList of(String text) {
        for (ManagementCommandList cmd : values()) {
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
    @Nullable public static ManagementCommandList fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
