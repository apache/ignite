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

package org.apache.ignite.internal.commandline.baseline;

import org.jetbrains.annotations.Nullable;

/**
 * Set of baseline commands.
 */
public enum BaselineCommand {
    /**
     * Add nodes to baseline.
     */
    ADD("add"),

    /**
     * Remove nodes from baseline.
     */
    REMOVE("remove"),

    /**
     * Collect information about baseline.
     */
    COLLECT("collect"),

    /**
     * Set new baseline.
     */
    SET("set"),

    /**
     * Check current topology version.
     */
    VERSION("version"),

    /**
     * Baseline auto-adjust configuration.
     */
    AUTO_ADJUST("auto_adjust");

    /** Enumerated values. */
    private static final BaselineCommand[] VALS = values();

    /** Name. */
    private final String name;

    /**
     * @param name Name.
     */
    BaselineCommand(String name) {
        this.name = name;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static BaselineCommand of(String text) {
        for (BaselineCommand cmd : BaselineCommand.values()) {
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
    @Nullable public static BaselineCommand fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
