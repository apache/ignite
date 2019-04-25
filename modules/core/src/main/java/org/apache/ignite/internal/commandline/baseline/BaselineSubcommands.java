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

package org.apache.ignite.internal.commandline.baseline;

import org.apache.ignite.internal.visor.baseline.VisorBaselineOperation;
import org.jetbrains.annotations.Nullable;

/**
 * Set of baseline commands.
 */
public enum BaselineSubcommands {
    /**
     * Add nodes to baseline.
     */
    ADD("add", VisorBaselineOperation.ADD),

    /**
     * Remove nodes from baseline.
     */
    REMOVE("remove", VisorBaselineOperation.REMOVE),

    /**
     * Collect information about baseline.
     */
    COLLECT("collect", VisorBaselineOperation.COLLECT),

    /**
     * Set new baseline.
     */
    SET("set", VisorBaselineOperation.SET),

    /**
     * Check current topology version.
     */
    VERSION("version", VisorBaselineOperation.VERSION),

    /**
     * Baseline auto-adjust configuration.
     */
    AUTO_ADJUST("auto_adjust", VisorBaselineOperation.AUTOADJUST);

    /** Enumerated values. */
    private static final BaselineSubcommands[] VALS = values();

    /** Name. */
    private final String name;

    /** Corresponding visor baseline operation. */
    private final VisorBaselineOperation visorBaselineOperation;

    /**
     * @param name Name.
     * @param operation
     */
    BaselineSubcommands(String name, VisorBaselineOperation operation) {
        this.name = name;
        visorBaselineOperation = operation;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static BaselineSubcommands of(String text) {
        for (BaselineSubcommands cmd : BaselineSubcommands.values()) {
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
     * @return {@link VisorBaselineOperation} which is associated with baseline subcommand.
     */
    public VisorBaselineOperation visorBaselineOperation() {
        return visorBaselineOperation;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static BaselineSubcommands fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
