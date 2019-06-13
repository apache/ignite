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

/**
 * This class contains all possible arguments after parsing rolling-upgrade command input.
 */
public class RollingUpgradeArguments {
    /** */
    private final RollingUpgradeSubCommands cmd;

    /** */
    private final boolean forcedMode;

    /**
     * Creates a new instance of RollingUpgradeArguments.
     *
     * @param builder Rolling upgrade arguments.
     */
    public RollingUpgradeArguments(Builder builder) {
        cmd = builder.cmd;
        forcedMode = builder.forcedMode;
    }

    /**
     * @return Rolling upgrade command.
     */
    public RollingUpgradeSubCommands command() {
        return cmd;
    }

    /**
     * @return {@code true} if strict mode enabled.
     */
    public boolean isForcedMode() {
        return RollingUpgradeSubCommands.ENABLE == cmd && forcedMode;
    }

    /** */
    public static class Builder {
        /** */
        private final RollingUpgradeSubCommands cmd;

        /** */
        private boolean forcedMode;

        /**
         * Creates a new instance of builder.
         */
        public Builder(RollingUpgradeSubCommands cmd) {
            this.cmd = cmd;
        }

        /**
         * @param forcedMode {@code true} if forced mode should be enabled.
         * @return This instance for chaining.
         */
        public Builder withForcedMode(boolean forcedMode) {
            this.forcedMode = forcedMode;

            return this;
        }

        /**
         * @return New instance of {@link RollingUpgradeArguments} with the given parameters.
         */
        public RollingUpgradeArguments build() {
            return new RollingUpgradeArguments(this);
        }
    }
}
