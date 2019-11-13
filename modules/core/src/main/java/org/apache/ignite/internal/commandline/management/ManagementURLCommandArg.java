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

import org.apache.ignite.internal.commandline.argument.CommandArg;

/**
 * {@link ManagementCommandList#URI} command arguments.
 */
public enum ManagementURLCommandArg implements CommandArg {
    /** SSL cipher suites. */
    CIPHER_SUITES("management-cipher-suites"),

    /** Path to server key store. */
    KEYSTORE("management-keystore"),

    /** Server key store password. */
    KEYSTORE_PASSWORD("management-keystore-password"),

    /** Path to server trust store. */
    TRUSTSTORE("management-truststore"),

    /** Server trust store password. */
    TRUSTSTORE_PASSWORD("management-truststore-password"),

    /** Session timeout. */
    SESSION_TIMEOUT("management-session-timeout"),

    /** Session expiration timeout. */
    SESSION_EXPIRATION_TIMEOUT("management-session-timeout");

    /** Option name. */
    private final String name;

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static ManagementURLCommandArg of(String text) {
        for (ManagementURLCommandArg cmd : values()) {
            if (cmd.name().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /** */
    ManagementURLCommandArg(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
