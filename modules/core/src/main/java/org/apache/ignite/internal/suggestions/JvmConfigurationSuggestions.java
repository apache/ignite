/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.suggestions;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Java Virtual Machine configuration suggestions.
 */
public class JvmConfigurationSuggestions {
    /** */
    private static final String XMX = "-Xmx";

    /** */
    private static final String MX = "-mx";

    /** */
    private static final String MAX_DIRECT_MEMORY_SIZE = "-XX:MaxDirectMemorySize";

    /** */
    private static final String DISABLE_EXPLICIT_GC = "-XX:+DisableExplicitGC";

    /** */
    private static final String NOT_USE_TLAB = "-XX:-UseTLAB";

    /** */
    private static final String SERVER = "-server";

    /** */
    private static final String USE_G1_GC = "-XX:+UseG1GC";

    /**
     * Checks JVM configurations and produces tuning suggestions.
     *
     * @return List of suggestions of Java Virtual Machine configuration tuning to increase Ignite performance.
     */
    public static synchronized List<String> getSuggestions() {
        List<String> suggestions = new ArrayList<>();

        List<String> args = U.jvmArgs();

        if (!U.jvmName().toLowerCase().contains("server"))
            suggestions.add("Enable server mode for JVM (add '" + SERVER + "' to JVM options)");

        if (!U.jdkVersion().equals("1.8"))
            suggestions.add("Switch to the most recent 1.8 JVM version");

        if (U.jdkVersion().equals("1.8") && !args.contains(USE_G1_GC))
            suggestions.add("Enable G1 Garbage Collector (add '" + USE_G1_GC + "' to JVM options)");

        if (!anyStartWith(args, XMX) && !anyStartWith(args, MX))
            suggestions.add("Specify JVM heap max size (add '" + XMX + "<size>[g|G|m|M|k|K]' to JVM options)");

        if (!anyStartWith(args, MAX_DIRECT_MEMORY_SIZE))
            suggestions.add("Set max direct memory size if getting 'OOME: Direct buffer memory' " +
                "(add '" + MAX_DIRECT_MEMORY_SIZE + "=<size>[g|G|m|M|k|K]' to JVM options)");

        if (args.contains(NOT_USE_TLAB))
            suggestions.add("Enable thread-local allocation buffer (add '-XX:+UseTLAB' to JVM options)");

        if (!args.contains(DISABLE_EXPLICIT_GC))
            suggestions.add("Disable processing of calls to System.gc() (add '" + DISABLE_EXPLICIT_GC + "' to JVM options)");

        return suggestions;
    }

    /**
     * @param lines Lines to check.
     * @param prefix Prefix.
     * @return {@code True} if found.
     */
    private static boolean anyStartWith(@NotNull List<String> lines, @NotNull String prefix) {
        for (String line : lines) {
            if (line.startsWith(prefix))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JvmConfigurationSuggestions.class, this);
    }
}
