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
    private static final String XMX = "-Xmx";
    private static final String MX = "-mx";
    private static final String MAX_DIRECT_MEMORY_SIZE = "-XX:MaxDirectMemorySize";
    private static final String DISABLE_EXPLICIT_GC = "-XX:+DisableExplicitGC";
    private static final String USE_TLAB = "-XX:+UseTLAB";
    private static final String SERVER = "-server";
    private static final String USE_G1_GC = "-XX:+UseG1GC";

    /**
     * Checks JVM configurations and produces tuning suggestions.
     *
     * @return - list of suggestions of Java Virtual Machine configuration tuning to increase Ignite performance.
     */
    @NotNull
    public static synchronized List<String> getSuggestions() {
        List<String> suggestions = new ArrayList<>();

        if (U.heapSize(1) > 30.5)
            suggestions.add("JVM heap size is greater than 30.5Gb, JVM will not use compressed oops");

        List<String> args = U.jvmArgs();

        if (!U.jvmName().toLowerCase().contains("server"))
            suggestions.add("Enable server mode for JVM (add '" + SERVER + "' to JVM options)");

        if (U.jdkVersion().equals("1.8") && !args.contains(USE_G1_GC))
            suggestions.add("Enable G1 Garbage Collector (add '" + USE_G1_GC + "' to JVM options");

        if (!anyStartWith(args, XMX) && !anyStartWith(args, MX))
            suggestions.add("Specify JVM heap max size (add '" + XMX + "<size>[g|G|m|M|k|K]' to JVM options)");

        if (!anyStartWith(args, MAX_DIRECT_MEMORY_SIZE))
            suggestions.add("Specify New I/O max total size (add '" + MAX_DIRECT_MEMORY_SIZE + "=<size>[g|G|m|M|k|K]' to JVM options)");

        if (!args.contains(USE_TLAB))
            suggestions.add("Enable using thread-local allocation blocks (add '" + USE_TLAB + "' to JVM options)");

        if (!args.contains(DISABLE_EXPLICIT_GC))
            suggestions.add("Disable processing of calls to System.gc() (add '" + DISABLE_EXPLICIT_GC + "' to JVM options)");

        return suggestions;
    }

    private static boolean anyStartWith(@NotNull List<String> lines, @NotNull String prefix) {
        for (String line : lines)
            if (line.startsWith(prefix))
                return true;

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JvmConfigurationSuggestions.class, this);
    }
}
