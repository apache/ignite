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

import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Java Virtual Machine performance suggestions.
 */
public class JvmPerformanceSuggestions {
    private static final String XMX = "-Xmx";
    private static final String MX = "-mx";
    private static final String MAX_DIRECT_MEMORY_SIZE = "-XX:MaxDirectMemorySize";
    private static final String USE_COMPRESSED_OOPS = "-XX:+UseCompressedOops";
    private static final String DISABLE_EXPLICIT_GC = "-XX:+DisableExplicitGC";
    private static final String USE_TLAB = "-XX:+UseTLAB";
    private static final String SERVER = "-server";

    /**
     * @param log Log.
     */
    public static synchronized void logSuggestions(IgniteLogger log) {
        List<String> jvmOptions = JvmPerformanceSuggestions.getRecommendedOptions();

        if (!jvmOptions.isEmpty()) {
            U.quietAndInfo(log, "Use the following JVM-options to increase Ignite performance:");
            for (String option : jvmOptions)
                U.quietAndInfo(log, "    " + option);
        }
    }

    @NotNull
    private static List<String> getRecommendedOptions() {
        List<String> options = new LinkedList<>();
        // option '-server' isn't in input arguments
        if (!U.jvmName().toLowerCase().contains("server"))
            options.add(SERVER);

        List<String> args = U.jvmArgs();

        if (!anyStartWith(args, XMX) && !anyStartWith(args, MX))
            options.add(XMX + "<size>[g|G|m|M|k|K]");

        if (!anyStartWith(args, MAX_DIRECT_MEMORY_SIZE))
            options.add(MAX_DIRECT_MEMORY_SIZE + "=size[g|G|m|M|k|K]");

        if (!args.contains(USE_TLAB))
            options.add(USE_TLAB);

        if (!args.contains(DISABLE_EXPLICIT_GC))
            options.add(DISABLE_EXPLICIT_GC);

        if (!args.contains(USE_COMPRESSED_OOPS))
            options.add(USE_COMPRESSED_OOPS);

        return options;
    }

    private static boolean anyStartWith(List<String> lines, String prefix) {
        for (String line : lines)
            if (line.startsWith(prefix))
                return true;

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JvmPerformanceSuggestions.class, this);
    }
}
