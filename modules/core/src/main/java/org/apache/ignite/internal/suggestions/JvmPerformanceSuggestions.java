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
import org.jetbrains.annotations.Nullable;

/**
 * Java Virtual Machine performance suggestions.
 */
public class JvmPerformanceSuggestions {
    private static final String XMX = "-Xmx";
    private static final String MX = "-mx";
    private static final String MAX_DIRECT_MEMORY_SIZE = "-XX:MaxDirectMemorySize";
    private static final String DISABLE_EXPLICIT_GC = "-XX:+DisableExplicitGC";
    private static final String USE_TLAB = "-XX:+UseTLAB";
    private static final String SERVER = "-server";
    private static final String PRINT_GC_DETAILS = "-XX:+PrintGCDetails";
    private static final String PRINT_GC_TIME_STAMPS = "-XX:+PrintGCTimeStamps";
    private static final String PRINT_GC_DATE_STAMPS = "-XX:+PrintGCDateStamps";
    private static final String USE_GC_LOG_FILE_ROTATION = "-XX:+UseGCLogFileRotation";
    private static final String NUMBER_OF_GC_LOG_FILES = "-XX:NumberOfGCLogFiles";
    private static final String GC_LOG_FILE_SIZE = "-XX:GCLogFileSize";
    private static final String XLOGGC = "-Xloggc";

    /**
     * Log suggestions of JVM-tuning to increase Ignite performance.
     *
     * @param log Log.
     */
    public static synchronized void logSuggestions(IgniteLogger log) {

        if (U.heapSize(1) > 30.5)
            U.quietAndInfo(log, "Heap size is greater than 30,5G, JVM can not use compressed oops!");

        List<String> args = U.jvmArgs();
        List<String> gcLoggingOptions = getGCLoggingOptions(args);

        if (!gcLoggingOptions.isEmpty()) {
            U.quietAndInfo(log, "JVM Garbage Collection logging is configured not completely.");
            U.quietAndInfo(log, "Please add the following parameters to the JVM configuration:");
            for (String option : gcLoggingOptions)
                U.quietAndInfo(log, "    " + option);
        }

        List<String> jvmOptions = getRecommendedOptions(args);

        if (!jvmOptions.isEmpty()) {
            U.quietAndInfo(log, "Use the following JVM-options to increase Ignite performance:");
            for (String option : jvmOptions)
                U.quietAndInfo(log, "    " + option);
        }
    }

    @NotNull
    private static List<String> getGCLoggingOptions(List<String> args) {
        List<String> options = new LinkedList<>();

        if (!args.contains(PRINT_GC_DETAILS))
            options.add(PRINT_GC_DETAILS);

        if (!args.contains(PRINT_GC_TIME_STAMPS))
            options.add(PRINT_GC_TIME_STAMPS);

        if (!args.contains(PRINT_GC_DATE_STAMPS))
            options.add(PRINT_GC_DATE_STAMPS);

        if (!args.contains(USE_GC_LOG_FILE_ROTATION))
            options.add(USE_GC_LOG_FILE_ROTATION);

        if (!anyStartWith(args, NUMBER_OF_GC_LOG_FILES))
            options.add(NUMBER_OF_GC_LOG_FILES + "=10");

        if (!anyStartWith(args, GC_LOG_FILE_SIZE))
            options.add(GC_LOG_FILE_SIZE + "=100M");

        if (!anyStartWith(args, XLOGGC))
            options.add(XLOGGC + ":/path/to/gc/logs/log.txt");

        return options;
    }

    @NotNull
    private static List<String> getRecommendedOptions(List<String> args) {
        List<String> options = new LinkedList<>();
        // option '-server' isn't in input arguments
        if (!U.jvmName().toLowerCase().contains("server"))
            options.add(SERVER);

        if (getMaxHeapSizeOption(args) == null)
            options.add(XMX + "<size>[g|G|m|M|k|K]");

        if (!anyStartWith(args, MAX_DIRECT_MEMORY_SIZE))
            options.add(MAX_DIRECT_MEMORY_SIZE + "=<size>[g|G|m|M|k|K]");

        if (!args.contains(USE_TLAB))
            options.add(USE_TLAB);

        if (!args.contains(DISABLE_EXPLICIT_GC))
            options.add(DISABLE_EXPLICIT_GC);

        return options;
    }

    private static boolean anyStartWith(List<String> lines, String prefix) {
        for (String line : lines)
            if (line.startsWith(prefix))
                return true;

        return false;
    }

    @Nullable
    private static String getMaxHeapSizeOption(List<String> lines) {
        for (String line : lines)
            if (line.startsWith(XMX) || line.startsWith(MX))
                return line;

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JvmPerformanceSuggestions.class, this);
    }
}
