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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.suggestions.JvmOptions.DISABLE_EXPLICIT_GC;
import static org.apache.ignite.internal.suggestions.JvmOptions.MAX_DIRECT_MEMORY_SIZE;
import static org.apache.ignite.internal.suggestions.JvmOptions.SERVER;
import static org.apache.ignite.internal.suggestions.JvmOptions.USE_COMPRESSED_OOPS;
import static org.apache.ignite.internal.suggestions.JvmOptions.USE_TLAB;
import static org.apache.ignite.internal.suggestions.JvmOptions.XMX;

/**
 * Java Virtual Machine performance suggestions.
 */
class JvmPerformanceSuggestions {

    /**
     * @return list of recommended jvm options
     */
    @NotNull static List<String> getRecommendedOptions() {
        List<String> options = new LinkedList<>();
        // option '-server' isn't in input arguments
        if (!U.jvmName().toLowerCase().contains("server"))
            options.add(SERVER);

        List<String> args = U.jvmArgs();

        if (!args.stream().anyMatch(o -> o.startsWith(XMX)))
            options.add(XMX + "<size>[g|G|m|M|k|K]");

        if (!args.stream().anyMatch(o -> o.startsWith(MAX_DIRECT_MEMORY_SIZE)))
            options.add(MAX_DIRECT_MEMORY_SIZE + "=size[g|G|m|M|k|K]");

        if (!args.contains(USE_TLAB))
            options.add(USE_TLAB);

        if (!args.contains(DISABLE_EXPLICIT_GC))
            options.add(DISABLE_EXPLICIT_GC);

        if (!args.contains(USE_COMPRESSED_OOPS))
            options.add(USE_COMPRESSED_OOPS);

        return options;
    }
}
