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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.suggestions.JvmOptions.*;

/**
 * JVM performance suggestions.
 */
class JvmPerformanceSuggestions {

    /**
     * @return list of recommended jvm options
     */
    @NotNull static List<String> getRecommendedOptions() {
        List<String> options = new LinkedList<>();
        // option '-server' isn't in input arguments
        if (!checkServerOption())
            options.add(SERVER);

        List<String> args = U.jvmArgs();

        if (!args.contains(USE_TLAB))
            options.add(USE_TLAB);

        if (!args.contains(USE_PAR_NEW_GC))
            options.add(USE_PAR_NEW_GC);

        if (!args.contains(USE_CONC_MARK_SWEEP_GC))
            options.add(USE_CONC_MARK_SWEEP_GC);

        if (!args.contains(USE_CMS_INITIATING_OCCUPANCY_ONLY))
            options.add(USE_CMS_INITIATING_OCCUPANCY_ONLY);

        if (!args.contains(DISABLE_EXPLICIT_GC))
            options.add(DISABLE_EXPLICIT_GC);

        if (!args.contains(NEW_SIZE))
            options.add(NEW_SIZE);

        if (!args.contains(MAX_NEW_SIZE))
            options.add(MAX_NEW_SIZE);

        if (!args.contains(MAX_TENURING_THRESHOLD))
            options.add(MAX_TENURING_THRESHOLD);

        if (!args.contains(SURVIVOR_RATIO))
            options.add(SURVIVOR_RATIO);

        if (!args.contains(CMS_INITIATING_OCCUPANCY_FRACTION))
            options.add(CMS_INITIATING_OCCUPANCY_FRACTION);

        return options;
    }

    private static boolean checkServerOption() {
        String arch = System.getProperty("sun.arch.data.model");
        // On a 64-bit capable JDK, only the Java Hotspot Server VM is supported so the -server option is implicit.
        if (arch == null || !arch.equals("64")) {
            String vmName = System.getProperty("java.vm.name");
            return vmName.toLowerCase().contains("server");
        }
        return true;
    }
}
