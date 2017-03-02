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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Operation System configuration suggestions.
 */
public class OsConfigurationSuggestions {
    /** */
    private static final String VM_PARAMS_BASE_PATH = "/proc/sys/vm/";

    /** */
    private static final String DIRTY_WRITEBACK_CENTISECS = "dirty_writeback_centisecs";

    /** */
    private static final String DIRTY_EXPIRE_CENTISECS = "dirty_expire_centisecs";

    /** */
    private static final String SWAPPINESS = "swappiness";

    /** */
    private static final String ZONE_RECLAIM_MODE = "zone_reclaim_mode";

    /** */
    private static final String EXTRA_FREE_KBYTES = "extra_free_kbytes";

    /**
     * Checks OS configurations and produces tuning suggestions.
     *
     * @return List of suggestions of Operation system configuration tuning to increase Ignite performance.
     */
    public static synchronized List<String> getSuggestions() {
        List<String> suggestions = new ArrayList<>();

        if (U.isRedHat()) {
            String value;
            String expected = "500";

            boolean dwcParamFlag = (value = readVmParam(DIRTY_WRITEBACK_CENTISECS)) != null && !value.equals(expected);
            boolean decParamFlag = (value = readVmParam(DIRTY_EXPIRE_CENTISECS)) != null && !value.equals(expected);

            if (dwcParamFlag || decParamFlag)
                suggestions.add(String.format("Speed up flushing of dirty pages by OS " +
                        "(alter %s%s%s parameter%s by setting to %s)",
                    (dwcParamFlag ? "vm." + DIRTY_WRITEBACK_CENTISECS : ""),
                    (dwcParamFlag && decParamFlag ? " and " : ""),
                    (decParamFlag ? "vm." + DIRTY_EXPIRE_CENTISECS : ""),
                    (dwcParamFlag && decParamFlag ? "s" : ""),
                    expected));

            if ((value = readVmParam(SWAPPINESS)) != null && !value.equals(expected = "10"))
                suggestions.add(String.format("Reduce pages swapping ratio (set vm.%s=%s)", SWAPPINESS, expected));

            if ((value = readVmParam(ZONE_RECLAIM_MODE)) != null && !value.equals(expected = "0"))
                suggestions.add(String.format("Disable NUMA memory reclaim (set vm.%s=%s)", ZONE_RECLAIM_MODE,
                    expected));

            if ((value = readVmParam(EXTRA_FREE_KBYTES)) != null && !value.equals(expected = "1240000"))
                suggestions.add(String.format("Avoid direct reclaim and page allocation failures (set vm.%s=%s)",
                    EXTRA_FREE_KBYTES, expected));
        }

        return suggestions;
    }

    /**
     * @param name Parameter name.
     * @return Value (possibly null).
     */
    @Nullable private static String readVmParam(@NotNull String name) {
        try {
            Path path = Paths.get(VM_PARAMS_BASE_PATH + name);

            if (!Files.exists(path))
                return null;

            return readLine(path);
        }
        catch (Exception ignored) {
            return null;
        }
    }

    /**
     * @param path Path.
     * @return Read line.
     * @throws IOException If failed.
     */
    @Nullable private static String readLine(@NotNull Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return reader.readLine();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OsConfigurationSuggestions.class, this);
    }
}
