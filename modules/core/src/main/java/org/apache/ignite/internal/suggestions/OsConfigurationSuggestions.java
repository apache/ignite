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

            if ((value = readVmParam(SWAPPINESS)) != null) {
                try {
                    double maxSwappiness = 10.0;

                    if (Float.parseFloat(value) > maxSwappiness)
                        suggestions.add(String.format("Reduce pages swapping ratio (set vm.%s=%f or less)", SWAPPINESS,
                                                      maxSwappiness));
                }
                catch (NumberFormatException ignored) {
                    // OS param not parsable as a number
                }
            }

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
