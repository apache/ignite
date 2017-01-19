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

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Operation System configuration suggestions.
 */
public class OsConfigurationSuggestions {
    private static final String VM_PARAMS_BASE_PATH = "/proc/sys/vm/";
    private static final String DIRTY_WRITEBACK_CENTISECS = "dirty_writeback_centisecs";
    private static final String DIRTY_EXPIRE_CENTISECS = "dirty_expire_centisecs";
    private static final String SWAPPINESS = "swappiness";
    private static final String ZONE_RECLAIM_MODE = "zone_reclaim_mode";
    private static final String EXTRA_FREE_KBYTES = "extra_free_kbytes";

    /**
     * Log suggestions of Operation system configuration tuning to increase Ignite performance.
     *
     * @param log - Logger.
     */
    public static synchronized void logSuggestions(@NotNull IgniteLogger log) {
        if (isRedHat()) {
            List<String> suggestions = new LinkedList<>();
            String sysctlWVM = "sysctl –w vm.";
            Integer value;

            if ((value = readVmParam(DIRTY_WRITEBACK_CENTISECS)) != null && value != 500)
                suggestions.add(sysctlWVM + DIRTY_WRITEBACK_CENTISECS);

            if ((value = readVmParam(DIRTY_EXPIRE_CENTISECS)) != null && value != 500)
                suggestions.add(sysctlWVM + DIRTY_EXPIRE_CENTISECS);

            if ((value = readVmParam(SWAPPINESS)) != null && value != 10)
                suggestions.add(sysctlWVM + SWAPPINESS);

            if ((value = readVmParam(ZONE_RECLAIM_MODE)) != null && value != 0)
                suggestions.add(sysctlWVM + ZONE_RECLAIM_MODE);

            if ((value = readVmParam(EXTRA_FREE_KBYTES)) != null && value != 1240000)
                suggestions.add(sysctlWVM + EXTRA_FREE_KBYTES);

            if (!suggestions.isEmpty()) {
                U.quietAndInfo(log, "Please, use the following commands to configure your OS:");
                for (String suggestion : suggestions)
                    U.quietAndInfo(log, suggestion);
            }
        }
    }

    @Nullable
    private static Integer readVmParam(@NotNull String name) {
        Path path = Paths.get(VM_PARAMS_BASE_PATH + name);

        if (!Files.exists(path))
            return null;
        try {
            byte[] bytes = Files.readAllBytes(path);
            return ByteBuffer.wrap(bytes).getInt();
        }
        catch (IOException | InvalidPathException | BufferUnderflowException ignores) {
            return null;
        }
    }

    private static boolean isRedHat() {
        return Files.exists(Paths.get("/etc/redhat-release")); // RedHat family OS (Fedora, CentOS, RedHat)
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OsConfigurationSuggestions.class, this);
    }
}
