/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.Locale;
import org.apache.ignite.lang.IgniteLogger;

/**
 *
 */
public class Platform {

    private static final IgniteLogger LOG = IgniteLogger.forClass(Platform.class);

    private static final boolean IS_WINDOWS = isWindows0();

    private static final boolean IS_MAC = isMac0();

    /**
     * Return {@code true} if the JVM is running on Windows
     */
    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    /**
     * Return {@code true} if the JVM is running on Mac OSX
     */
    public static boolean isMac() {
        return IS_MAC;
    }

    private static boolean isMac0() {
        final boolean mac = SystemPropertyUtil.get("os.name", "") //
            .toLowerCase(Locale.US) //
            .contains("mac os x");
        if (mac) {
            LOG.debug("Platform: Mac OS X");
        }
        return mac;
    }

    private static boolean isWindows0() {
        final boolean windows = SystemPropertyUtil.get("os.name", "") //
            .toLowerCase(Locale.US) //
            .contains("win");
        if (windows) {
            LOG.debug("Platform: Windows");
        }
        return windows;
    }
}
