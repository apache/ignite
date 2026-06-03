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

package org.apache.ignite.internal;

import java.time.LocalDate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;

import static java.time.ZoneOffset.UTC;

/**
 * Ignite version utils.
 */
public class IgniteVersionUtils {
    /** Ignite artifact version. See {@link IgniteProperties} file generation by Maven build scripts. */
    public static final String VER_STR;

    /** Ignite version. */
    public static final IgniteProductVersion VER;

    /** Copyright blurb. */
    public static final String COPYRIGHT;

    static {
        VER_STR = IgniteProperties.get("ignite.version");

        long revTs = Long.parseLong(IgniteProperties.get("ignite.build"));

        String revHash = IgniteProperties.get("ignite.revision");

        VER = IgniteProductVersion.fromString(VER_STR + '-' + revTs + '-' + revHash);

        int year = revTs == 0 ? LocalDate.now(UTC).getYear() : VER.buildTime().atZone(UTC).getYear();

        COPYRIGHT = year + " Copyright(C) Apache Software Foundation";
    }

    /** */
    public static IgniteProductVersion clearStage(IgniteProductVersion ver) {
        if (F.isEmpty(ver.stage()))
            return ver;

        return new IgniteProductVersion(ver.major(), ver.minor(), ver.maintenance(), ver.revisionTimestamp(), ver.revisionHash());
    }

    /**
     * Private constructor.
     */
    private IgniteVersionUtils() {
        // No-op.
    }
}
