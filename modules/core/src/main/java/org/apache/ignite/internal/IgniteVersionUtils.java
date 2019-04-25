/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Ignite version utils.
 */
public class IgniteVersionUtils {
    /** Ignite version in String form. */
    public static final String VER_STR;

    /** Ignite version. */
    public static final IgniteProductVersion VER;

    /** Formatted build date. */
    public static final String BUILD_TSTAMP_STR;

    /** Build timestamp in seconds. */
    public static final long BUILD_TSTAMP;

    /** Build timestamp string property value. */
    private static final String BUILD_TSTAMP_FROM_PROPERTY;

    /** Revision hash. */
    public static final String REV_HASH_STR;

    /** Release date. */
    public static final String RELEASE_DATE_STR;

    /** Compound version. */
    public static final String ACK_VER_STR;

    /** Copyright blurb. */
    public static final String COPYRIGHT;

    /**
     * Static initializer.
     */
    static {
        VER_STR = IgniteProperties.get("ignite.version")
            .replace(".a", "-a") // Backward compatibility fix.
            .replace(".b", "-b")
            .replace(".final", "-final");

        BUILD_TSTAMP_FROM_PROPERTY = IgniteProperties.get("ignite.build");

        //Development ignite.properties file contains ignite.build = 0, so we will add the check for it.
        BUILD_TSTAMP = !BUILD_TSTAMP_FROM_PROPERTY.isEmpty() && Long.parseLong(BUILD_TSTAMP_FROM_PROPERTY) != 0
            ? Long.parseLong(BUILD_TSTAMP_FROM_PROPERTY) : System.currentTimeMillis() / 1000;

        BUILD_TSTAMP_STR = new SimpleDateFormat("yyyyMMdd").format(new Date(BUILD_TSTAMP * 1000));

        COPYRIGHT = BUILD_TSTAMP_STR.substring(0, 4) + " Copyright(C) Apache Software Foundation";

        REV_HASH_STR = IgniteProperties.get("ignite.revision");

        RELEASE_DATE_STR = IgniteProperties.get("ignite.rel.date");

        String rev = REV_HASH_STR.length() > 8 ? REV_HASH_STR.substring(0, 8) : REV_HASH_STR;

        ACK_VER_STR = VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + rev;

        VER = IgniteProductVersion.fromString(VER_STR + '-' + BUILD_TSTAMP + '-' + REV_HASH_STR);
    }

    /**
     * Private constructor.
     */
    private IgniteVersionUtils() {
        // No-op.
    }
}
