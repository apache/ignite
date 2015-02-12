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

import org.apache.ignite.lang.*;

import java.text.*;
import java.util.*;

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

    /** Revision hash. */
    public static final String REV_HASH_STR;

    /** Release date. */
    public static final String RELEASE_DATE_STR;

    /** Compound version. */
    public static final String ACK_VER_STR;

    /** Copyright blurb. */
    public static final String COPYRIGHT = "2015 Copyright(C) Apache Software Foundation";

    /**
     * Static initializer.
     */
    static {
        VER_STR = GridProperties.get("ignite.version");

        BUILD_TSTAMP = Long.valueOf(GridProperties.get("ignite.build"));
        BUILD_TSTAMP_STR = new SimpleDateFormat("yyyyMMdd").format(new Date(BUILD_TSTAMP * 1000));

        REV_HASH_STR = GridProperties.get("ignite.revision");
        RELEASE_DATE_STR = GridProperties.get("ignite.rel.date");

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
