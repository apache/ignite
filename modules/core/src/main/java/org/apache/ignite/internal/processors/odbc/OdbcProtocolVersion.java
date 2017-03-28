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
package org.apache.ignite.internal.processors.odbc;

import java.util.HashMap;
import java.util.Map;

/**
 * ODBC protocol version.
 */
public enum OdbcProtocolVersion {
    /** First version of the ODBC. Released with Ignite 1.6 */
    VERSION_1_6_0(1),

    /** Second version of the ODBC. Released with Ignite 1.8 */
    VERSION_1_8_0(makeVersion(1,8,0)),

    /** Unknown version. */
    VERSION_UNKNOWN(Long.MIN_VALUE);

    /** Mask to get 2 lowest bytes of the value and cast to long. */
    private static final long LONG_MASK = 0x000000000000FFFFL;

    /** Long value to enum map. */
    private static final Map<Long, OdbcProtocolVersion> versions = new HashMap<>();

    /** Enum value to Ignite version map */
    private static final Map<OdbcProtocolVersion, String> since = new HashMap<>();

    /**
     * Map long values to version.
     */
    static {
        for (OdbcProtocolVersion version : values())
            versions.put(version.longValue(), version);

        since.put(VERSION_1_6_0, "1.6.0");
        since.put(VERSION_1_8_0, "1.8.0");
    }

    /** Long value for version. */
    private final long longVal;

    /**
     * @param longVal Long value.
     */
    OdbcProtocolVersion(long longVal) {
        this.longVal = longVal;
    }

    /**
     * Make long value for the version.
     *
     * @param major Major version.
     * @param minor Minor version.
     * @param maintenance Maintenance version.
     * @return Long value for the version.
     */
    private static long makeVersion(int major, int minor, int maintenance) {
        return ((major & LONG_MASK) << 48) | ((minor & LONG_MASK) << 32) | ((maintenance & LONG_MASK) << 16);
    }

    /**
     * @param longVal Long value.
     * @return Protocol version.
     */
    public static OdbcProtocolVersion fromLong(long longVal) {
        OdbcProtocolVersion res = versions.get(longVal);

        return res == null ? VERSION_UNKNOWN : res;
    }

    /**
     * @return Current version.
     */
    public static OdbcProtocolVersion current() {
        return VERSION_1_8_0;
    }

    /**
     * @return Long value.
     */
    public long longValue() {
        return longVal;
    }

    /**
     * @return {@code true} if this version is unknown.
     */
    public boolean isUnknown() {
        return longVal == VERSION_UNKNOWN.longVal;
    }

    /**
     * @return {@code true} if this version supports distributed joins.
     */
    public boolean isDistributedJoinsSupported() {
        assert !isUnknown();

        return longVal >= VERSION_1_8_0.longVal;
    }

    /**
     * @return Ignite version when introduced.
     */
    public String since() {
        assert !isUnknown();

        return since.get(this);
    }
}
