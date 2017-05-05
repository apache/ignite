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

/**
 * SQL listener protocol version.
 */
public enum SqlListenerProtocolVersion {
    /** Version 2.0.0. */
    VER_2_0_0(makeVersion(2, 0, 0), "2.0.0"),

    /** ODBC/JDBC version of the protocol. Released with Ignite 2.1 */
    VERSION_2_1_0(makeVersion(2,1,0)),

    /** Unknown version. */
    UNKNOWN(Long.MIN_VALUE, "UNKNOWN");

    /** Mask to get 2 lowest bytes of the value and cast to long. */
    private static final long LONG_MASK = 0x000000000000FFFFL;

    /** Long value for version. */
    private final long longVal;

    /** Since string. */
    private final String since;

    /**
     * Constructor.
     *
     * @param longVal Long value.
     * @param since Since string.
     */
    SqlListenerProtocolVersion(long longVal, String since) {
        this.longVal = longVal;
        this.since = since;
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
    public static SqlListenerProtocolVersion fromLong(long longVal) {
        for (SqlListenerProtocolVersion ver : SqlListenerProtocolVersion.values()) {
            if (ver.longValue() == longVal)
                return ver;
        }

        return UNKNOWN;
    }

    /**
     * @return Long value.
     */
    public long longValue() {
        return longVal;
    }

    /**
     * @return Ignite version when introduced.
     */
    public String since() {
        return since;
    }
}
