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

package org.apache.ignite.internal.client.thin;

/** Thin client protocol version. */
public final class ProtocolVersion implements Comparable<ProtocolVersion> {
    /** Protocol version: 1.7.0. Features support. */
    public static final ProtocolVersion V1_7_0 = new ProtocolVersion((short)1, (short)7, (short)0);

    /** Protocol version: 1.6.0. Expiration policy support. */
    public static final ProtocolVersion V1_6_0 = new ProtocolVersion((short)1, (short)6, (short)0);

    /** Protocol version: 1.5.0. Transactions support. */
    public static final ProtocolVersion V1_5_0 = new ProtocolVersion((short)1, (short)5, (short)0);

    /** Protocol version: 1.4.0. Partition awareness. */
    public static final ProtocolVersion V1_4_0 = new ProtocolVersion((short)1, (short)4, (short)0);

    /** Protocol version: 1.3.0. */
    public static final ProtocolVersion V1_3_0 = new ProtocolVersion((short)1, (short)3, (short)0);

    /** Protocol version: 1.2.0. */
    public static final ProtocolVersion V1_2_0 = new ProtocolVersion((short)1, (short)2, (short)0);

    /** Protocol version: 1.1.0. */
    public static final ProtocolVersion V1_1_0 = new ProtocolVersion((short)1, (short)1, (short)0);

    /** Protocol version 1.0.0. */
    public static final ProtocolVersion V1_0_0 = new ProtocolVersion((short)1, (short)0, (short)0);

    /** The most actual version. */
    public static final ProtocolVersion LATEST_VER = V1_7_0;

    /** Major. */
    private final short major;

    /** Minor. */
    private final short minor;

    /** Patch. */
    private final short patch;

    /** Constructor. */
    ProtocolVersion(short major, short minor, short patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    /**
     * @return Major.
     */
    public short major() {
        return major;
    }

    /**
     * @return Minor.
     */
    public short minor() {
        return minor;
    }

    /**
     * @return Patch.
     */
    public short patch() {
        return patch;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof ProtocolVersion))
            return false;

        ProtocolVersion other = (ProtocolVersion)obj;

        return major == other.major &&
            minor == other.minor &&
            patch == other.patch;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 11;
        res = 31 * res + major;
        res = 31 * res + minor;
        res = 31 * res + patch;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(ProtocolVersion other) {
        int diff = major - other.major;

        if (diff != 0)
            return diff;

        diff = minor - other.minor;

        if (diff != 0)
            return diff;

        return patch - other.patch;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.format("%s.%s.%s", major, minor, patch);
    }
}
