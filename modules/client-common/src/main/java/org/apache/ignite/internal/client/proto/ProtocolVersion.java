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

package org.apache.ignite.internal.client.proto;

import java.io.IOException;
import org.apache.ignite.internal.tostring.S;

/** Thin client protocol version. */
public final class ProtocolVersion implements Comparable<ProtocolVersion> {
    /** Protocol version: 3.0.0. */
    public static final ProtocolVersion V3_0_0 = new ProtocolVersion((short) 3, (short) 0, (short) 0);

    /** The most actual version. */
    public static final ProtocolVersion LATEST_VER = V3_0_0;

    /** Major. */
    private final short major;

    /** Minor. */
    private final short minor;

    /** Patch. */
    private final short patch;

    /**
     * Constructor.
     *
     * @param major Major part.
     * @param minor Minor part.
     * @param patch Patch part.
     */
    public ProtocolVersion(short major, short minor, short patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    /**
     * Reads version from unpacker.
     *
     * @param unpacker Unpacker.
     * @return Version.
     * @throws IOException when underlying input throws IOException.
     */
    public static ProtocolVersion unpack(ClientMessageUnpacker unpacker) throws IOException {
        return new ProtocolVersion(unpacker.unpackShort(), unpacker.unpackShort(), unpacker.unpackShort());
    }

    /**
     * Writes this instance to the specified packer.
     *
     * @param packer Packer.
     * @throws IOException when underlying output throws IOException.
     */
    public void pack(ClientMessagePacker packer) throws IOException {
        packer.packShort(major).packShort(minor).packShort(patch);
    }

    /**
     * Gets the major part.
     *
     * @return Major.
     */
    public short major() {
        return major;
    }

    /**
     * Gets the minor part.
     *
     * @return Minor.
     */
    public short minor() {
        return minor;
    }

    /**
     * Gets the patch part.
     *
     * @return Patch.
     */
    public short patch() {
        return patch;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ProtocolVersion)) {
            return false;
        }

        ProtocolVersion other = (ProtocolVersion) obj;

        return major == other.major && minor == other.minor && patch == other.patch;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int res = 31 * major;
        res += ((minor & 0xFFFF) << 16) & (patch & 0xFFFF);

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(ProtocolVersion other) {
        int diff = major - other.major;

        if (diff != 0) {
            return diff;
        }

        diff = minor - other.minor;

        if (diff != 0) {
            return diff;
        }

        return patch - other.patch;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ProtocolVersion.class, this);
    }
}
