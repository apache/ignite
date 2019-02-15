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

package org.apache.ignite.internal.client.thin;

/** Thin client protocol version. */
public final class ProtocolVersion implements Comparable<ProtocolVersion> {
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
