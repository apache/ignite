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

package org.apache.ignite.yardstick.cache.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Organization record used for query test.
 */
public class Organization implements Externalizable, Binarylizable {
    /** Organization ID. */
    @QuerySqlField(index = true)
    private int id;

    /** Organization name. */
    @QuerySqlField(index = true)
    private String name;

    /**
     * Constructs empty organization.
     */
    public Organization() {
        // No-op.
    }

    /**
     * Constructs organization with given ID.
     *
     * @param id Organization ID.
     * @param name Organization name.
     */
    public Organization(int id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * @return Organization id.
     */
    public int getId() {
        return id;
    }

    /**
     * @param id Organization id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Organization name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Organization name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        name = in.readUTF();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeInt("id", id);
        writer.writeString("name", name);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        id = reader.readInt("id");
        name = reader.readString("name");
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || (o instanceof Organization) && id == ((Organization)o).id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization [id=" + id +
            ", name=" + name + ']';
    }
}