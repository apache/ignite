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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Binary type implementation.
 */
public class BinaryTypeImpl implements BinaryType {
    /** Binary context. */
    @GridToStringExclude
    private final BinaryContext ctx;

    /** Type metadata. */
    private final BinaryMetadata meta;

    /**
     * Constructor.
     *
     * @param ctx Binary context.
     * @param meta Type  metadata.
     */
    public BinaryTypeImpl(BinaryContext ctx, BinaryMetadata meta) {
        this.ctx = ctx;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return meta.typeName();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return meta.typeId();
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fieldNames() {
        return meta.fields();
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName(String fieldName) {
        return meta.fieldTypeName(fieldName);
    }

    /** {@inheritDoc} */
    @Override public BinaryFieldImpl field(String fieldName) {
        return ctx.createField(meta.typeId(), fieldName);
    }

    /** {@inheritDoc} */
    @Override public String affinityKeyFieldName() {
        return meta.affinityKeyFieldName();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnum() {
        return meta.isEnum();
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> enumValues() {
        Collection<Integer> ordinals = meta.enumMap().values();

        ArrayList<BinaryObject> enumValues = new ArrayList<>(ordinals.size());

        for (Integer ord: ordinals)
            enumValues.add(new BinaryEnumObjectImpl(ctx, typeId(), typeName(), ord));

        return enumValues;
    }

    /**
     * @return Context.
     */
    public BinaryContext context() {
        return ctx;
    }

    /**
     * @return Metadata.
     */
    public BinaryMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryTypeImpl.class, this);
    }
}
