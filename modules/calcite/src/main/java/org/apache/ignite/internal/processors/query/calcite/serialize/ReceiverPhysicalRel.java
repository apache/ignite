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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;


/**
 * Describes {@link IgniteReceiver}.
 */
public class ReceiverPhysicalRel implements PhysicalRel {
    /** */
    private DataType rowType;

    /** */
    private long sourceFragmentId;

    /** */
    private List<UUID> sources;

    /** */
    private List<RelCollation> collations;

    /** */
    public ReceiverPhysicalRel() {
    }

    /** */
    public ReceiverPhysicalRel(DataType rowType, long sourceFragmentId, List<UUID> sources, List<RelCollation> collations) {
        this.rowType = rowType;
        this.sourceFragmentId = sourceFragmentId;
        this.sources = sources;
        this.collations = collations;
    }

    /** */
    public DataType rowType() {
        return rowType;
    }

    /** */
    public long sourceFragmentId() {
        return sourceFragmentId;
    }

    /** */
    public Collection<UUID> sources() {
        return sources;
    }

    /** */
    public List<RelCollation> collations() {
        return collations;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(rowType);
        out.writeLong(sourceFragmentId);

        out.writeInt(sources.size());

        for (UUID source : sources)
            out.writeObject(source);

        if (collations == null)
            out.writeInt(-1);
        else {
            out.writeInt(collations.size());

            for (RelCollation collation : collations) {
                List<RelFieldCollation> fields = collation.getFieldCollations();

                out.writeInt(fields.size());

                for (RelFieldCollation field : fields) {
                    out.writeInt(field.getFieldIndex());
                    out.writeByte(field.direction.ordinal());
                    out.writeByte(field.nullDirection.ordinal());
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowType = (DataType) in.readObject();
        sourceFragmentId = in.readLong();

        final int sourcesSize = in.readInt();

        List<UUID> sources = new ArrayList<>(sourcesSize);

        for (int i = 0; i < sourcesSize; i++)
            sources.add((UUID) in.readObject());

        int collationsSize = in.readInt();

        if (collationsSize == -1)
            return;

        List<RelCollation> collations = new ArrayList<>(collationsSize);

        for (int i = 0; i < collationsSize; i++) {
            int fieldsSize = in.readInt();

            List<RelFieldCollation> fields = new ArrayList<>(fieldsSize);

            for (int j = 0; j < fieldsSize; j++) {
                int fieldIndex = in.readInt();
                RelFieldCollation.Direction direction = RelFieldCollation.Direction.values()[in.readByte()];
                RelFieldCollation.NullDirection nullDirection = RelFieldCollation.NullDirection.values()[in.readByte()];

                fields.add(new RelFieldCollation(fieldIndex, direction, nullDirection));
            }
            collations.add(RelCollationTraitDef.INSTANCE.canonize(RelCollations.of(fields)));
        }

        this.collations = collations;
    }
}
