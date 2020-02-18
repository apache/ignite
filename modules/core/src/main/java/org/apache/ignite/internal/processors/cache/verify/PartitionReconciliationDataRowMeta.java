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

package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Data row meta including information about key, value and repair meta within the context of partition reconciliation.
 */
public class PartitionReconciliationDataRowMeta extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Binary and string representation of a versioned key. */
    private PartitionReconciliationKeyMeta keyMeta;

    /** Binary and string representation of a values per corresponding node ids. */
    private Map<UUID, PartitionReconciliationValueMeta> valMeta;

    /** Binary and string representation of a value. */

    /**
     * Repair meta including:
     * <ul>
     * <li>boolean flag that indicates whether data was fixed or not;</li>
     * <li>value that was used to fix entry;</li>
     * <li>repair algorithm that was used;</li>
     * </ul>
     */
    private PartitionReconciliationRepairMeta repairMeta;

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationDataRowMeta() {
    }

    /**
     * Constructor.
     *
     * @param keyMeta Binary and string representation of a versioned key.
     * @param valMeta Binary and string representation of a value.
     */
    public PartitionReconciliationDataRowMeta(
        PartitionReconciliationKeyMeta keyMeta,
        Map<UUID, PartitionReconciliationValueMeta> valMeta) {
        this.keyMeta = keyMeta;
        this.valMeta = valMeta;
    }

    /**
     * Constructor.
     *
     * @param keyMeta Binary and string representation of a versioned key.
     * @param valMeta Binary and string representation of a value.
     * @param repairMeta Repair meta including:
     * <ul>
     * <li>boolean flag that indicates whether data was fixed or not;</li>
     * <li>value that was used to fix entry;</li>
     * <li>repair algorithm that was used;</li>
     * </ul>
     */
    public PartitionReconciliationDataRowMeta(
        PartitionReconciliationKeyMeta keyMeta,
        Map<UUID, PartitionReconciliationValueMeta> valMeta,
        PartitionReconciliationRepairMeta repairMeta) {
        this.keyMeta = keyMeta;
        this.valMeta = valMeta;
        this.repairMeta = repairMeta;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(keyMeta);
        U.writeMap(out, valMeta);
        out.writeObject(repairMeta);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        keyMeta = (PartitionReconciliationKeyMeta)in.readObject();
        valMeta = U.readMap(in);
        repairMeta = (PartitionReconciliationRepairMeta)in.readObject();
    }

    /**
     * @return Binary and string representation of a versioned key.
     */
    public PartitionReconciliationKeyMeta keyMeta() {
        return keyMeta;
    }

    /**
     * @return Binary and string representation of a values per corresponding node ids.
     */
    public Map<UUID, PartitionReconciliationValueMeta> valueMeta() {
        return valMeta;
    }

    /**
     * @return Repair meta including: <ul> <li>boolean flag that indicates whether data was fixed or not;</li> <li>value
     * that was used to fix entry;</li> <li>repair algorithm that was used;</li> </ul>
     */
    public PartitionReconciliationRepairMeta repairMeta() {
        return repairMeta;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionReconciliationDataRowMeta.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionReconciliationDataRowMeta meta = (PartitionReconciliationDataRowMeta)o;

        if (!Objects.equals(keyMeta, meta.keyMeta))
            return false;
        if (!Objects.equals(valMeta, meta.valMeta))
            return false;
        return Objects.equals(repairMeta, meta.repairMeta);
    }
}
