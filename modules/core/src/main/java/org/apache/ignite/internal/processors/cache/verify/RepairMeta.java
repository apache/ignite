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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Data container for result of repair.
 */
public class RepairMeta extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Boolean flag that indicates whether data was fixed or not. */
    private boolean fixed;

    /** Value that was used to fix entry. */
    private CacheObject val;

    /** Repair algorithm that was used. */
    private RepairAlgorithm repairAlg;

    /** Previous value per node. */
    private Map<UUID, VersionedValue> previousValue;

    /**
     * Default constructor for externalization.
     */
    public RepairMeta() {
    }

    /**
     * Constructor.
     *
     * @param fixed Boolean flag that indicates whether data was fixed or not.
     * @param val Value that was used to fix entry.
     * @param repairAlg Repair algorithm that was used.
     */
    public RepairMeta(
        boolean fixed,
        CacheObject val,
        RepairAlgorithm repairAlg,
        Map<UUID, VersionedValue> previousValue
    ) {
        this.fixed = fixed;
        this.val = val;
        this.repairAlg = repairAlg;
        this.previousValue = previousValue;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(fixed);
        out.writeObject(val);
        U.writeEnum(out, repairAlg);
        U.writeMap(out, previousValue);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        fixed = in.readBoolean();
        val = (CacheObject)in.readObject();
        repairAlg = RepairAlgorithm.fromOrdinal(in.readByte());
        previousValue = U.readMap(in);
    }

    /**
     * @return Previous value, before fix.
     */
    public Map<UUID, VersionedValue> getPreviousValue() {
        return previousValue;
    }

    /**
     * @return Boolean flag that indicates whether data was fixed or not.
     */
    public boolean fixed() {
        return fixed;
    }

    /**
     * @return Value that was used to fix entry.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Repair algorithm that was used.
     */
    public RepairAlgorithm repairAlg() {
        return repairAlg;
    }
}
