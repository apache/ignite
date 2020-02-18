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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Container for skipped entries with their reason.
 */
public class PartitionReconciliationSkippedEntityHolder<T> extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Skipped entity. */
    private T skippedEntity;

    /** Skipping reason. */
    private SkippingReason skippingReason;

    /**
     * Default constructor.
     */
    public PartitionReconciliationSkippedEntityHolder() {
    }

    /**
     * @param skippedEntity Skipped entity.
     * @param skippingReason Skipping reason.
     */
    public PartitionReconciliationSkippedEntityHolder(T skippedEntity,
        SkippingReason skippingReason) {
        this.skippedEntity = skippedEntity;
        this.skippingReason = skippingReason;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(skippedEntity);
        U.writeEnum(out, skippingReason);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        skippedEntity = (T)in.readObject();

        skippingReason = SkippingReason.fromOrdinal(in.readByte());
    }

    /**
     * @return Skipped entity.
     */
    public T skippedEntity() {
        return skippedEntity;
    }

    /**
     * @param skippedEntity New skipped entity.
     */
    public void skippedEntity(T skippedEntity) {
        this.skippedEntity = skippedEntity;
    }

    /**
     * @return Skipping reason.
     */
    public SkippingReason skippingReason() {
        return skippingReason;
    }

    /**
     * @param skippingReason New skipping reason.
     */
    public void skippingReason(SkippingReason skippingReason) {
        this.skippingReason = skippingReason;
    }

    /**
     * Enumerates possible reasons for skipping entries.
     */
    public enum SkippingReason {
        /**
         *
         */
        ENTITY_WITH_TTL("Given entity has ttl enabled."),

        /**
         *
         */
        KEY_WAS_NOT_REPAIRED("Key was not repaired. Repair attempts were over.");

        /** Reason. */
        private String reason;

        /**
         * @param reason Reason.
         */
        SkippingReason(String reason) {
            this.reason = reason;
        }

        /**
         * @return Reason.
         */
        public String reason() {
            return reason;
        }

        /** Enumerated values. */
        private static final SkippingReason[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        public static @Nullable SkippingReason fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }
}
