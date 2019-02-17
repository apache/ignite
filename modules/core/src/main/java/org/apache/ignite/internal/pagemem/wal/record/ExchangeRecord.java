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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition map exchange WAL record.
 */
public class ExchangeRecord extends TimeStampRecord {
    /** Event. */
    private Short constId;

    /** Type. */
    private Type type;

    /**
     * @param constId Const id.
     * @param type Type.
     * @param timeStamp TimeStamp.
     */
    public ExchangeRecord(Short constId, Type type, long timeStamp) {
        super(timeStamp);

        this.constId = constId;
        this.type = type;
    }

    /**
     * @param constId Const id.
     * @param type Type.
     */
    public ExchangeRecord(Short constId, Type type) {
        this.constId = constId;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.EXCHANGE;
    }

    /**
     *
     */
    public Short getConstId() {
        return constId;
    }

    /**
     *
     */
    public Type getType() {
        return type;
    }

    /**
     *
     */
    public enum Type {
        /** Join. */
        JOIN,
        /** Left. */
        LEFT
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeRecord.class, this, "super", super.toString());
    }
}
