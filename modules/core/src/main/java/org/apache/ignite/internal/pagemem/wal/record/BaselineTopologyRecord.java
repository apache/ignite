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

import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Record for storing baseline topology compact node ID to consistent node ID mapping.
 */
public class BaselineTopologyRecord extends WALRecord {
    /** Id. */
    private int id;

    /** Compact ID to consistent ID mapping. */
    private Map<Short, Object> mapping;

    /**
     * Default constructor.
     */
    private BaselineTopologyRecord() {
        // No-op, used from factory methods.
    }

    /**
     * @param id Baseline topology ID.
     * @param mapping Compact ID to consistent ID mapping.
     */
    public BaselineTopologyRecord(int id, Map<Short, Object> mapping) {
        this.id = id;
        this.mapping = mapping;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BASELINE_TOP_RECORD;
    }

    /**
     * Returns baseline topology ID.
     *
     * @return Baseline topology ID.
     */
    public int id() {
        return id;
    }

    /**
     * Returns mapping.
     *
     * @return Compact ID to consistent ID mapping.
     */
    public Map<Short, Object> mapping() {
        return mapping;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineTopologyRecord.class, this);
    }
}
