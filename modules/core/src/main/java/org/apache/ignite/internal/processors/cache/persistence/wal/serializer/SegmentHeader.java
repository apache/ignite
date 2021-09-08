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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * WAL segment header info.
 */
public class SegmentHeader {
    /** Serializer version. */
    private int serializerVersion;

    /** Compacted flag. */
    private boolean isCompacted;

    /**
     * @param serializerVersion Serializer version.
     * @param isCompacted Compacted flag.
     */
    public SegmentHeader(int serializerVersion, boolean isCompacted) {
        this.serializerVersion = serializerVersion;
        this.isCompacted = isCompacted;
    }

    /**
     * @return Record serializer version.
     */
    public int getSerializerVersion() {
        return serializerVersion;
    }

    /**
     * @return Comacted flag.
     */
    public boolean isCompacted() {
        return isCompacted;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SegmentHeader.class, this);
    }
}
