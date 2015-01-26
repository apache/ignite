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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import org.apache.ignite.portables.*;

import java.io.Serializable;
import java.util.*;

/**
 * Client query result.
 */
public class GridCacheClientQueryResult implements PortableMarshalAware, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query ID. */
    private long qryId;

    /** Result items. */
    private Collection<?> items;

    /** Last flag. */
    private boolean last;

    /** Node ID. */
    private UUID nodeId;

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     */
    public void queryId(long qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Items.
     */
    public Collection<?> items() {
        return items;
    }

    /**
     * @param items Items.
     */
    public void items(Collection<?> items) {
        this.items = items;
    }

    /**
     * @return Last flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @param last Last flag.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeBoolean(last);
        rawWriter.writeLong(qryId);
        rawWriter.writeUuid(nodeId);
        rawWriter.writeCollection(items);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader rawReader = reader.rawReader();

        last = rawReader.readBoolean();
        qryId = rawReader.readLong();
        nodeId = rawReader.readUuid();
        items = rawReader.readCollection();
    }
}
