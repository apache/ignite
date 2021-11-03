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

package org.apache.ignite.client.proto.query.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query fetch result.
 */
public class QueryFetchResult extends Response {
    /** Query result rows. */
    private List<List<Object>> items;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /**
     * Default constructor is used for deserialization.
     */
    public QueryFetchResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public QueryFetchResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param items Query result rows.
     * @param last  Flag indicating the query has no unfetched results.
     */
    public QueryFetchResult(List<List<Object>> items, boolean last) {
        Objects.requireNonNull(items);

        this.items = items;
        this.last = last;

        hasResults = true;
    }

    /**
     * Get the result rows.
     *
     * @return Query result rows.
     */
    public List<List<Object>> items() {
        return items;
    }

    /**
     * Get the last flag.
     *
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        packer.packBoolean(last);

        packer.packArrayHeader(items.size());

        for (List<Object> item : items) {
            packer.packObjectArray(item.toArray());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        last = unpacker.unpackBoolean();

        int size = unpacker.unpackArrayHeader();

        items = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            items.add(Arrays.asList(unpacker.unpackObjectArray()));
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(QueryFetchResult.class, this);
    }
}
