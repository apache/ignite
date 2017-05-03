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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Execute cache query request.
 */
public class GridClientCacheQueryExecuteRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** SQL statement */
    private String sql;

    /** Arguments of the SQL statement. */
    private Object[] args;

    /** Page size. */
    private int pageSize;

    /** Page size. */
    private boolean distributedJoins;

    /**
     * Default constructor needs for Externalizable.
     */
    public GridClientCacheQueryExecuteRequest() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     * @param sql SQL statement.
     * @param args Parameters of the SQL statements.
     * @param pageSize Page size.
     * @param distributedJoins Distributed joins enabled.
     */
    public GridClientCacheQueryExecuteRequest(String cacheName, String sql, Object[] args, int pageSize,
        boolean distributedJoins) {
        this.cacheName = cacheName;
        this.sql = sql;
        this.args = args;
        this.pageSize = pageSize;
        this.distributedJoins = distributedJoins;
    }

    /**
     * Gets cache name.
     *
     * @return Cache name, or {@code null} if not set.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Gets cache name.
     *
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return SQL statement
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Arguments of the SQL statement.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Page size.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeString(out, cacheName);
        U.writeString(out, sql);
        U.writeArray(out, args);

        out.writeInt(pageSize);
        out.writeBoolean(distributedJoins);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        cacheName = U.readString(in);
        sql = U.readString(in);
        args = U.readArray(in);

        pageSize = in.readInt();
        distributedJoins = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientCacheQueryExecuteRequest.class, this);
    }
}