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
 *
 */

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.typedef.internal.S;

public class SqlGlobalQueryId {
    /** Node order id. */
    private int nodeOrderId;
    /** Node query id. */
    private long nodeQryId;

    /**
     * @param nodeQryId Node query id.
     * @param nodeOrderId Node order id.
     */
    public SqlGlobalQueryId(int nodeOrderId, long nodeQryId) {
        this.nodeOrderId = nodeOrderId;
        this.nodeQryId = nodeQryId;
    }

    /**
     * @return Node order id.
     */
    public int nodeOrderId() {
        return nodeOrderId;
    }

    /**
     * @param nodeOrderId Node order id.
     */
    public void nodeOrderId(int nodeOrderId) {
        this.nodeOrderId = nodeOrderId;
    }

    /**
     * @return Node query id.
     */
    public long nodeQryId() {
        return nodeQryId;
    }

    /**
     * @param nodeQryId Node query id.
     */
    public void nodeQryId(long nodeQryId) {
        this.nodeQryId = nodeQryId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlGlobalQueryId.class, this);
    }
}
