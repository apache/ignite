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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.processors.rest.client.message.*;

import java.io.*;

/**
 * Cache query request.
 */
public class GridRestCacheQueryRequest extends GridRestRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request message. */
    private GridClientCacheQueryRequest msg;

    /**
     * @param msg Client request message.
     */
    public GridRestCacheQueryRequest(GridClientCacheQueryRequest msg) {
        this.msg = msg;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return msg.queryId();
    }

    /**
     * @return Operation.
     */
    public GridClientCacheQueryRequest.GridQueryOperation operation() {
        return msg.operation();
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return msg.cacheName();
    }

    /**
     * @return Query clause.
     */
    public String clause() {
        return msg.clause();
    }

    /**
     * @return Query type.
     */
    public GridClientCacheQueryRequest.GridQueryType type() {
        return msg.type();
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return msg.pageSize();
    }

    /**
     * @return Class name.
     */
    public String className() {
        return msg.className();
    }


    /**
     * @return Query arguments.
     */
    public Object[] queryArguments() {
        return msg.queryArguments();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return msg.toString();
    }
}
