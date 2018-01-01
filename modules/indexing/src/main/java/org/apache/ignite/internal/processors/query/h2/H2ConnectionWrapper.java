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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;

/**
 * Wrapper to store connection and flag is schema set or not.
 */
public class H2ConnectionWrapper {
    /** */
    private Connection conn;

    /** */
    private volatile String schema;

    /**
     * @param conn Connection to use.
     */
    H2ConnectionWrapper(Connection conn) {
        this.conn = conn;
    }

    /**
     * @return Schema name if schema is set, null otherwise.
     */
    public String schema() {
        return schema;
    }

    /**
     * @param schema Schema name set on this connection.
     */
    public void schema(@Nullable String schema) {
        this.schema = schema;
    }

    /**
     * @return Connection.
     */
    public Connection connection() {
        return conn;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ConnectionWrapper.class, this);
    }
}
