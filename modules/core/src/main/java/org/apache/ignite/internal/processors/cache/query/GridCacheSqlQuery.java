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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Query.
 */
public class GridCacheSqlQuery implements Externalizable {
    /** */
    private static final Object[] EMPTY_PARAMS = {};

    /** */
    String alias;

    /** */
    @GridToStringInclude
    String qry;

    /** */
    @GridToStringInclude
    Object[] params;

    /**
     * For {@link Externalizable}.
     */
    public GridCacheSqlQuery() {
        // No-op.
    }

    /**
     * @param alias Alias.
     * @param qry Query.
     * @param params Query parameters.
     */
    GridCacheSqlQuery(String alias, String qry, Object[] params) {
        A.ensure(!F.isEmpty(qry), "qry must not be empty");

        this.alias = alias;
        this.qry = qry;

        this.params = F.isEmpty(params) ? EMPTY_PARAMS : params;
    }

    /**
     * @return Alias.
     */
    public String alias() {
        return alias;
    }

    /**
     * @return Query.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, alias);
        U.writeString(out, qry);
        U.writeArray(out, params);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        alias = U.readString(in);
        qry = U.readString(in);
        params = U.readArray(in);

        if (F.isEmpty(params))
            params = EMPTY_PARAMS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSqlQuery.class, this);
    }
}
