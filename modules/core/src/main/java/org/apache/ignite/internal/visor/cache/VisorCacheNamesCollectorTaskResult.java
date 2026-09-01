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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Result for {@link VisorCacheNamesCollectorTask}.
 */
public class VisorCacheNamesCollectorTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache names and deployment IDs. */
    @Order(0)
    Map<String, IgniteUuid> caches;
    
    /** Cache names and Comments. */
    @Order(1)
    Map<String, String> cachesComment;
    
    /** Cache sqlSchemas. */
    @Order(2)
    Map<String, String> sqlSchemas;
    
    /** Cache tableNames. */
    @Order(3)
    Map<String, String> types;

    /**
     * Default constructor.
     */
    public VisorCacheNamesCollectorTaskResult() {
        // No-op.
    }

    /**
     * @param caches Cache names and deployment IDs.
     */
    public VisorCacheNamesCollectorTaskResult(Map<String, IgniteUuid> caches, Map<String, String> cachesComment, Map<String, String> sqlSchemas, Map<String, String> types) {
        this.caches = caches;
        this.cachesComment = cachesComment;
        this.sqlSchemas = sqlSchemas;
        this.types = types;
    }

    /**
     * @return Value for specified key or number of modified rows..
     */
    public Map<String, IgniteUuid> getCaches() {
        return caches;
    }
    
    /**
     * @return Value for specified key or number of modified rows..
     */
    public Map<String, String> getCachesComment() {
        return cachesComment;
    }

    /**
     * @return Value for specified key or number of modified rows..
     */
    public Map<String, String> getSqlSchemas() {
        return sqlSchemas;
    }
    
    /**
     * @return Value for specified key or number of modified rows..
     */
    public Map<String, String> getTypes() {
        return types;
    }


    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeMap(out, caches);
        U.writeMap(out, cachesComment);
        U.writeMap(out, sqlSchemas);
        U.writeMap(out, types);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        caches = U.readMap(in);
        cachesComment = U.readMap(in);
        sqlSchemas = U.readMap(in);
        types = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheNamesCollectorTaskResult.class, this);
    }
}
