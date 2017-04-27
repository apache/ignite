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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Schema key.
 */
public class SchemaKey {
    /** Space. */
    private final String space;

    /** Deployment ID. */
    private final IgniteUuid depId;

    /**
     * Constructor.
     *
     * @param space Space.
     * @param depId Deployment ID.
     */
    public SchemaKey(String space, IgniteUuid depId) {
        this.space = space;
        this.depId = depId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (space != null ? space.hashCode() : 0) + depId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj instanceof SchemaKey) {
            SchemaKey other = (SchemaKey)obj;

            return F.eq(space, other.space) && F.eq(depId, other.depId);
        }

        return false;
    }
}
