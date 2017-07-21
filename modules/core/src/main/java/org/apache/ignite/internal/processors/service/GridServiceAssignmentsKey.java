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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Service configuration key.
 */
public class GridServiceAssignmentsKey extends GridCacheUtilityKey<GridServiceAssignmentsKey> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private final String name;

    /**
     * @param name Service ID.
     */
    public GridServiceAssignmentsKey(String name) {
        assert name != null;

        this.name = name;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override protected boolean equalsx(GridServiceAssignmentsKey that) {
        return name.equals(that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceAssignmentsKey.class, this);
    }
}