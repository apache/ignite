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

package org.apache.ignite.hadoop.util;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Chained user name mapper. Delegate name conversion to child mappers.
 */
public class ChainedUserNameMapper implements UserNameMapper, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Child mappers. */
    private UserNameMapper[] mappers;

    /** {@inheritDoc} */
    @Nullable @Override public String map(String name) {
        for (UserNameMapper mapper : mappers)
            name = mapper.map(name);

        return name;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (mappers == null)
            throw new IgniteException("Mappers cannot be null.");

        for (int i = 0; i < mappers.length; i++) {
            if (mappers[i] == null)
                throw new IgniteException("Mapper cannot be null [index=" + i + ']');
        }

        for (UserNameMapper mapper : mappers) {
            if (mapper instanceof LifecycleAware)
                ((LifecycleAware)mapper).start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        assert mappers != null;

        for (UserNameMapper mapper : mappers) {
            if (mapper instanceof LifecycleAware)
                ((LifecycleAware)mapper).stop();
        }
    }

    /**
     * Get child mappers.
     *
     * @return Child mappers.
     */
    public UserNameMapper[] getMappers() {
        return mappers;
    }

    /**
     * Set child mappers.
     *
     * @param mappers Child mappers.
     */
    public void setMappers(UserNameMapper... mappers) {
        this.mappers = mappers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChainedUserNameMapper.class, this,
            "mappers", mappers != null ? Arrays.toString(mappers) : null);
    }
}
