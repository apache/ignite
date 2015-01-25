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

package org.apache.ignite;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.management.*;

/**
 * Implementation of {@link javax.cache.management.CacheMXBean}.
 */
public class IgniteCacheMXBean implements CacheMXBean {
    /** */
    private final Cache<?, ?> cache;

    /**
     * @param cache Cache.
     */
    public IgniteCacheMXBean(Cache<?, ?> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return cache.getConfiguration(CompleteConfiguration.class).getKeyType().getName();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return cache.getConfiguration(CompleteConfiguration.class).getValueType().getName();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return cache.getConfiguration(CompleteConfiguration.class).isReadThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return cache.getConfiguration(CompleteConfiguration.class).isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return cache.getConfiguration(CompleteConfiguration.class).isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return cache.getConfiguration(CompleteConfiguration.class).isStatisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return cache.getConfiguration(CompleteConfiguration.class).isManagementEnabled();
    }
}
