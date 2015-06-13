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

package org.apache.ignite.spring.autowired;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.resources.*;
import org.springframework.beans.factory.annotation.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Test store.
 */
public class SpringAutowiredTestStore extends CacheStoreAdapter<Integer, Integer> {
    /** */
    public static final Set<UUID> load = new ConcurrentSkipListSet<>();

    /** */
    public static final Set<UUID> write = new ConcurrentSkipListSet<>();

    /** */
    public static final Set<UUID> delete = new ConcurrentSkipListSet<>();

    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @Autowired
    private SpringAutowiredBean bean;

    /** {@inheritDoc} */
    @Override public Integer load(Integer key) throws CacheLoaderException {
        load.add(ignite.cluster().localNode().id());

        assert bean != null;
        assert "test-bean".equals(bean.getName()) : bean.getName();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
        write.add(ignite.cluster().localNode().id());

        assert bean != null;
        assert "test-bean".equals(bean.getName()) : bean.getName();
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        delete.add(ignite.cluster().localNode().id());

        assert bean != null;
        assert "test-bean".equals(bean.getName()) : bean.getName();
    }
}
