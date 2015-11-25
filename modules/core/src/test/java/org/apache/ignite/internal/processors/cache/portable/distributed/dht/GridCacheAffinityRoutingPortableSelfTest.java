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

package org.apache.ignite.internal.processors.cache.portable.distributed.dht;

import java.util.Collections;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.marshaller.portable.BinaryMarshaller;
import org.apache.ignite.binary.BinaryTypeConfiguration;

/**
 *
 */
public class GridCacheAffinityRoutingPortableSelfTest extends GridCacheAffinityRoutingSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();

        typeCfg.setTypeName(AffinityTestKey.class.getName());

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration(AffinityTestKey.class.getName(), "affKey");

        cfg.setCacheKeyCfg(keyCfg);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setTypeConfigurations(Collections.singleton(typeCfg));

        cfg.setBinaryConfiguration(bCfg);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }
}