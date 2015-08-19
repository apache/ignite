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

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.marshaller.portable.*;

import java.util.*;

/**
 *
 */
public class GridCacheOffHeapTieredPortableSelfTest extends GridCacheOffHeapTieredSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean portableEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        // Enable portables.
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(TestValue.class.getName()));

        cfg.setMarshaller(marsh);

        return cfg;
    }
}
