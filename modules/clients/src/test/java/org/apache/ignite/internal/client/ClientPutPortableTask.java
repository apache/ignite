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

package org.apache.ignite.internal.client;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.resources.*;

/**
 * Task creates portable object and puts it in cache.
 */
public class ClientPutPortableTask extends TaskSingleJobSplitAdapter {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, Object arg) throws IgniteCheckedException {
        String cacheName = (String)arg;

        GridCache<Object, Object> cache = ignite.cache(cacheName);

        ClientTestPortable p = new ClientTestPortable(100, true);

        cache.put(1, p);

        return true;
    }
}
