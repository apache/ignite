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

package org.apache.ignite.math.impls;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import java.util.*;

/**
 * Distribution-related misc. support.
 */
public class DistributionSupport {
    /**
     * Gets local Ignite instance.
     */
    protected Ignite ignite() {
        return Ignition.localIgnite();
    }

    /**
     *
     * @param cacheName
     * @param run
     */
    protected void broadcastForCache(String cacheName, IgniteRunnable run) {
        ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(run);
    }

    /**
     *
     * @param cacheName
     * @return
     */
    protected int partitions(String cacheName) {
        return ignite().affinity(cacheName).partitions();
    }

    /**
     * 
     * @param cacheName
     * @param call
     * @param <A>
     * @return
     */
    protected <A> Collection<A> broadcastForCache(String cacheName, IgniteCallable<A> call) {
        return ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(call);
    }
}
