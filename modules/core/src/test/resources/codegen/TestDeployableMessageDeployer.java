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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.TestDeployableMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestDeployableMessageDeployer implements GridCacheMessageDeployer<TestDeployableMessage> {
    /** */
    @Override public void prepareDeployment(TestDeployableMessage msg, GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        GridCacheContext<?, ?> cctx = ctx.cacheContext(msg.cacheId());

        GridCacheMessageDeployer.prepareCacheObject(msg, msg.key, cctx);

        msg.prepareDeployment(ctx);
    }
}