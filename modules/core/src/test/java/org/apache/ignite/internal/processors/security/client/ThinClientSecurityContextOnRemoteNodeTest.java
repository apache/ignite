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

package org.apache.ignite.internal.processors.security.client;

import org.apache.ignite.configuration.CacheConfiguration;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Runs operations of a thin client to check that thin client security context is
 * available on local and remote nodes.
 */
public class ThinClientSecurityContextOnRemoteNodeTest extends ThinClientPermissionCheckTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration<?, ?>[] cacheConfigurations() {
        CacheConfiguration<?, ?>[] ccfgs = super.cacheConfigurations();

        for (CacheConfiguration<?, ?> ccfg : ccfgs)
            ccfg.setCacheMode(REPLICATED);

        return ccfgs;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getConfiguration(0));

        super.beforeTestsStarted();
    }
}
