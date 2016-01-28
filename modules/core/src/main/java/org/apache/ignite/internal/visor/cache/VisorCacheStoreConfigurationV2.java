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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 * Data transfer object for cache store configuration properties.
 */
public class VisorCacheStoreConfigurationV2 extends VisorCacheStoreConfiguration {
    /** */
    private static final long serialVersionUID = 0L;

    /** Keep binary in store flag. */
    private boolean storeKeepBinary;

    /** {@inheritDoc} */
    @Override public VisorCacheStoreConfiguration from(IgniteEx ignite, CacheConfiguration ccfg) {
        super.from(ignite, ccfg);

        storeKeepBinary = ccfg.isStoreKeepBinary();

        return this;
    }

    /**
     * @return Keep binary in store flag.
     */
    public boolean storeKeepBinary() {
        return storeKeepBinary;
    }
}
