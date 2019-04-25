/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests.p2p;

import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * Test store session listener factory for cache deployment tests.
 */
public class CacheDeploymentStoreSessionListenerFactory implements Factory<CacheStoreSessionListener> {
    /** */
    private String name;

    /**
     *
     */
    public CacheDeploymentStoreSessionListenerFactory() {
    }

    /**
     * @param name Name.
     */
    public CacheDeploymentStoreSessionListenerFactory(String name) {
        this.name = name;
    }

    @Override public CacheStoreSessionListener create() {
        return new CacheDeploymentSessionListener(name);
    }

    /**
     */
    private static class CacheDeploymentSessionListener implements CacheStoreSessionListener, LifecycleAware {
        /** */
        private final String name;

        /**
         * @param name Name.
         */
        private CacheDeploymentSessionListener(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {

        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {

        }

        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {

        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {

        }
    }
}