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

package org.apache.ignite.internal.processors.security;

import java.security.Permissions;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Common class for security tests.
 */
public class AbstractSecurityTest extends GridCommonAbstractTest {
    /** Empty array of permissions. */
    protected static final SecurityPermission[] EMPTY_PERMS = new SecurityPermission[0];

    /** Global authentication flag. */
    protected boolean globalAuth;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param instanceName Instance name.
     * @param pluginProv Security plugin provider.
     */
    protected IgniteConfiguration getConfiguration(String instanceName,
        AbstractTestSecurityPluginProvider pluginProv) throws Exception {

        return getConfiguration(instanceName)
            .setPluginProviders(pluginProv);
    }

    /** */
    protected IgniteEx startGridAllowAll(String login) throws Exception {
        return startGrid(login, ALLOW_ALL, false);
    }

    /** */
    protected IgniteEx startClientAllowAll(String login) throws Exception {
        return startGrid(login, ALLOW_ALL, true);
    }

    /**
     * @param login Login.
     * @param prmSet Security permission set.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(String login, SecurityPermissionSet prmSet, boolean isClient) throws Exception {
        return startGrid(login, prmSet, null, isClient);
    }

    /** */
    protected IgniteEx startGrid(String login, SecurityPermissionSet prmSet,
        Permissions sandboxPerms, boolean isClient) throws Exception {
        return startGrid(getConfiguration(login,
            new TestSecurityPluginProvider(login, "", prmSet, sandboxPerms, globalAuth))
            .setClientMode(isClient));
    }

    /** */
    protected static class TestFutureAdapter<T> implements Future<T> {
        /** */
        private final IgniteFuture<T> igniteFut;

        /** */
        public TestFutureAdapter(IgniteFuture<T> igniteFut) {
            this.igniteFut = igniteFut;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return igniteFut.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return igniteFut.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return igniteFut.isDone();
        }

        /** {@inheritDoc} */
        @Override public T get() throws InterruptedException, ExecutionException {
            return igniteFut.get();
        }

        /** {@inheritDoc} */
        @Override public T get(long timeout,
            @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return igniteFut.get(timeout, unit);
        }
    }

    /** */
    protected static class TestStoreFactory implements Factory<TestCacheStore> {
        /** */
        private final T2<Object, Object> keyVal;

        /** */
        public TestStoreFactory(Object key, Object val) {
            keyVal = new T2<>(key, val);
        }

        /** {@inheritDoc} */
        @Override public TestCacheStore create() {
            return new TestCacheStore(keyVal);
        }
    }

    /** */
    private static class TestCacheStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private final T2<Object, Object> keyVal;

        /** Constructor. */
        public TestCacheStore(T2<Object, Object> keyVal) {
            this.keyVal = keyVal;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            clo.apply(keyVal.getKey(), keyVal.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
