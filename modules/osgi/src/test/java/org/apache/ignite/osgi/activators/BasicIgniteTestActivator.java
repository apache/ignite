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

package org.apache.ignite.osgi.activators;

import java.util.Hashtable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.osgi.IgniteAbstractOsgiContextActivator;
import org.apache.ignite.osgi.classloaders.OsgiClassLoadingStrategyType;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.BundleContext;

/**
 * Basic Ignite Activator for testing.
 */
public class BasicIgniteTestActivator extends IgniteAbstractOsgiContextActivator {
    /** Flags to report our state to a watcher. */
    private TestOsgiFlagsImpl flags = new TestOsgiFlagsImpl();

    /**
     * @return Ignite config.
     */
    @Override public IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration config = new IgniteConfiguration();

        config.setIgniteInstanceName("testGrid");

        return config;
    }

    /**
     * @return Strategy.
     */
    @Override public OsgiClassLoadingStrategyType classLoadingStrategy() {
        return OsgiClassLoadingStrategyType.BUNDLE_DELEGATING;
    }

    /** {@inheritDoc} */
    @Override protected void onBeforeStart(BundleContext ctx) {
        flags.onBeforeStartInvoked = Boolean.TRUE;

        // Export the flags as an OSGi service.
        ctx.registerService(TestOsgiFlags.class, flags, new Hashtable<String, Object>());
    }

    /** {@inheritDoc} */
    @Override protected void onAfterStart(BundleContext ctx, @Nullable Throwable t) {
        flags.onAfterStartInvoked = Boolean.TRUE;
        flags.onAfterStartThrowable = t;
    }

    /** {@inheritDoc} */
    @Override protected void onBeforeStop(BundleContext ctx) {
        flags.onBeforeStopInvoked = Boolean.TRUE;
    }

    /** {@inheritDoc} */
    @Override protected void onAfterStop(BundleContext ctx, @Nullable Throwable t) {
        flags.onAfterStopInvoked = Boolean.TRUE;
        flags.onAfterStopThrowable = t;
    }
}
