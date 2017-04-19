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

package org.apache.ignite.internal.processors.platform;

import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.ServiceLoader;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for platform nodes.
 */
@SuppressWarnings("UnusedDeclaration")
public class PlatformIgnition {
    /** Map with active instances. */
    private static final HashMap<String, PlatformProcessor> instances = new HashMap<>();

    /**
     * Start Ignite node in platform mode.
     *
     * @param springCfgPath Spring configuration path.
     * @param igniteInstanceName Ignite instance name.
     * @param factoryId Factory ID.
     * @param envPtr Environment pointer.
     * @param dataPtr Optional pointer to additional data required for startup.
     * @return Ignite instance.
     */
    public static synchronized PlatformProcessor start(@Nullable String springCfgPath,
        @Nullable String igniteInstanceName, int factoryId, long envPtr, long dataPtr) {
        if (envPtr <= 0)
            throw new IgniteException("Environment pointer must be positive.");

        ClassLoader oldClsLdr = Thread.currentThread().getContextClassLoader();

        Thread.currentThread().setContextClassLoader(PlatformProcessor.class.getClassLoader());

        try {
            PlatformBootstrap bootstrap = bootstrap(factoryId);

            // This should be done before Spring XML initialization so that redirected stream is picked up.
            bootstrap.init();

            IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> cfg = configuration(springCfgPath);

            if (igniteInstanceName != null)
                cfg.get1().setIgniteInstanceName(igniteInstanceName);
            else
                igniteInstanceName = cfg.get1().getIgniteInstanceName();

            PlatformProcessor proc = bootstrap.start(cfg.get1(), cfg.get2(), envPtr, dataPtr);

            PlatformProcessor old = instances.put(igniteInstanceName, proc);

            assert old == null;

            return proc;
        }
        finally {
            Thread.currentThread().setContextClassLoader(oldClsLdr);
        }
    }

    /**
     * Get instance by environment pointer.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Instance or {@code null} if it doesn't exist (never started or stopped).
     */
    @Nullable public static synchronized PlatformProcessor instance(@Nullable String igniteInstanceName) {
        return instances.get(igniteInstanceName);
    }

    /**
     * Get environment pointer of the given instance.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Environment pointer or {@code 0} in case grid with such name doesn't exist.
     */
    public static synchronized long environmentPointer(@Nullable String igniteInstanceName) {
        PlatformProcessor proc = instance(igniteInstanceName);

        return proc != null ? proc.environmentPointer() : 0;
    }

    /**
     * Stop single instance.
     *
     * @param igniteInstanceName Ignite instance name,
     * @param cancel Cancel flag.
     * @return {@code True} if instance was found and stopped.
     */
    public static synchronized boolean stop(@Nullable String igniteInstanceName, boolean cancel) {
        if (Ignition.stop(igniteInstanceName, cancel)) {
            PlatformProcessor old = instances.remove(igniteInstanceName);

            assert old != null;

            return true;
        }
        else
            return false;
    }

    /**
     * Stop all instances.
     *
     * @param cancel Cancel flag.
     */
    public static synchronized void stopAll(boolean cancel) {
        for (PlatformProcessor proc : instances.values())
            Ignition.stop(proc.ignite().name(), cancel);

        instances.clear();
    }

    /**
     * Create configuration.
     *
     * @param springCfgPath Path to Spring XML.
     * @return Configuration.
     */
    private static IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> configuration(
        @Nullable String springCfgPath) {
        if (springCfgPath == null)
            return new T2<>(new IgniteConfiguration(), null);

        try {
            URL url = U.resolveSpringUrl(springCfgPath);

            return IgnitionEx.loadConfiguration(url);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to instantiate configuration from Spring XML: " + springCfgPath, e);
        }
    }

    /**
     * Create bootstrap for the given factory ID.
     *
     * @param factoryId Factory ID.
     * @return Bootstrap.
     */
    private static PlatformBootstrap bootstrap(final int factoryId) {
        PlatformBootstrapFactory factory = AccessController.doPrivileged(
            new PrivilegedAction<PlatformBootstrapFactory>() {
                @Override public PlatformBootstrapFactory run() {
                    for (PlatformBootstrapFactory factory : ServiceLoader.load(PlatformBootstrapFactory.class)) {
                        if (factory.id() == factoryId)
                            return factory;
                    }

                    return null;
                }
            });

        if (factory == null)
            throw new IgniteException("Interop factory is not found (did you put into the classpath?): " + factoryId);

        return factory.create();
    }

    /**
     * Private constructor.
     */
    private PlatformIgnition() {
        // No-op.
    }
}