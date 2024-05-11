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

package org.apache.ignite.osgi;

import java.util.Dictionary;
import java.util.Hashtable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.osgi.classloaders.BundleDelegatingClassLoader;
import org.apache.ignite.osgi.classloaders.ContainerSweepClassLoader;
import org.apache.ignite.osgi.classloaders.OsgiClassLoadingStrategyType;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * This {@link BundleActivator} starts Apache Ignite inside the OSGi container when the bundle is started.
 * <p>
 * Create an implementation of this class and set the {@code Bundle-Activator} OSGi Manifest header to the FQN of
 * your class.
 * <p>
 * You must provide the {@link IgniteConfiguration} to start by implementing the {@link #igniteConfiguration()}
 * abstract method. The return value of this method cannot be {@code null}. For example, if your implementation is
 * called {@code org.myorg.osgi.IgniteActivator}, your bundle must provide the following header:
 * <pre>
 * Bundle-Activator: org.myorg.osgi.IgniteActivator
 * </pre>
 * You may use the
 * <a href="https://felix.apache.org/documentation/subprojects/apache-felix-maven-bundle-plugin-bnd.html">Maven
 * Bundle Plugin</a> to generate your bundle (or bundle manifest), including the required header.
 * <p>
 * This activator also exports the Ignite instance as an OSGi service, with the property {@code ignite.name} set
 * to the value of {@link Ignite#name()}, if and only if the name is not null.
 * <p>
 * Currently, Ignite only allows a single instance per container. We may remove this limitation if enough demand
 * builds up in the community.
 *
 * @see <a href="http://wiki.osgi.org/wiki/Bundle-Activator">Bundle-Activator OSGi Manifest header</a>
 *
 */
public abstract class IgniteAbstractOsgiContextActivator implements BundleActivator {
    /** OSGI service property name. */
    public static final String OSGI_SERVICE_PROP_IGNITE_NAME = "ignite.name";

    /** The instance of Ignite started by this Activator. */
    protected Ignite ignite;

    /** Our bundle context. */
    private BundleContext bundleCtx;

    /** Ignite logger. */
    private IgniteLogger log;

    /**
     * Method invoked by OSGi to start the bundle. It starts the specified Ignite configuration.
     *
     * @param ctx Bundle context.
     * @throws Exception
     */
    @Override public final void start(BundleContext ctx) throws Exception {
        // Ensure that no other instances are running.
        if (IgniteOsgiUtils.gridCount() > 0) {
            throw new IgniteException("Failed to start Ignite instance (another instance is already running " +
                "and ignite-osgi is currently limited to a single instance per container).");
        }

        bundleCtx = ctx;

        // Start the Ignite configuration specified by the user.
        IgniteConfiguration cfg = igniteConfiguration();

        A.notNull(cfg, "Ignite configuration");

        // Override the classloader with the classloading strategy chosen by the user.
        ClassLoader clsLdr;

        if (classLoadingStrategy() == OsgiClassLoadingStrategyType.BUNDLE_DELEGATING)
            clsLdr = new BundleDelegatingClassLoader(bundleCtx.getBundle(), Ignite.class.getClassLoader());
        else
            clsLdr = new ContainerSweepClassLoader(bundleCtx.getBundle(), Ignite.class.getClassLoader());

        cfg.setClassLoader(clsLdr);

        onBeforeStart(ctx);

        // Start Ignite.
        try {
            ignite = Ignition.start(cfg);
        }
        catch (Throwable t) {
            U.error(log, "Failed to start Ignite via OSGi Activator [errMsg=" + t.getMessage() + ']', t);

            onAfterStart(ctx, t);

            return;
        }

        log = ignite.log();

        if (log.isInfoEnabled())
            log.info("Started Ignite from OSGi Activator [name=" + ignite.name() + ']');

        // Add into Ignite's OSGi registry.
        IgniteOsgiUtils.classloaders().put(ignite, clsLdr);

        // Export Ignite as a service.
        exportOsgiService(ignite);

        onAfterStart(ctx, null);
    }

    /**
     * Stops Ignite when the bundle is stopping.
     *
     * @param ctx Bundle context.
     * @throws Exception If failed.
     */
    @Override public final void stop(BundleContext ctx) throws Exception {
        onBeforeStop(ctx);

        try {
            ignite.close();
        }
        catch (Throwable t) {
            U.error(log, "Failed to stop Ignite via OSGi Activator [errMsg=" + t.getMessage() + ']', t);

            onAfterStop(ctx, t);

            return;
        }

        if (log.isInfoEnabled())
            log.info("Stopped Ignite from OSGi Activator [name=" + ignite.name() + ']');

        IgniteOsgiUtils.classloaders().remove(ignite);

        onAfterStop(ctx, null);
    }

    /**
     * This method is called before Ignite initialises.
     * <p>
     * The default implementation is empty. Override it to introduce custom logic.
     *
     * @param ctx The {@link BundleContext}.
     */
    protected void onBeforeStart(BundleContext ctx) {
        // No-op.
    }

    /**
     * This method is called after Ignite initialises, only if initialization succeeded.
     * <p>
     * The default implementation is empty. Override it to introduce custom logic.
     *
     * @param ctx The {@link BundleContext}.
     * @param t Throwable in case an error occurred when starting. {@code null} otherwise.
     */
    protected void onAfterStart(BundleContext ctx, @Nullable Throwable t) {
        // No-op.
    }

    /**
     * This method is called before Ignite stops.
     * <p>
     * The default implementation is empty. Override it to introduce custom logic.
     *
     * @param ctx The {@link BundleContext}.
     */
    protected void onBeforeStop(BundleContext ctx) {
        // No-op.
    }

    /**
     * This method is called after Ignite stops, only if the operation succeeded.
     * <p>
     * The default implementation is empty. Override it to introduce custom logic.
     *
     * @param ctx The {@link BundleContext}.
     * @param t Throwable in case an error occurred when stopping. {@code null} otherwise.
     */
    protected void onAfterStop(BundleContext ctx, @Nullable Throwable t) {
        // No-op.
    }

    /**
     * Override this method to provide the Ignite configuration this bundle will start.
     *
     * @return The Ignite configuration.
     */
    public abstract IgniteConfiguration igniteConfiguration();

    /**
     * Override this method to indicate which classloading strategy to use.
     *
     * @return The strategy.
     */
    public OsgiClassLoadingStrategyType classLoadingStrategy() {
        return OsgiClassLoadingStrategyType.BUNDLE_DELEGATING;
    }

    /**
     * Exports the Ignite instance onto the OSGi Service Registry.
     *
     * @param ignite Ignite.
     */
    private void exportOsgiService(Ignite ignite) {
        Dictionary<String, String> dict = new Hashtable<>();

        // Only add the service property if the Ignite instance name != null.
        if (ignite.name() != null)
            dict.put(OSGI_SERVICE_PROP_IGNITE_NAME, ignite.name());

        bundleCtx.registerService(Ignite.class, ignite, dict);

        if (log.isInfoEnabled())
            log.info("Exported OSGi service for Ignite with properties: " + dict);
    }
}
