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
import org.apache.ignite.osgi.classloaders.BundleDelegatingClassLoader;
import org.apache.ignite.osgi.classloaders.ContainerSweepClassLoader;
import org.apache.ignite.osgi.classloaders.OsgiClassLoadingStrategyType;

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
 *
 * <pre>
 *     Bundle-Activator: org.myorg.osgi.IgniteActivator
 * </pre>
 *
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
 * @see <a href="http://wiki.osgi.org/wiki/Bundle-Activator">Bundle-Activator OSGi Manifest header</a>.
 *
 */
public abstract class IgniteOsgiContextActivator implements BundleActivator {

    /** OSGI service property name. */
    public static final String OSGI_SERVICE_PROP_IGNITE_NAME = "ignite.name";

    /** The instance of Ignite started by this Activator. */
    protected Ignite ignite;

    /** Our bundle context. */
    private BundleContext bundleContext;

    /** Ignite logger. */
    private IgniteLogger logger;

    /**
     * Method invoked by OSGi to start the bundle. It starts the specified Ignite configuration.
     *
     * @param context Bundle context.
     * @throws Exception
     */
    @Override public void start(BundleContext context) throws Exception {
        // Ensure that no other instances are running.
        if (IgniteOsgiUtils.gridCount() > 0) {
            throw new IgniteException("Failed to start Ignite instance (another instance is already running " +
                "and ignite-osgi is currently limited to a single instance per container).");
        }

        bundleContext = context;

        // Start the Ignite configuration specified by the user.
        IgniteConfiguration config = igniteConfiguration();

        A.notNull(config, "Ignite configuration cannot be null");

        // Override the classloader with the classloading strategy chosen by the user.
        ClassLoader clsLdr;

        if (classLoadingStrategy() == OsgiClassLoadingStrategyType.BUNDLE_DELEGATING)
            clsLdr = new BundleDelegatingClassLoader(bundleContext.getBundle(), Ignite.class.getClassLoader());
        else
            clsLdr = new ContainerSweepClassLoader(bundleContext.getBundle(), Ignite.class.getClassLoader());

        config.setClassLoader(clsLdr);

        // Start Ignite.
        ignite = Ignition.start(config);

        logger = ignite.log();

        logger.info("Started Ignite from OSGi Activator [name=" + ignite.name() + ']');

        // Add into Ignite's OSGi registry.
        IgniteOsgiUtils.classloaders().put(ignite, clsLdr);

        // Export Ignite as a service.
        exportOsgiService(ignite);

    }

    /**
     * Stops Ignite when the bundle is stopping.
     *
     * @param context Bundle context.
     * @throws Exception
     */
    @Override public void stop(BundleContext context) throws Exception {
        try {
            ignite.close();
        }
        catch (Exception e) {
            logger.error("Failed to stop Ignite via OSGi Activator [errMsg=" + e.getMessage() + ']');
            return;
        }

        logger.info("Stopped Ignite from OSGi Activator [name=" + ignite.name() + ']');

        IgniteOsgiUtils.classloaders().remove(ignite);
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
     * @param ignite
     */
    private void exportOsgiService(Ignite ignite) {
        Dictionary<String, String> dict = new Hashtable<>();

        // Only add the service property if the grid name != null.
        if (ignite.name() != null) {
            dict.put(OSGI_SERVICE_PROP_IGNITE_NAME, ignite.name());
        }

        bundleContext.registerService(Ignite.class, ignite, dict);

        logger.info("Exported OSGi service for Ignite with properties " + dict);
    }

}
