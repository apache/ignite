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

package org.apache.ignite.osgi.classloaders;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.jsr166.ConcurrentHashMap8;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;

/**
 * A {@link ClassLoader} implementation that first attempts to load the class from the associated {@link Bundle}. As
 * a fallback, it sweeps the entire OSGi container to find the requested class, returning the first hit.
 * <p>
 * It keeps a cache of resolved classes and unresolvable classes, in order to optimize subsequent lookups.
 */
public class ContainerSweepClassLoader extends BundleDelegatingClassLoader {
    /** Classes resolved previously. */
    private final ConcurrentMap<String, Bundle> resolved = new ConcurrentHashMap8<>();

    /** Unresolvable classes. */
    private final Set<String> nonResolvable = new GridConcurrentHashSet<>();

    /**
     * Constructor with a {@link Bundle} only.
     *
     * @param bundle The bundle.
     */
    public ContainerSweepClassLoader(Bundle bundle) {
        super(bundle);
    }

    /**
     * Constructor with a {@link Bundle} and another {@link ClassLoader} to check.
     *
     * @param bundle The bundle.
     * @param classLoader The other classloader to check.
     */
    public ContainerSweepClassLoader(Bundle bundle, ClassLoader classLoader) {
        super(bundle, classLoader);
    }

    /**
     * Runs the same logic to find the class as {@link BundleDelegatingClassLoader}, but if not found, sweeps the
     * OSGi container to locate the first {@link Bundle} that can load the class, and uses it to do so.
     *
     * @param name The classname.
     * @param resolve Whether to resolve the class or not.
     * @return The loaded class.
     * @throws ClassNotFoundException
     */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // If we know it's not resolvable, throw the exception immediately.
        if (nonResolvable.contains(name))
            throw classNotFoundException(name);

        Class<?> cls;

        // First, delegate to super, and return the class if found.
        try {
            cls = super.loadClass(name, resolve);
            return cls;
        }
        catch (ClassNotFoundException ignored) {
            // Continue.
        }

        // Else, check the cache.
        if (resolved.containsKey(name))
            return resolved.get(name).loadClass(name);

        // If still unresolved, sweep the container.
        cls = sweepContainer(name);

        // If still unresolved, throw the exception.
        if (cls == null)
            throw classNotFoundException(name);

        return cls;
    }

    /**
     * Sweeps the OSGi container to find the first {@link Bundle} that can load the class.
     *
     * @param name The classname.
     * @return The loaded class.
     */
    protected Class<?> sweepContainer(String name) {
        Class<?> cls = null;

        Bundle[] bundles = bundle.getBundleContext().getBundles();

        int bundleIdx = 0;

        for (; bundleIdx < bundles.length; bundleIdx++) {
            Bundle b = bundles[bundleIdx];

            // Skip bundles that haven't reached RESOLVED state; skip fragments.
            if (b.getState() <= Bundle.RESOLVED || b.getHeaders().get(Constants.FRAGMENT_HOST) != null)
                continue;

            try {
                cls = b.loadClass(name);
                break;
            }
            catch (ClassNotFoundException ignored) {
                // No-op.
            }
        }

        if (cls == null)
            nonResolvable.add(name);
        else
            resolved.put(name, bundles[bundleIdx]);

        return cls;
    }
}
