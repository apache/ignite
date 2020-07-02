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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

/**
 * Helper class for OSGi.
 */
public class IgniteOsgiUtils {
    /** Whether we are running in an OSGi container. */
    private static boolean osgi = FrameworkUtil.getBundle(IgniteOsgiUtils.class) != null;

    /** Maps Ignite instances to the ClassLoaders of the bundles they were started from. */
    private static final ConcurrentMap<Ignite, ClassLoader> CLASSLOADERS = new ConcurrentHashMap<>();

    /**
     * Private constructor.
     */
    private IgniteOsgiUtils() { }

    /**
     * Returns whether we are running in an OSGi environment.
     *
     * @return {@code true/false}.
     */
    public static boolean isOsgi() {
        return osgi;
    }

    /**
     * Returns a {@link Map} of {@link Ignite} instances and the classloaders of the {@link Bundle}s they were
     * started from.
     *
     * @return The {@link Map}.
     */
    protected static Map<Ignite, ClassLoader> classloaders() {
        return CLASSLOADERS;
    }

    /**
     * Returns the number of grids currently running in this OSGi container.
     *
     * @return The grid count.
     */
    public static int gridCount() {
        return CLASSLOADERS.size();
    }
}
