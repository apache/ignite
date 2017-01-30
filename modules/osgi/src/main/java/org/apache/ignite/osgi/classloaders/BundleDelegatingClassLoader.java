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

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.osgi.framework.Bundle;

/**
 * A {@link ClassLoader} implementation delegating to a given OSGi bundle, and to the specified {@link ClassLoader}
 * as a fallback.
 */
public class BundleDelegatingClassLoader extends ClassLoader {
    /** The bundle which loaded Ignite. */
    protected final Bundle bundle;

    /** The fallback classloader, expected to be the ignite-core classloader. */
    @GridToStringExclude
    protected final ClassLoader clsLdr;

    /**
     * Constructor.
     *
     * @param bundle The bundle
     */
    public BundleDelegatingClassLoader(Bundle bundle) {
        this(bundle, null);
    }

    /**
     * Constructor.
     *
     * @param bundle The bundle.
     * @param classLoader Fallback classloader.
     */
    public BundleDelegatingClassLoader(Bundle bundle, ClassLoader classLoader) {
        this.bundle = bundle;
        this.clsLdr = classLoader;
    }

    /** {@inheritDoc} */
    @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
        return bundle.loadClass(name);
    }

    /** {@inheritDoc} */
    @Override protected URL findResource(String name) {
        URL resource = bundle.getResource(name);

        if (resource == null && clsLdr != null)
            resource = clsLdr.getResource(name);

        return resource;
    }

    /**
     * Finds a given resource from within the {@link #bundle}.
     *
     * @param name The resource name.
     * @return URLs of resources.
     * @throws IOException
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Enumeration findResources(String name) throws IOException {
        return bundle.getResources(name);
    }

    /**
     * Loads a class trying the {@link #bundle} first, falling back to the ClassLoader {@link #clsLdr}.
     *
     * @param name Class name.
     * @param resolve {@code true} to resolve the class.
     * @return The Class.
     * @throws ClassNotFoundException
     */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> cls;

        try {
            cls = findClass(name);
        }
        catch (ClassNotFoundException ignored) {
            if (clsLdr == null)
                throw classNotFoundException(name);

            try {
                cls = clsLdr.loadClass(name);
            }
            catch (ClassNotFoundException ignored2) {
                throw classNotFoundException(name);
            }

        }

        if (resolve)
            resolveClass(cls);

        return cls;
    }

    /**
     * Returns the {@link Bundle} to which this ClassLoader is associated.
     *
     * @return The Bundle.
     */
    public Bundle getBundle() {
        return bundle;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BundleDelegatingClassLoader.class, this);
    }

    /**
     * Builds a {@link ClassNotFoundException}.
     *
     * @param clsName Class name.
     * @return The exception.
     */
    protected ClassNotFoundException classNotFoundException(String clsName) {
        String s = "Failed to resolve class [name=" + clsName +
            ", bundleId=" + bundle.getBundleId() +
            ", symbolicName=" + bundle.getSymbolicName() +
            ", fallbackClsLdr=" + clsLdr + ']';

        return new ClassNotFoundException(s);
    }
}
