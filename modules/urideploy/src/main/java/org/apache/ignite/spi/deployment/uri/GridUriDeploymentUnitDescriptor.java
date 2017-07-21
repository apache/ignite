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

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.deployment.uri.GridUriDeploymentUnitDescriptor.Type.CLASS;
import static org.apache.ignite.spi.deployment.uri.GridUriDeploymentUnitDescriptor.Type.FILE;

/**
 * Container for information about tasks and file where classes placed. It also contains tasks instances.
 */
class GridUriDeploymentUnitDescriptor {
    /**
     * Container type.
     */
    @SuppressWarnings({"PackageVisibleInnerClass"}) enum Type {
        /**
         * Container has reference to the file with tasks.
         */
        FILE,

        /**
         * Container keeps tasks deployed directly.
         */
        CLASS
    }

    /**
     * Container type.
     */
    private final Type type;

    /**
     * If type is {@link Type#FILE} contains URI of {@link #file} otherwise must be null.
     */
    @GridToStringExclude
    private final String uri;

    /**
     * If type is {@link Type#FILE} contains file with tasks otherwise must be null.
     */
    private final File file;

    /**
     * Tasks deployment timestamp.
     */
    private final long tstamp;

    /** */
    private final ClassLoader clsLdr;

    /** Set of all resources. */
    private final Set<Class<?>> rsrcs = new HashSet<>();

    /** Map of resources by alias. */
    private final Map<String, Class<?>> rsrcsByAlias = new HashMap<>();

    /**
     * If type is {@link Type#FILE} and digest was computed successfully contains MD5 digest of the source file.
     */
    private final String md5;

    /**
     * Constructs descriptor for GAR file.
     *
     * @param uri GAR file URI.
     * @param file File itself.
     * @param tstamp Tasks deployment timestamp.
     * @param clsLdr Class loader.
     * @param md5 Hash of unit file of directory.
     */
    GridUriDeploymentUnitDescriptor(String uri, File file, long tstamp, ClassLoader clsLdr, String md5) {
        assert uri != null;
        assert file != null;
        assert tstamp > 0;

        this.uri = uri;
        this.file = file;
        this.tstamp = tstamp;
        this.clsLdr = clsLdr;
        this.md5 = md5;

        type = FILE;
    }

    /**
     * Constructs deployment unit descriptor based on timestamp and {@link org.apache.ignite.compute.ComputeTask} instances.
     *
     * @param tstamp Tasks deployment timestamp.
     * @param clsLdr Class loader.
     */
    GridUriDeploymentUnitDescriptor(long tstamp, ClassLoader clsLdr) {
        assert clsLdr != null;
        assert tstamp > 0;

        this.tstamp = tstamp;
        this.clsLdr = clsLdr;

        uri = null;
        file = null;
        md5 = null;
        type = CLASS;
    }

    /**
     * Gets descriptor type.
     *
     * @return Descriptor type.
     */
    public Type getType() {
        return type;
    }

    /**
     * Gets file URL.
     *
     * @return {@code null} if tasks were deployed directly and reference to the GAR file URI if tasks were deployed
     *         from the file.
     */
    public String getUri() {
        return uri;
    }

    /**
     * Tasks GAR file.
     *
     * @return {@code null} if tasks were deployed directly and GAR file if tasks were deployed from it.
     */
    public File getFile() {
        return file;
    }

    /**
     * Gets tasks deployment timestamp.
     *
     * @return Tasks deployment timestamp.
     */
    public long getTimestamp() {
        return tstamp;
    }

    /**
     * Source file MD5 digest.
     *
     * @return MD5 digest of the source file if it is available, otherwise {@code null}.
     */
    public String getMd5() {
        return md5;
    }

    /**
     * Deployed task.
     *
     * @return Deployed task.
     */
    public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /**
     * Adds resource to this descriptor.
     *
     * @param cls Resource class.
     */
    public void addResource(Class<?> cls) {
        rsrcs.add(cls);
    }

    /**
     * Adds resource by alias to this descriptor.
     *
     * @param alias Resource alias.
     * @param cls Resource class.
     */
    public void addResource(String alias, Class<?> cls) {
        rsrcsByAlias.put(alias, cls);

        rsrcs.add(cls);
    }

    /**
     * Gets resource by alias.
     *
     * @param alias Resource alias.
     * @return Resource class or {@code null}, if there is no resource
     *         for this alias.
     */
    @Nullable public Class<?> getResourceByAlias(String alias) {
        return rsrcsByAlias.get(alias);
    }

    /**
     * Gets resource by alias map.
     *
     * @return Unmodifiable resource by alias map.
     */
    public Map<String, Class<?>> getResourcesByAlias() {
        return Collections.unmodifiableMap(rsrcsByAlias);
    }

    /**
     * Gets a full resource set.
     *
     * @return Unmodifiable resource set.
     */
    public Set<Class<?>> getResources() {
        return Collections.unmodifiableSet(rsrcs);
    }

    /**
     * Looks up for resource by name (either full class name or alias).
     *
     * @param rsrcName Resource name (either full class name or alias).
     * @return Tuple of resource class and alias or {@code null}, if
     *         resource is not found. The second tuple member is
     *         {@code null} if the resource has no alias.
     */
    @Nullable public IgniteBiTuple<Class<?>, String> findResource(final String rsrcName) {
        // Find by alias.
        Class<?> cls = rsrcsByAlias.get(rsrcName);

        if (cls != null)
            return F.<Class<?>, String>t(cls, rsrcName);

        // Find by class name.
        cls = F.find(rsrcs, null, new P1<Class<?>>() {
            @Override public boolean apply(Class<?> cls0) {
                return cls0.getName().equals(rsrcName);
            }
        });

        return cls != null ? F.<Class<?>, String>t(cls, null) : null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return clsLdr.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof GridUriDeploymentUnitDescriptor &&
            clsLdr.equals(((GridUriDeploymentUnitDescriptor)obj).clsLdr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentUnitDescriptor.class, this, "uri", U.hidePassword(uri));
    }
}