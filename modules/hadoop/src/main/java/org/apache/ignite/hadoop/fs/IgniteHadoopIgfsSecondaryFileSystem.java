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

package org.apache.ignite.hadoop.fs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.hadoop.HadoopClassLoader;
import org.apache.ignite.internal.processors.hadoop.HadoopCommonUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopIgfsSecondaryFileSystemDelegate;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Secondary file system which delegates calls to Hadoop {@code org.apache.hadoop.fs.FileSystem}.
 * <p>
 * Target {@code FileSystem}'s are created on per-user basis using passed {@link HadoopFileSystemFactory}.
 */
public class IgniteHadoopIgfsSecondaryFileSystem implements IgfsSecondaryFileSystem, LifecycleAware {
    /** The default user name. It is used if no user context is set. */
    private String dfltUsrName;

    /** Factory. */
    private HadoopFileSystemFactory factory;

    /** Kernal context. */
    private volatile GridKernalContext ctx;

    /** Target. */
    private volatile HadoopIgfsSecondaryFileSystemDelegate target;

    /**
     * Default constructor for Spring.
     */
    public IgniteHadoopIgfsSecondaryFileSystem() {
        // No-op.
    }

    /**
     * Simple constructor that is to be used by default.
     *
     * @param uri URI of file system.
     * @throws IgniteCheckedException In case of error.
     * @deprecated Use {@link #getFileSystemFactory()} instead.
     */
    @Deprecated
    public IgniteHadoopIgfsSecondaryFileSystem(String uri) throws IgniteCheckedException {
        this(uri, null, null);
    }

    /**
     * Constructor.
     *
     * @param uri URI of file system.
     * @param cfgPath Additional path to Hadoop configuration.
     * @throws IgniteCheckedException In case of error.
     * @deprecated Use {@link #getFileSystemFactory()} instead.
     */
    @Deprecated
    public IgniteHadoopIgfsSecondaryFileSystem(@Nullable String uri, @Nullable String cfgPath)
        throws IgniteCheckedException {
        this(uri, cfgPath, null);
    }

    /**
     * Constructor.
     *
     * @param uri URI of file system.
     * @param cfgPath Additional path to Hadoop configuration.
     * @param userName User name.
     * @throws IgniteCheckedException In case of error.
     * @deprecated Use {@link #getFileSystemFactory()} instead.
     */
    @Deprecated
    public IgniteHadoopIgfsSecondaryFileSystem(@Nullable String uri, @Nullable String cfgPath,
        @Nullable String userName) throws IgniteCheckedException {
        setDefaultUserName(userName);

        CachingHadoopFileSystemFactory fac = new CachingHadoopFileSystemFactory();

        fac.setUri(uri);

        if (cfgPath != null)
            fac.setConfigPaths(cfgPath);

        setFileSystemFactory(fac);
    }

    /**
     * Gets default user name.
     * <p>
     * Defines user name which will be used during file system invocation in case no user name is defined explicitly
     * through {@code FileSystem.get(URI, Configuration, String)}.
     * <p>
     * Also this name will be used if you manipulate {@link IgniteFileSystem} directly and do not set user name
     * explicitly using {@link IgfsUserContext#doAs(String, IgniteOutClosure)} or
     * {@link IgfsUserContext#doAs(String, Callable)} methods.
     * <p>
     * If not set value of system property {@code "user.name"} will be used. If this property is not set either,
     * {@code "anonymous"} will be used.
     *
     * @return Default user name.
     */
    @Nullable public String getDefaultUserName() {
        return dfltUsrName;
    }

    /**
     * Sets default user name. See {@link #getDefaultUserName()} for details.
     *
     * @param dfltUsrName Default user name.
     * @return {@code this} for chaining.
     */
    public IgniteHadoopIgfsSecondaryFileSystem setDefaultUserName(@Nullable String dfltUsrName) {
        this.dfltUsrName = dfltUsrName;

        return this;
    }

    /**
     * Gets secondary file system factory.
     * <p>
     * This factory will be used whenever a call to a target {@code FileSystem} is required.
     * <p>
     * If not set, {@link CachingHadoopFileSystemFactory} will be used.
     *
     * @return Secondary file system factory.
     */
    public HadoopFileSystemFactory getFileSystemFactory() {
        return factory;
    }

    /**
     * Sets secondary file system factory. See {@link #getFileSystemFactory()} for details.
     *
     * @param factory Secondary file system factory.
     * @return {@code this} for chaining.
     */
    public IgniteHadoopIgfsSecondaryFileSystem setFileSystemFactory(HadoopFileSystemFactory factory) {
        this.factory = factory;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        return target.exists(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile update(IgfsPath path, Map<String, String> props) {
        return target.update(path, props);
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) {
        target.rename(src, dest);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgfsPath path, boolean recursive) {
        return target.delete(path, recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        target.mkdirs(path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) {
        target.mkdirs(path, props);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) {
        return target.listPaths(path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) {
        return target.listFiles(path);
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize) {
        return target.open(path, bufSize);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, boolean overwrite) {
        return target.create(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        return target.create(path, bufSize, overwrite, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        return target.append(path, bufSize, create, props);
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(final IgfsPath path) {
        return target.info(path);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        return target.usedSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgfsPath path, long modificationTime, long accessTime) throws IgniteException {
        target.setTimes(path, modificationTime, accessTime);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len,
        long maxLen) throws IgniteException {
        return target.affinity(path, start, len, maxLen);
    }

    /**
     * @param ignite Ignite instance.
     */
    @IgniteInstanceResource
    public void setIgniteInstance(IgniteEx ignite) {
        ctx = ignite.context();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        HadoopClassLoader ldr = ctx.hadoopHelper().commonClassLoader();

        ClassLoader oldLdr = HadoopCommonUtils.setContextClassLoader(ldr);

        try {
            target = HadoopDelegateUtils.secondaryFileSystemDelegate(ldr, this);

            target.start();
        }
        finally {
            HadoopCommonUtils.restoreContextClassLoader(oldLdr);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (target != null)
            target.stop();
    }
}
