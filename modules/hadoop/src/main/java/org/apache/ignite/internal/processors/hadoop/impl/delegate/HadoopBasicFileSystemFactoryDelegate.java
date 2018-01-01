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

package org.apache.ignite.internal.processors.hadoop.impl.delegate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.BasicHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.hadoop.util.UserNameMapper;
import org.apache.ignite.internal.processors.hadoop.HadoopCommonUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

/**
 * Basic Hadoop file system factory delegate.
 */
public class HadoopBasicFileSystemFactoryDelegate implements HadoopFileSystemFactoryDelegate {
    /** Proxy. */
    protected final HadoopFileSystemFactory proxy;

    /** Configuration of the secondary filesystem, never null. */
    protected Configuration cfg;

    /** Resulting URI. */
    protected URI fullUri;

    /** User name mapper. */
    private UserNameMapper usrNameMapper;

    /** Work directory. */
    protected Path workDir;

    /**
     * Constructor.
     *
     * @param proxy Proxy.
     */
    public HadoopBasicFileSystemFactoryDelegate(BasicHadoopFileSystemFactory proxy) {
        this.proxy = proxy;
    }

    /** {@inheritDoc} */
    @Override public FileSystem get(String name) throws IOException {
        String name0 = IgfsUtils.fixUserName(name);

        if (usrNameMapper != null)
            name0 = IgfsUtils.fixUserName(usrNameMapper.map(name0));

        return getWithMappedName(name0);
    }

    /**
     * Internal file system create routine.
     *
     * @param usrName User name.
     * @return File system.
     * @throws IOException If failed.
     */
    protected FileSystem getWithMappedName(String usrName) throws IOException {
        assert cfg != null;

        try {
            // FileSystem.get() might delegate to ServiceLoader to get the list of file system implementation.
            // And ServiceLoader is known to be sensitive to context classloader. Therefore, we change context
            // classloader to classloader of current class to avoid strange class-cast-exceptions.
            ClassLoader oldLdr = HadoopCommonUtils.setContextClassLoader(getClass().getClassLoader());

            try {
                return create(usrName);
            }
            finally {
                HadoopCommonUtils.restoreContextClassLoader(oldLdr);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Failed to create file system due to interrupt.", e);
        }
    }

    /**
     * Internal file system creation routine, invoked in correct class loader context.
     *
     * @param usrName User name.
     * @return File system.
     * @throws IOException If failed.
     * @throws InterruptedException if the current thread is interrupted.
     */
    protected FileSystem create(String usrName) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(fullUri, cfg, usrName);

        if (workDir != null)
            fs.setWorkingDirectory(workDir);

        return fs;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        BasicHadoopFileSystemFactory proxy0 = (BasicHadoopFileSystemFactory)proxy;

        cfg = HadoopUtils.safeCreateConfiguration();

        if (proxy0.getConfigPaths() != null) {
            for (String cfgPath : proxy0.getConfigPaths()) {
                if (cfgPath == null)
                    throw new NullPointerException("Configuration path cannot be null: " +
                        Arrays.toString(proxy0.getConfigPaths()));
                else {
                    URL url = U.resolveIgniteUrl(cfgPath);

                    if (url == null) {
                        // If secConfPath is given, it should be resolvable:
                        throw new IgniteException("Failed to resolve secondary file system configuration path " +
                            "(ensure that it exists locally and you have read access to it): " + cfgPath);
                    }

                    cfg.addResource(url);
                }
            }
        }

        // If secondary fs URI is not given explicitly, try to get it from the configuration:
        if (proxy0.getUri() == null)
            fullUri = FileSystem.getDefaultUri(cfg);
        else {
            try {
                fullUri = new URI(proxy0.getUri());
            }
            catch (URISyntaxException ignored) {
                throw new IgniteException("Failed to resolve secondary file system URI: " + proxy0.getUri());
            }
        }

        String strWorkDir = fullUri.getPath();

        if (!"/".equals(strWorkDir))
            workDir = new Path(strWorkDir);

        usrNameMapper = proxy0.getUserNameMapper();

        if (usrNameMapper != null && usrNameMapper instanceof LifecycleAware)
            ((LifecycleAware)usrNameMapper).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (usrNameMapper != null && usrNameMapper instanceof LifecycleAware)
            ((LifecycleAware)usrNameMapper).stop();
    }
}
