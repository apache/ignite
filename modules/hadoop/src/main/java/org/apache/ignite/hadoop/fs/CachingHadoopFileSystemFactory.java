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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.processors.hadoop.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.hadoop.fs.HadoopLazyConcurrentMap;
import org.apache.ignite.internal.processors.igfs.IgfsPaths;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * The class is to be instantiated as a Spring beans, so it must have public zero-arg constructor.
 * The class is serializable as it will be transferred over the network as a part of {@link IgfsPaths} object.
 */
public class CachingHadoopFileSystemFactory implements HadoopFileSystemFactory, Externalizable, LifecycleAware {
    /** Lazy per-user cache for the file systems. It is cleared and nulled in #close() method. */
    private final transient HadoopLazyConcurrentMap<String, FileSystem> fileSysLazyMap = new HadoopLazyConcurrentMap<>(
        new HadoopLazyConcurrentMap.ValueFactory<String, FileSystem>() {
            @Override public FileSystem createValue(String key) {
                try {
                    assert !F.isEmpty(key);

                    return createFileSystem(key);
                }
                catch (IOException ioe) {
                    throw new IgniteException(ioe);
                }
            }
        }
    );

    /** Configuration of the secondary filesystem, never null. */
    protected transient Configuration cfg;

    /** */
    protected transient URI uri;

    /** */
    protected String uriStr;

    /** */
    protected List<String> cfgPathStr;

    /**
     * Public non-arg constructor.
     */
    public CachingHadoopFileSystemFactory() {
        // noop
    }

    /** {@inheritDoc} */
    @Override public FileSystem create(String userName) throws IOException {
        A.ensure(cfg != null, "cfg");

        return fileSysLazyMap.getOrCreate(userName);
    }

    /**
     * Uri setter.
     *
     * @param uriStr The URI to set.
     */
    public void setUri(String uriStr) {
        this.uriStr = uriStr;
    }

    /**
     * Gets the URI.
     *
     * @return The URI.
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Configuration(s) setter, to be invoked from Spring config.
     *
     * @param cfgPaths The config paths collection to set.
     */
    public void setConfigPaths(List<String> cfgPaths) {
        this.cfgPathStr = cfgPaths;
    }

    /**
     * Gets the config paths collection.
     *
     * @return The config paths collection.
     */
    public List<String> getConfigPaths() {
        return cfgPathStr;
    }

    /**
     * @return {@link org.apache.hadoop.fs.FileSystem}  instance for this secondary Fs.
     * @throws IOException
     */
    protected FileSystem createFileSystem(String userName) throws IOException {
        userName = IgfsUtils.fixUserName(userName);

        assert cfg != null;

        final FileSystem fileSys;

        try {
            fileSys = FileSystem.get(uri, cfg, userName);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Failed to create file system due to interrupt.", e);
        }

        return fileSys;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, uriStr);

        U.writeCollection(out, cfgPathStr);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uriStr = U.readString(in);

        cfgPathStr = new ArrayList(U.readCollection(in));
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        cfg = HadoopUtils.safeCreateConfiguration();

        if (cfgPathStr != null) {
            for (String confPath : cfgPathStr) {
                if (confPath == null)
                    throw new IgniteException("Null config path encountered.");
                else {
                    URL url = U.resolveIgniteUrl(confPath);

                    if (url == null) {
                        // If secConfPath is given, it should be resolvable:
                        throw new IgniteException("Failed to resolve secondary file system configuration path " +
                            "(ensure that it exists locally and you have read access to it): " + confPath);
                    }

                    cfg.addResource(url);
                }
            }
        }

        // if secondary fs URI is not given explicitly, try to get it from the configuration:
        if (uriStr == null)
            uri = FileSystem.getDefaultUri(cfg);
        else {
            try {
                uri = new URI(uriStr);
            }
            catch (URISyntaxException use) {
                throw new IgniteException("Failed to resolve secondary file system URI: " + uriStr);
            }
        }

        assert uriStr != null;

        // Disable caching:
        String prop = HadoopFileSystemsUtils.disableFsCachePropertyName(uri.getScheme());

        cfg.setBoolean(prop, true);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        try {
            fileSysLazyMap.close();
        }
        catch (IgniteCheckedException ice) {
            throw new IgniteException(ice);
        }
    }
}
