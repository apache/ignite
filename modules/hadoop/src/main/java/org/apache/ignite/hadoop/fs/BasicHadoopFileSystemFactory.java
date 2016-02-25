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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

/**
 * Simple Hadoop file system factory which delegates to {@code FileSystem.get()} on each call.
 * <p>
 * If {@code "fs.[prefix].impl.disable.cache"} is set to {@code true}, file system instances will be cached by Hadoop.
 */
public class BasicHadoopFileSystemFactory implements HadoopFileSystemFactory, Externalizable, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** File system URI. */
    protected String uri;

    /** File system config paths. */
    protected String[] cfgPaths;

    /** Configuration of the secondary filesystem, never null. */
    protected transient Configuration cfg;

    /** Resulting URI. */
    protected transient URI fullUri;

    /**
     * Constructor.
     */
    public BasicHadoopFileSystemFactory() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public FileSystem get(String usrName) throws IOException {
        return get0(IgfsUtils.fixUserName(usrName));
    }

    /**
     * Internal file system create routine.
     *
     * @param usrName User name.
     * @return File system.
     * @throws IOException If failed.
     */
    protected FileSystem get0(String usrName) throws IOException {
        assert cfg != null;

        try {
            // FileSystem.get() might delegate to ServiceLoader to get the list of file system implementation.
            // And ServiceLoader is known to be sensitive to context classloader. Therefore, we change context
            // classloader to classloader of current class to avoid strange class-cast-exceptions.
            ClassLoader ctxClsLdr = Thread.currentThread().getContextClassLoader();
            ClassLoader clsLdr = getClass().getClassLoader();

            if (ctxClsLdr == clsLdr)
                return create(usrName);
            else {
                Thread.currentThread().setContextClassLoader(clsLdr);

                try {
                    return create(usrName);
                }
                finally {
                    Thread.currentThread().setContextClassLoader(ctxClsLdr);
                }
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
        return FileSystem.get(fullUri, cfg, usrName);
    }

    /**
     * Gets file system URI.
     * <p>
     * This URI will be used as a first argument when calling {@link FileSystem#get(URI, Configuration, String)}.
     * <p>
     * If not set, default URI will be picked from file system configuration using
     * {@link FileSystem#getDefaultUri(Configuration)} method.
     *
     * @return File system URI.
     */
    @Nullable public String getUri() {
        return uri;
    }

    /**
     * Sets file system URI. See {@link #getUri()} for more information.
     *
     * @param uri File system URI.
     */
    public void setUri(@Nullable String uri) {
        this.uri = uri;
    }

    /**
     * Gets paths to additional file system configuration files (e.g. core-site.xml).
     * <p>
     * Path could be either absolute or relative to {@code IGNITE_HOME} environment variable.
     * <p>
     * All provided paths will be loaded in the order they provided and then applied to {@link Configuration}. It means
     * that path order might be important in some cases.
     * <p>
     * <b>NOTE!</b> Factory can be serialized and transferred to other machines where instance of
     * {@link IgniteHadoopFileSystem} resides. Corresponding paths must exist on these machines as well.
     *
     * @return Paths to file system configuration files.
     */
    @Nullable public String[] getConfigPaths() {
        return cfgPaths;
    }

    /**
     * Set paths to additional file system configuration files (e.g. core-site.xml). See {@link #getConfigPaths()} for
     * more information.
     *
     * @param cfgPaths Paths to file system configuration files.
     */
    public void setConfigPaths(@Nullable String... cfgPaths) {
        this.cfgPaths = cfgPaths;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        cfg = HadoopUtils.safeCreateConfiguration();

        if (cfgPaths != null) {
            for (String cfgPath : cfgPaths) {
                if (cfgPath == null)
                    throw new NullPointerException("Configuration path cannot be null: " + Arrays.toString(cfgPaths));
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
        if (uri == null)
            fullUri = FileSystem.getDefaultUri(cfg);
        else {
            try {
                fullUri = new URI(uri);
            }
            catch (URISyntaxException use) {
                throw new IgniteException("Failed to resolve secondary file system URI: " + uri);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, uri);

        if (cfgPaths != null) {
            out.writeInt(cfgPaths.length);

            for (String cfgPath : cfgPaths)
                U.writeString(out, cfgPath);
        }
        else
            out.writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uri = U.readString(in);

        int cfgPathsCnt = in.readInt();

        if (cfgPathsCnt != -1) {
            cfgPaths = new String[cfgPathsCnt];

            for (int i = 0; i < cfgPathsCnt; i++)
                cfgPaths[i] = U.readString(in);
        }
    }
}
