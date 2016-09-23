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
import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.hadoop.util.UserNameMapper;
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
    private String uri;

    /** File system config paths. */
    private String[] cfgPaths;

    /** User name mapper. */
    private UserNameMapper usrNameMapper;

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
    @Override public final FileSystem get(String name) throws IOException {
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
            ClassLoader oldLdr = HadoopUtils.setContextClassLoader(getClass().getClassLoader());

            try {
                return create(usrName);
            }
            finally {
                HadoopUtils.restoreContextClassLoader(oldLdr);
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

    /**
     * Get optional user name mapper.
     * <p>
     * When IGFS is invoked from Hadoop, user name is passed along the way to ensure that request will be performed
     * with proper user context. User name is passed in a simple form and doesn't contain any extended information,
     * such as host, domain or Kerberos realm. You may use name mapper to translate plain user name to full user
     * name required by security engine of the underlying file system.
     * <p>
     * For example you may want to use {@link KerberosUserNameMapper} to user name from {@code "johndoe"} to
     * {@code "johndoe@YOUR.REALM.COM"}.
     *
     * @return User name mapper.
     */
    @Nullable public UserNameMapper getUserNameMapper() {
        return usrNameMapper;
    }

    /**
     * Set optional user name mapper. See {@link #getUserNameMapper()} for more information.
     *
     * @param usrNameMapper User name mapper.
     */
    public void setUserNameMapper(@Nullable UserNameMapper usrNameMapper) {
        this.usrNameMapper = usrNameMapper;
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

        if (usrNameMapper != null && usrNameMapper instanceof LifecycleAware)
            ((LifecycleAware)usrNameMapper).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (usrNameMapper != null && usrNameMapper instanceof LifecycleAware)
            ((LifecycleAware)usrNameMapper).stop();
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

        out.writeObject(usrNameMapper);
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

        usrNameMapper = (UserNameMapper)in.readObject();
    }
}
