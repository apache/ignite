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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
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
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Secure Hadoop file system factory that can work with underlying file system protected with Kerberos.
 * It uses "impersonation" mechanism, to be able to work on behalf of arbitrary client user.
 * Please see https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html for details.
 * The principal and the key tab name to be used for Kerberos authentication are set explicitly
 * in the factory configuration.
 *
 * <p>The factory spawns a dedicated thread to periodically check and re-login from the key tab to
 * prevent the ticket expiration.
 *
 * <p>
 * This factory does not cache any file system instances. If {@code "fs.[prefix].impl.disable.cache"} is set
 * to {@code true}, file system instances will be cached by Hadoop.
 */
public class SecureHadoopFileSystemFactory implements HadoopFileSystemFactory, Externalizable, LifecycleAware {
    /** Logger */
    private static final Logger LOG = LoggerFactory.getLogger(SecureHadoopFileSystemFactory.class);

    /** The default interval used to re-login from the key tab, in milliseconds. */
    private static final long DFLT_RELOGIN_INTERVAL = 10 * 60 * 1000L;

    /** */
    private static final long serialVersionUID = 0L;

    /** File system URI. */
    protected String uri;

    /** File system config paths. */
    protected String[] cfgPaths;

    /** Keytab full file name (e.g. "/etc/security/keytabs/hdfs.headless.keytab" or "/etc/krb5.keytab"). */
    protected String keyTab;

    /** Keytab principal short name (e.g. "hdfs-Sandbox"). */
    protected String keyTabPrincipal;

    /**
     * The interval used to re-login from the key tab, in milliseconds.
     * Important that the value should not be large than the Kerberos ticket life time multiplied by 0.2. This is
     * because the ticket renew window starts from {@code 0.8 * ticket life time}.
     * Default ticket life time is 1 day (24 hours), so the default re-login interval (10 min)
     * is obeys this rule well.
     */
    protected long reloginInterval = DFLT_RELOGIN_INTERVAL;

    /** Configuration of the secondary filesystem, never null. */
    protected transient Configuration cfg;

    /** Resulting URI. */
    protected transient URI fullUri;

    /**
     * Time of last re-login attempt, in system milliseconds.
     * It is transient because server and client may be on different hosts, so
     * After de-serialization a re-login is needed any way.
     */
    protected transient volatile long lastReloginTime;

    /**
     * Constructor.
     */
    public SecureHadoopFileSystemFactory() {
        // No-op.
    }

    /**
     * Re-logins the user if needed.
     * First, the re-login interval defined in factory is checked. The re-login attempts will be not more
     * frequent than one attempt per {@code reloginInterval}.
     * Second, {@link UserGroupInformation#checkTGTAndReloginFromKeytab()} method invoked that gets existing
     * TGT and checks its validity. If the TGT is expired or is close to expiry, it performs re-login.
     *
     * <p>This operation expected to be called upon each operation with the file system created with the factory.
     * As long as {@link #get(String)} operation is invoked upon each file {@link IgniteHadoopFileSystem}, there
     * is no need to invoke it otherwise specially.
     *
     * @throws IOException If login fails.
     */
    public void reloginIfNeeded() throws IOException {
        long now = System.currentTimeMillis();

        if (now >= lastReloginTime + reloginInterval) {
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();

            lastReloginTime = now;
        }
    }

    /** {@inheritDoc} */
    @Override public FileSystem get(String userName) throws IOException {
        reloginIfNeeded();

        return create0(IgfsUtils.fixUserName(userName));
    }

    /**
     * Internal file system create routine.
     *
     * @param userName User name.
     * @return File system.
     * @throws IOException If failed.
     */
    protected FileSystem create0(String userName) throws IOException {
        assert cfg != null;

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(userName,
            UserGroupInformation.getLoginUser());

        try {
            return proxyUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override public FileSystem run() throws Exception {
                    return getFileSystem(fullUri, cfg);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Failed to create file system due to interrupt.", e);
        }
    }

    /**
     * Service method to get the file system using given URI and Configuration objects.
     *
     * @param uri The uri.
     * @param cfg The configuration.
     * @return The file system.
     * @throws IOException On error.
     */
    protected FileSystem getFileSystem(URI uri, Configuration cfg) throws IOException {
        ClassLoader ctxClsLdr = Thread.currentThread().getContextClassLoader();

        ClassLoader clsLdr = getClass().getClassLoader();

        if (ctxClsLdr == clsLdr)
            return FileSystem.get(uri, cfg);
        else {
            Thread.currentThread().setContextClassLoader(clsLdr);

            try {
                return FileSystem.get(uri, cfg);
            }
            finally {
                Thread.currentThread().setContextClassLoader(ctxClsLdr);
            }
        }
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
     * Gets the key tab principal name.
     *
     * @return The key tab principal.
     */
    @Nullable public String getKeyTabPrincipal() {
        return keyTabPrincipal;
    }

    /**
     * Set the key tab principal name.
     *
     * @param keyTabPrincipal The key tab principal name.
     */
    public void setKeyTabPrincipal(@Nullable String keyTabPrincipal) {
        this.keyTabPrincipal = keyTabPrincipal;
    }

    /**
     * Gets the key tab file name.
     *
     * @return The key tab file name.
     */
    @Nullable public String getKeyTab() {
        return keyTab;
    }

    /**
     * Sets the key tab file name.
     *
     * @param keyTab The key tab file name.
     */
    public void setKeyTab(@Nullable String keyTab) {
        this.keyTab = keyTab;
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
    public void setConfigPaths(String... cfgPaths) {
        this.cfgPaths = cfgPaths;
    }

    /**
     * Gets the re-login interval
     *
     * @return The re-login interval, in millisecoonds.
     */
    public long getReloginInterval() {
        return reloginInterval;
    }

    /**
     * Sets the relogin interval in milliseconds.
     *
     * @param reloginInterval The re-login interval, in milliseconds.
     */
    public void setReloginInterval(long reloginInterval) {
        this.reloginInterval = reloginInterval;
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

        if (!F.isEmpty(keyTab)
                && !F.isEmpty(keyTabPrincipal)) {
            try {
                loginFromKeyTab();
            } catch (IOException ioe) {
                throw new IgniteException("Failed login from keytab. [keyTab="
                    + keyTab + ", keyTabPrincipal=" + keyTabPrincipal + ']', ioe);
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
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, uri);

        if (!F.isEmpty(keyTab) && !F.isEmpty(keyTabPrincipal)) {
            out.writeBoolean(true);
            U.writeString(out, keyTab);
            U.writeString(out, keyTabPrincipal);
        } else
            out.writeBoolean(false);

        if (cfgPaths != null) {
            out.writeInt(cfgPaths.length);

            for (String cfgPath : cfgPaths)
                U.writeString(out, cfgPath);
        } else
            out.writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uri = U.readString(in);

        boolean isKeyTab = in.readBoolean();

        if (isKeyTab) {
            keyTab = U.readString(in);

            keyTabPrincipal = U.readString(in);
        }

        int cfgPathsCnt = in.readInt();

        if (cfgPathsCnt != -1) {
            cfgPaths = new String[cfgPathsCnt];

            for (int i = 0; i < cfgPathsCnt; i++)
                cfgPaths[i] = U.readString(in);
        }
    }

    /**
     * Implements initial key tab login.
     *
     * @throws IOException If login failed.
     */
    private void loginFromKeyTab() throws IOException {
        synchronized (UserGroupInformation.class) {
            UserGroupInformation.setConfiguration(cfg);

            if (LOG.isDebugEnabled()) {
                LOG.debug("keyTabFile=" + keyTab);

                LOG.debug("kerberosPrinciple=" + keyTabPrincipal);
            }

            UserGroupInformation.loginUserFromKeytab(keyTabPrincipal, keyTab);

            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

            if (LOG.isDebugEnabled())
                LOG.debug("KeyTab TGT Login Success for " + loginUser.getUserName());
        }
    }
}