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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
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
public class SecureHadoopFileSystemFactory extends BasicHadoopFileSystemFactory
        implements HadoopFileSystemFactory, Externalizable, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** The default interval used to re-login from the key tab, in milliseconds. */
    private static final long DFLT_RELOGIN_INTERVAL = 10 * 60 * 1000L;

    /** Logger */
    private static final Logger LOG = LoggerFactory.getLogger(SecureHadoopFileSystemFactory.class);

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
     * Zero value means that re-login should be attempted on each operation.
     */
    protected long reloginInterval = DFLT_RELOGIN_INTERVAL;

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
    protected void reloginIfNeeded() throws IOException {
        long now = 0;

        if (reloginInterval == 0
                || (now = System.currentTimeMillis()) >= lastReloginTime + reloginInterval) {
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();

            lastReloginTime = now;
        }
    }

    /** {@inheritDoc} */
    @Override public FileSystem get(String userName) throws IOException {
        reloginIfNeeded();

        return super.get(userName);
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
        validateValues();

        super.start();

        try {
            loginFromKeyTab();
        } catch (IOException ioe) {
            throw new IgniteException("Failed login from keytab. [keyTab="
                + keyTab + ", keyTabPrincipal=" + keyTabPrincipal + ']', ioe);
        }
    }

    /**
     * Checks that the values injected via the setters are correct.
     */
    protected void validateValues() {
        A.ensure(!F.isEmpty(keyTab), "keyTab property should not be empty.");

        A.ensure(!F.isEmpty(keyTabPrincipal), "keyTabPrincipal property should not be empty.");
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
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeString(out, keyTab);
        U.writeString(out, keyTabPrincipal);
        out.writeLong(reloginInterval);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        keyTab = U.readString(in);
        keyTabPrincipal = U.readString(in);
        reloginInterval = in.readLong();
    }
}