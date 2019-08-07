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

import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Secure Hadoop file system factory that can work with underlying file system protected with Kerberos.
 * It uses "impersonation" mechanism, to be able to work on behalf of arbitrary client user.
 * Please see https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html for details.
 * The principal and the key tab name to be used for Kerberos authentication are set explicitly
 * in the factory configuration.
 *
 * <p>This factory does not cache any file system instances. If {@code "fs.[prefix].impl.disable.cache"} is set
 * to {@code true}, file system instances will be cached by Hadoop.
 */
public class KerberosHadoopFileSystemFactory extends BasicHadoopFileSystemFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** The default interval used to re-login from the key tab, in milliseconds. */
    public static final long DFLT_RELOGIN_INTERVAL = 10 * 60 * 1000L;

    /** Keytab full file name. */
    private String keyTab;

    /** Keytab principal. */
    private String keyTabPrincipal;

    /** The re-login interval. See {@link #getReloginInterval()} for more information. */
    private long reloginInterval = DFLT_RELOGIN_INTERVAL;

    /**
     * Constructor.
     */
    public KerberosHadoopFileSystemFactory() {
        // No-op.
    }

    /**
     * Gets the key tab principal short name (e.g. "hdfs").
     *
     * @return The key tab principal.
     */
    @Nullable public String getKeyTabPrincipal() {
        return keyTabPrincipal;
    }

    /**
     * Set the key tab principal name. See {@link #getKeyTabPrincipal()} for more information.
     *
     * @param keyTabPrincipal The key tab principal name.
     */
    public void setKeyTabPrincipal(@Nullable String keyTabPrincipal) {
        this.keyTabPrincipal = keyTabPrincipal;
    }

    /**
     * Gets the key tab full file name (e.g. "/etc/security/keytabs/hdfs.headless.keytab" or "/etc/krb5.keytab").
     *
     * @return The key tab file name.
     */
    @Nullable public String getKeyTab() {
        return keyTab;
    }

    /**
     * Sets the key tab file name. See {@link #getKeyTab()} for more information.
     *
     * @param keyTab The key tab file name.
     */
    public void setKeyTab(@Nullable String keyTab) {
        this.keyTab = keyTab;
    }

    /**
     * The interval used to re-login from the key tab, in milliseconds.
     * Important that the value should not be larger than the Kerberos ticket life time multiplied by 0.2. This is
     * because the ticket renew window starts from {@code 0.8 * ticket life time}.
     * Default ticket life time is 1 day (24 hours), so the default re-login interval (10 min)
     * is obeys this rule well.
     *
     * <p>Zero value means that re-login should be attempted on each file system operation.
     * Negative values are not allowed.
     *
     * <p>Note, however, that it does not make sense to make this value small, because Hadoop does not allow to
     * login if less than {@code org.apache.hadoop.security.UserGroupInformation.MIN_TIME_BEFORE_RELOGIN} milliseconds
     * have passed since the time of the previous login.
     *
     * @return The re-login interval, in milliseconds.
     */
    public long getReloginInterval() {
        return reloginInterval;
    }

    /**
     * Sets the relogin interval in milliseconds. See {@link #getReloginInterval()} for more information.
     *
     * @param reloginInterval The re-login interval, in milliseconds.
     */
    public void setReloginInterval(long reloginInterval) {
        this.reloginInterval = reloginInterval;
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
