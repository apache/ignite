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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.KerberosHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Kerberos Hadoop file system factory delegate.
 */
public class HadoopKerberosFileSystemFactoryDelegate extends HadoopBasicFileSystemFactoryDelegate {
    /** The re-login interval. */
    private long reloginInterval;

    /** Time of last re-login attempt, in system milliseconds. */
    private volatile long lastReloginTime;

    /** Login user. */
    private UserGroupInformation user;

    /**
     * Constructor.
     *
     * @param proxy Proxy.
     */
    public HadoopKerberosFileSystemFactoryDelegate(KerberosHadoopFileSystemFactory proxy) {
        super(proxy);
    }

    /** {@inheritDoc} */
    @Override public FileSystem getWithMappedName(String name) throws IOException {
        reloginIfNeeded();

        return super.getWithMappedName(name);
    }

    /** {@inheritDoc} */
    @Override protected FileSystem create(String usrName) throws IOException, InterruptedException {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(usrName, user);

        return proxyUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override public FileSystem run() throws Exception {
                FileSystem fs = FileSystem.get(fullUri, cfg);

                if (workDir != null)
                    fs.setWorkingDirectory(workDir);

                return fs;
            }
        });
    }

    @Override public void start() throws IgniteException {
        super.start();

        KerberosHadoopFileSystemFactory proxy0 = (KerberosHadoopFileSystemFactory)proxy;

        A.ensure(!F.isEmpty(proxy0.getKeyTab()), "keyTab cannot not be empty.");
        A.ensure(!F.isEmpty(proxy0.getKeyTabPrincipal()), "keyTabPrincipal cannot not be empty.");
        A.ensure(proxy0.getReloginInterval() >= 0, "reloginInterval cannot not be negative.");

        reloginInterval = proxy0.getReloginInterval();

        try {
            UserGroupInformation.setConfiguration(cfg);

            user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(proxy0.getKeyTabPrincipal(),
                proxy0.getKeyTab());
        }
        catch (IOException ioe) {
            throw new IgniteException("Failed login from keytab [keyTab=" + proxy0.getKeyTab() +
                ", keyTabPrincipal=" + proxy0.getKeyTabPrincipal() + ']', ioe);
        }
    }

    /**
     * Re-logins the user if needed.
     * First, the re-login interval defined in factory is checked. The re-login attempts will be not more
     * frequent than one attempt per {@code reloginInterval}.
     * Second, {@code UserGroupInformation.checkTGTAndReloginFromKeytab()} method invoked that gets existing
     * TGT and checks its validity. If the TGT is expired or is close to expiry, it performs re-login.
     *
     * <p>This operation expected to be called upon each operation with the file system created with the factory.
     * As long as {@link #get(String)} operation is invoked upon each file {@link IgniteHadoopFileSystem}, there
     * is no need to invoke it otherwise specially.
     *
     * @throws IOException If login fails.
     */
    private void reloginIfNeeded() throws IOException {
        long now = System.currentTimeMillis();

        if (now >= lastReloginTime + reloginInterval) {
            user.checkTGTAndReloginFromKeytab();

            lastReloginTime = now;
        }
    }
}
