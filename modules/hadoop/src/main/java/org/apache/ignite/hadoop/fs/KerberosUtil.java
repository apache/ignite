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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * KerberosUtil
 */
public class KerberosUtil {
    /** Logger */
    private static final Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

    /**
     *
     * @param configuration
     * @throws IOException
     */
    public synchronized static UserGroupInformation loginFromKeyTabAndAutoReLogin(Configuration configuration,
            String keyTabFile, String kerberosPrinciple, long reloginInterval)
        throws IOException {
        UserGroupInformation.setConfiguration(configuration);

        if (LOG.isDebugEnabled()) {
            LOG.debug("keyTabFile=" + keyTabFile);

            LOG.debug("kerberosPrinciple=" + kerberosPrinciple);
        }

        UserGroupInformation.loginUserFromKeytab(kerberosPrinciple, keyTabFile);

        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

        spawnAutoReloginThreadForKerberosKeyTab(loginUser, reloginInterval);

        if (LOG.isDebugEnabled())
            LOG.debug("KeyTab TGT Login Success for " + loginUser.getUserName());

        return loginUser;
    }

    /**
     *
     */
    private static void spawnAutoReloginThreadForKerberosKeyTab(final UserGroupInformation loginUser,
             final long reloginInterval) {
        if (UserGroupInformation.isSecurityEnabled()
                && loginUser.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS
                && loginUser.isFromKeytab()) {
            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    while (true) {
                        try {
                            Thread.sleep(reloginInterval);
                        }
                        catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();

                            LOG.warn("TGT Relogin thread interrupted.");

                            break;
                        }

                        try {
                            loginUser.checkTGTAndReloginFromKeytab();

                            LOG.info("TGT Relogin Success for " + loginUser.getUserName());
                        } catch (IOException e) {
                            LOG.error("TGT Relogin Failure for " + loginUser.getUserName(), e);
                        }
                    }
                }
            });

            t.setDaemon(true);

            t.setName("TGT Relogin for " + loginUser.getUserName());

            t.start();
        }
    }
}