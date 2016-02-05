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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * KerberosUtil
 */
public class KerberosUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

//
//    public static final String KERBEROS_PRINCIPLE_PROPERTY_SUFFIX = "kerberos.principal";
//    public static final String KEYTAB_FILE_PROPERTY_SUFFIX = "keytab.file";
//    public static final String PROPERTY_SEPERATOR = ".";


    /** Leave 10 minutes between relogin attempts. */
    // TODO: make it configurable: should be a factory parameter:
    private static final long MIN_TIME_BEFORE_RELOGIN = 1 * 30 * 1000L; // was 10 min

//    public static String getKerberosPrincipleProperty(String appName){
//        return new String(appName + PROPERTY_SEPERATOR + KERBEROS_PRINCIPLE_PROPERTY_SUFFIX).intern();
//    }
//
//    public static String getKeyTabFileProperty(String appName){
//        return new String(appName + PROPERTY_SEPERATOR + KEYTAB_FILE_PROPERTY_SUFFIX).intern();
//    }

    /**
     *
     * @param configuration
     * @throws IOException
     */
    public synchronized static void loginFromKeyTabAndAutoReLogin(Configuration configuration,
            String keyTabFile, String kerberosPrinciple)
                    ///                                                          String appName)
        throws IOException {
        UserGroupInformation.setConfiguration(configuration);

        //String keyTabFile = configuration.get(getKeyTabFileProperty(appName));
        LOG.info("keyTabFile=" + keyTabFile);

        //String kerberosPrinciple = configuration.get(getKerberosPrincipleProperty(appName));
        LOG.info("kerberosPrinciple=" + kerberosPrinciple);

        if (StringUtils.isNotEmpty(keyTabFile)
                && StringUtils.isNotEmpty(kerberosPrinciple)) {
            UserGroupInformation.loginUserFromKeytab(kerberosPrinciple, keyTabFile);

            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

            spawnAutoReloginThreadForKerberosKeyTab(loginUser);

            LOG.info("KeyTab TGT Login Success for " + loginUser.getUserName());
        } else {
            LOG.error("KeyTab File or Kerberos Principle is not found in configuration");
            throw new IllegalStateException("KeyTab File or Kerberos Principle is not found in configuration");
        }
    }

    /**
     *
     */
    private static void spawnAutoReloginThreadForKerberosKeyTab(final UserGroupInformation loginUser) {
        if (loginUser.isSecurityEnabled()
                && loginUser.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS
                && loginUser.isFromKeytab()) {
            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    while (true) {
                        // TODO: honor interrupts
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(MIN_TIME_BEFORE_RELOGIN);

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
