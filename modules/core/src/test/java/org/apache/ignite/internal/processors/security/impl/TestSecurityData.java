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

package org.apache.ignite.internal.processors.security.impl;

import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 * Test security data for subject configuration.
 */
public class TestSecurityData {
    /** Login. */
    private String login;

    /** Password. */
    private String pwd;

    /** Security permission set. */
    private SecurityPermissionSet prmSet;

    /**
     * Default constructor.
     */
    public TestSecurityData() {
        // No-op.
    }

    /**
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Permissions.
     */
    public TestSecurityData(String login, String pwd, SecurityPermissionSet prmSet) {
        this.login = login;
        this.pwd = pwd;
        this.prmSet = prmSet;
    }

    /**
     * @param login Login.
     * @param prmSet Permissions.
     */
    public TestSecurityData(String login, SecurityPermissionSet prmSet) {
        this(login, "", prmSet);
    }

    /**
     * Getting security permission set.
     */
    public SecurityPermissionSet getPermissions() {
        return prmSet;
    }

    /**
     * @param prmSet Security permission set.
     */
    public TestSecurityData setPermissions(SecurityPermissionSet prmSet) {
        this.prmSet = prmSet;

        return this;
    }

    /**
     * Login.
     */
    public String getLogin() {
        return login;
    }

    /**
     * @param login Login.
     */
    public TestSecurityData setLogin(String login) {
        this.login = login;

        return this;
    }

    /**
     * Password.
     */
    public String getPwd() {
        return pwd;
    }

    /**
     * @param pwd Password.
     */
    public TestSecurityData setPwd(String pwd) {
        this.pwd = pwd;

        return this;
    }

    /**
     * @return Security credentials.
     */
    public SecurityCredentials credentials() {
        return new SecurityCredentials(getLogin(), getPwd(), null);
    }
}
