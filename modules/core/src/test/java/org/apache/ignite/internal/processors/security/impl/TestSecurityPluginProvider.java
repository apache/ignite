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

import java.util.Arrays;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/** */
public class TestSecurityPluginProvider extends AbstractTestSecurityPluginProvider {
    /** Login. */
    private final String login;

    /** Password. */
    private final String pwd;

    /** Permissions. */
    private final SecurityPermissionSet perms;

    /** Global authentication. */
    private final boolean globalAuth;

    /** Users security data. */
    private final TestSecurityData[] clientData;

    /** */
    public TestSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms, boolean globalAuth,
        TestSecurityData... clientData) {
        this.login = login;
        this.pwd = pwd;
        this.perms = perms;
        this.globalAuth = globalAuth;
        this.clientData = clientData.clone();
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms),
            Arrays.asList(clientData),
            globalAuth);
    }
}
