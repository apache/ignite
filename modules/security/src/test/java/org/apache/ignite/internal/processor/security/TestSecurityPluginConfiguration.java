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

package org.apache.ignite.internal.processor.security;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 * Security configuration for test.
 */
public class TestSecurityPluginConfiguration implements PluginConfiguration {
    /** Node security data. */
    private TestSecurityData nodeSecData = new TestSecurityData();

    /** Thin clients security data. */
    private Collection<TestSecurityData> thinClientsSecData = Collections.emptyList();

    /** Security processor class name. */
    private String secProcCls;

    /**
     * Getting security permission set.
     */
    public SecurityPermissionSet getPermissions() {
        return nodeSecData.getPermissions();
    }

    /**
     * @param prmSet Security permission set.
     */
    public TestSecurityPluginConfiguration setPermissions(SecurityPermissionSet prmSet) {
        nodeSecData.setPermissions(prmSet);

        return this;
    }

    /**
     * Login.
     */
    public String getLogin() {
        return nodeSecData.getLogin();
    }

    /**
     * @param login Login.
     */
    public TestSecurityPluginConfiguration setLogin(String login) {
        nodeSecData.setLogin(login);

        return this;
    }

    /**
     * Password.
     */
    public String getPwd() {
        return nodeSecData.getPwd();
    }

    /**
     * @param pwd Password.
     */
    public TestSecurityPluginConfiguration setPwd(String pwd) {
        nodeSecData.setPwd(pwd);

        return this;
    }

    /**
     * @param nodeSecData Node security data.
     */
    public TestSecurityPluginConfiguration nodeSecData(TestSecurityData nodeSecData) {
        this.nodeSecData = nodeSecData;

        return this;
    }

    /**
     * @return Node security data.
     */
    public TestSecurityData nodeSecData() {
        return nodeSecData;
    }

    /**
     * @param data Array of thin client security data.
     */
    public TestSecurityPluginConfiguration thinClientSecData(TestSecurityData... data) {
        thinClientsSecData = Collections.unmodifiableCollection(Arrays.asList(data));

        return this;
    }

    /**
     * @return Collection of thin client security data.
     */
    public Collection<TestSecurityData> thinClientsSecData() {
        return thinClientsSecData;
    }

    /**
     * Getting security processor class name.
     */
    public String getSecurityProcessorClass() {
        return secProcCls;
    }

    /**
     * @param secProcCls Security processor class name.
     */
    public TestSecurityPluginConfiguration setSecurityProcessorClass(String secProcCls) {
        this.secProcCls = secProcCls;

        return this;
    }
}