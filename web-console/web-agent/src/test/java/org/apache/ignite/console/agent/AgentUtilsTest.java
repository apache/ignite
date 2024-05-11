/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent;

import org.apache.ignite.IgniteException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.ignite.console.agent.AgentUtils.getPasswordFromKeyStore;
import static org.junit.Assert.assertEquals;

/**
 * Agent utils tests.
 */
public class AgentUtilsTest {
    /** Rule for expected exception. */
    @Rule
    public final ExpectedException ruleForExpEx = ExpectedException.none();

    /**
     * Should return passwords from key store.
     */
    @Test
    public void shouldReturnPasswordFromKeyStore() {
        String path = AgentUtilsTest.class.getClassLoader().getResource("passwords.p12").getPath();
        String nodePwd = getPasswordFromKeyStore("node-password", path, "123456");
        String nodeKeyStorePwd = getPasswordFromKeyStore("node-key-store-password", path, "123456");
        String nodeTrustStorePwd = getPasswordFromKeyStore("node-trust-store-password", path, "123456");
        String srvKeyStorePwd = getPasswordFromKeyStore("server-key-store-password", path, "123456");
        String srvTrustStorePwd = getPasswordFromKeyStore("server-trust-store-password", path, "123456");

        assertEquals("1234", nodePwd);
        assertEquals("123456", nodeKeyStorePwd);
        assertEquals("12345678", nodeTrustStorePwd);
        assertEquals("123123", srvKeyStorePwd);
        assertEquals("123123", srvTrustStorePwd);
    }

    /**
     * Should throw exception if store pass is incorrect.
     */
    @Test
    public void shouldThrowExceptionIfStorePassIncorrect() {
        ruleForExpEx.expect(IgniteException.class);
        ruleForExpEx.expectMessage("Failed to read password from key store, please check key store password");

        String path = AgentUtilsTest.class.getClassLoader().getResource("passwords.p12").getPath();
        getPasswordFromKeyStore("node-password", path, "12345678");
    }

    /**
     * Should throw exception if store path is incorrect.
     */
    @Test
    public void shouldThrowExceptionIfStorePathIncorrect() {
        ruleForExpEx.expect(IgniteException.class);
        ruleForExpEx.expectMessage("Failed to open passwords key store: /super-key-store.p911");

        getPasswordFromKeyStore("node-password", "/super-key-store.p911", "12345678");
    }

    /**
     * Should throw exception if password name not exists in key sotre.
     */
    @Test
    public void shouldThrowExceptionIfPasswordNotExistsInKeyStore() {
        String name = "node-node-password";
        String path = AgentUtilsTest.class.getClassLoader().getResource("passwords.p12").getPath();

        ruleForExpEx.expect(IgniteException.class);
        ruleForExpEx.expectMessage(String.format("Failed to find password in key store: [name=%s, keyStorePath=%s]", name, path));

        getPasswordFromKeyStore(name, path, "123456");
    }

    /**
     * Should throw exception if path is empty.
     */
    @Test
    public void shouldThrowExceptionIfPathIsEmpty() {
        ruleForExpEx.expect(IgniteException.class);
        ruleForExpEx.expectMessage("Empty path to key store with passwords");

        getPasswordFromKeyStore("node-password", "", "123456");
    }
}
