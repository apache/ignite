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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Agent launcher test.
 */
public class AgentLauncherTest {
    /** Rule for expected exception. */
    @Rule
    public final ExpectedException ruleForExpEx = ExpectedException.none();

    /**
     * Should return the decrypted password from key store.
     */
    @Test
    public void shouldReturnPasswordFromKeyStore() {
        String path = AgentUtilsTest.class.getClassLoader().getResource("passwords.p12").getPath();
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-pks", path, "-pksp", "123456", "-t", "token"});

        assertEquals("1234", cfg.nodePassword());
    }

    /**
     * Should return the plain password if we don't use key store.
     */
    @Test
    public void shouldReturnPlainPasswordIfPasswordKeyStoreNotSpecified() {
        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[] {"-sksp", "123", "-t", "token"});

        assertEquals("123", cfg.serverKeyStorePassword());
    }
}
