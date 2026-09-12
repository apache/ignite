

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
