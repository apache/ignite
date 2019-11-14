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

package org.apache.ignite.agent.action;

import java.util.UUID;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Session registry test.
 */
public class SessionRegistryTest extends AgentCommonAbstractTest {
    /** Ignite. */
    private IgniteEx ignite;

    /**
     * Start ignite instance.
     */
    @Before
    public void setup() throws Exception {
        ignite = (IgniteEx) startGrid();

        ignite.cluster().active(true);
    }


    /**
     * Should save session.
     */
    @Test
    public void shouldSaveSession() throws Exception {
        Session ses = Session.random();

        SessionRegistry registry = new SessionRegistry(ignite.context());

        registry.saveSession(ses);

        assertNotNull(registry.getSession(ses.id()));
    }

    /**
     * Should throw exception whrn we try to get session by null id.
     */
    @Test
    public void shouldThrowExceptionWhenTryToGetSessionByNullId() throws Exception {
        SessionRegistry registry = new SessionRegistry(ignite.context());

        assertThrows(null, () -> {
            registry.getSession(null);

            return null;
        }, IgniteAuthenticationException.class, "Invalid session ID: null");
    }


    /**
     * Should throw exception whrn we try to get session by not existing id.
     */
    @Test
    public void shouldThrowExceptionWhenTryToGetSessionByNotExistingId() throws Exception {
        SessionRegistry registry = new SessionRegistry(ignite.context());

        UUID sesId = UUID.randomUUID();

        assertThrows(null, () -> {
            registry.getSession(sesId);

            return null;
        }, IgniteAuthenticationException.class, "Session not found for ID: " + sesId);
    }

    /**
     * Should remove timed out session.
     */
    @Test
    public void shouldRemoveTimeoutedSession() throws Exception {
        Session ses = Session.random();

        ignite.context().managementConsole().configuration().setSecuritySessionTimeout(100);

        SessionRegistry registry = new SessionRegistry(ignite.context());

        registry.saveSession(ses);

        Thread.sleep(200);

        assertThrows(null, () -> {
            registry.getSession(ses.id());

            return null;
        }, IgniteAuthenticationException.class, "Session not found for ID: " + ses.id());
    }

    /**
     * Should not remove session if we actively use it.
     */
    @Test
    public void shouldNotRemoveSessionIfWeTouchIt() throws Exception {
        Session ses = Session.random();

        ignite.context().managementConsole().configuration().setSecuritySessionTimeout(100);

        SessionRegistry registry = new SessionRegistry(ignite.context());

        registry.saveSession(ses);

        for (int i = 0; i < 5; i++) {
            Thread.sleep(50);
            registry.getSession(ses.id());
        }

        assertNotNull(registry.getSession(ses.id()));
    }

    /**
     * Should update last invalidate time.
     */
    @Test
    public void shouldUpdateLastInvalidateTime() throws Exception {
        Session ses = Session.random();

        ses.credentials(new SecurityCredentials("ignite", "ignite"));

        long oldLastInvalidateTime = ses.lastInvalidateTime();

        ignite.context().managementConsole().configuration().setSecuritySessionExpirationTimeout(100);

        SessionRegistry registry = new SessionRegistry(ignite.context());

        registry.saveSession(ses);

        Thread.sleep(110);

        assertTrue(oldLastInvalidateTime < registry.getSession(ses.id()).lastInvalidateTime());
    }

    /**
     * Should remove session if we get exception in invalidation process.
     */
    @Test
    public void shouldRemoveSessionAfterInvalidateSessionWithoutCredentials() throws Exception {
        Session ses = Session.random();

        ignite.context().managementConsole().configuration().setSecuritySessionExpirationTimeout(100);

        SessionRegistry registry = new SessionRegistry(ignite.context());

        registry.saveSession(ses);

        Thread.sleep(110);

        assertThrows(null, () -> {
            registry.getSession(ses.id());

            return null;
        }, IgniteAccessControlException.class, "The user name or password is incorrect [userName=" + ses.credentials().getLogin() + ']');

        assertThrows(null, () -> {
            registry.getSession(ses.id());

            return null;
        }, IgniteAuthenticationException.class, "Session not found for ID: " + ses.id());
    }

    /**
     * Should remove session if we get exception in invalidation process.
     */
    @Test
    public void shouldRemoveSessionAfterInvalidateSessionWithIncorrectCredentials() throws Exception {
        Session ses = Session.random();

        ses.credentials(new SecurityCredentials("ignite", "ignite2"));

        ignite.context().managementConsole().configuration().setSecuritySessionExpirationTimeout(100);

        SessionRegistry registry = new SessionRegistry(ignite.context());

        registry.saveSession(ses);

        Thread.sleep(110);

        assertThrows(null, () -> {
            registry.getSession(ses.id());

            return null;
        }, IgniteAccessControlException.class, "The user name or password is incorrect [userName=" + ses.credentials().getLogin() + ']');

        assertThrows(null, () -> {
            registry.getSession(ses.id());

            return null;
        }, IgniteAuthenticationException.class, "Session not found for ID: " + ses.id());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs) {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName, rsrcs);

        configuration.setAuthenticationEnabled(true);

        return configuration;
    }
}
