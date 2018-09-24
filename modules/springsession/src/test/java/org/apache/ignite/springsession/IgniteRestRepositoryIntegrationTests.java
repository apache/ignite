package org.apache.ignite.springsession;

import org.apache.ignite.Ignite;
import org.apache.ignite.springsession.annotation.rest.EnableRestIgniteHttpSession;
import org.apache.ignite.springsession.annotation.IgniteRestSessionRepository;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@WebAppConfiguration
public class IgniteRestRepositoryIntegrationTests {

    private static Ignite ignite;

    @Autowired
    private IgniteRestSessionRepository repository;

    @Configuration
    @EnableRestIgniteHttpSession(sessionCacheName = "session.cache.v2", url = "http://localhost:8080/")
    static class TestConfiguration {
    }

    @BeforeClass
    public static void setup() {
        ignite = IgniteTestUtils.getIgniteServerInstance();
    }

    @AfterClass
    public static void teardown() {
        if (ignite != null) {
            ignite.close();
        }
    }

    @Test
    public void testSaveSession() {
        final IgniteSession session = repository.createSession();

        final String id = session.getId();

        IgniteSession storedSes = repository.getSession(id);

        assertThat(storedSes).isNull();

        repository.save(session);

        storedSes = repository.getSession(id);

        assertThat(storedSes).isNotNull();

        repository.delete(id);

        storedSes = repository.getSession(id);

        assertThat(storedSes).isNull();
    }

    @Test
    public void testSaveSessionWithAttributes() {
        final IgniteSession session = repository.createSession();

        final String id = session.getId();

        final String attrName = "attribute";
        String attrValue = "value";

        int i = 0;
        while (i < 14) {
            attrValue = attrValue + attrValue;
            i++;
        }

        IgniteSession storedSes = repository.getSession(id);

        assertThat(storedSes).isNull();

        repository.save(session);

        storedSes = repository.getSession(id);

        assertThat(storedSes).isNotNull();
        assertThat(storedSes.getAttributeNames()).isEmpty();

        session.setAttribute(attrName, attrValue);

        repository.save(session);

        storedSes = repository.getSession(id);

        assertThat(storedSes.getAttribute(attrName).toString()).isEqualTo(attrValue);

        repository.delete(id);

        storedSes = repository.getSession(id);

        assertThat(storedSes).isNull();
    }

    @Test
    public void testExpiredSessionRemovalOnGet() {
        final IgniteSession session = this.repository.createSession();

        session.setMaxInactiveIntervalInSeconds(30);
        session.setLastAccessedTime(session.getLastAccessedTime() - TimeUnit.MINUTES.toMillis(1));

        repository.save(session);

        final IgniteSession fetchedSession = this.repository.getSession(session.getId());

        assertThat(fetchedSession).isNull();
    }
}
