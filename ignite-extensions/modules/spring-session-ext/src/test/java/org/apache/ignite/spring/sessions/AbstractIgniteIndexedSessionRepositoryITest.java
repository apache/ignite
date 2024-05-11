package org.apache.ignite.spring.sessions;

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

import java.time.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.spring.sessions.IgniteIndexedSessionRepository.IgniteSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.session.FindByIndexNameSessionRepository;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for {@link IgniteIndexedSessionRepository} integration tests.
 */
abstract class AbstractIgniteIndexedSessionRepositoryITest {
    /** */
    private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

    /** */
    @Autowired
    private Ignite ignite;

    /** */
    @Autowired
    private IgniteIndexedSessionRepository repository;

    /** */
    @Test
    void createAndDestroySession() {
        IgniteIndexedSessionRepository.IgniteSession sessionToSave = this.repository.createSession();
        String sessionId = sessionToSave.getId();

        IgniteCache<String, IgniteIndexedSessionRepository.IgniteSession> cache = this.ignite
                .getOrCreateCache(IgniteIndexedSessionRepository.DEFAULT_SESSION_MAP_NAME);

        assertThat(cache.size()).isEqualTo(0);

        this.repository.save(sessionToSave);

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(sessionId)).isEqualTo(sessionToSave);

        this.repository.deleteById(sessionId);

        assertThat(cache.size()).isEqualTo(0);
    }

    /** */
    @Test
    void changeSessionIdWhenOnlyChangeId() {
        String attrName = "changeSessionId";
        String attrValue = "changeSessionId-value";
        IgniteSession toSave = this.repository.createSession();
        toSave.setAttribute(attrName, attrValue);

        this.repository.save(toSave);

        IgniteSession findById = this.repository.findById(toSave.getId());

        assertThat(findById.<String>getAttribute(attrName)).isEqualTo(attrValue);

        String originalFindById = findById.getId();
        String changeSessionId = findById.changeSessionId();

        this.repository.save(findById);

        assertThat(this.repository.findById(originalFindById)).isNull();

        IgniteSession findByChangeSessionId = this.repository.findById(changeSessionId);

        assertThat(findByChangeSessionId.<String>getAttribute(attrName)).isEqualTo(attrValue);

        this.repository.deleteById(changeSessionId);
    }

    /** */
    @Test
    void changeSessionIdWhenChangeTwice() {
        IgniteSession toSave = this.repository.createSession();

        this.repository.save(toSave);

        String originalId = toSave.getId();
        String changeId1 = toSave.changeSessionId();
        String changeId2 = toSave.changeSessionId();

        this.repository.save(toSave);

        assertThat(this.repository.findById(originalId)).isNull();
        assertThat(this.repository.findById(changeId1)).isNull();
        assertThat(this.repository.findById(changeId2)).isNotNull();

        this.repository.deleteById(changeId2);
    }

    /** */
    @Test
    void changeSessionIdWhenSetAttributeOnChangedSession() {
        String attrName = "changeSessionId";
        String attrValue = "changeSessionId-value";

        IgniteSession toSave = this.repository.createSession();

        this.repository.save(toSave);

        IgniteSession findById = this.repository.findById(toSave.getId());

        findById.setAttribute(attrName, attrValue);

        String originalFindById = findById.getId();
        String changeSessionId = findById.changeSessionId();

        this.repository.save(findById);

        assertThat(this.repository.findById(originalFindById)).isNull();

        IgniteSession findByChangeSessionId = this.repository.findById(changeSessionId);

        assertThat(findByChangeSessionId.<String>getAttribute(attrName)).isEqualTo(attrValue);

        this.repository.deleteById(changeSessionId);
    }

    /** */
    @Test
    void changeSessionIdWhenHasNotSaved() {
        IgniteSession toSave = this.repository.createSession();
        String originalId = toSave.getId();
        toSave.changeSessionId();

        this.repository.save(toSave);

        assertThat(this.repository.findById(toSave.getId())).isNotNull();
        assertThat(this.repository.findById(originalId)).isNull();

        this.repository.deleteById(toSave.getId());
    }

    /** */
    @Test
    void attemptToUpdateSessionAfterDelete() {
        IgniteSession session = this.repository.createSession();
        String sessionId = session.getId();
        this.repository.save(session);
        session = this.repository.findById(sessionId);
        session.setAttribute("attributeName", "attributeValue");
        this.repository.deleteById(sessionId);
        this.repository.save(session);

        assertThat(this.repository.findById(sessionId)).isNull();
    }

    /** */
    @Test
    void expireSession() {
        IgniteSession session = this.repository.createSession();
        String sessionId = session.getId();

        session.setMaxInactiveInterval(Duration.ofNanos(0));

        this.repository.save(session);
        assertThat(this.repository.findById(sessionId)).isNull();
    }

    /** */
    @Test
    void createAndUpdateSession() {
        IgniteSession session = this.repository.createSession();
        String sessionId = session.getId();

        this.repository.save(session);

        session = this.repository.findById(sessionId);
        session.setAttribute("attributeName", "attributeValue");

        this.repository.save(session);

        assertThat(this.repository.findById(sessionId)).isNotNull();
    }

    /** */
    @Test
    void createSessionWithSecurityContextAndFindById() {
        IgniteSession session = this.repository.createSession();
        String sessionId = session.getId();

        Authentication authentication = new UsernamePasswordAuthenticationToken("saves-" + System.currentTimeMillis(),
                "password", AuthorityUtils.createAuthorityList("ROLE_USER"));
        SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(authentication);
        session.setAttribute(SPRING_SECURITY_CONTEXT, securityContext);

        this.repository.save(session);

        assertThat(this.repository.findById(sessionId)).isNotNull();
    }

    /** */
    @Test
    void createSessionWithSecurityContextAndFindByPrincipal() {
        IgniteSession session = this.repository.createSession();

        String username = "saves-" + System.currentTimeMillis();
        Authentication authentication = new UsernamePasswordAuthenticationToken(username, "password",
                AuthorityUtils.createAuthorityList("ROLE_USER"));
        SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(authentication);
        session.setAttribute(SPRING_SECURITY_CONTEXT, securityContext);

        this.repository.save(session);

        assertThat(this.repository
                .findByIndexNameAndIndexValue(FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME, username))
                .hasSize(1);
    }

}
