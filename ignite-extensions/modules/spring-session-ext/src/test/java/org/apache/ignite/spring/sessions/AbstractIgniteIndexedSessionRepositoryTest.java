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

package org.apache.ignite.spring.sessions;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.session.FindByIndexNameSessionRepository;

import static org.apache.ignite.spring.sessions.IgniteSession.SPRING_SECURITY_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for {@link IgniteIndexedSessionRepository} integration tests.
 */
abstract class AbstractIgniteIndexedSessionRepositoryTest {
    /** */
    @Autowired
    protected IgniteIndexedSessionRepository repo;

    /** */
    @Test
    void changeSessionIdWhenOnlyChangeId() {
        String attrName = "changeSessionId";
        String attrVal = "changeSessionId-value";
        IgniteSession toSave = repo.createSession();
        toSave.setAttribute(attrName, attrVal);

        repo.save(toSave);

        IgniteSession findById = repo.findById(toSave.getId());

        assertThat(findById.<String>getAttribute(attrName)).isEqualTo(attrVal);

        String originalFindById = findById.getId();
        String changeSesId = findById.changeSessionId();

        repo.save(findById);

        assertThat(repo.findById(originalFindById)).isNull();

        IgniteSession findByChangeSesId = repo.findById(changeSesId);

        assertThat(findByChangeSesId.<String>getAttribute(attrName)).isEqualTo(attrVal);

        repo.deleteById(changeSesId);
    }

    /** */
    @Test
    void changeSessionIdWhenChangeTwice() {
        IgniteSession toSave = repo.createSession();

        repo.save(toSave);

        String originalId = toSave.getId();
        String changeId1 = toSave.changeSessionId();
        String changeId2 = toSave.changeSessionId();

        repo.save(toSave);

        assertThat(repo.findById(originalId)).isNull();
        assertThat(repo.findById(changeId1)).isNull();
        assertThat(repo.findById(changeId2)).isNotNull();

        repo.deleteById(changeId2);
    }

    /** */
    @Test
    void changeSessionIdWhenSetAttributeOnChangedSession() {
        String attrName = "changeSessionId";
        String attrVal = "changeSessionId-value";

        IgniteSession toSave = repo.createSession();

        repo.save(toSave);

        IgniteSession findById = repo.findById(toSave.getId());

        findById.setAttribute(attrName, attrVal);

        String originalFindById = findById.getId();
        String changeSesId = findById.changeSessionId();

        repo.save(findById);

        assertThat(repo.findById(originalFindById)).isNull();

        IgniteSession findByChangeSesId = repo.findById(changeSesId);

        assertThat(findByChangeSesId.<String>getAttribute(attrName)).isEqualTo(attrVal);

        repo.deleteById(changeSesId);
    }

    /** */
    @Test
    void changeSessionIdWhenHasNotSaved() {
        IgniteSession toSave = repo.createSession();
        String originalId = toSave.getId();
        toSave.changeSessionId();

        repo.save(toSave);

        assertThat(repo.findById(toSave.getId())).isNotNull();
        assertThat(repo.findById(originalId)).isNull();

        repo.deleteById(toSave.getId());
    }

    /** */
    @Test
    void attemptToUpdateSessionAfterDelete() {
        IgniteSession ses = repo.createSession();
        String sesId = ses.getId();
        repo.save(ses);
        ses = repo.findById(sesId);
        ses.setAttribute("attributeName", "attributeValue");
        repo.deleteById(sesId);
        repo.save(ses);

        assertThat(repo.findById(sesId)).isNull();
    }

    /** */
    @Test
    void expireSession() {
        IgniteSession ses = repo.createSession();
        String sesId = ses.getId();

        ses.setMaxInactiveInterval(Duration.ofNanos(0));

        repo.save(ses);
        assertThat(repo.findById(sesId)).isNull();
    }

    /** */
    @Test
    void createAndUpdateSession() {
        IgniteSession ses = repo.createSession();
        String sesId = ses.getId();

        repo.save(ses);

        ses = repo.findById(sesId);
        ses.setAttribute("attributeName", "attributeValue");

        repo.save(ses);

        assertThat(repo.findById(sesId)).isNotNull();
    }

    /** */
    @Test
    void createSessionWithSecurityContextAndFindById() {
        IgniteSession ses = repo.createSession();
        String sesId = ses.getId();

        Authentication authentication = new UsernamePasswordAuthenticationToken("saves-" + System.currentTimeMillis(),
                "password", AuthorityUtils.createAuthorityList("ROLE_USER"));
        SecurityContext securityCtx = SecurityContextHolder.createEmptyContext();
        securityCtx.setAuthentication(authentication);
        ses.setAttribute(SPRING_SECURITY_CONTEXT, securityCtx);

        repo.save(ses);

        assertThat(repo.findById(sesId)).isNotNull();
    }

    /** */
    @Test
    void createSessionWithSecurityContextAndFindByPrincipal() {
        IgniteSession ses = repo.createSession();

        String username = "saves-" + System.currentTimeMillis();
        Authentication authentication = new UsernamePasswordAuthenticationToken(username, "password",
                AuthorityUtils.createAuthorityList("ROLE_USER"));
        SecurityContext securityCtx = SecurityContextHolder.createEmptyContext();
        securityCtx.setAuthentication(authentication);
        ses.setAttribute(SPRING_SECURITY_CONTEXT, securityCtx);

        repo.save(ses);

        assertThat(repo
                .findByIndexNameAndIndexValue(FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME, username))
                .hasSize(1);
    }
}
