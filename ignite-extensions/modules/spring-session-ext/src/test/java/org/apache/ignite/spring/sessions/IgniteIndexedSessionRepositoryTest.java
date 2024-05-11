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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.spring.sessions.proxy.SessionProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.session.FlushMode;
import org.springframework.session.MapSession;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

/**
 * Tests for {@link IgniteIndexedSessionRepository}.
 */
public class IgniteIndexedSessionRepositoryTest {
    /** */
    private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

    /** */
    private final SessionProxy sessions = mock(SessionProxy.class);

    /** */
    private IgniteIndexedSessionRepository repo;

    /** */
    @BeforeEach
    void setUp() {
        given(sessions.withExpiryPolicy(any())).willReturn(sessions);
        
        repo = new IgniteIndexedSessionRepository(sessions);
    }

    /** */
    @Test
    void constructorNullIgnite() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IgniteIndexedSessionRepository(null))
                .withMessage("Session proxy must not be null");
    }

    /** */
    @Test
    void setSaveModeNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> repo.setSaveMode(null))
                .withMessage("saveMode must not be null");
    }

    /** */
    @Test
    void createSessionDefaultMaxInactiveInterval() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();

        assertThat(ses.getMaxInactiveInterval()).isEqualTo(new MapSession().getMaxInactiveInterval());
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void createSessionCustomMaxInactiveInterval() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        int interval = 1;
        repo.setDefaultMaxInactiveInterval(interval);

        IgniteSession ses = repo.createSession();

        assertThat(ses.getMaxInactiveInterval()).isEqualTo(Duration.ofSeconds(interval));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveNewFlushModeOnSave() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();
        verifyNoMoreInteractions(sessions);

        repo.save(ses);
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveNewFlushModeImmediate() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        repo.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession ses = repo.createSession();
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUpdatedAttributeFlushModeOnSave() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();
        ses.setAttribute("testName", "testValue");
        verifyNoMoreInteractions(sessions);

        repo.save(ses);
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUpdatedAttributeFlushModeImmediate() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        repo.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession ses = repo.createSession();
        ses.setAttribute("testName", "testValue");
        verify(sessions, times(2)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).replace(eq(ses.getId()), eq(ses));

        repo.save(ses);
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void removeAttributeFlushModeOnSave() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();
        ses.removeAttribute("testName");
        verifyNoMoreInteractions(sessions);

        repo.save(ses);
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void removeAttributeFlushModeImmediate() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        repo.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession ses = repo.createSession();
        ses.removeAttribute("testName");
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).replace(eq(ses.getId()), eq(ses));
        verify(sessions, times(2)).withExpiryPolicy(eq(createExpiryPolicy(ses)));

        repo.save(ses);
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUpdatedLastAccessedTimeFlushModeOnSave() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();
        ses.setLastAccessedTime(Instant.now());
        verifyNoMoreInteractions(sessions);

        repo.save(ses);
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUpdatedLastAccessedTimeFlushModeImmediate() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        repo.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession ses = repo.createSession();
        ses.setLastAccessedTime(Instant.now());
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).replace(eq(ses.getId()), eq(ses));
        verify(sessions, times(2)).withExpiryPolicy(eq(createExpiryPolicy(ses)));

        repo.save(ses);
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUpdatedMaxInactiveIntervalInSecondsFlushModeOnSave() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();
        ses.setMaxInactiveInterval(Duration.ofSeconds(1));
        verifyNoMoreInteractions(sessions);

        repo.save(ses);
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUpdatedMaxInactiveIntervalInSecondsFlushModeImmediate() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        repo.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession ses = repo.createSession();
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));
        String sesId = ses.getId();
        ses.setMaxInactiveInterval(Duration.ofSeconds(1));
        verify(sessions, times(1)).put(eq(sesId), eq(ses));
        verify(sessions, times(1)).replace(eq(sesId), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));

        repo.save(ses);
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUnchangedFlushModeOnSave() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession ses = repo.createSession();
        repo.save(ses);
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));

        repo.save(ses);
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void saveUnchangedFlushModeImmediate() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        repo.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession ses = repo.createSession();
        verify(sessions, times(1)).put(eq(ses.getId()), eq(ses));
        verify(sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(ses)));

        repo.save(ses);
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void getSessionNotFound() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        String sesId = "testSessionId";

        IgniteSession ses = repo.findById(sesId);

        assertThat(ses).isNull();
        verify(sessions, times(1)).get(eq(sesId));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void getSessionExpired() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession expired = repo.createSession();

        expired.setLastAccessedTime(Instant.now().minusSeconds(MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS + 1));
        given(sessions.get(eq(expired.getId()))).willReturn(expired);

        IgniteSession ses = repo.findById(expired.getId());

        assertThat(ses).isNull();
        verify(sessions, times(1)).get(eq(expired.getId()));
        verify(sessions, times(1)).remove(eq(expired.getId()));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void getSessionFound() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        IgniteSession saved = repo.createSession();
        saved.setAttribute("savedName", "savedValue");
        given(sessions.get(eq(saved.getId()))).willReturn(saved);

        IgniteSession ses = repo.findById(saved.getId());

        assertThat(ses.getId()).isEqualTo(saved.getId());
        assertThat(ses.<String>getAttribute("savedName")).isEqualTo("savedValue");
        verify(sessions, times(1)).get(eq(saved.getId()));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void delete() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        String sesId = "testSessionId";

        repo.deleteById(sesId);

        verify(sessions, times(1)).remove(eq(sesId));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void findByIndexNameAndIndexValueUnknownIndexName() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        String idxVal = "testIndexValue";

        Map<String, IgniteSession> sesMap = repo.findByIndexNameAndIndexValue("testIndexName", idxVal);

        assertThat(sesMap).isEmpty();
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void findByIndexNameAndIndexValuePrincipalIndexNameNotFound() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        String principal = "username";

        Map<String, IgniteSession> sesMap = repo.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, principal);

        assertThat(sesMap).isEmpty();

        verify(sessions, times(1)).query(any(Query.class));
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void findByIndexNameAndIndexValuePrincipalIndexNameFound() {
        verify(sessions, times(1)).registerCacheEntryListener(any());

        String principal = "username";
        Authentication authentication = new UsernamePasswordAuthenticationToken(principal, "notused",
                AuthorityUtils.createAuthorityList("ROLE_USER"));

        List<Object> saved = new ArrayList<>(2);

        final MapSession ses1 = new MapSession();
        ses1.setAttribute(SPRING_SECURITY_CONTEXT, authentication);
        saved.add(asList(UUID.randomUUID().toString(), ses1, authentication.getPrincipal()));

        final MapSession ses2 = new MapSession();
        ses2.setAttribute(SPRING_SECURITY_CONTEXT, authentication);
        saved.add(asList(UUID.randomUUID().toString(), ses2, authentication.getPrincipal()));

        given(sessions.<List<?>>query(any())).willReturn(new QueryCursor<List<?>>() {
            /** {@inheritDoc} */
            @Override public List<List<?>> getAll() {
                return (List)saved;
            }

            /** {@inheritDoc} */
            @Override public void close() {
            }

            /** {@inheritDoc} */
            @NotNull
            @Override public Iterator<List<?>> iterator() {
                return (Iterator)saved.iterator();
            }
        });

        Map<String, IgniteSession> sesMap = repo.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, principal);

        assertThat(sesMap).hasSize(2);
        verify(sessions, times(1)).query(any());
        verifyNoMoreInteractions(sessions);
    }

    /** */
    @Test
    void getAttributeNamesAndRemove() {
        IgniteSession ses = repo.createSession();
        ses.setAttribute("attribute1", "value1");
        ses.setAttribute("attribute2", "value2");

        for (String attrName : ses.getAttributeNames())
            ses.removeAttribute(attrName);

        assertThat(ses.getAttributeNames()).isEmpty();
    }

    /** */
    private static TouchedExpiryPolicy createExpiryPolicy(IgniteSession ses) {
        return new TouchedExpiryPolicy(
                new javax.cache.expiry.Duration(TimeUnit.SECONDS, ses.getMaxInactiveInterval().getSeconds()));
    }
}
