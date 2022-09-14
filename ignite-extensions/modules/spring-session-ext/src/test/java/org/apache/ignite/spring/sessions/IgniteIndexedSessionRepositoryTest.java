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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spring.sessions.IgniteIndexedSessionRepository.IgniteSession;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.FlushMode;
import org.springframework.session.MapSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for {@link IgniteIndexedSessionRepository}.
 */
public class IgniteIndexedSessionRepositoryTest {
    /** */
    private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

    /** */
    private final Ignite ignite = mock(Ignite.class);

    /** */
    @SuppressWarnings("unchecked")
    private final IgniteCache<String, IgniteSession> sessions = mock(IgniteCache.class);

    /** */
    private IgniteIndexedSessionRepository repository;

    /** */
    @BeforeEach
    void setUp() {
        given(this.ignite.<String, IgniteSession>getOrCreateCache(
                ArgumentMatchers.<CacheConfiguration<String, IgniteSession>>any())).willReturn(this.sessions);
        given(this.sessions.withExpiryPolicy(ArgumentMatchers.any())).willReturn(this.sessions);
        this.repository = new IgniteIndexedSessionRepository(this.ignite);
        this.repository.init();
    }

    /** */
    @Test
    void constructorNullIgnite() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IgniteIndexedSessionRepository(null))
                .withMessage("Ignite must not be null");
    }

    /** */
    @Test
    void setSaveModeNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.repository.setSaveMode(null))
                .withMessage("saveMode must not be null");
    }

    /** */
    @Test
    void createSessionDefaultMaxInactiveInterval() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();

        assertThat(session.getMaxInactiveInterval()).isEqualTo(new MapSession().getMaxInactiveInterval());
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void createSessionCustomMaxInactiveInterval() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        int interval = 1;
        this.repository.setDefaultMaxInactiveInterval(interval);

        IgniteSession session = this.repository.createSession();

        assertThat(session.getMaxInactiveInterval()).isEqualTo(Duration.ofSeconds(interval));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveNewFlushModeOnSave() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();
        verifyNoMoreInteractions(this.sessions);

        this.repository.save(session);
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveNewFlushModeImmediate() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        this.repository.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession session = this.repository.createSession();
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUpdatedAttributeFlushModeOnSave() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();
        session.setAttribute("testName", "testValue");
        verifyNoMoreInteractions(this.sessions);

        this.repository.save(session);
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUpdatedAttributeFlushModeImmediate() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        this.repository.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession session = this.repository.createSession();
        session.setAttribute("testName", "testValue");
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).replace(eq(session.getId()), eq(session));

        this.repository.save(session);
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void removeAttributeFlushModeOnSave() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();
        session.removeAttribute("testName");
        verifyNoMoreInteractions(this.sessions);

        this.repository.save(session);
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void removeAttributeFlushModeImmediate() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        this.repository.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession session = this.repository.createSession();
        session.removeAttribute("testName");
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).replace(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));

        this.repository.save(session);
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUpdatedLastAccessedTimeFlushModeOnSave() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();
        session.setLastAccessedTime(Instant.now());
        verifyNoMoreInteractions(this.sessions);

        this.repository.save(session);
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUpdatedLastAccessedTimeFlushModeImmediate() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        this.repository.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession session = this.repository.createSession();
        session.setLastAccessedTime(Instant.now());
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).replace(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));

        this.repository.save(session);
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUpdatedMaxInactiveIntervalInSecondsFlushModeOnSave() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();
        session.setMaxInactiveInterval(Duration.ofSeconds(1));
        verifyNoMoreInteractions(this.sessions);

        this.repository.save(session);
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUpdatedMaxInactiveIntervalInSecondsFlushModeImmediate() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        this.repository.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession session = this.repository.createSession();
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));
        String sessionId = session.getId();
        session.setMaxInactiveInterval(Duration.ofSeconds(1));
        verify(this.sessions, times(1)).put(eq(sessionId), eq(session));
        verify(this.sessions, times(1)).replace(eq(sessionId), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));

        this.repository.save(session);
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUnchangedFlushModeOnSave() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession session = this.repository.createSession();
        this.repository.save(session);
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));

        this.repository.save(session);
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void saveUnchangedFlushModeImmediate() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        this.repository.setFlushMode(FlushMode.IMMEDIATE);

        IgniteSession session = this.repository.createSession();
        verify(this.sessions, times(1)).put(eq(session.getId()), eq(session));
        verify(this.sessions, times(1)).withExpiryPolicy(eq(createExpiryPolicy(session)));

        this.repository.save(session);
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void getSessionNotFound() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        String sessionId = "testSessionId";

        IgniteSession session = this.repository.findById(sessionId);

        assertThat(session).isNull();
        verify(this.sessions, times(1)).get(eq(sessionId));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void getSessionExpired() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession expired = this.repository.new IgniteSession(new MapSession(), true);

        expired.setLastAccessedTime(Instant.now().minusSeconds(MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS + 1));
        given(this.sessions.get(eq(expired.getId()))).willReturn(expired);

        IgniteSession session = this.repository.findById(expired.getId());

        assertThat(session).isNull();
        verify(this.sessions, times(1)).get(eq(expired.getId()));
        verify(this.sessions, times(1)).remove(eq(expired.getId()));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void getSessionFound() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        IgniteSession saved = this.repository.new IgniteSession(new MapSession(), true);
        saved.setAttribute("savedName", "savedValue");
        given(this.sessions.get(eq(saved.getId()))).willReturn(saved);

        IgniteSession session = this.repository.findById(saved.getId());

        assertThat(session.getId()).isEqualTo(saved.getId());
        assertThat(session.<String>getAttribute("savedName")).isEqualTo("savedValue");
        verify(this.sessions, times(1)).get(eq(saved.getId()));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void delete() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        String sessionId = "testSessionId";

        this.repository.deleteById(sessionId);

        verify(this.sessions, times(1)).remove(eq(sessionId));
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void findByIndexNameAndIndexValueUnknownIndexName() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        String indexValue = "testIndexValue";

        Map<String, IgniteSession> sessions = this.repository.findByIndexNameAndIndexValue("testIndexName", indexValue);

        assertThat(sessions).isEmpty();
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void findByIndexNameAndIndexValuePrincipalIndexNameNotFound() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        String principal = "username";

        Map<String, IgniteSession> sessions = this.repository
                .findByIndexNameAndIndexValue(FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME, principal);

        verify(this.sessions, times(1)).query(ArgumentMatchers
                .argThat((argument) -> ("SELECT * FROM IgniteSession WHERE principal='" + principal + "'")
                        .equals(argument.getSql())));

        assertThat(sessions).isEmpty();
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void findByIndexNameAndIndexValuePrincipalIndexNameFound() {
        verify(this.sessions, times(1)).registerCacheEntryListener(ArgumentMatchers.any());

        String principal = "username";
        Authentication authentication = new UsernamePasswordAuthenticationToken(principal, "notused",
                AuthorityUtils.createAuthorityList("ROLE_USER"));

        List<Object> saved = new ArrayList<>(2);

        final MapSession ses1 = new MapSession();
        ses1.setAttribute(SPRING_SECURITY_CONTEXT, authentication);
        IgniteSession saved1 = this.repository.new IgniteSession(ses1, true);
        saved.add(Arrays.asList(ses1, authentication.getPrincipal()));
        final MapSession ses2 = new MapSession();
        ses2.setAttribute(SPRING_SECURITY_CONTEXT, authentication);
        IgniteSession saved2 = this.repository.new IgniteSession(ses2, true);
        saved.add(Arrays.asList(ses2, authentication.getPrincipal()));

        given(this.sessions.query(ArgumentMatchers.any())).willReturn(new FieldsQueryCursor<List<?>>() {
            /** */
            @Override public String getFieldName(int idx) {
                return null;
            }

            /** */
            @Override public int getColumnsCount() {
                return 2;
            }

            /** */
            @Override public List<List<?>> getAll() {
                return (List)saved;
            }

            /** */
            @Override public void close() {
            }

            /** */
            @NotNull
            @Override public Iterator<List<?>> iterator() {
                return (Iterator)saved.iterator();
            }
        });

        Map<String, IgniteSession> sessions = this.repository
                .findByIndexNameAndIndexValue(FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME, principal);

        assertThat(sessions).hasSize(2);
        verify(this.sessions, times(1)).query(any());
        verifyNoMoreInteractions(this.sessions);
    }

    /** */
    @Test
    void getAttributeNamesAndRemove() {
        IgniteSession session = this.repository.createSession();
        session.setAttribute("attribute1", "value1");
        session.setAttribute("attribute2", "value2");

        for (String attributeName : session.getAttributeNames()) {
            session.removeAttribute(attributeName);
        }

        assertThat(session.getAttributeNames()).isEmpty();
    }

    /** */
    private static TouchedExpiryPolicy createExpiryPolicy(IgniteSession session) {
        return new TouchedExpiryPolicy(
                new javax.cache.expiry.Duration(TimeUnit.SECONDS, session.getMaxInactiveInterval().getSeconds()));
    }

}
