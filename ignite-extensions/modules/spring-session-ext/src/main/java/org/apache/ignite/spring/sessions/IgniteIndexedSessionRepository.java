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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.spring.sessions.proxy.SessionProxy;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.session.DelegatingIndexResolver;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.FlushMode;
import org.springframework.session.IndexResolver;
import org.springframework.session.MapSession;
import org.springframework.session.PrincipalNameIndexResolver;
import org.springframework.session.SaveMode;
import org.springframework.session.Session;
import org.springframework.session.events.AbstractSessionEvent;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionExpiredEvent;
import org.springframework.util.Assert;

import static java.util.Collections.emptyMap;

/**
 * A {@link org.springframework.session.SessionRepository} implementation that stores
 * sessions in Apache Ignite distributed {@link SessionProxy}.
 *
 * <p>
 * An example of how to create a new instance can be seen below:
 * <pre class="code">
 * IgniteConfiguration config = new IgniteConfiguration();
 * // ... configure Ignite ...
 * Ignite ignite = IgnitionEx.start(config);
 * IgniteIndexedSessionRepository sessionRepository =
 *         new IgniteIndexedSessionRepository(ignite);
 * </pre>
 *
 * In order to support finding sessions by principal name using
 * {@link #findByIndexNameAndIndexValue(String, String)} method, custom configuration of
 * {@link SessionProxy} supplied to this implementation is required.
 * This implementation listens for events on the Ignite-backed SessionRepository and
 * translates those events into the corresponding Spring Session events. Publish the
 * Spring Session events with the given {@link ApplicationEventPublisher}.
 *
 * <ul>
 * <li>entryAdded - {@link SessionCreatedEvent}</li>
 * <li>entryEvicted - {@link SessionExpiredEvent}</li>
 * <li>entryRemoved - {@link SessionDeletedEvent}</li>
 * </ul>
 *
 */
public class IgniteIndexedSessionRepository
        implements FindByIndexNameSessionRepository<IgniteSession>,
        CacheEntryCreatedListener<String, IgniteSession>,
        CacheEntryRemovedListener<String, IgniteSession>,
        CacheEntryExpiredListener<String, IgniteSession> {
    /**
     * The default name of map used by Spring Session to store sessions.
     */
    public static final String DEFAULT_SESSION_MAP_NAME = "spring:session:sessions";

    /**
     * Maximum of attempts for atomicity replace. If something wrong with IgniteSession, old value can never be equal to
     * value from repository. In this case replace will never end the loop. If this value is exceeded, then plain
     * {@link SessionProxy#replace(String, IgniteSession)} will be used.
     */
    private static final int MAX_UPDATE_ATTEMPT = 100;

    /** */
    private static final Log logger = LogFactory.getLog(IgniteIndexedSessionRepository.class);

    /** */
    private ApplicationEventPublisher evtPublisher = (event) -> {};

    /**
     * If non-null, this value is used to override
     * {@link MapSession#setMaxInactiveInterval(Duration)}.
     */
    private Integer dfltMaxInactiveInterval;

    /** */
    private FlushMode flushMode = FlushMode.ON_SAVE;

    /** The save mode. */
    private SaveMode saveMode = SaveMode.ALWAYS;

    /** The index resolver. */
    private IndexResolver<Session> idxResolver = new DelegatingIndexResolver<>(new PrincipalNameIndexResolver<>());

    /** Sessions cache proxy. */
    private final SessionProxy sessions;

    /** */
    private final CacheEntryListenerConfiguration<String, IgniteSession> listenerConfiguration;

    /**
     * Create a new {@link IgniteIndexedSessionRepository} instance.
     * @param sessions the {@link SessionProxy} instance to use for managing sessions
     */
    public IgniteIndexedSessionRepository(SessionProxy sessions) {
        Assert.notNull(sessions, "Session proxy must not be null");
        this.sessions = sessions;
        
        this.listenerConfiguration = new CacheEntryListenerConfiguration<String, IgniteSession>() {
            /** {@inheritDoc} */
            @Override public Factory<CacheEntryListener<? super String, ? super IgniteSession>> getCacheEntryListenerFactory() {
                return () -> IgniteIndexedSessionRepository.this;
            }

            /** {@inheritDoc} */
            @Override public boolean isOldValueRequired() {
                return true;
            }

            /** {@inheritDoc} */
            @Override public Factory<CacheEntryEventFilter<? super String, ? super IgniteSession>> getCacheEntryEventFilterFactory() {
                return null;
            }

            /** {@inheritDoc} */
            @Override public boolean isSynchronous() {
                return false;
            }
        };

        this.sessions.registerCacheEntryListener(this.listenerConfiguration);
    }

    /** */
    @PreDestroy
    public void close() {
        this.sessions.deregisterCacheEntryListener(this.listenerConfiguration);
    }

    /**
     * Sets the {@link ApplicationEventPublisher} that is used to publish
     * {@link AbstractSessionEvent session events}. The default is to not publish session
     * events.
     * @param applicationEvtPublisher the {@link ApplicationEventPublisher} that is used
     * to publish session events. Cannot be null.
     */
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEvtPublisher) {
        Assert.notNull(applicationEvtPublisher, "ApplicationEventPublisher cannot be null");
        evtPublisher = applicationEvtPublisher;
    }

    /**
     * Set the maximum inactive interval in seconds between requests before newly created
     * sessions will be invalidated. A negative time indicates that the session will never timeout.
     * The default is 1800 (30 minutes).
     * @param dfltMaxInactiveInterval the maximum inactive interval in seconds
     */
    public void setDefaultMaxInactiveInterval(Integer dfltMaxInactiveInterval) {
        this.dfltMaxInactiveInterval = dfltMaxInactiveInterval;
    }

    /**
     * Set the {@link IndexResolver} to use.
     * @param idxResolver the index resolver
     */
    public void setIndexResolver(IndexResolver<Session> idxResolver) {
        Assert.notNull(idxResolver, "indexResolver cannot be null");
        
        this.idxResolver = idxResolver;
    }

    /**
     * Sets the flush mode. Default flush mode is {@link FlushMode#ON_SAVE}.
     * @param flushMode the new flush mode
     */
    public void setFlushMode(FlushMode flushMode) {
        Assert.notNull(flushMode, "flushMode cannot be null");
        this.flushMode = flushMode;
    }

    /**
     * Set the save mode.
     * @param saveMode the save mode
     */
    public void setSaveMode(SaveMode saveMode) {
        Assert.notNull(saveMode, "saveMode must not be null");
        this.saveMode = saveMode;
    }

    /** {@inheritDoc} */
    @Override public IgniteSession createSession() {
        MapSession cached = new MapSession();
        
        if (this.dfltMaxInactiveInterval != null)
            cached.setMaxInactiveInterval(Duration.ofSeconds(this.dfltMaxInactiveInterval));

        return new IgniteSession(cached, idxResolver, true, saveMode, this::flushImmediateIfNecessary);
    }

    /** {@inheritDoc} */
    @Override public void save(IgniteSession ses) {
        if (ses.isNew())
            ttlSessions(ses.getMaxInactiveInterval()).put(ses.getId(), ses);
        else {
            String originalId = ses.getOriginalId();

            if (!ses.getId().equals(originalId)) {
                sessions.remove(originalId);

                ses.resetOriginalId();
                ttlSessions(ses.getMaxInactiveInterval()).put(ses.getId(), ses);
            }
            else if (ses.hasChanges()) {
                if (saveMode == SaveMode.ALWAYS)
                    ttlSessions(ses.getMaxInactiveInterval()).replace(ses.getId(), ses);
                else
                    updatePartial(ses);
            }
        }
        
        ses.clearChangeFlags();
    }

    /** {@inheritDoc} */
    @Override public IgniteSession findById(String id) {
        IgniteSession saved = sessions.get(id);
        
        if (saved == null)
            return null;

        if (saved.isExpired()) {
            deleteById(saved.getId());
            
            return null;
        }

        return new IgniteSession(saved.getDelegate(), idxResolver, false, saveMode, this::flushImmediateIfNecessary);
    }

    /** {@inheritDoc} */
    @Override public void deleteById(String id) {
        this.sessions.remove(id);
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgniteSession> findByIndexNameAndIndexValue(String idxName, String idxVal) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(idxName))
            return emptyMap();

        QueryCursor<List<?>> cursor = sessions.query(
            new SqlFieldsQuery("SELECT * FROM IgniteSession WHERE principal = ?").setArgs(idxVal)
        );

        if (cursor == null)
            return emptyMap();

        return cursor.getAll().stream()
            .map(res -> (MapSession)res.get(1))
            .collect(Collectors.toMap(
                MapSession::getId,
                ses -> new IgniteSession(ses, idxResolver, false, saveMode, this::flushImmediateIfNecessary)
            ));
    }

    /** {@inheritDoc} */
    @Override public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends IgniteSession>> events)
            throws CacheEntryListenerException {
        events.forEach((event) -> {
            IgniteSession ses = event.getValue();
            
            if (ses.getId().equals(ses.getDelegate().getOriginalId())) {
                if (logger.isDebugEnabled())
                    logger.debug("Session created with id: " + ses.getId());

                evtPublisher.publishEvent(new SessionCreatedEvent(this, ses));
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends IgniteSession>> events)
        throws CacheEntryListenerException {
        events.forEach((event) -> {
            if (logger.isDebugEnabled())
                logger.debug("Session expired with id: " + event.getOldValue().getId());

            evtPublisher.publishEvent(new SessionExpiredEvent(this, event.getOldValue()));
        });
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends IgniteSession>> events)
            throws CacheEntryListenerException {
        events.forEach((event) -> {
            IgniteSession ses = event.getOldValue();
            
            if (ses != null) {
                if (logger.isDebugEnabled())
                    logger.debug("Session deleted with id: " + ses.getId());

                evtPublisher.publishEvent(new SessionDeletedEvent(this, ses));
            }
        });
    }

    /**
     * Get cache view with custom duration expiry policy.
     * @param duration expiry duration for IgniteSession.
     * @return cache with custom duration expiry policy.
     */
    private SessionProxy ttlSessions(Duration duration) {
        return sessions.withExpiryPolicy(createPolicy(duration));
    }

    /**
     * Create expiry policy from {@link Duration}.
     * @param duration expiry duration.
     * @return expiry policy.
     */
    private static TouchedExpiryPolicy createPolicy(Duration duration) {
        return new TouchedExpiryPolicy(new javax.cache.expiry.Duration(TimeUnit.SECONDS, duration.getSeconds()));
    }

    /**
     * Creates a new Session that is capable of being persisted by this SessionRepository.
     * 
     * @param ses Session.
     */
    private void flushImmediateIfNecessary(IgniteSession ses) {
        if (flushMode == FlushMode.IMMEDIATE)
            save(ses);
    }

    /**
     * @param targetSes Target session.
     * @param activeSes Active session.
     */
    private void copyChanges(IgniteSession targetSes, IgniteSession activeSes) {
        if (activeSes.isLastAccessedTimeChanged())
            targetSes.setLastAccessedTime(activeSes.getLastAccessedTime());

        Map<String, Object> changes = activeSes.getAttributesChanges();

        if (!changes.isEmpty())
            changes.forEach(targetSes::setAttribute);
    }

    /**
     * @param ses Session.
     */
    private void updatePartial(IgniteSession ses) {
        IgniteSession oldSes, updatedSes;
        int attempt = 0;

        do {
            attempt++;

            oldSes = sessions.get(ses.getId());

            if (oldSes == null)
                break;

            updatedSes = new IgniteSession(oldSes.getDelegate(), idxResolver, false, saveMode, this::flushImmediateIfNecessary);
            copyChanges(updatedSes, ses);

            if (attempt > MAX_UPDATE_ATTEMPT) {
                logger.warn("Session maximum update attempts has been reached," +
                    " 'replace' will be used instead [id=" + updatedSes.getId() + "]");

                ttlSessions(ses.getMaxInactiveInterval()).replace(ses.getId(), updatedSes);
                break;
            }
        } while (ttlSessions(ses.getMaxInactiveInterval()).replace(ses.getId(), oldSes, updatedSes));
    }
}
