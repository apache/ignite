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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.internal.GridDirectTransient;
import org.springframework.session.IndexResolver;
import org.springframework.session.MapSession;
import org.springframework.session.SaveMode;
import org.springframework.session.Session;

import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;
import static org.springframework.session.SaveMode.ON_GET_ATTRIBUTE;

/**
 * A custom implementation of {@link Session} that uses a {@link MapSession} as the basis for its mapping. It keeps
 * track if changes have been made since last save.
 */
public class IgniteSession implements Session {
    /** */
    public static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

    /** The map session. */
    private MapSession delegate;

    /** Cached principal name for query. */
    @SuppressWarnings("unused")
    private String principal;

    /** */
    @GridDirectTransient
    private transient boolean isNew;

    /** */
    @GridDirectTransient
    private transient boolean lastAccessedTimeChanged;

    /** */
    @GridDirectTransient
    private transient boolean maxInactiveIntervalChanged;

    /** */
    @GridDirectTransient
    private final transient Map<String, Object> delta = new HashMap<>();

    /** The index resolver. */
    @GridDirectTransient
    private final transient IndexResolver<Session> idxResolver;

    /** Session save mode. */
    @GridDirectTransient
    private final transient SaveMode saveMode;

    /** */
    @GridDirectTransient
    private final transient Consumer<IgniteSession> flusher;

    /**
     * @param delegate The map session.
     * @param idxResolver The index resolver.
     * @param isNew Is new flag.
     * @param saveMode Mode of tracking and saving session changes to session store.
     * @param flusher Flusher for session store.
     */
    IgniteSession(
        MapSession delegate,
        IndexResolver<Session> idxResolver,
        boolean isNew,
        SaveMode saveMode,
        Consumer<IgniteSession> flusher
    ) {
        this.delegate = delegate;
        this.isNew = isNew;

        this.idxResolver = idxResolver;
        this.saveMode = saveMode;
        this.flusher = flusher;

        principal = this.delegate.getAttribute(PRINCIPAL_NAME_INDEX_NAME);

        if (this.isNew || this.saveMode == SaveMode.ALWAYS)
            getAttributeNames().forEach(attrName -> delta.put(attrName, this.delegate.getAttribute(attrName)));

        if (isNew)
            this.flusher.accept(this);
    }

    /** {@inheritDoc} */
    @Override public void setLastAccessedTime(Instant lastAccessedTime) {
        delegate.setLastAccessedTime(lastAccessedTime);
        lastAccessedTimeChanged = true;

        flusher.accept(this);
    }

    /** {@inheritDoc} */
    @Override public boolean isExpired() {
        return delegate.isExpired();
    }

    /** {@inheritDoc} */
    @Override public Instant getCreationTime() {
        return delegate.getCreationTime();
    }

    /** {@inheritDoc} */
    @Override public String getId() {
        return delegate.getId();
    }

    /** {@inheritDoc} */
    @Override public String changeSessionId() {
        return delegate.changeSessionId();
    }

    /** {@inheritDoc} */
    @Override public Instant getLastAccessedTime() {
        return delegate.getLastAccessedTime();
    }

    /** {@inheritDoc} */
    @Override public void setMaxInactiveInterval(Duration interval) {
        delegate.setMaxInactiveInterval(interval);
        maxInactiveIntervalChanged = true;

        flusher.accept(this);
    }

    /** {@inheritDoc} */
    @Override public Duration getMaxInactiveInterval() {
        return delegate.getMaxInactiveInterval();
    }

    /** {@inheritDoc} */
    @Override public <T> T getAttribute(String attrName) {
        T attrVal = this.delegate.getAttribute(attrName);

        if (attrVal != null && saveMode.equals(ON_GET_ATTRIBUTE))
            delta.put(attrName, attrVal);

        return attrVal;
    }

    /** {@inheritDoc} */
    @Override public Set<String> getAttributeNames() {
        return this.delegate.getAttributeNames();
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(String attrName, Object attrVal) {
        delegate.setAttribute(attrName, attrVal);
        delta.put(attrName, attrVal);

        if (SPRING_SECURITY_CONTEXT.equals(attrName)) {
            Map<String, String> indexes = idxResolver.resolveIndexesFor(this);
            String principal = (attrVal != null) ? indexes.get(PRINCIPAL_NAME_INDEX_NAME) : null;

            this.principal = principal;
            
            delegate.setAttribute(PRINCIPAL_NAME_INDEX_NAME, principal);
            delta.put(PRINCIPAL_NAME_INDEX_NAME, principal);
        }

        flusher.accept(this);
    }

    /** {@inheritDoc} */
    @Override public void removeAttribute(String attrName) {
        setAttribute(attrName, null);
    }

    /**
     * @return Is new session.
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * @return Internal session object.
     */
    public MapSession getDelegate() {
        return delegate;
    }

    /**
     * @return {@code True} if session is changed.
     */
    public Map<String, Object> getAttributesChanges() {
        return new HashMap<>(delta);
    }

    /**
     * Get the original session id.
     * @return the original session id.
     * @see #changeSessionId()
     */
    public String getOriginalId() {
        return delegate.getOriginalId();
    }

    /**
     * Reset the original session id.
     * @see #changeSessionId()
     */
    public void resetOriginalId() {
        delegate = new MapSession(delegate);
    }

    /** Reset the change flags. */
    public void clearChangeFlags() {
        isNew = false;
        lastAccessedTimeChanged = false;
        maxInactiveIntervalChanged = false;
        delta.clear();
    }

    /**
     * @return Last accessed time changed.
     */
    public boolean isLastAccessedTimeChanged() {
        return lastAccessedTimeChanged;
    }

    /**
     * @return Session changed.
     */
    public boolean hasChanges() {
        return lastAccessedTimeChanged || maxInactiveIntervalChanged || !delta.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IgniteSession ses = (IgniteSession)o;
        
        return delegate.equals(ses.delegate);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(delegate);
    }
}
