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

package org.apache.ignite.cache.websession;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.websession.WebSessionEntity;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Session implementation that uses internal entity, which stores binary attributes,
 * for safe caching and processing on remote nodes.
 */
class WebSessionV2 implements HttpSession {
    /** Empty session context. */
    @SuppressWarnings("deprecation")
    private static final HttpSessionContext EMPTY_SES_CTX = new HttpSessionContext() {
        @Nullable @Override public HttpSession getSession(String id) {
            return null;
        }

        @Override public Enumeration<String> getIds() {
            return Collections.enumeration(Collections.<String>emptyList());
        }
    };

    /** Placeholder for removed attribute. */
    private static final Object REMOVED_ATTR = new Object();

    /** Servlet context. */
    @GridToStringExclude
    private final ServletContext ctx;

    /** Entity that holds binary attributes. */
    private WebSessionEntity entity;

    /** Attributes. */
    protected Map<String, Object> attrs;

    /** Timestamp that shows when this object was created. (Last access time from user request) */
    private final long accessTime;

    /** Cached session TTL since last query. */
    private int maxInactiveInterval;

    /** Flag indicates if {@link #maxInactiveInterval} waiting for update in cache. */
    private boolean maxInactiveIntervalChanged;

    /** New session flag. */
    private boolean isNew;

    /** Session invalidation flag. */
    private boolean invalidated;

    /** Grid marshaller. */
    private final Marshaller marsh;

    /** Original session to delegate invalidation. */
    private final HttpSession genuineSes;

    /**
     * Constructs new web session.
     *
     * @param id Session ID.
     * @param ses Session.
     * @param isNew Is new flag.
     * @param ctx Servlet context.
     * @param entity Entity.
     * @param marsh Marshaller.
     */
    WebSessionV2(final String id, final @Nullable HttpSession ses, final boolean isNew, final ServletContext ctx,
        @Nullable WebSessionEntity entity, final Marshaller marsh) {
        assert id != null;
        assert marsh != null;
        assert ctx != null;
        assert ses != null || entity != null;

        this.marsh = marsh;
        this.ctx = ctx;
        this.isNew = isNew;
        this.genuineSes = ses;

        accessTime = System.currentTimeMillis();

        if (entity == null) {
            entity = new WebSessionEntity(id, ses.getCreationTime(), accessTime,
                ses.getMaxInactiveInterval());
        }

        this.entity = entity;

        maxInactiveInterval = entity.maxInactiveInterval();

        if (ses != null) {
            final Enumeration<String> names = ses.getAttributeNames();

            while (names.hasMoreElements()) {
                final String name = names.nextElement();

                attributes().put(name, ses.getAttribute(name));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long getCreationTime() {
        assertValid();

        return entity.createTime();
    }

    /** {@inheritDoc} */
    @Override public String getId() {
        return entity.id();
    }

    /**
     * @return Session ID without throwing exception.
     */
    public String id() {
        return entity.id();
    }

    /** {@inheritDoc} */
    @Override public long getLastAccessedTime() {
        assertValid();

        return accessTime;
    }

    /** {@inheritDoc} */
    @Override public ServletContext getServletContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public void setMaxInactiveInterval(final int interval) {
        maxInactiveInterval = interval;

        maxInactiveIntervalChanged = true;
    }

    /**
     * @return {@code True} if {@link #setMaxInactiveInterval(int)} was invoked.
     */
    public boolean isMaxInactiveIntervalChanged() {
        return maxInactiveIntervalChanged;
    }

    /** {@inheritDoc} */
    @Override public int getMaxInactiveInterval() {
        return maxInactiveInterval;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public HttpSessionContext getSessionContext() {
        return EMPTY_SES_CTX;
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(final String name) {
        assertValid();

        Object attr = attributes().get(name);

        if (attr == REMOVED_ATTR)
            return null;

        if (attr == null) {
            final byte[] bytes = entity.attributes().get(name);

            if (bytes != null) {
                // deserialize
                try {
                    attr = unmarshal(bytes);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }

                attributes().put(name, attr);
            }
        }

        return attr;
    }

    /** {@inheritDoc} */
    @Override public Object getValue(final String name) {
        return getAttribute(name);
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(final String name, final Object val) {
        assertValid();

        if (val == null)
            removeAttribute(name);
        else
            attributes().put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void putValue(final String name, final Object val) {
        setAttribute(name, val);
    }

    /** {@inheritDoc} */
    @Override public Enumeration<String> getAttributeNames() {
        assertValid();

        return Collections.enumeration(attributeNames());
    }

    /**
     * @return Set of attribute names.
     */
    private Set<String> attributeNames() {
        if (!F.isEmpty(attrs)) {
            final Set<String> names = new HashSet<>(entity.attributes().size() + attrs.size());

            names.addAll(entity.attributes().keySet());

            for (final Map.Entry<String, Object> entry : attrs.entrySet()) {
                if (entry != REMOVED_ATTR)
                    names.add(entry.getKey());
            }

            return names;
        }

        return entity.attributes().keySet();
    }

    /** {@inheritDoc} */
    @Override public String[] getValueNames() {
        assertValid();

        final Set<String> names = attributeNames();

        return names.toArray(new String[names.size()]);
    }

    /** {@inheritDoc} */
    @Override public void removeAttribute(final String name) {
        assertValid();

        attributes().put(name, REMOVED_ATTR);
    }

    /** {@inheritDoc} */
    @Override public void removeValue(final String name) {
        removeAttribute(name);
    }

    /** {@inheritDoc} */
    @Override public void invalidate() {
        assertValid();

        if (genuineSes != null) {
            try {
                genuineSes.invalidate();
            }
            catch (IllegalStateException ignored) {
                // Already invalidated, keep going.
            }
        }

        invalidated = true;
    }

    /** {@inheritDoc} */
    @Override public boolean isNew() {
        assertValid();

        return isNew;
    }

    /**
     * @return Marshaled updates or empty map if no params.
     * @throws IOException
     */
    public Map<String, byte[]> binaryUpdatesMap() throws IOException {
        final Map<String, Object> map = attributes();

        if (F.isEmpty(map))
            return Collections.emptyMap();

        final Map<String, byte[]> res = new HashMap<>(map.size());

        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            Object val = entry.getValue() == REMOVED_ATTR ? null : entry.getValue();

            res.put(entry.getKey(), marshal(val));
        }

        return res;
    }

    /**
     * Unmarshal object.
     *
     * @param bytes Data.
     * @param <T> Expected type.
     * @return Unmarshaled object.
     * @throws IOException If unarshaling failed.
     */
    @Nullable private <T> T unmarshal(final byte[] bytes) throws IOException {
        if (marsh != null) {
            try {
                return U.unmarshal(marsh, bytes, getClass().getClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }

        return null;
    }

    /**
     * Marshal object.
     *
     * @param obj Object to marshal.
     * @return Binary data.
     * @throws IOException If marshaling failed.
     */
    @Nullable private byte[] marshal(final Object obj) throws IOException {
        if (marsh != null) {
            try {
                return U.marshal(marsh, obj);
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }

        return null;
    }

    /**
     * Marshal all attributes and save to serializable entity.
     */
    public WebSessionEntity marshalAttributes() throws IOException {
        final WebSessionEntity marshaled = new WebSessionEntity(getId(), entity.createTime(), accessTime,
            maxInactiveInterval);

        for (final Map.Entry<String, Object> entry : attributes().entrySet())
            marshaled.putAttribute(entry.getKey(), marshal(entry.getValue()));

        return marshaled;
    }

    /**
     * @return Session attributes.
     */
    private Map<String, Object> attributes() {
        if (attrs == null)
            attrs = new HashMap<>();

        return attrs;
    }

    /**
     * @return {@code True} if session wasn't invalidated.
     */
    public boolean isValid() {
        return !invalidated;
    }

    /**
     * Throw {@link IllegalStateException} if session was invalidated.
     */
    private void assertValid() {
        if (invalidated)
            throw new IllegalStateException("Session was invalidated.");
    }
}
