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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Session implementation.
 */
@SuppressWarnings({"deprecation", "NonSerializableObjectBoundToHttpSession"})
class WebSession implements HttpSession, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Empty session context. */
    private static final HttpSessionContext EMPTY_SES_CTX = new HttpSessionContext() {
        @Nullable @Override public HttpSession getSession(String id) {
            return null;
        }

        @Override public Enumeration<String> getIds() {
            return Collections.enumeration(Collections.<String>emptyList());
        }
    };

    /** Session ID. */
    private String id;

    /** Creation time. */
    private long createTime;

    /** Last access time. */
    private long accessTime;

    /** Maximum inactive interval. */
    private int maxInactiveInterval;

    /** Attributes. */
    @GridToStringInclude
    private Map<String, Object> attrs;

    /** Servlet context. */
    @GridToStringExclude
    private transient ServletContext ctx;

    /** Listener. */
    @GridToStringExclude
    private transient WebSessionListener lsnr;

    /** New session flag. */
    private transient boolean isNew;

    /** Updates list. */
    private transient Collection<T2<String, Object>> updates;

    /**
     * Required by {@link Externalizable}.
     */
    public WebSession() {
        // No-op.
    }

    /**
     * @param ses Session.
     */
    WebSession(HttpSession ses) {
        assert ses != null;

        id = ses.getId();
        createTime = ses.getCreationTime();
        accessTime = ses.getLastAccessedTime();
        maxInactiveInterval = ses.getMaxInactiveInterval();
        isNew = ses.isNew();

        attrs = new HashMap<>();

        Enumeration<String> names = ses.getAttributeNames();

        while (names.hasMoreElements()) {
            String name = names.nextElement();

            attrs.put(name, ses.getAttribute(name));
        }
    }

    /**
     * @param ses Session.
     * @param isNew Is new flag.
     */
    WebSession(HttpSession ses, boolean isNew) {
        this(ses);

        this.isNew = isNew;
    }

    /**
     * @param accessTime Last access time.
     */
    void accessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    /**
     * @param ctx Servlet context.
     */
    public void servletContext(ServletContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /**
     * @param lsnr Listener.
     */
    public void listener(WebSessionListener lsnr) {
        assert lsnr != null;

        this.lsnr = lsnr;
    }

    /**
     * Resets updates list.
     */
    public void resetUpdates() {
        updates = new LinkedList<>();
    }

    /**
     * @return Updates list.
     */
    public Collection<T2<String, Object>> updates() {
        Collection<T2<String, Object>> updates0 = updates;

        updates = null;

        return updates0;
    }

    /** {@inheritDoc} */
    @Override public String getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public ServletContext getServletContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public long getCreationTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long getLastAccessedTime() {
        return accessTime;
    }

    /** {@inheritDoc} */
    @Override public int getMaxInactiveInterval() {
        return maxInactiveInterval;
    }

    /** {@inheritDoc} */
    @Override public void setMaxInactiveInterval(int interval) {
        maxInactiveInterval = interval;
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(String name) {
        return attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public Object getValue(String name) {
        return attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public Enumeration<String> getAttributeNames() {
        return Collections.enumeration(attrs.keySet());
    }

    /** {@inheritDoc} */
    @Override public String[] getValueNames() {
        return attrs.keySet().toArray(new String[attrs.size()]);
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(String name, Object val) {
        attrs.put(name, val);

        if (updates != null)
            updates.add(new T2<>(name, val));
    }

    /** {@inheritDoc} */
    @Override public void putValue(String name, Object val) {
        setAttribute(name, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAttribute(String name) {
        attrs.remove(name);

        if (updates != null)
            updates.add(new T2<>(name, null));
    }

    /** {@inheritDoc} */
    @Override public void removeValue(String name) {
        removeAttribute(name);
    }

    /** {@inheritDoc} */
    @Override public void invalidate() {
        attrs.clear();

        updates = null;

        lsnr.destroySession(id);
    }

    /**
     * @param isNew New session flag.
     */
    void setNew(boolean isNew) {
        this.isNew = isNew;
    }

    /** {@inheritDoc} */
    @Override public boolean isNew() {
        return isNew;
    }

    /** {@inheritDoc} */
    @Override public HttpSessionContext getSessionContext() {
        return EMPTY_SES_CTX;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, id);
        out.writeLong(createTime);
        out.writeLong(accessTime);
        out.writeInt(maxInactiveInterval);
        U.writeMap(out, attrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readString(in);
        createTime = in.readLong();
        accessTime = in.readLong();
        maxInactiveInterval = in.readInt();
        attrs = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSession.class, this);
    }
}