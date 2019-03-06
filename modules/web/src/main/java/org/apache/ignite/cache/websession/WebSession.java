/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

    /** Flag indicating if the session is valid. */
    private transient volatile boolean isValid = true;

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

    /** Web session filter. */
    @GridToStringExclude
    private transient WebSessionFilter filter;

    /** New session flag. */
    private transient boolean isNew;

    /** Updates list. */
    private transient Collection<T2<String, Object>> updates;

    /** Genuine http session. */
    private transient HttpSession genSes;

    /**
     * Required by {@link Externalizable}.
     */
    public WebSession() {
        // No-op.
    }

    /**
     * @param id Session ID.
     * @param ses Session.
     */
    WebSession(String id, HttpSession ses) {
        assert id != null;
        assert ses != null;

        this.id = id;

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
     * @param id Session ID.
     * @param ses Session.
     * @param isNew Is new flag.
     */
    WebSession(String id, HttpSession ses, boolean isNew) {
        this(id, ses);

        this.isNew = isNew;
    }

    /**
     * Sets the genuine http session.
     *
     * @param genSes Genuine http session.
     */
    protected void genSes(HttpSession genSes) {
        this.genSes = genSes;
    }

    /**
     * @param ctx Servlet context.
     */
    public void servletContext(ServletContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /**
     * @param filter Filter.
     */
    public void filter(final WebSessionFilter filter) {
        assert filter != null;

        this.filter = filter;
    }

    /**
     * Checks if the session is valid.
     *
     * @return True is valid, otherwise false.
     */
    protected boolean isValid() {
        return this.isValid;
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

    /**
     * Sets a session id.
     *
     * @param id Session id.
     */
    protected void setId(String id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public ServletContext getServletContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public long getCreationTime() {
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long getLastAccessedTime() {
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

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
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

        Object val = attrs.get(name);

        if (val != null && updates != null)
            updates.add(new T2<>(name, val));

        return val;
    }

    /** {@inheritDoc} */
    @Override public Object getValue(String name) {
        return getAttribute(name);
    }

    /** {@inheritDoc} */
    @Override public Enumeration<String> getAttributeNames() {
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

        return Collections.enumeration(attrs.keySet());
    }

    /** {@inheritDoc} */
    @Override public String[] getValueNames() {
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

        return attrs.keySet().toArray(new String[attrs.size()]);
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(String name, Object val) {
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

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
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

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
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

        attrs.clear();

        updates = null;

        filter.destroySession(id);

        genSes.invalidate();

        isValid = false;
    }

    /** {@inheritDoc} */
    @Override public boolean isNew() {
        if (!isValid)
            throw new IllegalStateException("Call on invalidated session!");

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
