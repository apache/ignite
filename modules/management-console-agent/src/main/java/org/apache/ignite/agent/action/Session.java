/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.security.SecurityCredentials;

/**
 * Session with security subject.
 */
public class Session {
    /** Expiration STATE. It's a final state of lastTouchTime. */
    private static final long TIMEDOUT_STATE = 0L;

    /** Session ID. */
    private final UUID id;

    /** Address. */
    private InetSocketAddress addr;

    /**
     * Time when session is used last time.
     * If this time was set at TIMEDOUT_FLAG, then it should never be changed.
     */
    private final AtomicLong lastTouchTime = new AtomicLong(System.currentTimeMillis());

    /** Time when session was invalidated last time. */
    private final AtomicLong lastInvalidateTime = new AtomicLong(System.currentTimeMillis());

    /** Security context. */
    private volatile SecurityContext secCtx;

    /** Authorization context. */
    private volatile AuthorizationContext authCtx;

    /** Credentials that can be used for security context invalidation. */
    @GridToStringExclude
    private volatile SecurityCredentials creds = new SecurityCredentials();

    /**
     * @param id Session ID.
     */
    private Session(UUID id) {
        this.id = id;
    }

    /**
     * Static constructor.
     *
     * @return New session instance with random session ID.
     */
    public static Session random() {
        return new Session(UUID.randomUUID());
    }

    /**
     * @return Session ID.
     */
    public UUID id() {
        return id;
    }

    /**
     * @return Security context.
     */
    public SecurityContext securityContext() {
        return secCtx;
    }

    /**
     * @param secCtx Security context.
     */
    public void securityContext(SecurityContext secCtx) {
        this.secCtx = secCtx;
    }

    /**
     * @return Authorization context.
     */
    public AuthorizationContext authorizationContext() {
        return authCtx;
    }

    /**
     * @param authCtx Authorization context.
     */
    public void authorizationContext(AuthorizationContext authCtx) {
        this.authCtx = authCtx;
    }

    /**
     * @return Security credentials.
     */
    public SecurityCredentials credentials() {
        return creds;
    }

    /**
     * @param creds Credentials.
     */
    public void credentials(SecurityCredentials creds) {
        this.creds = creds;
    }

    /**
     * @param time Time.
     */
    public void lastInvalidateTime(long time) {
        lastInvalidateTime.set(time);
    }

    /**
     * @return Last invalidate time.
     */
    public long lastInvalidateTime() {
        return lastInvalidateTime.get();
    }

    /**
     * @return Remote client address.
     */
    public InetSocketAddress address() {
        return addr;
    }

    /**
     * @param addr Address.
     */
    public void address(InetSocketAddress addr) {
        this.addr = addr;
    }

    /**
     * Checks expiration of session and if expired then sets TIMEDOUT_FLAG.
     *
     * @param sesTimeout Session timeout.
     * @return {@code True} if expired.
     * @see #touch()
     */
    public boolean timedOut(long sesTimeout) {
        long time0 = lastTouchTime.get();

        if (time0 == TIMEDOUT_STATE)
            return true;

        return System.currentTimeMillis() - time0 > sesTimeout && lastTouchTime.compareAndSet(time0, TIMEDOUT_STATE);
    }

    /**
     * Checks if session should be invalidated.
     *
     * @param sesTokTtl Session expire time.
     * @return {@code true} if session should be invalidated.
     */
    public boolean sessionExpired(long sesTokTtl) {
        return System.currentTimeMillis() - lastInvalidateTime.get() > sesTokTtl;
    }

    /**
     * Checks whether session at expired state (EXPIRATION_FLAG) or not, if not then tries to update last touch time.
     *
     * @return {@code False} if session timed out (not successfully touched).
     * @see #timedOut(long)
     */
    public boolean touch() {
        while (true) {
            long time0 = lastTouchTime.get();

            if (time0 == TIMEDOUT_STATE)
                return false;

            if (lastTouchTime.compareAndSet(time0, System.currentTimeMillis()))
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Session ses = (Session) o;

        return id.equals(ses.id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Session.class, this);
    }
}
