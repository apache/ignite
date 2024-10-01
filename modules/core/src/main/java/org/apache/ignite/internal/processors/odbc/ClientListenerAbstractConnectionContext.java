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

package org.apache.ignite.internal.processors.odbc;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Base connection context.
 */
public abstract class ClientListenerAbstractConnectionContext implements ClientListenerConnectionContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Nio session. */
    protected final GridNioSession ses;

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx;

    /** Connection ID. */
    private long connId;

    /** User attributes. */
    protected Map<String, String> userAttrs;

    /**
     * Describes the client connection:
     * - thin cli: "cli:host:port@user_name"
     * - thin JDBC: "jdbc-thin:host:port@user_name"
     * - ODBC: "odbc:host:port@user_name"
     *
     * Used by the running query view to display query initiator.
     */
    private String clientDesc;

    /** Active tx count limit. */
    private final int maxActiveTxCnt;

    /** Tx id. */
    private final AtomicInteger txIdSeq = new AtomicInteger();

    /** Transactions by transaction id. */
    private final Map<Integer, ClientTxContext> txs = new ConcurrentHashMap<>();

    /** Active transactions count. */
    private final AtomicInteger txsCnt = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param ses Client's NIO session.
     * @param connId Connection ID.
     * @param maxActiveTxCnt Maximum active transactions count.
     */
    protected ClientListenerAbstractConnectionContext(
        GridKernalContext ctx, GridNioSession ses, long connId, int maxActiveTxCnt) {
        this.ctx = ctx;
        this.connId = connId;
        this.ses = ses;
        this.maxActiveTxCnt = maxActiveTxCnt;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public SecurityContext securityContext() {
        return secCtx;
    }

    /** {@inheritDoc} */
    @Override public long connectionId() {
        return connId;
    }

    /**
     * Perform authentication.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void authenticate(GridNioSession ses, String user, String pwd) throws IgniteCheckedException {
        if (!ctx.security().enabled())
            return;

        SecurityCredentials cred = new SecurityCredentials(user, pwd);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());
        authCtx.nodeAttributes(F.isEmpty(userAttrs) ? Collections.emptyMap() : userAttrs);
        authCtx.credentials(cred);
        authCtx.address(ses.remoteAddress());
        authCtx.certificates(ses.certificates());

        secCtx = ctx.security().authenticate(authCtx);

        if (secCtx == null) {
            throw new IgniteAccessControlException(
                String.format("The user name or password is incorrect [userName=%s]", user)
            );
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        cleanupTxs();

        if (ctx.security().enabled())
            ctx.security().onSessionExpired(secCtx.subject().id());
    }

    /** */
    protected void initClientDescriptor(String prefix) {
        clientDesc = prefix + ":" + ses.remoteAddress().getHostString() + ":" + ses.remoteAddress().getPort();

        if (secCtx != null)
            clientDesc += "@" + secCtx.subject().login();
    }

    /**
     * Describes the client connection:
     * - thin cli: "cli:host:port@user_name"
     * - thin JDBC: "jdbc-thin:host:port@user_name"
     * - ODBC: "odbc:host:port@user_name"
     *
     * Used by the running query view to display query initiator.
     *
     * @return Client descriptor string.
     */
    public String clientDescriptor() {
        return clientDesc;
    }

    /**
     * Next transaction id for this connection.
     */
    public int nextTxId() {
        int txId = txIdSeq.incrementAndGet();

        return txId == 0 ? txIdSeq.incrementAndGet() : txId;
    }

    /**
     * Transaction context by transaction id.
     *
     * @param txId Tx ID.
     */
    public ClientTxContext txContext(int txId) {
        return txs.get(txId);
    }

    /**
     * Add new transaction context to connection.
     *
     * @param txCtx Tx context.
     */
    public void addTxContext(ClientTxContext txCtx) {
        if (txsCnt.incrementAndGet() > maxActiveTxCnt) {
            txsCnt.decrementAndGet();

            throw tooManyTransactionsException(maxActiveTxCnt);
        }

        txs.put(txCtx.txId(), txCtx);
    }

    /** */
    protected abstract RuntimeException tooManyTransactionsException(int maxActiveTxCnt);

    /**
     * Remove transaction context from connection.
     *
     * @param txId Tx ID.
     */
    public void removeTxContext(int txId) {
        txs.remove(txId);

        txsCnt.decrementAndGet();
    }

    /**
     *
     */
    private void cleanupTxs() {
        for (ClientTxContext txCtx : txs.values())
            txCtx.close();

        txs.clear();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> attributes() {
        return F.isEmpty(userAttrs) ? Collections.emptyMap() : Collections.unmodifiableMap(userAttrs);
    }
}
