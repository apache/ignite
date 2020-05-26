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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MailboxRegistryImpl extends AbstractService implements MailboxRegistry {
    /** */
    private final Map<MailboxKey, Outbox<?>> locals;

    /** */
    private final Map<MailboxKey, Inbox<?>> remotes;

    /**
     * @param ctx Kernal.
     */
    public MailboxRegistryImpl(GridKernalContext ctx) {
        super(ctx);

        locals = new ConcurrentHashMap<>();
        remotes = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> register(Inbox<?> inbox) {
        Inbox<?> old = remotes.putIfAbsent(new MailboxKey(inbox.queryId(), inbox.exchangeId()), inbox);

        return old != null ? old : inbox;
    }

    /** {@inheritDoc} */
    @Override public void unregister(Inbox<?> inbox) {
        remotes.remove(new MailboxKey(inbox.queryId(), inbox.exchangeId()));
    }

    /** {@inheritDoc} */
    @Override public void register(Outbox<?> outbox) {
        Outbox<?> res = locals.put(new MailboxKey(outbox.queryId(), outbox.exchangeId()), outbox);

        assert res == null : res;
    }

    /** {@inheritDoc} */
    @Override public void unregister(Outbox<?> outbox) {
        locals.remove(new MailboxKey(outbox.queryId(), outbox.exchangeId()));
    }

    /** {@inheritDoc} */
    @Override public Outbox<?> outbox(UUID qryId, long exchangeId) {
        return locals.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public Inbox<?> inbox(UUID qryId, long exchangeId) {
        return remotes.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public Collection<Inbox<?>> inboxes(@Nullable UUID qryId) {
        if (qryId == null)
            return remotes.values();

        return remotes.entrySet().stream()
            .filter(e -> e.getKey().qryId.equals(qryId))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public Collection<Outbox<?>> outboxes(@Nullable UUID qryId) {
        if (qryId == null)
            return locals.values();

        return locals.entrySet().stream()
            .filter(e -> e.getKey().qryId.equals(qryId))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    }

    /** */
    private static class MailboxKey {
        /** */
        private final UUID qryId;

        /** */
        private final long exchangeId;

        /** */
        private MailboxKey(UUID qryId, long exchangeId) {
            this.qryId = qryId;
            this.exchangeId = exchangeId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            MailboxKey that = (MailboxKey) o;

            if (exchangeId != that.exchangeId)
                return false;
            return qryId.equals(that.qryId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = qryId.hashCode();
            res = 31 * res + (int) (exchangeId ^ (exchangeId >>> 32));
            return res;
        }
    }
}
