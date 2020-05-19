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
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.util.Service;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface MailboxRegistry<Row> extends Service {
    /**
     * Tries to register and inbox node and returns it if success or returns previously registered inbox otherwise.
     *
     * @param inbox Inbox.
     * @return Registered inbox.
     */
    Inbox<Row> register(Inbox<Row> inbox);

    /**
     * Unregisters an inbox.
     *
     * @param inbox Inbox to unregister.
     */
    void unregister(Inbox<Row> inbox);

    /**
     * Registers an outbox.
     *
     * @param outbox Outbox to register.
     */
    void register(Outbox<Row> outbox);

    /**
     * Unregisters an outbox.
     *
     * @param outbox Outbox to unregister.
     */
    void unregister(Outbox<Row> outbox);

    /**
     * Returns a registered outbox by provided query ID, exchange ID pair.
     *
     * @param qryId Query ID.
     * @param exchangeId Exchange ID.
     *
     * @return Registered outbox. May be {@code null} if execution was cancelled.
     */
    Outbox<Row> outbox(UUID qryId, long exchangeId);

    /**
     * Returns a registered inbox by provided query ID, exchange ID pair.
     *
     * @param qryId Query ID.
     * @param exchangeId Exchange ID.
     *
     * @return Registered inbox. May be {@code null} if execution was cancelled.
     */
    Inbox<Row> inbox(UUID qryId, long exchangeId);

    /**
     * Returns all registered inboxes for provided query ID.
     *
     * @param qryId Query ID. {@code null} means return all registered inboxes.
     * @return Registered inboxes.
     */
    Collection<Inbox<Row>> inboxes(@Nullable UUID qryId);

    /**
     * Returns all registered outboxes for provided query ID.
     *
     * @param qryId Query ID. {@code null} means return all registered outboxes.
     * @return Registered outboxes.
     */
    Collection<Outbox<Row>> outboxes(@Nullable UUID qryId);
}
