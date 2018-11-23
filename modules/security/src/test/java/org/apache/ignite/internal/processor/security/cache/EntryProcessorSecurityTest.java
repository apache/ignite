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

package org.apache.ignite.internal.processor.security.cache;

import java.util.Collections;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Security tests for EntityProcessor.
 */
public class EntryProcessorSecurityTest extends AbstractCacheSecurityTest {
    /** */
    public void testEntryProcessor() {
        successEntryProcessor(clntAllPerms, srvAllPerms);
        successEntryProcessor(clntAllPerms, srvReadOnlyPerm);
        successEntryProcessor(srvAllPerms, srvReadOnlyPerm);

        failEntryProcessor(clntReadOnlyPerm, srvAllPerms);
        failEntryProcessor(srvReadOnlyPerm, srvAllPerms);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successEntryProcessor(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        invoke(initiator, remote);
        invokeAll(initiator, remote);
        invokeAsync(initiator, remote);
        invokeAllAsync(initiator, remote);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failEntryProcessor(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        failCall(() -> invoke(initiator, remote));
        failCall(() -> invokeAll(initiator, remote));
        failCall(() -> invokeAsync(initiator, remote));
        failCall(() -> invokeAllAsync(initiator, remote));
    }

    /**
     * @param r Runnable.
     */
    private void failCall(Runnable r) {
        try {
            r.run();
        }
        catch (Throwable e) {
            assertCause(e);
        }
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     */
    private void invoke(IgniteEx initiator, IgniteEx remote) {
        initiator.<Integer, Integer>cache(CACHE_WITHOUT_PERMS).invoke(
            primaryKey(remote),
            new TestEntryProcessor(remote.localNode().id())
        );
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     */
    private void invokeAsync(IgniteEx initiator, IgniteEx remote) {
        initiator.<Integer, Integer>cache(CACHE_WITHOUT_PERMS).invokeAsync(
            primaryKey(remote),
            new TestEntryProcessor(remote.localNode().id())
        ).get();
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     */
    private void invokeAll(IgniteEx initiator, IgniteEx remote) {
        initiator.<Integer, Integer>cache(CACHE_WITHOUT_PERMS).invokeAll(
            Collections.singleton(primaryKey(remote)),
            new TestEntryProcessor(remote.localNode().id())
        ).values().stream().findFirst().ifPresent(EntryProcessorResult::get);
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     */
    private void invokeAllAsync(IgniteEx initiator, IgniteEx remote) {
        initiator.<Integer, Integer>cache(CACHE_WITHOUT_PERMS).invokeAllAsync(
            Collections.singleton(primaryKey(remote)),
            new TestEntryProcessor(remote.localNode().id())
        ).get().values().stream().findFirst().ifPresent(EntryProcessorResult::get);
    }

    /**
     * Entry processor for tests.
     */
    static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        /** Remote node id. */
        protected final UUID remoteId;

        /**
         * @param remoteId Remote id.
         */
        public TestEntryProcessor(UUID remoteId) {
            this.remoteId = remoteId;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry,
            Object... objects) throws EntryProcessorException {
            IgniteEx loc = (IgniteEx)Ignition.localIgnite();

            assertEquals(remoteId, loc.localNode().id());

            loc.context().security().authorize(CACHE_NAME, SecurityPermission.CACHE_PUT);

            return null;
        }
    }
}
