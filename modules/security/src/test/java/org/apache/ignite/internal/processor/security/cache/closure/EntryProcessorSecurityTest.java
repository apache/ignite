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

package org.apache.ignite.internal.processor.security.cache.closure;

import java.util.Collections;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheSecurityTest;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when EntryProcessor closure is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class EntryProcessorSecurityTest extends AbstractCacheSecurityTest {
    /** */
    @Test
    public void testEntryProcessor() {
        checkInvoke(clntAllPerms, srvAllPerms, true);
        checkInvoke(clntAllPerms, srvReadOnlyPerm, true);
        checkInvoke(srvAllPerms, srvReadOnlyPerm, true);

        checkInvoke(clntReadOnlyPerm, srvAllPerms, false);
        checkInvoke(srvReadOnlyPerm, srvAllPerms, false);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void checkInvoke(IgniteEx initiator, IgniteEx remote, boolean isSuccess) {
        assert !remote.localNode().isClient();

        final UUID remoteId = remote.localNode().id();

        final Integer key = primaryKey(remote);

        for (InvokeMethodEnum ime : InvokeMethodEnum.values()) {
            if (isSuccess)
                invoke(ime, initiator, new TestEntryProcessor(remoteId), key);
            else {
                forbiddenRun(
                    () -> invoke(ime, initiator, new TestEntryProcessor(remoteId), key)
                );
            }
        }

        final UUID transitionId = srvTransitionAllPerms.localNode().id();

        final Integer transitionKey = primaryKey(srvTransitionAllPerms);

        for (InvokeMethodEnum ime : InvokeMethodEnum.values()) {
            if (isSuccess) {
                invoke(ime, initiator,
                    new TestTransitionEntryProcessor(ime, transitionId, remoteId, key),
                    transitionKey);
            }
            else {
                forbiddenRun(
                    () -> invoke(ime, initiator,
                        new TestTransitionEntryProcessor(ime, transitionId, remoteId, key),
                        transitionKey)
                );
            }
        }
    }

    /**
     * @param ime Invoke Method.
     * @param initiator Initiator.
     * @param ep Entry Processor.
     * @param key Key.
     */
    static void invoke(InvokeMethodEnum ime, IgniteEx initiator,
        EntryProcessor<Integer, Integer, Object> ep, Integer key) {
        switch (ime) {
            case INVOKE:
                initiator.<Integer, Integer>cache(COMMON_USE_CACHE)
                    .invoke(key, ep);

                break;
            case INVOKE_ALL:
                initiator.<Integer, Integer>cache(COMMON_USE_CACHE)
                    .invokeAll(Collections.singleton(key), ep)
                    .values().stream().findFirst().ifPresent(EntryProcessorResult::get);

                break;
            case INVOKE_ASYNC:
                initiator.<Integer, Integer>cache(COMMON_USE_CACHE)
                    .invokeAsync(key, ep).get();

                break;
            case INVOKE_ALL_ASYNC:
                initiator.<Integer, Integer>cache(COMMON_USE_CACHE)
                    .invokeAllAsync(Collections.singleton(key), ep)
                    .get().values().stream().findFirst().ifPresent(EntryProcessorResult::get);

                break;
            default:
                throw new IllegalArgumentException("Unknown invoke method " + ime);
        }
    }

    /** Enum for ways to invoke EntryProcessor. */
    enum InvokeMethodEnum {
        /** Invoke. */
        INVOKE,
        /** Invoke all. */
        INVOKE_ALL,
        /** Invoke async. */
        INVOKE_ASYNC,
        /** Invoke all async. */
        INVOKE_ALL_ASYNC
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

    /**
     * Entry processor for tests with transition invoke call.
     */
    static class TestTransitionEntryProcessor extends TestEntryProcessor {
        /** */
        private final InvokeMethodEnum ime;
        /** Transition node id */
        private final UUID transitionId;

        /** The key that is contained on primary partition on remote node for given cache. */
        private final Integer remoteKey;

        /**
         * @param remoteId Remote id.
         */
        public TestTransitionEntryProcessor(InvokeMethodEnum ime, UUID transitionId, UUID remoteId, Integer remoteKey) {
            super(remoteId);

            this.ime = ime;
            this.transitionId = transitionId;
            this.remoteKey = remoteKey;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry,
            Object... objects) throws EntryProcessorException {
            IgniteEx loc = (IgniteEx)Ignition.localIgnite();

            assertEquals(transitionId, loc.localNode().id());

            invoke(ime, loc, new TestEntryProcessor(remoteId), remoteKey);

            return null;
        }
    }
}
