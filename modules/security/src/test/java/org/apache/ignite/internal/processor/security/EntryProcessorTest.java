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

package org.apache.ignite.internal.processor.security;

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
public class EntryProcessorTest extends AbstractContextResolverSecurityProcessorTest {
    /** */
    public void testEntryProcessor() {
        successEntryProcessor(clnt, srv);
        successEntryProcessor(clnt, srvNoPutPerm);
        successEntryProcessor(srv, srvNoPutPerm);

        failEntryProcessor(clntNoPutPerm, srv);
        failEntryProcessor(srvNoPutPerm, srv);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successEntryProcessor(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        new Invoke(initiator, remote).call();
        new InvokeAll(initiator, remote).call();
        new InvokeAsync(initiator, remote).call();
        new InvokeAllAsync(initiator, remote).call();
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failEntryProcessor(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        failCall(new Invoke(initiator, remote));
        failCall(new InvokeAll(initiator, remote));
        failCall(new InvokeAsync(initiator, remote));
        failCall(new InvokeAllAsync(initiator, remote));
    }

    /**
     * @param c CustomInvoke.
     */
    private void failCall(CustomInvoke c) {
        try {
            c.call();
        }
        catch (Throwable e) {
            assertCauseMessage(e);
        }
    }

    /** */
    abstract class CustomInvoke {
        /** Initiator. */
        protected final IgniteEx initiator;

        /** Remote. */
        protected final IgniteEx remote;

        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        protected CustomInvoke(IgniteEx initiator, IgniteEx remote) {
            this.initiator = initiator;
            this.remote = remote;
        }

        /**
         * Calling of invokeXXX method
         */
        abstract void call();
    }

    /**
     * Call invoke method.
     */
    class Invoke extends CustomInvoke {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public Invoke(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void call() {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invoke(
                primaryKey(remote),
                new TestEntryProcessor(remote.localNode().id())
            );
        }
    }

    /**
     * Call invokeAsync method.
     */
    class InvokeAsync extends CustomInvoke {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public InvokeAsync(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void call() {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invokeAsync(
                primaryKey(remote),
                new TestEntryProcessor(remote.localNode().id())
            ).get();
        }
    }

    /**
     * Call invokeAll method.
     */
    class InvokeAll extends CustomInvoke {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public InvokeAll(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void call() {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invokeAll(
                Collections.singleton(primaryKey(remote)),
                new TestEntryProcessor(remote.localNode().id())
            ).values().stream().findFirst().ifPresent(EntryProcessorResult::get);
        }
    }

    /**
     * Call invokeAllAsync method.
     */
    class InvokeAllAsync extends CustomInvoke {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public InvokeAllAsync(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void call() {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invokeAllAsync(
                Collections.singleton(primaryKey(remote)),
                new TestEntryProcessor(remote.localNode().id())
            ).get().values().stream().findFirst().ifPresent(EntryProcessorResult::get);
        }
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

            if (remoteId.equals(loc.localNode().id()))
                loc.context().security().authorize(CACHE_NAME, SecurityPermission.CACHE_PUT);

            return null;
        }
    }
}
