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
import java.util.function.Consumer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.internal.IgniteEx;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for EntityProcessor.
 */
public class EntryProcessorTest extends AbstractContextResolverSecurityProcessorTest {
    /** */
    public void testEntryProcessor() throws Exception {
        successEntryProcessor(succsessClnt, succsessSrv);
        successEntryProcessor(succsessClnt, failSrv);
        successEntryProcessor(succsessSrv, failSrv);

        failEntryProcessor(failClnt, succsessSrv);
        failEntryProcessor(failSrv, succsessSrv);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successEntryProcessor(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        successCall(new Invoke(initiator, remote));
        successCall(new InvokeAll(initiator, remote));
        successCall(new InvokeAsync(initiator, remote));
        successCall(new InvokeAllAsync(initiator, remote));
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
     * @param c Consumer.
     */
    private void successCall(Consumer<Integer> c){
        Integer val = values.getAndIncrement();

        c.accept(val);

        assertThat(succsessSrv.cache(CACHE_NAME).get(val), is(val));
    }

    /**
     * @param c Consumer.
     */
    private void failCall(Consumer<Integer> c) {
        try {
            c.accept(0);
        }
        catch (Throwable e) {
            assertCauseMessage(e);
        }

        assertThat(succsessSrv.cache(CACHE_NAME).get(0), nullValue());
    }

    /** */
    abstract class CommonConsumer implements Consumer<Integer> {
        /** Initiator. */
        protected final IgniteEx initiator;

        /** Remote. */
        protected final IgniteEx remote;

        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        protected CommonConsumer(IgniteEx initiator, IgniteEx remote) {
            this.initiator = initiator;
            this.remote = remote;
        }
    }

    /**
     * Call invoke method.
     */
    class Invoke extends CommonConsumer {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public Invoke(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void accept(Integer key) {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invoke(
                primaryKey(remote),
                new TestEntryProcessor(remote.localNode().id(), key)
            );
        }
    }

    /**
     * Call invokeAsync method.
     */
    class InvokeAsync extends CommonConsumer {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public InvokeAsync(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void accept(Integer key) {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invokeAsync(
                primaryKey(remote),
                new TestEntryProcessor(remote.localNode().id(), key)
            ).get();
        }
    }

    /**
     * Call invokeAll method.
     */
    class InvokeAll extends CommonConsumer {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public InvokeAll(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void accept(Integer key) {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invokeAll(
                Collections.singleton(primaryKey(remote)),
                new TestEntryProcessor(remote.localNode().id(), key)
            ).values().stream().findFirst().ifPresent(EntryProcessorResult::get);
        }
    }

    /**
     * Call invokeAllAsync method.
     */
    class InvokeAllAsync extends CommonConsumer {
        /**
         * @param initiator Initiator.
         * @param remote Remote.
         */
        public InvokeAllAsync(IgniteEx initiator, IgniteEx remote) {
            super(initiator, remote);
        }

        /** {@inheritDoc} */
        @Override public void accept(Integer key) {
            initiator.<Integer, Integer>cache(SEC_CACHE_NAME).invokeAllAsync(
                Collections.singleton(primaryKey(remote)),
                new TestEntryProcessor(remote.localNode().id(), key)
            ).get().values().stream().findFirst().ifPresent(EntryProcessorResult::get);
        }
    }

    /**
     * Getting the key that is contained on primary partition on passed node.
     *
     * @param ignite Node.
     * @return Key.
     */
    private Integer primaryKey(IgniteEx ignite) {
        Affinity<Integer> affinity = ignite.affinity(SEC_CACHE_NAME);

        int i = 0;
        do {
            if (affinity.isPrimary(ignite.localNode(), ++i))
                return i;

        }
        while (i <= 1_000);

        throw new IllegalStateException(ignite.name() + " isn't primary node for any key.");
    }

    /**
     * Entry processor for tests.
     */
    static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        /** Remote node id. */
        protected final UUID remoteId;

        /** Key. */
        private final Integer key;

        /**
         * @param remoteId Remote id.
         * @param key Key.
         */
        public TestEntryProcessor(UUID remoteId, Integer key) {
            this.remoteId = remoteId;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry,
            Object... objects) throws EntryProcessorException {
            IgniteEx loc = (IgniteEx)Ignition.localIgnite();

            if (remoteId.equals(loc.localNode().id()))
                loc.cache(CACHE_NAME).put(key, key);

            return null;
        }
    }
}
