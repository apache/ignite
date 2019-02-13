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
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when EntryProcessor closure is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class EntryProcessorRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR));
        runAndCheck(grid(CLNT_INITIATOR));
    }

    /**
     * @param initiator Node that initiates an execution.
     */
    private void runAndCheck(IgniteEx initiator) {
        UUID secSubjectId = secSubjectId(initiator);

        for (InvokeMethodEnum ime : InvokeMethodEnum.values()) {
            VERIFIER.start(secSubjectId)
                .add(SRV_TRANSITION, 1)
                .add(SRV_ENDPOINT, 1);

            invoke(ime, initiator,
                new TestEntryProcessor(ime, SRV_ENDPOINT), prmKey(grid(SRV_TRANSITION)));

            VERIFIER.checkResult();
        }
    }

    /**
     * @param ime Invoke Method.
     * @param initiator Initiator.
     * @param ep Entry Processor.
     * @param key Key.
     */
    private static void invoke(InvokeMethodEnum ime, IgniteEx initiator,
        EntryProcessor<Integer, Integer, Object> ep, Integer key) {
        switch (ime) {
            case INVOKE:
                initiator.<Integer, Integer>cache(CACHE_NAME)
                    .invoke(key, ep);

                break;
            case INVOKE_ALL:
                initiator.<Integer, Integer>cache(CACHE_NAME)
                    .invokeAll(Collections.singleton(key), ep)
                    .values().stream().findFirst().ifPresent(EntryProcessorResult::get);

                break;
            case INVOKE_ASYNC:
                initiator.<Integer, Integer>cache(CACHE_NAME)
                    .invokeAsync(key, ep).get();

                break;
            case INVOKE_ALL_ASYNC:
                initiator.<Integer, Integer>cache(CACHE_NAME)
                    .invokeAllAsync(Collections.singleton(key), ep)
                    .get().values().stream().findFirst().ifPresent(EntryProcessorResult::get);

                break;
            default:
                throw new IllegalArgumentException("Unknown invoke method " + ime);
        }
    }

    /** Enum for ways to invoke EntryProcessor. */
    private enum InvokeMethodEnum {
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
     * Entry processor for tests with transition invoke call.
     */
    static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        /** Invoke method. */
        private final InvokeMethodEnum ime;

        /** Endpoint node name. */
        private final String endpoint;

        /**
         * @param ime Invoke method.
         * @param endpoint Endpoint node name.
         */
        public TestEntryProcessor(InvokeMethodEnum ime, String endpoint) {
            this.ime = ime;
            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry,
            Object... objects) throws EntryProcessorException {
            IgniteEx loc = (IgniteEx)Ignition.localIgnite();

            VERIFIER.verify(loc);

            if (endpoint != null) {
                invoke(
                    ime, loc, new TestEntryProcessor(ime, null), prmKey(endpoint)
                );
            }

            return null;
        }
    }
}
