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

package org.apache.ignite.tensorflow.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link ProcessManagerWrapper}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ProcessManagerWrapperTest {
    /** Delegate. */
    @Mock
    private ProcessManager<String> delegate;

    /** Process manager wrapper. */
    private ProcessManagerWrapper<String, Integer> wrapper;

    /** Initializes tests. */
    @Before
    public void init() {
        wrapper = new TestProcessManagerWrapper(delegate);
    }

    /** */
    @Test
    public void testStart() {
        wrapper.start(Arrays.asList(1, 2, 3));

        verify(delegate).start(eq(Arrays.asList("1", "2", "3")));
    }

    /** */
    @Test
    public void testPing() {
        Map<UUID, List<UUID>> procIds = Collections.emptyMap();
        wrapper.ping(procIds);

        verify(delegate).ping(eq(procIds));
    }

    /** */
    @Test
    public void testStop() {
        Map<UUID, List<UUID>> procIds = Collections.emptyMap();
        wrapper.stop(procIds, true);

        verify(delegate).stop(eq(procIds), eq(true));
    }

    /** */
    @Test
    public void testClear() {
        Map<UUID, List<UUID>> procIds = Collections.emptyMap();
        wrapper.clear(procIds);

        verify(delegate).clear(eq(procIds));
    }

    /**
     * Process manager wrapper to be used in tests.
     */
    private static class TestProcessManagerWrapper extends ProcessManagerWrapper<String, Integer> {
        /** */
        private static final long serialVersionUID = 7562628311662129855L;

        /**
         * Constructs a new instance of process manager wrapper.
         *
         * @param delegate Delegate.
         */
        public TestProcessManagerWrapper(ProcessManager<String> delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override protected String transformSpecification(Integer spec) {
            return spec.toString();
        }
    }
}
