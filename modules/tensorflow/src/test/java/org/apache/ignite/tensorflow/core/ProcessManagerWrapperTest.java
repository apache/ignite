/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
