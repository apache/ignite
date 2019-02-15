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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * {@link IgfsEntryInfo} test case.
 */
@RunWith(JUnit4.class)
public class IgfsFileInfoSelfTest extends IgfsCommonAbstractTest {
    /** Marshaller to test {@link Externalizable} interface. */
    private final Marshaller marshaller;

    /** Ctor. */
    public IgfsFileInfoSelfTest() throws IgniteCheckedException {
        marshaller = createStandaloneBinaryMarshaller();
    }


    /**
     * Test node info serialization.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSerialization() throws Exception {
        marshaller.setContext(new MarshallerContextTestImpl());

        multithreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @SuppressWarnings("deprecation") // Suppress due to default constructor should never be used directly.
            @Nullable @Override public Object call() throws IgniteCheckedException {
                testSerialization(IgfsUtils.createDirectory(IgniteUuid.randomUuid()));

                return null;
            }
        }, 20);
    }

    /**
     * Test node info serialization.
     *
     * @param info Node info to test serialization for.
     * @throws IgniteCheckedException If failed.
     */
    public void testSerialization(IgfsEntryInfo info) throws IgniteCheckedException {
        assertEquals(info, mu(info));
    }

    /**
     * Marshal/unmarshal object.
     *
     * @param obj Object to marshal/unmarshal.
     * @return Marshalled and then unmarshalled object.
     * @throws IgniteCheckedException In case of any marshalling exception.
     */
    private <T> T mu(T obj) throws IgniteCheckedException {
        return marshaller.unmarshal(marshaller.marshal(obj), null);
    }
}
