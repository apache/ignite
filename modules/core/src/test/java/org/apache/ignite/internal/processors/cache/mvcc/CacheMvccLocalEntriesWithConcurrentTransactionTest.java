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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 *
 */
public class CacheMvccLocalEntriesWithConcurrentTransactionTest extends CacheMvccAbstractFeatureTest {
    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLocalEntries() throws Exception {
        doTestConsistency(clo);
    }

    /** Test closure. */
    private final IgniteClosure2X<CountDownLatch, CountDownLatch, List<Person>> clo =
        new IgniteClosure2X<CountDownLatch, CountDownLatch, List<Person>>() {
        @Override public List<Person> applyx(CountDownLatch startLatch, CountDownLatch endLatch2)
            throws IgniteCheckedException {
            Iterator<Cache.Entry<Integer, Person>> it = cache().localEntries(CachePeekMode.PRIMARY).iterator();

            List<Cache.Entry<Integer, Person>> pres = new ArrayList<>();

            for (int i = 0; i < 10; i++)
                pres.add(it.next());

            if (startLatch != null)
                startLatch.countDown();

            while (it.hasNext())
                pres.add(it.next());

            if (endLatch2 != null)
                U.await(endLatch2);

            return entriesToPersons(pres);
        }
    };
}
