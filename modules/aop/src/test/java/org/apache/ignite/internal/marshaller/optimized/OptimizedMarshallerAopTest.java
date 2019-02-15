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

package org.apache.ignite.internal.marshaller.optimized;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.compute.gridify.Gridify;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * Test use GridOptimizedMarshaller and AspectJ AOP.
 *
 * The following configuration needs to be applied to enable AspectJ byte code
 * weaving.
 * <ul>
 * <li>
 *      JVM configuration should include:
 *      <tt>-javaagent:[IGNITE_HOME]/libs/aspectjweaver-1.7.2.jar</tt>
 * </li>
 * <li>
 *      Classpath should contain the <tt>[IGNITE_HOME]/modules/tests/config/aop/aspectj</tt> folder.
 * </li>
 * </ul>
 */
@RunWith(JUnit4.class)
public class OptimizedMarshallerAopTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicInteger cntr = new AtomicInteger();

    /**
     * Constructs a test.
     */
    public OptimizedMarshallerAopTest() {
        super(false /* start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setMarshaller(new OptimizedMarshaller());

        G.start(cfg);

        assert G.allGrids().size() == 1;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUp() throws Exception {
        G.ignite().events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                cntr.incrementAndGet();

                return true;
            }
        }, EVT_TASK_FINISHED);

        gridify1();

        assertEquals("Method gridify() wasn't executed on grid.", 1, cntr.get());
    }

    /**
     * Method grid-enabled with {@link org.apache.ignite.compute.gridify.Gridify} annotation.
     * <p>
     * Note that default {@code Gridify} configuration is used, so this method
     * will be executed on remote node with the same argument.
     */
    @Gridify
    private void gridify1() {
        X.println("Executes on grid");
    }
}
