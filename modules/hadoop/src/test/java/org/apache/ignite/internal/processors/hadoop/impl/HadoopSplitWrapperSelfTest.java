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

package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopSplitWrapper;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Self test of {@link HadoopSplitWrapper}.
 */
public class HadoopSplitWrapperSelfTest extends HadoopAbstractSelfTest {
    /**
     * Tests serialization of wrapper and the wrapped native split.
     * @throws Exception If fails.
     */
    @Test
    public void testSerialization() throws Exception {
        FileSplit nativeSplit = new FileSplit(new Path("/path/to/file"), 100, 500, new String[]{"host1", "host2"});

        assertEquals("/path/to/file:100+500", nativeSplit.toString());

        HadoopSplitWrapper split = HadoopUtils.wrapSplit(10, nativeSplit, nativeSplit.getLocations());

        assertEquals("[host1, host2]", Arrays.toString(split.hosts()));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        ObjectOutput out = new ObjectOutputStream(buf);

        out.writeObject(split);

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray()));

        final HadoopSplitWrapper res = (HadoopSplitWrapper)in.readObject();

        assertEquals("/path/to/file:100+500", HadoopUtils.unwrapSplit(res).toString());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                res.hosts();

                return null;
            }
        }, AssertionError.class, null);
    }


}
