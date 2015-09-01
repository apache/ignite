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

package org.apache.ignite.internal.processors.hadoop;

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
import org.apache.ignite.internal.processors.hadoop.v2.HadoopSplitWrapper;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Self test of {@link org.apache.ignite.internal.processors.hadoop.v2.HadoopSplitWrapper}.
 */
public class HadoopSplitWrapperSelfTest extends HadoopAbstractSelfTest {
    /**
     * Tests serialization of wrapper and the wrapped native split.
     * @throws Exception If fails.
     */
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