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

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Self test of {@link GridHadoopSplitWrapper}.
 */
public class GridHadoopSplitWrapperSelfTest extends GridHadoopAbstractSelfTest {
    /**
     * Tests serialization of wrapper and the wrapped native split.
     * @throws Exception If fails.
     */
    public void testSerialization() throws Exception {
        FileSplit nativeSplit = new FileSplit(new Path("/path/to/file"), 100, 500, new String[]{"host1", "host2"});

        assertEquals("/path/to/file:100+500", nativeSplit.toString());

        GridHadoopSplitWrapper split = GridHadoopUtils.wrapSplit(10, nativeSplit, nativeSplit.getLocations());

        assertEquals("[host1, host2]", Arrays.toString(split.hosts()));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        ObjectOutput out = new ObjectOutputStream(buf);

        out.writeObject(split);

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray()));

        final GridHadoopSplitWrapper res = (GridHadoopSplitWrapper)in.readObject();

        assertEquals("/path/to/file:100+500", GridHadoopUtils.unwrapSplit(res).toString());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                res.hosts();

                return null;
            }
        }, AssertionError.class, null);
    }


}
