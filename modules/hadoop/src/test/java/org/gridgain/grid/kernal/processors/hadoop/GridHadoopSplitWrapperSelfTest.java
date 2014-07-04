/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.gridgain.grid.hadoop.*;
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

        GridHadoopSplitWrapper split = new GridHadoopSplitWrapper(10, nativeSplit, nativeSplit.getLocations());

        assertEquals("[host1, host2]", Arrays.toString(split.hosts()));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        ObjectOutput out = new ObjectOutputStream(buf);

        out.writeObject(split);

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray()));

        final GridHadoopSplitWrapper res = (GridHadoopSplitWrapper)in.readObject();

        assertEquals("/path/to/file:100+500", res.innerSplit().toString());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                res.hosts();

                return null;
            }
        }, AssertionError.class, null);
    }


}
