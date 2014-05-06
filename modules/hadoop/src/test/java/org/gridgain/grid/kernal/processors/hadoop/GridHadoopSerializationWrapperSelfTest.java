/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;
import org.gridgain.grid.hadoop.GridHadoopSerialization;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.testframework.junits.common.GridCommonAbstractTest;

import java.io.*;
import java.util.*;

public class GridHadoopSerializationWrapperSelfTest extends GridCommonAbstractTest {

    public void testWritableSerialization() throws Exception {
        Serialization nativeSer = new WritableSerialization();

        //GridHadoopSerialization ser = new GridHadoopSerializationWrapper(nativeSer, IntWritable.class);

        GridHadoopSerialization ser = new GridHadoopWritableSerialization(IntWritable.class);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(buf);
        ser.write(out, new IntWritable(3));
        ser.write(out, new IntWritable(-5));

        System.out.println(Arrays.toString(buf.toByteArray()));

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf.toByteArray()));

        System.out.println(ser.read(in, null));
        System.out.println(ser.read(in, new IntWritable()));
    }

}
