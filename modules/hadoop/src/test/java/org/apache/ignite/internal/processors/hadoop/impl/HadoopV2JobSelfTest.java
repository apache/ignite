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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.UUID;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopDefaultJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.processors.hadoop.HadoopHelperImpl;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopSerializationWrapper;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2Job;
import org.junit.Test;

import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 * Self test of {@link org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2Job}.
 */
public class HadoopV2JobSelfTest extends HadoopAbstractSelfTest {
    /** */
    private static final String TEST_SERIALIZED_VALUE = "Test serialized value";

    /**
     * Custom serialization class that accepts {@link Writable}.
     */
    private static class CustomSerialization extends WritableSerialization {
        /** {@inheritDoc} */
        @Override public Deserializer<Writable> getDeserializer(Class<Writable> c) {
            return new Deserializer<Writable>() {
                @Override public void open(InputStream in) { }

                @Override public Writable deserialize(Writable writable) {
                    return new Text(TEST_SERIALIZED_VALUE);
                }

                @Override public void close() { }
            };
        }
    }

    /**
     * Tests that {@link HadoopJobEx} provides wrapped serializer if it's set in configuration.
     *
     * @throws IgniteCheckedException If fails.
     */
    @Test
    public void testCustomSerializationApplying() throws IgniteCheckedException {
        JobConf cfg = new JobConf();

        cfg.setMapOutputKeyClass(IntWritable.class);
        cfg.setMapOutputValueClass(Text.class);
        cfg.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, CustomSerialization.class.getName());

        HadoopDefaultJobInfo info = createJobInfo(cfg, null);

        final UUID uuid = UUID.randomUUID();

        HadoopJobId id = new HadoopJobId(uuid, 1);

        HadoopJobEx job = info.createJob(HadoopV2Job.class, id, log, null, new HadoopHelperImpl());

        HadoopTaskContext taskCtx = job.getTaskContext(new HadoopTaskInfo(HadoopTaskType.MAP, null, 0, 0,
            null));

        HadoopSerialization ser = taskCtx.keySerialization();

        assertEquals(HadoopSerializationWrapper.class.getName(), ser.getClass().getName());

        DataInput in = new DataInputStream(new ByteArrayInputStream(new byte[0]));

        assertEquals(TEST_SERIALIZED_VALUE, ser.read(in, null).toString());

        ser = taskCtx.valueSerialization();

        assertEquals(HadoopSerializationWrapper.class.getName(), ser.getClass().getName());

        assertEquals(TEST_SERIALIZED_VALUE, ser.read(in, null).toString());
    }
}
