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
import org.apache.ignite.internal.processors.hadoop.v2.HadoopSerializationWrapper;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopV2Job;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Self test of {@link org.apache.ignite.internal.processors.hadoop.v2.HadoopV2Job}.
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
     * Tests that {@link HadoopJob} provides wrapped serializer if it's set in configuration.
     *
     * @throws IgniteCheckedException If fails.
     */
    public void testCustomSerializationApplying() throws IgniteCheckedException {
        JobConf cfg = new JobConf();

        cfg.setMapOutputKeyClass(IntWritable.class);
        cfg.setMapOutputValueClass(Text.class);
        cfg.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, CustomSerialization.class.getName());

        HadoopDefaultJobInfo info = createJobInfo(cfg);

        final UUID uuid = UUID.randomUUID();

        HadoopJobId id = new HadoopJobId(uuid, 1);

        HadoopJob job = info.createJob(HadoopV2Job.class, id, log);

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