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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopV2TaskContext;

/**
 * Context for test purpose.
 */
class HadoopTestTaskContext extends HadoopV2TaskContext {
    /**
     * Simple key-vale pair.
     * @param <K> Key class.
     * @param <V> Value class.
     */
    public static class Pair<K,V> {
        /** Key */
        private K key;

        /** Value */
        private V val;

        /**
         * @param key key.
         * @param val value.
         */
        Pair(K key, V val) {
            this.key = key;
            this.val = val;
        }

        /**
         * Getter of key.
         * @return key.
         */
        K key() {
            return key;
        }

        /**
         * Getter of value.
         * @return value.
         */
        V value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return key + "," + val;
        }
    }

    /** Mock output container- result data of task execution if it is not overridden. */
    private List<Pair<String, Integer>> mockOutput = new ArrayList<>();

    /** Mock input container- input data if it is not overridden. */
    private Map<Object,List> mockInput = new TreeMap<>();

    /** Context output implementation to write data into mockOutput. */
    private HadoopTaskOutput output = new HadoopTaskOutput() {
        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) {
            //Check of casting and extract/copy values
            String strKey = new String(((Text)key).getBytes());
            int intVal = ((IntWritable)val).get();

            mockOutput().add(new Pair<>(strKey, intVal));
        }

        /** {@inheritDoc} */
        @Override public void close() {
            throw new UnsupportedOperationException();
        }
    };

    /** Context input implementation to read data from mockInput. */
    private HadoopTaskInput input = new HadoopTaskInput() {
        /** Iterator of keys and associated lists of values. */
        Iterator<Map.Entry<Object, List>> iter;

        /** Current key and associated value list. */
        Map.Entry<Object, List> currEntry;

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (iter == null)
                iter = mockInput().entrySet().iterator();

            if (iter.hasNext())
                currEntry = iter.next();
            else
                currEntry = null;

            return currEntry != null;
        }

        /** {@inheritDoc} */
        @Override public Object key() {
            return currEntry.getKey();
        }

        /** {@inheritDoc} */
        @Override public Iterator<?> values() {
            return currEntry.getValue().iterator() ;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Getter of mock output container - result of task if it is not overridden.
     *
     * @return mock output.
     */
    public List<Pair<String, Integer>> mockOutput() {
        return mockOutput;
    }

    /**
     * Getter of mock input container- input data if it is not overridden.
     *
     * @return mock output.
     */
    public Map<Object, List> mockInput() {
        return mockInput;
    }

    /**
     * Generate one-key-multiple-values tree from array of key-value pairs, and wrap its into Writable objects.
     * The result is placed into mock input.
     *
     * @param flatData list of key-value pair.
     */
    public void makeTreeOfWritables(Iterable<Pair<String, Integer>> flatData) {
        Text key = new Text();

        for (HadoopTestTaskContext.Pair<String, Integer> pair : flatData) {
            key.set(pair.key);
            ArrayList<IntWritable> valList;

            if (!mockInput.containsKey(key)) {
                valList = new ArrayList<>();
                mockInput.put(key, valList);
                key = new Text();
            }
            else
                valList = (ArrayList<IntWritable>) mockInput.get(key);
            valList.add(new IntWritable(pair.value()));
        }
    }

    /**
     * @param taskInfo Task info.
     * @param gridJob Grid Hadoop job.
     */
    public HadoopTestTaskContext(HadoopTaskInfo taskInfo, HadoopJob gridJob) throws IgniteCheckedException {
        super(taskInfo, gridJob, gridJob.id(), null, jobConfDataInput(gridJob));
    }

    /**
     * Creates DataInput to read JobConf.
     *
     * @param job Job.
     * @return DataInput with JobConf.
     * @throws IgniteCheckedException If failed.
     */
    private static DataInput jobConfDataInput(HadoopJob job) throws IgniteCheckedException {
        JobConf jobConf = new JobConf();

        for (Map.Entry<String, String> e : ((HadoopDefaultJobInfo)job.info()).properties().entrySet())
            jobConf.set(e.getKey(), e.getValue());

        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        try {
            jobConf.write(new DataOutputStream(buf));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return new DataInputStream(new ByteArrayInputStream(buf.toByteArray()));
    }

    /** {@inheritDoc} */
    @Override public HadoopTaskOutput output() {
        return output;
    }

    /** {@inheritDoc} */
    @Override public HadoopTaskInput input() {
        return input;
    }
}