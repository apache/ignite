/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.io.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;

import java.util.*;

/**
 * Context for test purpose.
 */
class GridHadoopTestTaskContext extends GridHadoopTaskContext {
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
    private GridHadoopTaskOutput output = new GridHadoopTaskOutput() {
        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) {
            //Check of casting and extract/copy values
            String strKey = new String(((Text)key).getBytes());
            int intVal = ((IntWritable)val).get();

            mockOutput().add(new Pair<>(strKey, intVal));
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> finish() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            throw new UnsupportedOperationException();
        }
    };

    /** Context input implementation to read data from mockInput. */
    private GridHadoopTaskInput input = new GridHadoopTaskInput() {
        /** Iterator of keys and associated lists of values. */
        Iterator<Map.Entry<Object, List>> iterator;

        /** Current key and associated value list. */
        Map.Entry<Object, List> currentEntry;

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (iterator == null) {
                iterator = mockInput().entrySet().iterator();
            }

            if (iterator.hasNext()) {
                currentEntry = iterator.next();
            } else {
                currentEntry = null;
            }

            return currentEntry != null;
        }

        /** {@inheritDoc} */
        @Override public Object key() {
            return currentEntry.getKey();
        }

        /** {@inheritDoc} */
        @Override public Iterator<?> values() {
            return currentEntry.getValue().iterator() ;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
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

        for (GridHadoopTestTaskContext.Pair<String, Integer> pair : flatData) {
            key.set(pair.key);
            ArrayList<IntWritable> valList;

            if (!mockInput.containsKey(key)) {
                valList = new ArrayList<>();
                mockInput.put(key, valList);
                key = new Text();
            }
            else {
                valList = (ArrayList<IntWritable>) mockInput.get(key);
            }
            valList.add(new IntWritable(pair.value()));
        }
    }

    /**
     * @param gridJob Grid Hadoop job.
     */
    public GridHadoopTestTaskContext(GridHadoopJob gridJob) {
        super(null, gridJob, null, null);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskOutput output() {
        return output;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskInput input() {
        return input;
    }
}
