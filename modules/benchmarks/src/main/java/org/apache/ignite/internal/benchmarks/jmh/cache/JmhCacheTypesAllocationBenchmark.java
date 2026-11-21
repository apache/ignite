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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

/** Cache put, get, getAll and invoke by key type, value type and value size; every key holds one value from setup. */
@SuppressWarnings("unchecked")
public class JmhCacheTypesAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Keys times {@link #valSize} the setup writes, at most; {@code valSize} sets the key count for every type. */
    private static final int DATA_SIZE = 10_000_000;

    /** Keys per {@link #getAll()}. */
    private static final int GET_ALL_KEYS = 10;

    /** Key type. */
    public enum KeyType {
        /** {@link Integer}. */
        INT,

        /** {@link String}. */
        STRING,

        /** {@link UUID}. */
        UUID,

        /** A class with an affinity key field. */
        AFFINITY_POJO,

        /** {@code byte[16]}. */
        BYTES;

        /** @return Key number {@code i} of this type. */
        Object key(int i) {
            switch (this) {
                case INT:
                    return i;

                case STRING:
                    return "key-" + i;

                case UUID:
                    return new UUID(0, i);

                case AFFINITY_POJO:
                    return new AffinityPojoKey(i, i / 10);

                case BYTES:
                    // Ignite hashes byte[] keys with Arrays.hashCode(): an index in the last bytes spreads them evenly.
                    return ByteBuffer.allocate(16).putInt(12, i).array();

                default:
                    throw new IllegalStateException(name());
            }
        }
    }

    /** Value type; the value size sets the length of strings, arrays and collections. */
    public enum ValueType {
        /** {@link Byte}. */
        BYTE,

        /** {@link Short}. */
        SHORT,

        /** {@link Integer}. */
        INT,

        /** {@link Long}. */
        LONG,

        /** {@link Float}. */
        FLOAT,

        /** {@link Double}. */
        DOUBLE,

        /** {@link Character}. */
        CHAR,

        /** {@link Boolean}. */
        BOOLEAN,

        /** A string of Latin letters. */
        STRING,

        /** A string of Cyrillic letters, two bytes each in UTF-8. */
        STRING_CYRILLIC,

        /** {@link UUID}. */
        UUID,

        /** {@link BigDecimal}. */
        BIG_DECIMAL,

        /** {@link Date}. */
        DATE,

        /** {@link Timestamp}. */
        TIMESTAMP,

        /** {@code byte[]}. */
        BYTES,

        /** {@code int[]}. */
        INTS,

        /** {@code long[]}. */
        LONGS,

        /** {@code Object[]} of {@link Integer}. */
        OBJECTS,

        /** {@link ArrayList} of {@link Integer}. */
        LIST,

        /** {@link HashMap} of {@link Integer} to {@link Integer}. */
        MAP,

        /** A class with one {@code int} field. */
        POJO,

        /** A class with eight fields of different types. */
        POJO8,

        /** A class with a field of {@link #POJO8}'s class. */
        NESTED_POJO,

        /** A class that implements {@link org.apache.ignite.binary.Binarylizable}. */
        BINARYLIZABLE,

        /** A class that implements only {@link Externalizable}. */
        EXTERNALIZABLE,

        /** An enum. */
        ENUM,

        /** {@link #POJO8} as a binary object, through the cache with {@code keepBinary}. */
        BINARY_OBJECT;

        /** @return Value of this type; {@code size} is the length of strings, arrays and collections. */
        Object value(int size) {
            switch (this) {
                case BYTE:
                    return (byte)100;

                case SHORT:
                    return (short)10_000;

                case INT:
                    return 1_000_000;

                case LONG:
                    return 1_000_000_000_000L;

                case FLOAT:
                    return 1.5f;

                case DOUBLE:
                    return 1.5d;

                case CHAR:
                    return 'c';

                case BOOLEAN:
                    return true;

                case STRING:
                    return "a".repeat(size);

                case STRING_CYRILLIC:
                    return "ж".repeat(size);

                case UUID:
                    return java.util.UUID.randomUUID();

                case BIG_DECIMAL:
                    return new BigDecimal("12345678901234567890.123456789");

                case DATE:
                    return new Date();

                case TIMESTAMP:
                    return new Timestamp(System.currentTimeMillis());

                case BYTES:
                    byte[] bytes = new byte[size];

                    Arrays.fill(bytes, (byte)1);

                    return bytes;

                case INTS:
                    int[] ints = new int[size];

                    Arrays.fill(ints, 1_000_000);

                    return ints;

                case LONGS:
                    long[] longs = new long[size];

                    Arrays.fill(longs, 1_000_000_000_000L);

                    return longs;

                case OBJECTS:
                    Object[] objs = new Object[size];

                    for (int i = 0; i < size; i++)
                        objs[i] = 1_000_000 + i;

                    return objs;

                case LIST:
                    List<Integer> list = new ArrayList<>(size);

                    for (int i = 0; i < size; i++)
                        list.add(1_000_000 + i);

                    return list;

                case MAP:
                    Map<Integer, Integer> map = new HashMap<>();

                    for (int i = 0; i < size; i++)
                        map.put(1_000_000 + i, 1_000_000 + i);

                    return map;

                case POJO:
                    return new Pojo(1_000_000);

                case NESTED_POJO:
                    return new NestedPojo(1_000_000, new Pojo8());

                case BINARYLIZABLE:
                    return new IntValue(1_000_000);

                case EXTERNALIZABLE:
                    return new ExternalizableValue(1_000_000);

                case ENUM:
                    return Color.GREEN;

                case POJO8:
                case BINARY_OBJECT:
                    return new Pojo8();

                default:
                    throw new IllegalStateException(name());
            }
        }
    }

    /** Atomicity mode. */
    @Param("ATOMIC")
    private CacheAtomicityMode atomicity;

    /** Key type. */
    @Param("INT")
    private KeyType keyType;

    /** Value type. */
    @Param("POJO")
    private ValueType valType;

    /** Length of string, array and collection values. */
    @Param("100")
    private int valSize;

    /** Keys the setup writes. */
    private Object[] keys;

    /** The value of every key. */
    private Object val;

    /** Sets of {@link #GET_ALL_KEYS} consecutive keys. */
    private Set<Object>[] keySets;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        val = valType.value(valSize);

        if (valType == ValueType.BINARY_OBJECT) {
            val = node.binary().toBinary(val);
            cache = cache.withKeepBinary();
        }

        keys = new Object[Math.min(CNT, DATA_SIZE / Math.max(valSize, 100))];

        Object[] vals = new Object[keys.length];

        for (int i = 0; i < keys.length; i++) {
            keys[i] = keyType.key(i);
            vals[i] = val;
        }

        populate(keys, vals);

        keySets = new Set[keys.length / GET_ALL_KEYS];

        for (int i = 0; i < keySets.length; i++)
            keySets[i] = new LinkedHashSet<>(Arrays.asList(keys).subList(i * GET_ALL_KEYS, (i + 1) * GET_ALL_KEYS));
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration().setAtomicityMode(atomicity);
    }

    /** Replaces the value of a key with the same value: an update, not an insert. */
    @Benchmark
    public void put() {
        cache.put(keys[ThreadLocalRandom.current().nextInt(keys.length)], val);
    }

    /** @return Value of a key. */
    @Benchmark
    public Object get() {
        return cache.get(keys[ThreadLocalRandom.current().nextInt(keys.length)]);
    }

    /** @return Values of {@link #GET_ALL_KEYS} consecutive keys. */
    @Benchmark
    public Object getAll() {
        return cache.getAll(keySets[ThreadLocalRandom.current().nextInt(keySets.length)]);
    }

    /** Runs an entry processor that writes the value back. */
    @Benchmark
    public void invoke() {
        cache.invoke(keys[ThreadLocalRandom.current().nextInt(keys.length)], SAME_VALUE);
    }

    /** Key with an affinity key field. */
    private static class AffinityPojoKey {
        /** Id. */
        private final int id;

        /** Affinity key. */
        @AffinityKeyMapped
        private final int affId;

        /** */
        AffinityPojoKey(int id, int affId) {
            this.id = id;
            this.affId = affId;
        }
    }

    /** Value with one field. */
    private static class Pojo {
        /** Value. */
        private final int val;

        /** */
        Pojo(int val) {
            this.val = val;
        }
    }

    /** Value with eight fields of different types. */
    private static class Pojo8 {
        /** */
        private int intVal = 1_000_000;

        /** */
        private long longVal = 1_000_000_000_000L;

        /** */
        private double doubleVal = 1.5d;

        /** */
        private boolean boolVal = true;

        /** */
        private String str = "abcdefghijklmnop";

        /** */
        private UUID uuid = UUID.randomUUID();

        /** */
        private Date date = new Date();

        /** */
        private byte[] bytes = new byte[16];
    }

    /** Value with a nested object. */
    private static class NestedPojo {
        /** Value. */
        private final int val;

        /** Nested object. */
        private final Pojo8 nested;

        /** */
        NestedPojo(int val, Pojo8 nested) {
            this.val = val;
            this.nested = nested;
        }
    }

    /** Value that implements only {@link Externalizable}. */
    public static class ExternalizableValue implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Value. */
        private int val;

        /** Constructor for deserialization. */
        public ExternalizableValue() {
            // No-op.
        }

        /** */
        ExternalizableValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            val = in.readInt();
        }
    }

    /** Enum value. */
    private enum Color {
        /** */
        RED,

        /** */
        GREEN
    }
}
