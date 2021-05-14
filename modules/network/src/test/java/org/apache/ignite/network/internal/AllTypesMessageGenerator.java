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

package org.apache.ignite.network.internal;

import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Generator for an {@link AllTypesMessage}.
 */
public class AllTypesMessageGenerator {
    /**
     * Generate a new {@link AllTypesMessage}.
     *
     * @param seed Random seed.
     * @param nestedMsg {@code true} if nested messages should be generated, {@code false} otherwise.
     * @return Message.
     * @throws Exception If failed.
     */
    public static AllTypesMessage generate(long seed, boolean nestedMsg) {
        try {
            var random = new Random(seed);

            var message = new AllTypesMessage();

            Field[] fields = AllTypesMessage.class.getDeclaredFields();

            for (Field field : fields) {
                TestFieldType annotation = field.getAnnotation(TestFieldType.class);

                if (annotation != null) {
                    field.set(message, randomValue(random, annotation.value(), nestedMsg));
                }
            }

            if (nestedMsg) {
                message.v = IntStream.range(0, 10).mapToObj(unused -> generate(seed, false)).toArray();

                message.w = IntStream.range(0, 10)
                    .mapToObj(unused -> generate(seed, false))
                    .collect(Collectors.toList());

                message.x = IntStream.range(0, 10)
                    .boxed()
                    .collect(Collectors.toMap(String::valueOf, unused -> generate(seed, false)));
            }

            return message;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generate random value.
     *
     * @param random Seeded random.
     * @param type Value type.
     * @param nestedMsg {@code true} if nested messages should be generated, {@code false} otherwise.
     * @return Random value.
     * @throws Exception If failed.
     */
    private static Object randomValue(Random random, MessageCollectionItemType type, boolean nestedMsg) throws Exception {
        switch (type) {
            case BYTE:
                return (byte) random.nextInt();

            case SHORT:
                return (short) random.nextInt();

            case INT:
                return random.nextInt();

            case LONG:
                return random.nextLong();

            case FLOAT:
                return random.nextFloat();

            case DOUBLE:
                return random.nextDouble();

            case CHAR:
                return (char) random.nextInt();

            case BOOLEAN:
                return random.nextBoolean();

            case BYTE_ARR:
                int byteArrLen = random.nextInt(1024);
                byte[] bytes = new byte[byteArrLen];
                random.nextBytes(bytes);
                return bytes;

            case SHORT_ARR:
                int shortArrLen = random.nextInt(1024);
                short[] shorts = new short[1024];
                for (int i = 0; i < shortArrLen; i++) {
                    shorts[i] = (short) random.nextInt();
                }
                return shorts;

            case INT_ARR:
                int intArrLen = random.nextInt(1024);
                int[] ints = new int[1024];
                for (int i = 0; i < intArrLen; i++) {
                    ints[i] = random.nextInt();
                }
                return ints;

            case LONG_ARR:
                int longArrLen = random.nextInt(1024);
                long[] longs = new long[1024];
                for (int i = 0; i < longArrLen; i++) {
                    longs[i] = random.nextLong();
                }
                return longs;

            case FLOAT_ARR:
                int floatArrLen = random.nextInt(1024);
                float[] floats = new float[1024];
                for (int i = 0; i < floatArrLen; i++) {
                    floats[i] = random.nextFloat();
                }
                return floats;

            case DOUBLE_ARR:
                int doubleArrLen = random.nextInt(1024);
                double[] doubles = new double[1024];
                for (int i = 0; i < doubleArrLen; i++) {
                    doubles[i] = random.nextDouble();
                }
                return doubles;

            case CHAR_ARR:
                int charArrLen = random.nextInt(1024);
                char[] chars = new char[1024];
                for (int i = 0; i < charArrLen; i++) {
                    chars[i] = (char) random.nextInt();
                }
                return chars;

            case BOOLEAN_ARR:
                int booleanArrLen = random.nextInt(1024);
                boolean[] booleans = new boolean[1024];
                for (int i = 0; i < booleanArrLen; i++) {
                    booleans[i] = random.nextBoolean();
                }
                return booleans;

            case STRING:
                int aLetter = 'a';
                int strLen = random.nextInt(1024);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < strLen; i++) {
                    int letter = aLetter + random.nextInt(26);
                    sb.append(letter);
                }
                return sb.toString();

            case BIT_SET:
                BitSet set = new BitSet();
                int setLen = random.nextInt(10);
                for (int i = 0; i < setLen; i++) {
                    if (random.nextBoolean()) {
                        set.set(i);
                    }
                }

                return set;

            case UUID:
                byte[] uuidBytes = new byte[16];
                random.nextBytes(uuidBytes);
                return UUID.nameUUIDFromBytes(uuidBytes);

            case IGNITE_UUID:
                byte[] igniteUuidBytes = new byte[16];
                random.nextBytes(igniteUuidBytes);
                var generator = new IgniteUuidGenerator(UUID.nameUUIDFromBytes(igniteUuidBytes), 0);
                return generator.randomUuid();

            case MSG:
                if (nestedMsg) {
                    return generate(random.nextLong(), false);
                }
                return null;

        }

        return null;
    }
}
