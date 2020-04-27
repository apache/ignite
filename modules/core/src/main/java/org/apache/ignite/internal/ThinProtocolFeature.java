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

package org.apache.ignite.internal;

import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;

/**
 * The base feature class.
 */
public interface ThinProtocolFeature {
    /**
     * @return Feature ID.
     */
    public int featureId();

    /**
     * @return Feature name.
     */
    public String name();

    /**
     * @param features Feature set.
     * @return Byte array representing all supported features.
     */
    public static <E extends Enum<E> & ThinProtocolFeature> byte[] featuresAsBytes(Collection<E> features) {
        final BitSet set = new BitSet();

        for (ThinProtocolFeature f : features)
            set.set(f.featureId());

        return set.toByteArray();
    }

    /**
     * Create EnumSet of supported features encoded by the bytes array.
     *
     * @param in Byte array representing all supported features.
     * @param enumCls Type of the enum encoded by the bits at the byte array.
     */
    public static <E extends Enum<E> & ThinProtocolFeature> EnumSet<E> enumSet(byte[] in, Class<E> enumCls) {
        EnumSet<E> set = EnumSet.noneOf(enumCls);

        if (in == null)
            return set;

        final BitSet bSet = BitSet.valueOf(in);

        for (E e : enumCls.getEnumConstants()) {
            if (bSet.get(e.featureId()))
                set.add(e);
        }

        return set;
    }
}
