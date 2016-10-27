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

package org.apache.ignite.binary;

import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * // TODO
 */
public class BinaryArrayIdentityResolver extends BinaryAbstractIdentityResolver {
    /** Singleton instance */
    private static final BinaryArrayIdentityResolver INSTANCE = new BinaryArrayIdentityResolver();

    /**
     * Get singleton instance.
     *
     * @return Singleton instance.
     */
    public static BinaryArrayIdentityResolver instance() {
        return INSTANCE;
    }

    /**
     * Default constructor.
     */
    public BinaryArrayIdentityResolver() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected int hashCode0(BinaryObject obj) {
        int hash = 1;

        if (obj instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

            int start = 0; // TODO;
            int end = 100; // TODO

            if (obj0.hasArray()) {
                // Handle heap object.
                byte[] data = obj0.array();

                for (int i = start; i < end; i++)
                    hash = 31 * hash + data[i];
            }
            else {
                // Handle offheap object.
                long ptr = obj0.offheapAddress();

                for (int i = start; i < end; i++)
                    hash = 31 * hash + BinaryPrimitives.readByte(ptr, i);
            }
        }
        else if (obj instanceof BinaryEnumObjectImpl) {
            int ord = obj.enumOrdinal();

            // Construct hash as if it was an int serialized in little-endian form.
            hash = 31 * hash + ord & 0x000000FF;
            hash = 31 * hash + ord & 0x0000FF00;
            hash = 31 * hash + ord & 0x00FF0000;
            hash = 31 * hash + ord & 0xFF000000;
        }
        else
            throw new BinaryObjectException("Array identity resolver cannot be used with provided BinaryObject " +
                "implementation: " + obj.getClass().getName());

        return hash;
    }

    /** {@inheritDoc} */
    @Override protected boolean equals0(BinaryObject o1, BinaryObject o2) {
        // TODO

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryArrayIdentityResolver.class, this);
    }
}
