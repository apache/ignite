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

package org.apache.ignite.internal.util.tostring;

import java.lang.reflect.Field;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Simple field descriptor containing field name and its order in the class descriptor.
 */
public class UnsafeToStringFieldDescriptor extends GridToStringFieldDescriptor {
    /**
     * @param field Field;
     */
    public UnsafeToStringFieldDescriptor(Field field) {
        super(field);
    }

    /** {@inheritDoc} */
    @Override long offset(Field field) {
        return GridUnsafe.objectFieldOffset(field);
    }

    /** {@inheritDoc} */
    @Override public Object objectValue(Object obj) {
        return GridUnsafe.getObjectField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(Object obj) {
        return GridUnsafe.getByteField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public boolean booleanValue(Object obj) {
        return GridUnsafe.getBooleanField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public char charValue(Object obj) {
        return GridUnsafe.getCharField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public short shortValue(Object obj) {
        return GridUnsafe.getShortField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public int intField(Object obj) {
        return GridUnsafe.getIntField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public float floatField(Object obj) {
        return GridUnsafe.getFloatField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public long longField(Object obj) {
        return GridUnsafe.getLongField(obj, offset());
    }

    /** {@inheritDoc} */
    @Override public double doubleField(Object obj) {
        return GridUnsafe.getDoubleField(obj, offset());
    }
}
