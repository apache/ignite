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

package org.apache.ignite.internal.binary;

import java.lang.reflect.Field;

/**
 * Field descriptor.
 */
class BinaryFieldDescriptor {
    /** Field ID. */
    final int id;

    /** Field name */
    final String name;

    /** Mode. */
    final BinaryWriteMode mode;

    /** Offset. Used for primitive fields, only. */
    final long offset;

    /** Target field. */
    final Field field;

    /** Dynamic accessor flag. */
    final boolean dynamic;

    /**
     * Protected constructor.
     *
     * @param field Field.
     * @param id Field ID.
     * @param mode Mode;
     * @param offset Offset of the field in the byte array
     * @param dynamic If {@code true} then field is not final.
     */
    protected BinaryFieldDescriptor(Field field, int id, BinaryWriteMode mode, long offset, boolean dynamic) {
        assert field != null;
        assert id != 0;
        assert mode != null;

        this.name = field.getName();
        this.id = id;
        this.mode = mode;
        this.offset = offset;
        this.field = field;
        this.dynamic = dynamic;
    }
}
