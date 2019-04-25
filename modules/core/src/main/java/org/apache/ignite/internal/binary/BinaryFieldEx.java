/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary;

import java.nio.ByteBuffer;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;

/**
 *
 */
public interface BinaryFieldEx extends BinaryField {
    /**
     * @return Type ID this field relates to.
     */
    public int typeId();

    /**
     * Writes field value to the given byte buffer.
     *
     * @param obj Object from which the field should be extracted.
     * @param buf Buffer to write the field value to.
     * @return {@code True} if the value was successfully written, {@code false} if there is not enough space
     *      for the field in the buffer.
     */
    public boolean writeField(BinaryObject obj, ByteBuffer buf);

    /**
     * Reads field value from the given byte buffer.
     *
     * @param buf Buffer to read value from.
     * @return Field value.
     */
    public <F> F readField(ByteBuffer buf);
}
