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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Schema describing binary object content.
 */
public interface BinarySchema {
    /**
     * @return Schema ID.
     */
    public int schemaId();

    /**
     * Try speculatively confirming order for the given field name.
     *
     * @param expOrder Expected order.
     * @param expName Expected name.
     * @return Field ID.
     */
    public Confirmation confirmOrder(int expOrder, String expName);

    /**
     * Add field name.
     *
     * @param order Order.
     * @param name Name.
     */
    public void clarifyFieldName(int order, String name);

    /**
     * Get field ID by order in footer.
     *
     * @param order Order.
     * @return Field ID.
     */
    public int fieldId(int order);

    /**
     * Get field order in footer by field ID.
     *
     * @param id Field ID.
     * @return Offset or {@code 0} if there is no such field.
     */
    public int order(int id);

    /**
     * The object implements the writeTo method to save its contents
     * by calling the methods of DataOutput for its primitive values and strings or
     * calling the writeTo method for other objects.
     *
     * @param out the stream to write the object to.
     * @throws IOException Includes any I/O exceptions that may occur.
     */
    public void writeTo(DataOutput out) throws IOException;

    /**
     * The object implements the readFrom method to restore its
     * contents by calling the methods of DataInput for primitive
     * types and strings or calling readFrom for other objects.  The
     * readFrom method must read the values in the same sequence
     * and with the same types as were written by writeTo.
     *
     * @param in the stream to read data from in order to restore the object
     * @throws IOException if I/O errors occur
     */
    public void readFrom(DataInput in) throws IOException;

    /**
     * Gets field ids array.
     *
     * @return Field ids.
     */
    public int[] fieldIds();

    /**
     * Order confirmation result.
     */
    public enum Confirmation {
        /** Confirmed. */
        CONFIRMED,

        /** Denied. */
        REJECTED,

        /** Field name clarification is needed. */
        CLARIFY
    }
}
