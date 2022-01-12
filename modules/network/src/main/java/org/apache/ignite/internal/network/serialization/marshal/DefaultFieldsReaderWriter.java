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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * Knows how to read/write fields in the 'default way' (that is, the way that is default for User Object Serialization,
 * as opposed to user customizations possible via writeObject()/readObject() methods).
 */
interface DefaultFieldsReaderWriter {
    /**
     * Writes object fields to the output stream using default marshalling.
     *
     * @param object     object which fields to write
     * @param descriptor object class descriptor
     * @param output     output to write to
     * @param context    marshalling context
     * @throws MarshalException if something goes wrong
     * @throws IOException      if I/O fails
     */
    void defaultWriteFields(Object object, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException;

    /**
     * Reads object fields from the input stream and stores them in the object using default marshalling.
     *
     * @param input      input from which to read
     * @param object     object to fill
     * @param descriptor object class descriptor
     * @param context    unmarshalling context
     * @throws IOException          if I/O fails
     * @throws UnmarshalException   if something goes wrong
     */
    void defaultFillFieldsFrom(DataInputStream input, Object object, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException;
}
