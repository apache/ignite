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

package org.apache.ignite.ml.inference.reader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Model reader that reads predefined array of bytes.
 */
public class InMemoryModelReader implements ModelReader {
    /** */
    private static final long serialVersionUID = -5518861989758691500L;

    /** Data. */
    private final byte[] data;

    /**
     * Constructs a new instance of in-memory model reader that returns specified byte array.
     *
     * @param data Data.
     */
    public InMemoryModelReader(byte[] data) {
        this.data = data;
    }

    /**
     * Constructs a new instance of in-memory model reader that returns serialized specified object.
     *
     * @param obj Data object.
     * @param <T> Type of data object.
     */
    public <T extends Serializable> InMemoryModelReader(T obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            oos.flush();

            this.data = baos.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] read() {
        return data;
    }
}
