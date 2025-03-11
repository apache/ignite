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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.Function;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/** Fullfill {@code data} Map for specific row. */
class AttributeCollectorVisitor implements SystemViewRowAttributeWalker.AttributeVisitor {
    /** Cache function. */
    private final Function<String, Boolean> cacheFunction;

    /** Map to store data. */
    private final DataOutputStream dataOutputStream;


    /**
     * @param byteArrOutputStream Byte array output stream.
     * @param cacheFunction Cache function.
     */
    public AttributeCollectorVisitor(ByteArrayOutputStream byteArrOutputStream, Function<String, Boolean> cacheFunction) {
        dataOutputStream = new DataOutputStream(byteArrOutputStream);
        this.cacheFunction = cacheFunction;
    }

    /** {@inheritDoc} */
    @Override public <T> void accept(int idx, String name, Class<T> clazz) {
        try {
            writeString(name);

            if (clazz.isPrimitive())
                writeString(clazz.getSimpleName());
            else
                writeString(String.class.getSimpleName());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param str String to write.
     */
    private void writeString(String str) throws IOException {
        boolean cached = cacheFunction.apply(str);
        dataOutputStream.writeByte(cached ? (byte)1 : 0);

        if (cached)
            dataOutputStream.writeInt(str.hashCode());
        else {
            byte[] bytes = str.getBytes();

            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
        }
    }
}
