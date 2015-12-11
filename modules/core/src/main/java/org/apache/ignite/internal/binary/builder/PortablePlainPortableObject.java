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

package org.apache.ignite.internal.binary.builder;

import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.binary.BinaryObject;

/**
 *
 */
public class PortablePlainPortableObject implements PortableLazyValue {
    /** */
    private final BinaryObject portableObj;

    /**
     * @param portableObj Portable object.
     */
    public PortablePlainPortableObject(BinaryObject portableObj) {
        this.portableObj = portableObj;
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        return portableObj;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(BinaryWriterExImpl writer, PortableBuilderSerializer ctx) {
        BinaryObject val = portableObj;

        if (val instanceof BinaryObjectOffheapImpl)
            val = ((BinaryObjectOffheapImpl)val).heapCopy();

        writer.doWritePortableObject((BinaryObjectImpl)val);
    }
}
