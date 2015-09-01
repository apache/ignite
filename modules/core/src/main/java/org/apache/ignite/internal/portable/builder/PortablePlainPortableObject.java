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

package org.apache.ignite.internal.portable.builder;

import org.apache.ignite.internal.portable.PortableObjectImpl;
import org.apache.ignite.internal.portable.PortableObjectOffheapImpl;
import org.apache.ignite.internal.portable.PortableWriterExImpl;
import org.apache.ignite.portable.PortableObject;

/**
 *
 */
public class PortablePlainPortableObject implements PortableLazyValue {
    /** */
    private final PortableObject portableObj;

    /**
     * @param portableObj Portable object.
     */
    public PortablePlainPortableObject(PortableObject portableObj) {
        this.portableObj = portableObj;
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        return portableObj;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        PortableObject val = portableObj;

        if (val instanceof PortableObjectOffheapImpl)
            val = ((PortableObjectOffheapImpl)val).heapCopy();

        writer.doWritePortableObject((PortableObjectImpl)val);
    }
}