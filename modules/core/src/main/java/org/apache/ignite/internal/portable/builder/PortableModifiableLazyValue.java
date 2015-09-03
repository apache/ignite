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

import org.apache.ignite.internal.portable.*;

/**
 *
 */
public class PortableModifiableLazyValue extends PortableAbstractLazyValue {
    /** */
    protected final int len;

    /**
     * @param reader
     * @param valOff
     * @param len
     */
    public PortableModifiableLazyValue(PortableBuilderReader reader, int valOff, int len) {
        super(reader, valOff);

        this.len = len;
    }

    /** {@inheritDoc} */
    @Override protected Object init() {
        return reader.reader().unmarshal(valOff);
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (val == null)
            writer.write(reader.array(), valOff, len);
        else
            writer.writeObject(val);
    }
}
