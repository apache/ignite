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

import java.util.Map;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

/** Fullfill {@code data} Map for specific row. */
class AttributeToMapVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
    /** Map to store data. */
    private Map<String, String> data;

    /**
     * Sets map.
     *
     * @param data Map to fill.
     */
    public void data(Map<String, String> data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptBoolean(int idx, String name, boolean val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptChar(int idx, String name, char val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptByte(int idx, String name, byte val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptShort(int idx, String name, short val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptInt(int idx, String name, int val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptLong(int idx, String name, long val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptFloat(int idx, String name, float val) {
        data.put(name, String.valueOf(val));
    }

    /** {@inheritDoc} */
    @Override public void acceptDouble(int idx, String name, double val) {
        data.put(name, String.valueOf(val));
    }
}
