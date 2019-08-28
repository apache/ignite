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

package org.apache.ignite.spi.metric;

import org.apache.ignite.internal.processors.metric.list.MonitoringRow;

/**
 *
 */
public interface MonitoringRowAttributeWalker<R extends MonitoringRow<?>> {
    /**
     * @return
     */
    public int count();

    /**
     * @param visitor
     */
    public void visitAll(AttributeVisitor visitor);

    /**
     * @param row
     * @param visitor
     */
    public void visitAllWithValues(R row, AttributeWithValueVisitor visitor);

    /** */
    public interface AttributeVisitor {
        /** */
        public <T> void accept(int idx, String name, Class<T> clazz);

        /** */
        public void acceptBoolean(int idx, String name);

        /** */
        public void acceptChar(int idx, String name);

        /** */
        public void acceptByte(int idx, String name);

        /** */
        public void acceptShort(int idx, String name);

        /** */
        public void acceptInt(int idx, String name);

        /** */
        public void acceptLong(int idx, String name);

        /** */
        public void acceptFloat(int idx, String name);

        /** */
        public void acceptDouble(int idx, String name);
    }

    /** */
    public interface AttributeWithValueVisitor {
        /** */
        public <T> void accept(int idx, String name, Class<T> clazz, T val);

        /** */
        public void acceptBoolean(int idx, String name, boolean val);

        /** */
        public void acceptChar(int idx, String name, char val);

        /** */
        public void acceptByte(int idx, String name, byte val);

        /** */
        public void acceptShort(int idx, String name, short val);

        /** */
        public void acceptInt(int idx, String name, int val);

        /** */
        public void acceptLong(int idx, String name, long val);

        /** */
        public void acceptFloat(int idx, String name, float val);

        /** */
        public void acceptDouble(int idx, String name, double val);
    }
}
