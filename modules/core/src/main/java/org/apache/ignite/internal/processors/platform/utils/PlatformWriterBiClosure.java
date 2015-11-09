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

package org.apache.ignite.internal.processors.platform.utils;

import org.apache.ignite.internal.portable.BinaryRawWriterEx;

/**
 * Interop writer bi-closure.
 */
public interface PlatformWriterBiClosure<T1, T2> {
    /**
     * Write values.
     *
     * @param writer Writer.
     * @param val1 Value 1.
     * @param val2 Value 2.
     */
    public void write(BinaryRawWriterEx writer, T1 val1, T2 val2);
}