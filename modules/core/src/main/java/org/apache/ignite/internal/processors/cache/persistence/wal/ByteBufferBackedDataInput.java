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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ByteBuffer backed data input
 */
public interface ByteBufferBackedDataInput extends DataInput {
    /**
     * @return ByteBuffer hold by data input
     */
    public ByteBuffer buffer();

    /**
     * ensure that requested count of byte is available in data input and will try to load data if not
     * @param requested Requested number of bytes.
     * @throws IOException If failed.
     */
    public void ensure(int requested) throws IOException;
}
