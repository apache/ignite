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

package org.apache.ignite.internal.dto;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @param <T>
 */
public interface IgniteDataTransferObjectSerializer<T> {
    /**
     *
     * @param instance
     * @param out Output stream to write object to.
     * @throws IOException
     */
    void writeExternal(T instance, ObjectOutput out) throws IOException;

    /**
     *
     * @param instance
     * @param in Input stream to read object from.
     * @return
     * @throws IOException If read operation failed.
     * @throws ClassNotFoundException
     */
    void readExternal(T instance, ObjectInput in) throws IOException, ClassNotFoundException;
}
