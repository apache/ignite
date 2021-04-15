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

package org.apache.ignite.table;

import java.io.Serializable;

/**
 * Invoke processor interface provides API to run code on server side against a table record
 * associated with provided key.
 * <p>
 * For non-binary projections row will be deserialized to user object(s) before the invocation
 * and serialized back if a new value was set via {@linkplain InvocationContext#value(Object)}.
 * <p>
 * Invoke operation arguments along with invoke operation result classes MUST be serializable
 * as they can be transferred over network.
 *
 * @param <K> Key object type.
 * @param <V> Value type.
 * @param <R> Processor result type.
 * @apiNote Distributed deployment MUST be used for processor code load instead of load form the classpath
 * to guarantee same code revision in the grid.
 */
public interface InvokeProcessor<K, V, R extends Serializable> extends Serializable {
    /**
     * Processes table record and return the result.
     *
     * @param ctx Invocation context.
     * @return Invoke processor result.
     * @throws InvokeProcessorException If failed during data processing.
     */
    R process(InvocationContext<K, V> ctx) throws InvokeProcessorException;
}
