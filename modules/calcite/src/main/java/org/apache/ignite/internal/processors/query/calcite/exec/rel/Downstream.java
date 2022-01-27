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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

/**
 * Represents an abstract data consumer.
 *
 * <p/><b>Note</b>: except several cases (like consumer node and mailboxes), {@link Node#request(int)},
 * {@link Downstream#push(Object)} and {@link Downstream#end()} methods should be used from one single thread.
 */
public interface Downstream<Row> {
    /**
     * Pushes a row to consumer.
     * @param row Data row.
     */
    void push(Row row) throws Exception;

    /**
     * Signals that data is over.
     */
    void end() throws Exception;

    /** */
    void onError(Throwable e);
}
