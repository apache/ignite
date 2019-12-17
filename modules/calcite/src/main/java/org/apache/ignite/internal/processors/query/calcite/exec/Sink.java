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

package org.apache.ignite.internal.processors.query.calcite.exec;

/**
 * Represents an abstract data consumer.
 */
public interface Sink<T> {
    /** */
    @SuppressWarnings("rawtypes")
    Sink NO_OP = new Sink() {
        /** {@inheritDoc} */
        @Override public boolean push(Object row) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void end() {}
    };

    /**
     * Pushes a row to consumer.
     * @param row Data row.
     * @return {@code True} if a row consumes and processed, {@code false} otherwise. In case the row was not consumed,
     *      the row has to be send once again as soon as a target consumer become able to process data.
     */
    boolean push(T row);

    /**
     * Signals that data is over.
     */
    void end();

    /**
     * @return No-op sink object.
     */
    static <T> Sink<T> noOp() {
        return (Sink<T>) NO_OP;
    }
}
