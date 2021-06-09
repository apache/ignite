/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.io.PrintWriter;

/**
 * Components that implement this interface need to be able to describe their own state and output state information via
 * the {@code describe} method.
 */
public interface Describer {
    void describe(final Printer out);

    interface Printer {

        /**
         * Prints an object.
         *
         * @param x The <code>Object</code> to be printed
         * @return this printer
         */
        Printer print(final Object x);

        /**
         * Prints an Object and then terminates the line.
         *
         * @param x The <code>Object</code> to be printed.
         * @return this printer
         */
        Printer println(final Object x);
    }

    class DefaultPrinter implements Describer.Printer {

        private final PrintWriter out;

        public DefaultPrinter(PrintWriter out) {
            this.out = out;
        }

        @Override
        public Describer.Printer print(final Object x) {
            this.out.print(x);
            return this;
        }

        @Override
        public Describer.Printer println(final Object x) {
            this.out.println(x);
            return this;
        }
    }
}
