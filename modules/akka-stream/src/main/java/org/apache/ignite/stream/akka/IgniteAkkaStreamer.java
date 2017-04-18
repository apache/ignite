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

package org.apache.ignite.stream.akka;

import akka.stream.javadsl.Sink;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.jetbrains.annotations.NotNull;
import scala.concurrent.ExecutionContext;

public class IgniteAkkaStreamer<T, K, V> extends StreamAdapter<T, K, V> {
    /**
     * Create {@link Sink} foreach method.
     *
     * @return {@link Sink} akka object.
     */
    public Sink foreach() {
        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null,
            "the extractor must be initialize.");

        return Sink.foreach(e -> {
            addMessage((T) e);
        });
    }

    /**
     * Create {@link Sink} foreachParallel method.
     *
     * @param threads Threads.
     * @param ec {@link ExecutionContext} object.
     * @return {@link Sink} akka object.
     */
    public Sink foreachParallel(int threads, @NotNull ExecutionContext ec) {
        A.ensure(threads > 0, "threads has to larger than 0.");
        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null,
            "the extractor must be initialize.");

        return Sink.foreachParallel(threads, e -> {
            addMessage((T) e);
        }, ec);
    }
}
