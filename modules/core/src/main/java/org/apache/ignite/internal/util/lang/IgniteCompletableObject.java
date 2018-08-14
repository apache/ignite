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

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCheckedException;

/**
 * Object represents pair of input object and result of computation.
 * @param <T> Type of input object.
 * @param <R> Type of result object.
 */
public class IgniteCompletableObject<T, R> implements Serializable {

    private final T input;
    private final CompletableFuture<R> future = new CompletableFuture<>();

    public IgniteCompletableObject(T input) {
        this.input = input;
    }

    public T getInput() {
        return input;
    }

    public void complete(R result) {
        future.complete(result);
    }

    public R getResult() throws IgniteCheckedException {
        try {
            return future.get(0, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IgniteCheckedException(e);
        }
    }


}
