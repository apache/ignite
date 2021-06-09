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
package org.apache.ignite.raft.jraft.util.concurrent;

/**
 * Factory that creates new {@link ExecutorChooser}s.
 */
public interface ExecutorChooserFactory {

    /**
     * Returns a new {@link ExecutorChooser}.
     */
    ExecutorChooser newChooser(final SingleThreadExecutor[] executors);

    interface ExecutorChooser {

        /**
         * Returns the next {@link SingleThreadExecutor} to use.
         */
        SingleThreadExecutor next();

        /**
         * Returns the chosen {@link SingleThreadExecutor} to use.
         */
        SingleThreadExecutor select(final int index);
    }
}
