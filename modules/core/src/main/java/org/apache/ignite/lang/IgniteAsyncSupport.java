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

package org.apache.ignite.lang;

/**
 * Allows to enable asynchronous mode on Ignite APIs.
 */
public interface IgniteAsyncSupport {
    /**
     * Gets instance of this component with asynchronous mode enabled.
     *
     * @return Instance of this component with asynchronous mode enabled.
     */
    public IgniteAsyncSupport withAsync();

    /**
     * @return {@code True} if asynchronous mode is enabled.
     */
    public boolean isAsync();

    /**
     * Gets and resets future for previous asynchronous operation.
     *
     * @return Future for previous asynchronous operation.
     */
    public <R> IgniteFuture<R> future();
}