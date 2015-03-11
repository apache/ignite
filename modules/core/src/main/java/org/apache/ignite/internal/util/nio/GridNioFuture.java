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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.internal.*;

/**
 * NIO future.
 */
public interface GridNioFuture<R> extends IgniteInternalFuture<R> {
    /**
     * Sets flag indicating that back pressure should be disabled for message send future.
     *
     * @param backPressureDisabled {@code True} if back pressure should be disabled for message future.
     */
    public void backPressureDisabled(boolean backPressureDisabled);

    /**
     * @return {@code True} if back pressure disabled for message future.
     */
    public boolean backPressureDisabled();

    /**
     * @return {@code True} if skip recovery for this operation.
     */
    public boolean skipRecovery();
}
