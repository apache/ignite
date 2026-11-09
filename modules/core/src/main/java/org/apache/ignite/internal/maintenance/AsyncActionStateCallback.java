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

package org.apache.ignite.internal.maintenance;

import java.util.function.Consumer;

/**
 * Async action completion listener.
 * <br>
 * To reflect the sequence of execute() status transitions in the ackNodeBasicMetrics() logs,
 * specify the following mappings:
 * <br>
 * {@code @onCompleted.run()}: Indicates the successful and expected completion of all processes
 * within the current Action.
 * <br>
 * {@code @onCancelled.run()}: Indicates the cancellation of the current Action.
 * <br>
 * {@code onFailed.accept(error)}: Indicates the erroneous (abnormal) termination of the current Action.
 */
public interface AsyncActionStateCallback {
    /**
     * Set state provider.
     * @param onCompleted Completion callback.
     * @param onCancelled Cancellation callback.
     * @param onFailed Failure callback.
     */
    void setStateCallbacks(Runnable onCompleted, Runnable onCancelled, Consumer<Throwable> onFailed);
}
