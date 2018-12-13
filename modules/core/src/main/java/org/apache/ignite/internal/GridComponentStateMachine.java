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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;

/**
 * State machine to track lifecycle of GirdComponents.
 *
 */
public class GridComponentStateMachine {
    /**
     * Grid component.
     */
    private final Object gridComponent;

    /**
     * Component state.
     */
    private GridComponentState componentState = GridComponentState.CREATED;

    /**
     *
     * @param gridComponent component to track states.
     */
    public GridComponentStateMachine(Object gridComponent) {
        this.gridComponent = gridComponent;
    }

    /**
     * Change state method.
     *
     * @param expected Expected state of component.
     * @param target Target state of component.
     * @param logger Logger.
     * @param callback Callback to invoke on state transition.
     * @throws IgniteCheckedException Thrown by callback.
     */
    public synchronized void changeState(
            GridComponentState expected,
            GridComponentState target,
            IgniteLogger logger,
            GridStateChangeCallback callback
    ) throws IgniteCheckedException {
        if (componentState != expected) {
            if (logger.isInfoEnabled())
                logger.info("State is " + componentState + ", skip callback, cause expected state is " + expected);

            return;
        }

        callback.apply();

        componentState = target;

        if (logger.isInfoEnabled())
            logger.info("Changed " + gridComponent +" state to " + componentState);

    }

}
