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
package org.apache.ignite.failure;

import org.apache.ignite.Ignite;

/**
 * Special version of {@link StopNodeFailureHandler} which delegates call to the parent implementation
 * but prevents FailureProcessor from invalidating stopping node by returning {@code false} from overridden method.
 */
public class StopNodeOnSegmentaionFailureHandler extends StopNodeFailureHandler {
    /** {@inheritDoc} */
    @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
        super.onFailure(ignite, failureCtx);

        return false;
    }
}
