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

import java.util.function.Consumer;
import org.apache.ignite.Ignite;

/**
 * Failure handler for test purposes that can execute callback to pass failure context inside of it.
 */
public class FailureHandlerWithCallback extends AbstractFailureHandler {
    /** */
    private final Consumer<FailureContext> cb;

    /** */
    public FailureHandlerWithCallback(Consumer<FailureContext> cb) {
        this.cb = cb;
    }

    /** {@inheritDoc} */
    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
        cb.accept(failureCtx);

        return true;
    }
}
