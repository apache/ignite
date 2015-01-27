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

package org.apache.ignite.internal.processors.email;

import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.future.*;

import java.util.*;

/**
 * No-op implementation of {@code GridEmailProcessorAdapter}.
 */
public class IgniteNoopEmailProcessor extends IgniteEmailProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public IgniteNoopEmailProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void sendNow(String subj, String body, boolean html) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sendNow(String subj, String body, boolean html, Collection<String> addrs) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> schedule(String subj, String body, boolean html) {
        return new GridFinishedFuture<>(ctx, true);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> schedule(String subj, String body, boolean html, Collection<String> addrs) {
        return new GridFinishedFuture<>(ctx, true);
    }
}
