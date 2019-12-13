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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.ViewAttribute;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.StripedExecutor.Stripe;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * {@link StripedExecutor} task representation for a {@link SystemView}.
 */
public class StripedExecutorTaskView {
    /** Stripe. */
    private final Stripe stripe;

    /** Task */
    private final Runnable task;

    /**
     * @param stripe Stripe.
     * @param task Task.
     */
    public StripedExecutorTaskView(Stripe stripe, Runnable task) {
        this.stripe = stripe;
        this.task = task;
    }

    /** @return Stripe index for task. */
    @ViewAttribute
    public int stripeIndex() {
        return stripe.index();
    }

    /** @return Task class name. */
    @ViewAttribute(order = 3)
    public String taskName() {
        return task.getClass().getName();
    }

    /** @return Task {@code toString} representation. */
    @ViewAttribute(order = 1)
    public String description() {
        return toStringSafe(task);
    }

    /** @return Name of the {@link Thread} executing the {@link #task}. */
    @ViewAttribute(order = 2)
    public String threadName() {
        return stripe.name();
    }
}
