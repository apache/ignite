package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
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
    @Order
    public int stripeIndex() {
        return stripe.index();
    }

    /** @return Task class name. */
    @Order(3)
    public String className() {
        return task.getClass().getName();
    }

    /** @return Task {@code toString} representation. */
    @Order(1)
    public String description() {
        return toStringSafe(task);
    }

    @Order(2)
    public String threadName() {
        return stripe.name();
    }
}
