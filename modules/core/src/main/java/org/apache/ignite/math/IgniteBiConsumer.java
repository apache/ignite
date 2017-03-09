package org.apache.ignite.math;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Serializable bi consumer.
 *
 * @see java.util.function.BiConsumer
 */
public interface IgniteBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {
}
