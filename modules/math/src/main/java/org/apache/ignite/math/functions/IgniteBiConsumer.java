package org.apache.ignite.math.functions;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Serializable binary consumer.
 *
 * @see java.util.function.BiConsumer
 */
public interface IgniteBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {
}
