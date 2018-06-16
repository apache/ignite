package org.apache.ignite.tensorflow.tf.util;

import java.io.Serializable;

/**
 * Component that have to be initialized before usage and destroyed after.
 */
public interface Component extends Serializable {
    /**
     * Initializes component.
     */
    public void init();

    /**
     * Destroys component.
     */
    public void destroy();
}
