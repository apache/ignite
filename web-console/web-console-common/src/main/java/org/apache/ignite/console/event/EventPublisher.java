

package org.apache.ignite.console.event;

import org.springframework.context.ApplicationEvent;

/**
 * Interface for generic event publisher.
 */
public interface EventPublisher {
    /**
     * @param evt Event.
     */
    public void publish(ApplicationEvent evt);
}
