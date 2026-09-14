

package org.apache.ignite.console.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Delegate for {@link ApplicationEventPublisher}.
 */
@Component
public class DelegateEventPublisher implements EventPublisher {
    /** Publisher. */
    private final ApplicationEventPublisher publisher;

    /**
     * @param publisher Publisher.
     */
    public DelegateEventPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    /** {@inheritDoc} */
    @Override public void publish(ApplicationEvent evt) {
        publisher.publishEvent(evt);
    }
}
