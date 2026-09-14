

package org.apache.ignite.console.services;

import org.apache.ignite.console.agent.service.IMailService;
import org.apache.ignite.console.notification.Notification;
import org.springframework.stereotype.Service;

/**
 * Noop mail sending service.
 */
@Service
public class NoopMailService implements IMailService {
    /** {@inheritDoc} */
    @Override public void send(Notification notification) {
        // No-op.
    }
}
