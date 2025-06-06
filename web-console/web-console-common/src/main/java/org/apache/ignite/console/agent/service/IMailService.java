

package org.apache.ignite.console.agent.service;

import org.apache.ignite.console.notification.Notification;

/**
 * Mail sending service.
 */
public interface IMailService {
    /**
     * Send email.
     *
     * @param notification Notification.
     */
    public void send(Notification notification) throws Exception;
}
