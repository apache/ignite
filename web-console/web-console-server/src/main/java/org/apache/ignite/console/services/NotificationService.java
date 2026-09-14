

package org.apache.ignite.console.services;

import org.apache.ignite.console.agent.service.IMailService;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.notification.Notification;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Notification service.
 */
@Service
public class NotificationService {
    /** */
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    /** Mail service. */
    private IMailService mailSrvc;

    /** Web console url. */
    @Value("${spring.mail.web-console-url:}")
    private String origin;

    /**
     * @param mailSrvc Mail service.
     */
    public NotificationService(IMailService mailSrvc) {
        this.mailSrvc = mailSrvc;
    }

    /**
     * @param desc Notification description.
     * @param acc Account.
     */
    public void sendEmail(NotificationDescriptor desc, Account acc) {
        try {
            Notification notification = new Notification(origin, acc, desc);

            mailSrvc.send(notification);
        }
        catch (Throwable e) {
            log.error("Failed to send notification email to user [type={}, accId={}]", desc, acc.getId(), e);
        }
    }
}
