

package org.apache.ignite.console.listener;

import org.apache.ignite.console.MockConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.apache.ignite.console.services.NotificationService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_DELETE;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_CHANGED;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_RESET;
import static org.apache.ignite.console.event.AccountEventType.RESET_ACTIVATION_TOKEN;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Notification event listener test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {MockConfiguration.class})
public class NotificationEventListenerTest {
    /** Publisher. */
    @Autowired
    private EventPublisher publisher;

    /** Notification Server. */
    @MockBean
    private NotificationService notificationSrv;

    /**
     * Test welcome letter sending.
     */
    @Test
    public void testOnUserCreateEvent() {
        Account acc = new Account();

        publisher.publish(new Event<>(ACCOUNT_CREATE, acc));

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.WELCOME_LETTER, acc);
    }

    /**
     * Test admin welcome letter sending.
     */
    @Test
    public void testOnUserCreateByAdminEvent() {
        Account acc = new Account();

        publisher.publish(new Event<>(ACCOUNT_CREATE_BY_ADMIN, acc));

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.ADMIN_WELCOME_LETTER, acc);
    }

    /**
     * Test account delete letter sending.
     */
    @Test
    public void testOnUserDeleteEvent() {
        Account acc = new Account();

        publisher.publish(new Event<>(ACCOUNT_DELETE, acc));

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.ACCOUNT_DELETED, acc);
    }

    /**
     * Test password reset letter sending.
     */
    @Test
    public void testOnPasswordResetEvent() {
        Account acc = new Account();

        publisher.publish(new Event<>(PASSWORD_RESET, acc));

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.PASSWORD_RESET, acc);
    }

    /**
     * Test password changed letter sending.
     */
    @Test
    public void testOnPasswordChangedEvent() {
        Account acc = new Account();

        publisher.publish(new Event<>(PASSWORD_CHANGED, acc));

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.PASSWORD_CHANGED, acc);
    }

    /**
     * Test reset activation tokent letter sending.
     */
    @Test
    public void testOnResetActivationTokenEvent() {
        Account acc = new Account();

        publisher.publish(new Event<>(RESET_ACTIVATION_TOKEN, acc));

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.ACTIVATION_LINK, acc);
    }
}
