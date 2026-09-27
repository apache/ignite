

package org.apache.ignite.console.services;

import org.apache.ignite.console.MockConfiguration;
import org.apache.ignite.console.agent.service.IMailService;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.notification.Notification;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Test for notification service.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {MockConfiguration.class})
public class NotificationServiceTest {
    /** Mail service. */
    @MockBean
    private IMailService mailSrvc;

    /** Notifivation service. */
    @Autowired
    private NotificationService srvc;

    /**
     * Should send notification.
     */
    @Test
    public void testSendNotification() throws Exception {
        Account acc = new Account();
        acc.setEmail("email@email");

        srvc.sendEmail(NotificationDescriptor.WELCOME_LETTER, acc);

        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        Mockito.verify(mailSrvc, Mockito.times(1)).send(captor.capture());

        Assert.assertEquals("http://localhost/", captor.getValue().getOrigin());
        Assert.assertEquals(NotificationDescriptor.WELCOME_LETTER, captor.getValue().getDescriptor());
        Assert.assertEquals(acc.getEmail(), captor.getValue().getRecipient().getEmail());
    }
}
