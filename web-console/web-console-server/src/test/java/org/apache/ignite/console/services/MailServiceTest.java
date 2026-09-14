

package org.apache.ignite.console.services;

import java.io.IOException;
import java.util.Properties;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.ignite.console.config.MailPropertiesEx;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.notification.INotificationDescriptor;
import org.apache.ignite.console.notification.IRecipient;
import org.apache.ignite.console.notification.Notification;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.junit4.SpringRunner;

import jakarta.mail.MessagingException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mail service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MailServiceTest {
    /** JavaMail sender. */
    @Mock
    private JavaMailSender mailSnd;

    /** Message properties. */
    @Autowired
    private MailPropertiesEx props;

    /** Argument capture  */
    @Captor
    private ArgumentCaptor<MimeMessage> captor;

    /** Mail service. */
    private MailService srvc;

    /** */
    @Before
    public void setup() {
        when(mailSnd.createMimeMessage())
            .thenReturn(new MimeMessage(Session.getDefaultInstance(new Properties())));

        srvc = new MailService(WebConsoleMessageSource.getAccessor(), mailSnd, props);
    }

    /** Test send e-mail. */
    @Test
    public void shouldSendEmail() throws MessagingException, IOException {
        INotificationDescriptor desc = new INotificationDescriptor() {
            @Override public String subjectCode() {
                return "notifications.simple.subject";
            }

            @Override public String messageCode() {
                return "notifications.simple.body";
            }
        };

        Notification notification = new Notification(
            "http://test.com",
            new TestRecipient(),
            desc
        );

        srvc.send(notification);

        verify(mailSnd).send(captor.capture());

        MimeMessage msg = captor.getValue();

        assertEquals("subject", msg.getSubject());
        assertEquals("text", msg.getContent());
        assertEquals(1, msg.getFrom().length);
        assertEquals("alias", ((InternetAddress)msg.getFrom()[0]).getPersonal());
    }

    /** Test send e-mail with message template. */
    @Test
    public void shouldSendEmailWithExpressionInSubject() throws MessagingException, IOException {
        INotificationDescriptor desc = new INotificationDescriptor() {
            @Override public String subjectCode() {
                return "notifications.spel.subject";
            }

            @Override public String messageCode() {
                return "notifications.spel.body";
            }
        };

        Notification notification = new Notification(
            "http://test.com",
            new TestRecipient(),
            desc
        );

        srvc.send(notification);

        verify(mailSnd).send(captor.capture());

        MimeMessage msg = captor.getValue();

        assertEquals("Hello firstName lastName! subject", msg.getSubject());
        assertEquals("text", msg.getContent());
    }

    /** */
    private static class TestRecipient implements IRecipient {
        /** First name. */
        @SuppressWarnings("FieldCanBeLocal")
        private String fn = "firstName";

        /** Last name. */
        @SuppressWarnings({"PublicField", "unused"})
        public String lastName = "lastName";

        /** {@inheritDoc} */
        @Override public String getEmail() {
            return "test@test.com";
        }

        /** {@inheritDoc} */
        @Override public String getPhone() {
            return null;
        }

        /**
         * @return First name.
         */
        public String getFirstName() {
            return fn;
        }
    }
}
