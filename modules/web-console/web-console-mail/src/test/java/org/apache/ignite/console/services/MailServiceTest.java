/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.services;

import java.io.IOException;
import java.util.Properties;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.config.MailPropertiesEx;
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
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mail service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MailServiceTest {
    /** Message source. */
    @Mock
    private MessageSourceAccessor accessor;

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
        when(accessor.getMessage(anyString(), isNull(Object[].class), anyString()))
            .thenAnswer(invocation -> invocation.getArguments()[2]);

        when(mailSnd.createMimeMessage())
            .thenReturn(new MimeMessage(Session.getDefaultInstance(new Properties())));

        srvc = new MailService(accessor, mailSnd, props);
    }

    /** Test send e-mail. */
    @Test
    public void shouldSendEmail() throws MessagingException, IOException {
        INotificationDescriptor desc = new INotificationDescriptor() {
            @Override public String subjectCode() {
                return "subject";
            }

            @Override public String messageCode() {
                return "text";
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
                return "Hello ${recipient.firstName} ${recipient.lastName}! subject";
            }

            @Override public String messageCode() {
                return "text";
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
