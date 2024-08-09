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

import org.apache.ignite.console.MockConfiguration;
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
