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

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.notification.Notification;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.common.Utils.currentRequestOrigin;

/**
 * Notification service.
 */
@Service
public class NotificationService {
    /** */
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    /** Mail service. */
    private IMailService mailSrv;

    /**
     * @param srv Mail service.
     */
    public NotificationService(IMailService srv) {
        mailSrv = srv;
    }

    /**
     * @param desc Notification description.
     * @param acc Account.
     */
    public void sendEmail(NotificationDescriptor desc, Account acc) {
        try {
            Notification notification = new Notification(currentRequestOrigin(), acc, desc);

            mailSrv.send(notification);
        }
        catch (Throwable e) {
            log.error("Failed to send notification email to user [type={}, accId={}]", desc, acc.getId(), e);
        }
    }
}
