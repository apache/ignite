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

package org.apache.ignite.console.listener;

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventType;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.apache.ignite.console.services.NotificationService;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_DELETE;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_CHANGED;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_RESET;
import static org.apache.ignite.console.event.AccountEventType.RESET_ACTIVATION_TOKEN;
import static org.apache.ignite.console.utils.Utils.entriesToMap;
import static org.apache.ignite.console.utils.Utils.entry;


/**
 * Notification event listener.
 */
@Component
public class NotificationEventListener {
    /** Notification server. */
    private NotificationService notificationSrv;

    /** Notification descriptor by event type. */
    private final Map<EventType, NotificationDescriptor> notificationDescByEvtType = Collections.unmodifiableMap(Stream.of(
            entry(ACCOUNT_CREATE_BY_ADMIN, NotificationDescriptor.ADMIN_WELCOME_LETTER),
            entry(ACCOUNT_CREATE, NotificationDescriptor.WELCOME_LETTER),
            entry(ACCOUNT_DELETE, NotificationDescriptor.ACCOUNT_DELETED),
            entry(PASSWORD_RESET, NotificationDescriptor.PASSWORD_RESET),
            entry(PASSWORD_CHANGED, NotificationDescriptor.PASSWORD_CHANGED),
            entry(RESET_ACTIVATION_TOKEN, NotificationDescriptor.ACTIVATION_LINK)).
            collect(entriesToMap()));

    /**
     * @param notificationSrv Notification server.
     */
    public NotificationEventListener(NotificationService notificationSrv) {
        this.notificationSrv = notificationSrv;
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onAccountEvent(Event<? extends Account> evt) {
        NotificationDescriptor desc = notificationDescByEvtType.get(evt.getType());
        if (desc != null)
            notificationSrv.sendEmail(desc, evt.getSource());
    }
}
