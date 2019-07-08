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
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.repositories.NotebooksRepository;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_DELETE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Admin service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {MockConfiguration.class})
public class AdminServiceTest {
    /** Activities service. */
    @Autowired
    private AdminService adminSrvc;

    /** Event publisher. */
    @MockBean
    private EventPublisher evtPublisher;

    /** Configurations repository. */
    @MockBean
    private ConfigurationsRepository configurationsRepo;

    /** Notebooks repository. */
    @MockBean
    private NotebooksRepository notebooksRepo;

    /** Accounts service. */
    @MockBean
    private AccountsService accountsSrvc;

    /**
     * Should publish event with ACCOUNT_DELETE type.
     */
    @Test
    public void shouldPublishUserDeleteEvent() {
        when(accountsSrvc.delete(any(UUID.class)))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setId(invocation.getArgumentAt(0, UUID.class));

                return acc;
            });

        UUID accId = UUID.randomUUID();
        adminSrvc.delete(accId);

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals(ACCOUNT_DELETE, captor.getValue().getType());
    }

    /**
     * Should publish event with ACCOUNT_CREATE_BY_ADMIN type.
     */
    @Test
    public void shouldPublishUserCreateByAdminEvent() {
        when(accountsSrvc.create(any(SignUpRequest.class)))
                .thenAnswer(invocation -> {
                    SignUpRequest req = invocation.getArgumentAt(0, SignUpRequest.class);
                    Account acc = new Account();
                    acc.setEmail(req.getEmail());
                    acc.setPassword(req.getPassword());

                    return acc;
                });

        SignUpRequest req = new SignUpRequest();
        req.setEmail("mail@mail");
        req.setPassword("1");

        adminSrvc.registerUser(req);

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals(ACCOUNT_CREATE_BY_ADMIN, captor.getValue().getType());
    }
}
