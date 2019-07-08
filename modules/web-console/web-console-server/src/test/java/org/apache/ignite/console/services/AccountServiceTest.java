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
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.config.SignUpConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventType;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.security.MissingConfirmRegistrationException;
import org.apache.ignite.console.web.socket.WebSocketsManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_UPDATE;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_CHANGED;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_RESET;
import static org.apache.ignite.console.event.AccountEventType.RESET_ACTIVATION_TOKEN;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Account service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {MockConfiguration.class})
public class AccountServiceTest {
    /** Account repository. */
    @Mock
    private AccountsRepository accountsRepo;

    /** Event publisher. */
    @Mock
    private EventPublisher evtPublisher;

    /** Tx manager. */
    @Autowired
    private TransactionManager txMgr;

    /** */
    @Before
    public void setup() {
        when(accountsRepo.create(any(Account.class)))
            .thenAnswer(invocation -> {
                Account acc = invocation.getArgumentAt(0, Account.class);

                if ("admin@admin".equalsIgnoreCase(acc.getUsername()))
                    acc.setAdmin(true);

                return acc;
            });

        when(accountsRepo.save(any(Account.class)))
            .thenAnswer(invocation -> invocation.getArgumentAt(0, Account.class));
    }

    /** Test sign up logic. */
    @Test
    public void disableSignUp() {
        AccountsService srvc = mockAccountsService(false, false);

        SignUpRequest adminReq = new SignUpRequest();

        adminReq.setEmail("admin@admin");
        adminReq.setPassword("1");

        srvc.register(adminReq);

        GridTestUtils.assertThrows(null, () -> {
            SignUpRequest userReq = new SignUpRequest();

            userReq.setEmail("user@user");
            userReq.setPassword("1");

            srvc.register(userReq);

            return null;
        }, AuthenticationServiceException.class, "Sign-up is not allowed. Ask your administrator to create account for you.");
    }

    /**
     * Should throw activation not enabled exception.
     */
    @Test
    public void shouldThrowTooManyActivationAttempts() {
        AccountsService srvc = mockAccountsService(true, true, 0);
        when(accountsRepo.getByEmail(anyString()))
                .thenAnswer(invocation -> {
                    Account acc = new Account();
                    acc.setEmail(invocation.getArgumentAt(0, String.class));
                    acc.resetActivationToken();

                    return acc;
                });

        GridTestUtils.assertThrows(null, () -> {
            srvc.resetActivationToken("mail@mail");
            return null;
        }, IllegalAccessError.class, "Too many activation attempts");
    }

    /**
     * Should throw activation not enabled exception.
     */
    @Test
    public void shouldThrowActivationNotEnabledException() {
        AccountsService srvc = mockAccountsService(true, false);

        GridTestUtils.assertThrows(null, () -> {
            srvc.resetActivationToken("mail@mail");
            return null;
        }, IllegalAccessError.class, "Activation was not enabled!");
    }

    /**
     * Should throw activation not enabled exception.
     */
    @Test
    public void shouldThrowAccountNotFoundByTokenException() {
        AccountsService srvc = mockAccountsService(true, false);
        when(accountsRepo.getByEmail(anyString()))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setEmail(invocation.getArgumentAt(0, String.class));
                acc.setResetPasswordToken("token");

                return acc;
            });

        GridTestUtils.assertThrows(null, () -> {
            srvc.resetPasswordByToken("mail@mail", "aa", "pwds");
            return null;
        }, IllegalStateException.class, "Failed to find account with this token! Please check link from email.");
    }

    /**
     * Should publish event with RESET_ACTIVATION_TOKEN type.
     */
    @Test
    public void shouldPublishResetActivationTokenEventWhileRegister() {
        AccountsService srvc = mockAccountsService(true, true);

        SignUpRequest userReq = new SignUpRequest();

        userReq.setEmail("user@user");
        userReq.setPassword("1");

        try {
            srvc.register(userReq);
        }
        catch (MissingConfirmRegistrationException exception) {
            Assert.assertEquals("Confirm your email", exception.getMessage());
        }

        assertEventType(RESET_ACTIVATION_TOKEN);
    }

    /**
     * Should publish event with RESET_ACTIVATION_TOKEN type.
     */
    @Test
    public void shouldPublishResetActivationTokenEvent() {
        AccountsService srvc = mockAccountsService(true, true);
        when(accountsRepo.getByEmail(anyString()))
                .thenAnswer(invocation -> {
                    Account acc = new Account();
                    acc.setEmail(invocation.getArgumentAt(0, String.class));
                    acc.resetActivationToken();

                    return acc;
                });

        srvc.resetActivationToken("mail@mail");

        assertEventType(RESET_ACTIVATION_TOKEN);
    }

    /**
     * Should publish event with ACCOUNT_CREATE type.
     */
    @Test
    public void shouldPublishAccountCreateEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        SignUpRequest userReq = new SignUpRequest();

        userReq.setEmail("user@user");
        userReq.setPassword("1");

        srvc.register(userReq);

        assertEventType(ACCOUNT_CREATE);
    }

    /**
     * Should publish event with ACCOUNT_UPDATE type.
     */
    @Test
    public void shouldPublishAccountUpdateEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        when(accountsRepo.getById(any(UUID.class)))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setEmail("fake@mail");
                acc.setId(invocation.getArgumentAt(0, UUID.class));
                acc.setToken("token");

                return acc;
            });

        ChangeUserRequest changes = new ChangeUserRequest();
        changes.setEmail("new@mail");
        changes.setToken("token");

        srvc.save(UUID.randomUUID(), changes);

        assertEventType(ACCOUNT_UPDATE);
    }

    /**
     * Should publish event with PASSWORD_RESET type.
     */
    @Test
    public void shouldPublishPasswordResetEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        when(accountsRepo.getByEmail(anyString()))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setEmail(invocation.getArgumentAt(0, String.class));

                return acc;
            });

        srvc.forgotPassword("mail@mail");

        assertEventType(PASSWORD_RESET);
    }

    /**
     * Should publish event with PASSWORD_CHANGED type.
     */
    @Test
    public void shouldPublishPasswordChangedEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        when(accountsRepo.getByEmail(anyString()))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setEmail(invocation.getArgumentAt(0, String.class));
                acc.setResetPasswordToken("token");

                return acc;
            });

        srvc.resetPasswordByToken("new_mail@mail", "token", "2");

        assertEventType(PASSWORD_CHANGED);
    }

    /**
     * @param disableSignUp Disable sign up.
     * @param enableActivation Enable activation.
     */
    private AccountsService mockAccountsService(boolean disableSignUp, boolean enableActivation) {
        return mockAccountsService(disableSignUp, enableActivation, 1000);
    }

    /**
     * @param disableSignUp Disable sign up.
     * @param enableActivation Enable activation.
     * @param sendTimeout Send timeout.
     */
    private AccountsService mockAccountsService(boolean disableSignUp, boolean enableActivation, long sendTimeout) {
        ActivationConfiguration activationCfg = new ActivationConfiguration(new NoopMailService()).setSendTimeout(sendTimeout);
        try {
            activationCfg.afterPropertiesSet();
        }
        catch (Exception e) {
            // No-op
        }
        activationCfg.setEnabled(enableActivation);

        return new AccountsService(
                new SignUpConfiguration().setEnabled(disableSignUp),
                activationCfg,
                NoOpPasswordEncoder.getInstance(),
                new WebSocketsManager(),
                accountsRepo,
                txMgr,
                evtPublisher
        );
    }

    /**
     * @param evtType Event type.
     */
    private void assertEventType(EventType evtType) {
        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals(evtType, captor.getValue().getType());
    }
}
