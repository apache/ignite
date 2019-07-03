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

import org.apache.ignite.console.TestConfiguration;
import org.apache.ignite.console.config.SignUpConfiguration;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.socket.WebSocketsManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Account service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class})
public class AccountServiceTest {
    /** Account repository. */
    @Mock
    private AccountsRepository accountsRepo;

    /** Tx manager. */
    @Autowired
    private TransactionManager txMgr;

    /** Account service. */
    private AccountsService srvc;

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
    }

    /** Test sign up logic. */
    @Test
    public void disableSignUp() {
        srvc = new AccountsService(
            new SignUpConfiguration().setEnabled(false),
            new ActivationConfiguration(new NoopMailService()),
            NoOpPasswordEncoder.getInstance(),
            new WebSocketsManager(),
            accountsRepo,
            txMgr,
            new NotificationService(new NoopMailService())
        );

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
        }, AuthenticationServiceException.class, null);
    }
}
