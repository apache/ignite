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

package org.apache.ignite.console.repositories;

import org.apache.ignite.IgniteException;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

/**
 * Accounts repository test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AccountsRepositoryTest {
    /** Accounts repository. */
    @Autowired
    private AccountsRepository accountsRepo;

    /**
     * Should throw account not found.
     */
    @Test
    public void testAccountNotFoundExceptionDuringDelete() {
        UUID accId = UUID.randomUUID();
        GridTestUtils.assertThrows(null, () -> {
            accountsRepo.delete(accId);
            return null;
        }, IllegalStateException.class, "Account not found for ID: " + accId);
    }

    /**
     * Should throw account already registered exception.
     */
    @Test
    public void testAccountExistByEmailDuringSave() {
        UUID accId = UUID.randomUUID();
        Account acc = new Account(
            "mail@mail",
            "pswd",
            "name",
            "lastName",
            "phone",
            "company",
            "country"
        );
        acc.setId(accId);

        GridTestUtils.assertThrows(null, () -> {
            accountsRepo.save(acc);
            acc.setId(UUID.randomUUID());
            accountsRepo.save(acc);

            return null;
        }, IgniteException.class, "The email address you have entered is already registered: " + acc.getUsername());
    }

    /**
     * Should throw account already exists exception.
     */
    @Test
    public void testAccountExistByTokenDuringSave() {
        UUID accId = UUID.randomUUID();
        Account acc = new Account(
                "mail1@mail",
                "pswd",
                "name",
                "lastName",
                "phone",
                "company",
                "country"
        );
        acc.setId(accId);
        acc.setToken("token");

        GridTestUtils.assertThrows(null, () -> {
            accountsRepo.save(acc);
            acc.setId(UUID.randomUUID());
            acc.setEmail("mail2@mail");
            accountsRepo.save(acc);

            return null;
        }, IgniteException.class, "Account with token " + acc.getToken() + " already exists");
    }
}
