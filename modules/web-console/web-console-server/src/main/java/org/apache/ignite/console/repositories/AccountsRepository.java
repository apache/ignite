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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with accounts.
 */
@Repository
public class AccountsRepository {
    /** Special key to check that first user should be granted admin rights. */
    private static final UUID FIRST_USER_MARKER_KEY = UUID.fromString("039d28e2-133d-4eae-ae2b-29d6db6d4974");

    /** */
    private final TransactionManager txMgr;

    /** Accounts collection. */
    private final Table<Account> accountsTbl;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public AccountsRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        accountsTbl = new Table<Account>(ignite, "accounts")
            .addUniqueIndex(a -> a.getUsername().trim().toLowerCase(), (acc) -> "Account with email '" + acc.getUsername() + "' already registered")
            .addUniqueIndex(Account::getToken, (acc) -> "Account with token '" + acc.getToken() + "' already registered");
    }

    /**
     * Get account by ID.
     *
     * @param accId Account ID.
     * @return Account.
     * @throws IllegalStateException If user not found.
     */
    public Account getById(UUID accId) throws IllegalStateException {
        try (Transaction ignored = txMgr.txStart()) {
            Account account = accountsTbl.load(accId);

            if (account == null)
                throw new IllegalStateException("Account not found with ID: " + accId);

            return account;
        }
    }

    /**
     * Get account by email.
     *
     * @param email E-mail.
     * @return Account.
     * @throws UsernameNotFoundException If user not found.
     */
    public Account getByEmail(String email) throws UsernameNotFoundException {
        try (Transaction ignored = txMgr.txStart()) {
            Account account = accountsTbl.getByIndex(email);

            if (account == null)
                throw new UsernameNotFoundException(email);

            return account;
        }
        catch (IgniteException e) {
            throw new UsernameNotFoundException(email, e);
        }
    }

    /**
     * @return {@code true} If current user is the first one.
     */
    @SuppressWarnings("unchecked")
    public boolean ensureFirstUser() {
        IgniteCache cache = accountsTbl.cache();

        return cache.getAndPutIfAbsent(FIRST_USER_MARKER_KEY, FIRST_USER_MARKER_KEY) == null;
    }

    /**
     * Save account.
     *
     * @param account Account to save.
     * @return Saved account.
     * @throws IgniteException if failed to save account.
     */
    public Account create(Account account) throws AuthenticationServiceException {
        try (Transaction tx = txMgr.txStart()) {
            account.setAdmin(ensureFirstUser());

            save(account);

            tx.commit();

            return account;
        }
    }

    /**
     * Save account.
     *
     * @param account Account to save.
     */
    public void save(Account account) {
        try (Transaction tx = txMgr.txStart()) {
            accountsTbl.save(account);

            tx.commit();
        }
    }

    /**
     * Delete account.
     *
     * @param accId Account ID.
     */
    public Account delete(UUID accId) {
        try (Transaction tx = txMgr.txStart()) {
            Account acc = accountsTbl.delete(accId);

            if (acc == null)
                throw new IllegalStateException("Account not found for ID: " + accId);

            tx.commit();

            return acc;
        }
    }

    /**
     * @return List of accounts.
     */
    public List<Account> list() {
        try {
            return accountsTbl.loadAll();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }


    /**
     * @param tokens Tokens to check.
     * @return Valid tokens.
     */
    public Collection<Account> getAllByTokens(Set<String> tokens) {
        try (Transaction ignored = txMgr.txStart()) {
            return accountsTbl.loadAllByIndex(tokens);
        }
    }
}
