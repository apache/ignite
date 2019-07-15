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
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Repository;

import static org.apache.ignite.console.errors.Errors.checkDatabaseNotAvailable;

/**
 * Repository to work with accounts.
 */
@Repository
public class AccountsRepository {
    /** Special key to check that first user should be granted admin rights. */
    private static final UUID FIRST_USER_MARKER_KEY = UUID.fromString("039d28e2-133d-4eae-ae2b-29d6db6d4974");

    /** */
    private final TransactionManager txMgr;

    /** Messages accessor. */
    private final WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** Accounts collection. */
    private Table<Account> accountsTbl;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public AccountsRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter("accounts", () ->
            accountsTbl = new Table<Account>(ignite, "wc_accounts")
                .addUniqueIndex(a -> a.getUsername().trim().toLowerCase(),
                    (acc) -> messages.getMessageWithArgs("err.account-with-email-exists", acc.getUsername()))
                .addUniqueIndex(Account::getToken,
                    (acc) -> messages.getMessageWithArgs("err.account-with-token-exists", acc.getToken()))
        );
    }

    /**
     * Get account by ID.
     *
     * @param accId Account ID.
     * @return Account.
     * @throws UsernameNotFoundException If user not found.
     */
    public Account getById(UUID accId) throws IllegalStateException {
        return txMgr.doInTransaction(() -> {
            try {
                Account account = accountsTbl.load(accId);

                if (account == null)
                    throw new UsernameNotFoundException(accId.toString());

                return account;
            }
            catch (IgniteException e) {
                if (checkDatabaseNotAvailable(e))
                    throw e;

                throw new UsernameNotFoundException(accId.toString(), e);
            }
        });
    }

    /**
     * Get account by email.
     *
     * @param email E-mail.
     * @return Account.
     * @throws UsernameNotFoundException If user not found.
     */
    public Account getByEmail(String email) throws UsernameNotFoundException {
        return txMgr.doInTransaction(() -> {
            try {
                Account account = accountsTbl.getByIndex(email);

                if (account == null)
                    throw new UsernameNotFoundException(email);

                return account;
            }
            catch (IgniteException e) {
                if (checkDatabaseNotAvailable(e))
                    throw e;

                throw new UsernameNotFoundException(email, e);
            }
        });
    }

    /**
     * @return {@code true} If at least one user was already registered.
     */
    public boolean hasUsers() {
        return accountsTbl.cache().containsKey(FIRST_USER_MARKER_KEY);
    }

    /**
     * Save account.
     *
     * @param acc Account to save.
     * @return Saved account.
     * @throws IgniteException if failed to save account.
     */
    @SuppressWarnings("unchecked")
    public Account create(Account acc) throws AuthenticationServiceException {
        return txMgr.doInTransaction(() -> {
            boolean firstUser = !hasUsers();

            acc.setAdmin(firstUser);

            if (firstUser) {
                IgniteCache cache = accountsTbl.cache();

                cache.put(FIRST_USER_MARKER_KEY, FIRST_USER_MARKER_KEY);
            }

            save(acc);

            return acc;
        });
    }

    /**
     * Save account.
     *
     * @param account Account to save.
     */
    public Account save(Account account) {
        return txMgr.doInTransaction(() -> accountsTbl.save(account));
    }

    /**
     * Delete account.
     *
     * @param accId Account ID.
     * @return Deleted account.
     */
    public Account delete(UUID accId) {
        return txMgr.doInTransaction(() -> {
            Account acc = accountsTbl.delete(accId);

            if (acc == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.account-not-found-by-id", accId));

            return acc;
        });
    }

    /**
     * @return List of accounts.
     */
    public List<Account> list() {
        return accountsTbl.loadAll();
    }


    /**
     * @param tokens Tokens to check.
     * @return Valid tokens.
     */
    public Collection<Account> getAllByTokens(Set<String> tokens) {
        return txMgr.doInTransaction(() -> accountsTbl.loadAllByIndex(tokens));
    }
}
