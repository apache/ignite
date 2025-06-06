

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
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

        txMgr.registerStarter(() ->
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
                Account account = accountsTbl.get(accId);

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
                // add@byron
                account.setAdmin(true);
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
    public int hasUsers() {
        return accountsTbl.cache().size(CachePeekMode.PRIMARY);
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
            int numUsers = hasUsers();

            if (numUsers==0) {
            	acc.setAdmin(true);
            }
            
            acc.setUid(numUsers+1);
            
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
