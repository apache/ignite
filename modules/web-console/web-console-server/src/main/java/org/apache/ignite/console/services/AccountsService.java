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

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.config.SignUpConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.security.MissingConfirmRegistrationException;
import org.apache.ignite.console.web.socket.WebSocketsManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.userdetails.UserDetailsChecker;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_UPDATE;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_CHANGED;
import static org.apache.ignite.console.event.AccountEventType.PASSWORD_RESET;
import static org.apache.ignite.console.event.AccountEventType.RESET_ACTIVATION_TOKEN;

/**
 * Service to handle accounts.
 */
@Service
public class AccountsService implements UserDetailsService {
    /** Tx manager. */
    protected TransactionManager txMgr;

    /** Accounts repository. */
    protected AccountsRepository accountsRepo;

    /** Web socket manager. */
    protected WebSocketsManager wsm;

    /** Event publisher. */
    protected EventPublisher evtPublisher;

    /** Password encoder. */
    protected PasswordEncoder encoder;

    /** User details getChecker. */
    protected UserDetailsChecker userDetailsChecker;

    /** Messages acessor. */
    protected final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** Flag if sign up disabled and new accounts can be created only by administrator. */
    private boolean disableSignup;

    /** Whether account should be activated by e-mail confirmation. */
    private boolean activationEnabled;

    /** Activation send email throttle. */
    private long activationSndTimeout;

    /**
     * @param signUpCfg Sign up configuration.
     * @param activationCfg Account activation configuration.
     * @param encoder Service interface for encoding passwords.
     * @param wsm Websocket manager.
     * @param accountsRepo Accounts repository.
     * @param txMgr Transactions manager.
     * @param evtPublisher Event publisher.
     */
    public AccountsService(
        SignUpConfiguration signUpCfg,
        ActivationConfiguration activationCfg,
        PasswordEncoder encoder,
        WebSocketsManager wsm,
        AccountsRepository accountsRepo,
        TransactionManager txMgr,
        EventPublisher evtPublisher
    ) {
        disableSignup = !signUpCfg.isEnabled();
        userDetailsChecker = activationCfg.getChecker();
        activationEnabled = activationCfg.isEnabled();
        activationSndTimeout = activationCfg.getSendTimeout();

        this.encoder = encoder;
        this.wsm = wsm;
        this.accountsRepo = accountsRepo;
        this.txMgr = txMgr;
        this.evtPublisher = evtPublisher;
    }

    /** {@inheritDoc} */
    @Override public Account loadUserByUsername(String email) throws UsernameNotFoundException {
        return accountsRepo.getByEmail(email);
    }

    /**
     * Create account for user.
     *
     * @param params Sign up params.
     * @return New account.
     */
    protected Account create(SignUpRequest params) {
        Account acc = new Account(
            params.getEmail(),
            encoder.encode(params.getPassword()),
            params.getFirstName(),
            params.getLastName(),
            params.getPhone(),
            params.getCompany(),
            params.getCountry()
        );

        if (activationEnabled)
            acc.resetActivationToken();

        return accountsRepo.create(acc);
    }

    /**
     * Register account for user.
     *
     * @param params SignUp params.
     */
    public void register(SignUpRequest params) {
        Account acc = txMgr.doInTransaction(() -> {
            Account acc0 = create(params);

            if (disableSignup && !acc0.isAdmin())
                throw new AuthenticationServiceException(messages.getMessage("err.sign-up-not-allowed"));

            return acc0;
        });

        if (activationEnabled) {
            evtPublisher.publish(new Event<>(RESET_ACTIVATION_TOKEN, acc));

            throw new MissingConfirmRegistrationException(messages.getMessage("err.confirm-email"), acc.getEmail());
        }

        evtPublisher.publish(new Event<>(ACCOUNT_CREATE, acc));
    }

    /**
     * Delete account by ID.
     *
     * @return All registered accounts.
     */
    public List<Account> list() {
        return accountsRepo.list();
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     */
    Account delete(UUID accId) {
        return accountsRepo.delete(accId);
    }

    /**
     * Update admin flag..
     *
     * @param accId Account ID.
     * @param adminFlag New value for admin flag.
     */
    public void toggle(UUID accId, boolean adminFlag) {
        txMgr.doInTransaction(() -> {
            Account account = accountsRepo.getById(accId);

            if (account.isAdmin() != adminFlag) {
                account.setAdmin(adminFlag);

                accountsRepo.save(account);
            }
        });
    }

    /**
     * Reset activation token for account
     *
     * @param accId Account id.
     */
    public void activateAccount(UUID accId) {
        txMgr.doInTransaction(() -> {
            Account acc = accountsRepo.getById(accId);

            acc.activate();

            accountsRepo.save(acc);
        });
    }

    /**
     * Reset activation token for account
     *
     * @param email Email.
     */
    public void resetActivationToken(String email) {
        if (!activationEnabled)
            throw new IllegalAccessError(messages.getMessage("err.activation-not-enabled"));

        Account acc = txMgr.doInTransaction(() -> {
            Account acc0 = accountsRepo.getByEmail(email);

            if (MILLIS.between(acc0.getActivationSentAt(), LocalDateTime.now()) >= activationSndTimeout)
                throw new IllegalAccessError(messages.getMessage("err.too-many-activation-attempts"));

            acc0.resetActivationToken();

            accountsRepo.save(acc0);

            return acc0;
        });

        evtPublisher.publish(new Event<>(RESET_ACTIVATION_TOKEN, acc));
    }

    /**
     * Save user.
     *
     * @param accId User ID.
     * @param changes Changes to apply to user.
     */
    public Account save(UUID accId, ChangeUserRequest changes) {
        T2<Account, String> res = txMgr.doInTransaction(() -> {
            Account acc = accountsRepo.getById(accId);

            acc.update(changes);

            String pwd = changes.getPassword();

            if (!F.isEmpty(pwd))
                acc.setPassword(encoder.encode(pwd));

            Account oldAcc = accountsRepo.save(acc);

            return new T2<>(acc, oldAcc.getToken());
        });

        Account acc = res.get1();
        String oldTok = res.get2();

        if (!oldTok.equals(acc.getToken()))
            wsm.revokeToken(acc, oldTok);

        evtPublisher.publish(new Event<>(ACCOUNT_UPDATE, acc));

        return acc;
    }

    /**
     * @param email User email to send reset password link.
     */
    public void forgotPassword(String email) {
        Account acc = txMgr.doInTransaction(() -> {
            Account acc0 = accountsRepo.getByEmail(email);

            userDetailsChecker.check(acc0);

            acc0.setResetPasswordToken(UUID.randomUUID().toString());

            accountsRepo.save(acc0);

            return acc0;
        });

        evtPublisher.publish(new Event<>(PASSWORD_RESET, acc));
    }

    /**
     * @param email E-mail of user that request password reset.
     * @param resetPwdTok Reset password token.
     * @param newPwd New password.
     */
    public void resetPasswordByToken(String email, String resetPwdTok, String newPwd) {
        txMgr.doInTransaction(() -> {
            Account acc = accountsRepo.getByEmail(email);

            if (!resetPwdTok.equals(acc.getResetPasswordToken()))
                throw new IllegalStateException(messages.getMessage("err.account-not-found-by-token"));

            userDetailsChecker.check(acc);

            acc.setPassword(encoder.encode(newPwd));
            acc.setResetPasswordToken(null);

            accountsRepo.save(acc);

            evtPublisher.publish(new Event<>(PASSWORD_CHANGED, acc));
        });
    }
}
