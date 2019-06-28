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
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.security.MissingConfirmRegistrationException;
import org.apache.ignite.console.web.socket.WebSocketsManager;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.userdetails.UserDetailsChecker;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.apache.ignite.console.notification.NotificationDescriptor.ACTIVATION_LINK;
import static org.apache.ignite.console.notification.NotificationDescriptor.PASSWORD_CHANGED;
import static org.apache.ignite.console.notification.NotificationDescriptor.PASSWORD_RESET;
import static org.apache.ignite.console.notification.NotificationDescriptor.WELCOME_LETTER;

/**
 * Service to handle accounts.
 */
@Service
public class AccountsService implements UserDetailsService {
    /** */
    protected TransactionManager txMgr;

    /** */
    protected AccountsRepository accountsRepo;

    /** */
    protected WebSocketsManager wsm;

    /** */
    protected NotificationService notificationSrv;

    /** */
    protected PasswordEncoder encoder;

    /** User details getChecker. */
    protected UserDetailsChecker userDetailsChecker;

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
     * @param notificationSrv Notification service.
     */
    public AccountsService(
        SignUpConfiguration signUpCfg,
        ActivationConfiguration activationCfg,
        PasswordEncoder encoder,
        WebSocketsManager wsm,
        AccountsRepository accountsRepo,
        TransactionManager txMgr,
        NotificationService notificationSrv
    ) {
        disableSignup = !signUpCfg.isEnabled();
        userDetailsChecker = activationCfg.getChecker();
        activationEnabled = activationCfg.isEnabled();
        activationSndTimeout = activationCfg.getSendTimeout();

        this.encoder = encoder;
        this.wsm = wsm;
        this.accountsRepo = accountsRepo;
        this.txMgr = txMgr;
        this.notificationSrv = notificationSrv;
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
                throw new AuthenticationServiceException("Sign-up is not allowed. Ask your administrator to create account for you.");

            return acc0;
        });

        if (activationEnabled) {
            notificationSrv.sendEmail(ACTIVATION_LINK, acc);

            throw new MissingConfirmRegistrationException("Confirm your email", acc.getEmail());
        }

        notificationSrv.sendEmail(WELCOME_LETTER, acc);
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
            throw new IllegalAccessError("Activation was not enabled!");

        Account acc = txMgr.doInTransaction(() -> {
            Account acc0 = accountsRepo.getByEmail(email);

            if (MILLIS.between(acc0.getActivationSentAt(), LocalDateTime.now()) >= activationSndTimeout)
                throw new IllegalAccessError("Too many activation attempts");

            acc0.resetActivationToken();

            accountsRepo.save(acc0);

            return acc0;
        });

        notificationSrv.sendEmail(ACTIVATION_LINK, acc);
    }

    /**
     * Save user.
     *
     * @param accId User ID.
     * @param changes Changes to apply to user.
     */
    public Account save(UUID accId, ChangeUserRequest changes) {
        Account acc = txMgr.doInTransaction(() -> {
            Account acc0 = accountsRepo.getById(accId);

            acc0.update(changes);

            String pwd = changes.getPassword();

            if (!F.isEmpty(pwd))
                acc0.setPassword(encoder.encode(pwd));

            accountsRepo.save(acc0);
            
            return acc0;
        });

        String oldTok = acc.getToken();

        if (!oldTok.equals(acc.getToken()))
            wsm.revokeToken(acc, oldTok);

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

        notificationSrv.sendEmail(PASSWORD_RESET, acc);
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
                throw new IllegalStateException("Failed to find account with this token! Please check link from email.");

            userDetailsChecker.check(acc);

            acc.setPassword(encoder.encode(newPwd));
            acc.setResetPasswordToken(null);

            accountsRepo.save(acc);

            notificationSrv.sendEmail(PASSWORD_CHANGED, acc);
        });
    }
}
