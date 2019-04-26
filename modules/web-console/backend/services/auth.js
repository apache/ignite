/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

// Fire me up!

const _ = require('lodash');

module.exports = {
    implements: 'services/auth',
    inject: ['mongo', 'settings', 'errors', 'services/utils', 'services/mails']
};

/**
 * @param mongo
 * @param settings
 * @param errors
 * @param {UtilsService} utilsService
 * @param {MailsService} mailsService
 * @returns {AuthService}
 */

module.exports.factory = (mongo, settings, errors, utilsService, mailsService) => {
    class AuthService {
        /**
         * Reset password reset token for user.
         *
         * @param host Web Console host.
         * @param email - user email
         * @returns {Promise.<mongo.Account>} - that resolves account found by email with new reset password token.
         */
        static resetPasswordToken(host, email) {
            return mongo.Account.findOne({email}).exec()
                .then((user) => {
                    if (!user)
                        throw new errors.MissingResourceException('Account with that email address does not exists!');

                    if (settings.activation.enabled && !user.activated)
                        throw new errors.MissingConfirmRegistrationException(user.email);

                    user.resetPasswordToken = utilsService.randomString(settings.tokenLength);

                    return user.save();
                })
                .then((user) => mailsService.sendResetLink(host, user)
                    .then(() => user));
        }

        /**
         * Reset password by reset token.
         *
         * @param host Web Console host.
         * @param {string} token - reset token
         * @param {string} newPassword - new password
         * @returns {Promise.<mongo.Account>} - that resolves account with new password
         */
        static resetPasswordByToken(host, token, newPassword) {
            return mongo.Account.findOne({resetPasswordToken: token}).exec()
                .then((user) => {
                    if (!user)
                        throw new errors.MissingResourceException('Failed to find account with this token! Please check link from email.');

                    if (settings.activation.enabled && !user.activated)
                        throw new errors.MissingConfirmRegistrationException(user.email);

                    return new Promise((resolve, reject) => {
                        user.setPassword(newPassword, (err, _user) => {
                            if (err)
                                return reject(new errors.AppErrorException('Failed to reset password: ' + err.message));

                            _user.resetPasswordToken = undefined; // eslint-disable-line no-undefined

                            resolve(_user.save());
                        });
                    });
                })
                .then((user) => mailsService.sendPasswordChanged(host, user)
                    .then(() => user));
        }

        /**
         * Find account by token.
         *
         * @param {string} token - reset token
         * @returns {Promise.<{token, email}>} - that resolves token and user email
         */
        static validateResetToken(token) {
            return mongo.Account.findOne({resetPasswordToken: token}).exec()
                .then((user) => {
                    if (!user)
                        throw new errors.IllegalAccessError('Invalid token for password reset!');

                    if (settings.activation.enabled && !user.activated)
                        throw new errors.MissingConfirmRegistrationException(user.email);

                    return {token, email: user.email};
                });
        }

        /**
         * Validate activationToken token.
         *
         * @param {mongo.Account} user - User object.
         * @param {string} activationToken - activate account token
         * @return {Error} If token is invalid.
         */
        static validateActivationToken(user, activationToken) {
            if (user.activated) {
                if (!_.isEmpty(activationToken) && user.activationToken !== activationToken)
                    return new errors.AuthFailedException('Invalid email or password!');
            }
            else {
                if (_.isEmpty(activationToken))
                    return new errors.MissingConfirmRegistrationException(user.email);

                if (user.activationToken !== activationToken)
                    return new errors.AuthFailedException('This activation token isn\'t valid.');
            }
        }

        /**
         * Check if activation token expired.
         *
         * @param {mongo.Account} user - User object.
         * @param {string} activationToken - activate account token
         * @return {boolean} If token was already expired.
         */
        static isActivationTokenExpired(user, activationToken) {
            return !user.activated &&
                new Date().getTime() - user.activationSentAt.getTime() >= settings.activation.timeout;
        }

        /**
         * Reset password reset token for user.
         *
         * @param host Web Console host.
         * @param email - user email.
         * @returns {Promise}.
         */
        static resetActivationToken(host, email) {
            return mongo.Account.findOne({email}).exec()
                .then((user) => {
                    if (!user)
                        throw new errors.MissingResourceException('Account with that email address does not exists!');

                    if (!settings.activation.enabled)
                        throw new errors.IllegalAccessError('Activation was not enabled!');

                    if (user.activationSentAt &&
                        new Date().getTime() - user.activationSentAt.getTime() < settings.activation.sendTimeout)
                        throw new errors.IllegalAccessError('Too Many Activation Attempts!');

                    user.activationToken = utilsService.randomString(settings.tokenLength);
                    user.activationSentAt = new Date();

                    return user.save();
                })
                .then((user) => mailsService.sendActivationLink(host, user));
        }
    }

    return AuthService;
};
