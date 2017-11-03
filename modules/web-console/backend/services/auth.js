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

module.exports = {
    implements: 'services/auth',
    inject: ['require(lodash)', 'mongo', 'settings', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param settings
 * @param errors
 * @returns {AuthService}
 */

module.exports.factory = (_, mongo, settings, errors) => {
    class AuthService {
        /**
         * Generate token string.
         *
         * @param length - length of string
         * @returns {string} - generated token
         */
        static generateResetToken(length) {
            length = length || settings.tokenLength;
            const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            const possibleLen = possible.length;

            let res = '';

            for (let i = 0; i < length; i++)
                res += possible.charAt(Math.floor(Math.random() * possibleLen));

            return res;
        }

        /**
         * Reset password reset token for user.
         *
         * @param email - user email
         * @returns {Promise.<mongo.Account>} - that resolves account found by email with new reset password token.
         */
        static resetPasswordToken(email) {
            return mongo.Account.findOne({email}).exec()
                .then((user) => {
                    if (!user)
                        throw new errors.MissingResourceException('Account with that email address does not exists!');

                    user.resetPasswordToken = AuthService.generateResetToken(settings.tokenLength);

                    return user.save();
                });
        }

        /**
         * Reset password by reset token.
         *
         * @param {string} token - reset token
         * @param {string} newPassword - new password
         * @returns {Promise.<mongo.Account>} - that resolves account with new password
         */
        static resetPasswordByToken(token, newPassword) {
            return mongo.Account.findOne({resetPasswordToken: token}).exec()
                .then((user) => {
                    if (!user)
                        throw new errors.MissingResourceException('Failed to find account with this token! Please check link from email.');

                    return new Promise((resolve, reject) => {
                        user.setPassword(newPassword, (err, _user) => {
                            if (err)
                                return reject(new errors.AppErrorException('Failed to reset password: ' + err.message));

                            _user.resetPasswordToken = undefined; // eslint-disable-line no-undefined

                            resolve(_user.save());
                        });
                    });
                });
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

                    return {token, email: user.email};
                });
        }
    }

    return AuthService;
};
