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
    implements: 'services/mails',
    inject: ['require(lodash)', 'require(nodemailer)', 'settings']
};

/**
 * @param _
 * @param nodemailer
 * @param settings
 * @returns {MailsService}
 */
module.exports.factory = (_, nodemailer, settings) => {
    /**
     * Send mail to user.
     *
     * @param {Account} user
     * @param {String} subject
     * @param {String} html
     * @param {String} sendErr
     * @throws {Error}
     * @return {Promise}
     */
    const send = (user, subject, html, sendErr) => {
        return new Promise((resolve, reject) => {
            const transportConfig = settings.mail;

            if (_.isEmpty(transportConfig.service) || _.isEmpty(transportConfig.auth.user) || _.isEmpty(transportConfig.auth.pass))
                throw new Error('Failed to send email. SMTP server is not configured. Please ask webmaster to setup SMTP server!');

            const mailer = nodemailer.createTransport(transportConfig);

            const sign = settings.mail.sign ? `<br><br>--------------<br>${settings.mail.sign}<br>` : '';

            const mail = {
                from: settings.mail.from,
                to: settings.mail.address(`${user.firstName} ${user.lastName}`, user.email),
                subject,
                html: html + sign
            };

            mailer.sendMail(mail, (err) => {
                if (err)
                    return reject(sendErr ? new Error(sendErr) : err);

                resolve(user);
            });
        });
    };

    class MailsService {
        /**
         * Send email to user for password reset.
         * @param host
         * @param user
         */
        static emailUserSignUp(host, user) {
            const resetLink = `${host}/password/reset?token=${user.resetPasswordToken}`;

            return send(user, `Thanks for signing up for ${settings.mail.greeting}.`,
                `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                `You are receiving this email because you have signed up to use <a href="${host}">${settings.mail.greeting}</a>.<br><br>` +
                'If you have not done the sign up and do not know what this email is about, please ignore it.<br>' +
                'You may reset the password by clicking on the following link, or paste this into your browser:<br><br>' +
                `<a href="${resetLink}">${resetLink}</a>`);
        }

        /**
         * Send email to user for password reset.
         * @param host
         * @param user
         */
        static emailUserResetLink(host, user) {
            const resetLink = `${host}/password/reset?token=${user.resetPasswordToken}`;

            return send(user, 'Password Reset',
                `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                'You are receiving this because you (or someone else) have requested the reset of the password for your account.<br><br>' +
                'Please click on the following link, or paste this into your browser to complete the process:<br><br>' +
                `<a href="${resetLink}">${resetLink}</a><br><br>` +
                'If you did not request this, please ignore this email and your password will remain unchanged.',
                'Failed to send email with reset link!');
        }

        /**
         * Send email to user for password reset.
         * @param host
         * @param user
         */
        static emailPasswordChanged(host, user) {
            return send(user, 'Your password has been changed',
                `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                `This is a confirmation that the password for your account on <a href="${host}">${settings.mail.greeting}</a> has just been changed.<br><br>`,
                'Password was changed, but failed to send confirmation email!');
        }

        /**
         * Send email to user when it was deleted.
         * @param host
         * @param user
         */
        static emailUserDeletion(host, user) {
            return send(user, 'Your account was removed',
                `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                `You are receiving this email because your account for <a href="${host}">${settings.mail.greeting}</a> was removed.`,
                'Account was removed, but failed to send email notification to user!');
        }
    }

    return MailsService;
};
