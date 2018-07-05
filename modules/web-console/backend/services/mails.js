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

const _ = require('lodash');
const nodemailer = require('nodemailer');

// Fire me up!

module.exports = {
    implements: 'services/mails',
    inject: ['settings']
};

/**
 * @param settings
 * @returns {MailsService}
 */
module.exports.factory = (settings) => {
    class MailsService {
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
        send(user, subject, html, sendErr) {
            const options = settings.mail;

            return new Promise((resolve, reject) => {
                if (_.isEmpty(options))
                    reject(new Error('SMTP server is not configured.'));

                if (!_.isEmpty(options.service)) {
                    if (_.isEmpty(options.auth) || _.isEmpty(options.auth.user) || _.isEmpty(options.auth.pass))
                        reject(new Error(`Credentials is not configured for service: ${options.service}`));
                }

                resolve(nodemailer.createTransport(options));
            })
                .then((transporter) => {
                    return transporter.verify().then(() => transporter);
                })
                .then((transporter) => {
                    const sign = options.sign ? `<br><br>--------------<br>${options.sign}<br>` : '';
                    const to = `"${user.firstName} ${user.lastName}" <${user.email}>`;

                    const mail = {
                        from: options.from,
                        to,
                        subject,
                        html: html + sign
                    };

                    return transporter.sendMail(mail);
                })
                .catch((err) => {
                    console.log('Failed to send email.', err);

                    return Promise.reject(sendErr ? new Error(sendErr) : err);
                });
        }

        /**
         * Send email to user for password reset.
         * @param host
         * @param user
         */
        emailUserSignUp(host, user) {
            const resetLink = `${host}/password/reset?token=${user.resetPasswordToken}`;

            return this.send(user, `Thanks for signing up for ${settings.mail.greeting}.`,
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
        emailUserResetLink(host, user) {
            const resetLink = `${host}/password/reset?token=${user.resetPasswordToken}`;

            return this.send(user, 'Password Reset',
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
        emailPasswordChanged(host, user) {
            return this.send(user, 'Your password has been changed',
                `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                `This is a confirmation that the password for your account on <a href="${host}">${settings.mail.greeting}</a> has just been changed.<br><br>`,
                'Password was changed, but failed to send confirmation email!');
        }

        /**
         * Send email to user when it was deleted.
         * @param host
         * @param user
         */
        emailUserDeletion(host, user) {
            return this.send(user, 'Your account was removed',
                `Hello ${user.firstName} ${user.lastName}!<br><br>` +
                `You are receiving this email because your account for <a href="${host}">${settings.mail.greeting}</a> was removed.`,
                'Account was removed, but failed to send email notification to user!');
        }
    }

    return new MailsService();
};
