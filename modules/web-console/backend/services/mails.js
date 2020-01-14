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

const fs = require('fs');
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
         * Read template file.
         * @param {String} template Path to template file.
         * @param template
         */
        readTemplate(template) {
            try {
                return fs.readFileSync(template, 'utf8');
            }
            catch (ignored) {
                throw new Error('Failed to find email template: ' + template);
            }
        }

        /**
         * Get message with resolved variables.
         *
         * @param {string} template Message template.
         * @param {object} ctx Context.
         * @return Prepared template.
         * @throws IOException If failed to prepare template.
         */
        getMessage(template, ctx) {
            _.forIn(ctx, (value, key) => template = template.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value || 'n/a'));

            return template;
        }

        /**
         * @param {string} host Web Console host.
         * @param {Account} user User that signed up.
         * @param {string} message Message.
         * @param {object} customCtx Custom context parameters.
         */
        buildContext(host, user, message, customCtx) {
            return {
                message,
                ...customCtx,
                greeting: settings.mail.greeting,
                sign: settings.mail.sign,
                firstName: user.firstName,
                lastName: user.lastName,
                email: user.res,
                host,
                activationLink: `${host}/signin?activationToken=${user.activationToken}`,
                resetLink: `${host}/password/reset?token=${user.resetPasswordToken}`
            };
        }

        /**
         * Send mail to user.
         *
         * @param {string} template Path to template file.
         * @param {string} host Web Console host.
         * @param {Account} user User that signed up.
         * @param {string} subject Email subject.
         * @param {string} message Email message.
         * @param {object} customCtx Custom context parameters.
         * @throws {Error}
         * @return {Promise}
         */
        send(template, host, user, subject, message, customCtx = {}) {
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
                    const context = this.buildContext(host, user, message, customCtx);

                    context.subject = this.getMessage(subject, context);

                    return transporter.sendMail({
                        from: options.from,
                        to: `"${user.firstName} ${user.lastName}" <${user.email}>`,
                        subject: context.subject,
                        html: this.getMessage(this.readTemplate(template), context)
                    });
                })
                .catch((err) => {
                    console.log('Failed to send email.', err);

                    return Promise.reject(err);
                });
        }

        /**
         * Send email when user signed up.
         *
         * @param host Web Console host.
         * @param user User that signed up.
         * @param createdByAdmin Whether user was created by admin.
         */
        sendWelcomeLetter(host, user, createdByAdmin) {
            if (createdByAdmin) {
                return this.send('templates/base.html', host, user, 'Account was created for ${greeting}.',
                    'You are receiving this email because administrator created account for you to use <a href="${host}">${greeting}</a>.<br><br>' +
                    'If you do not know what this email is about, please ignore it.<br>' +
                    'You may reset the password by clicking on the following link, or paste this into your browser:<br><br>' +
                    '<a href="${resetLink}">${resetLink}</a>'
                );
            }

            return this.send('templates/base.html', host, user, 'Thanks for signing up for ${greeting}.',
                'You are receiving this email because you have signed up to use <a href="${host}">${greeting}</a>.<br><br>' +
                'If you do not know what this email is about, please ignore it.<br>' +
                'You may reset the password by clicking on the following link, or paste this into your browser:<br><br>' +
                '<a href="${resetLink}">${resetLink}</a>'
            );
        }

        /**
         * Send email to user for password reset.
         *
         * @param host
         * @param user
         */
        sendActivationLink(host, user) {
            return this.send('templates/base.html', host, user, 'Confirm your account on ${greeting}',
                'You are receiving this email because you have signed up to use <a href="${host}">${greeting}</a>.<br><br>' +
                'Please click on the following link, or paste this into your browser to activate your account:<br><br>' +
                '<a href="${activationLink}">${activationLink}</a>'
            )
                .catch(() => Promise.reject(new Error('Failed to send email with confirm account link!')));
        }

        /**
         * Send email to user for password reset.
         *
         * @param host
         * @param user
         */
        sendResetLink(host, user) {
            return this.send('templates/base.html', host, user, 'Password Reset',
                'You are receiving this because you (or someone else) have requested the reset of the password for your account.<br><br>' +
                'Please click on the following link, or paste this into your browser to complete the process:<br><br>' +
                '<a href="${resetLink}">${resetLink}</a><br><br>' +
                'If you did not request this, please ignore this email and your password will remain unchanged.'
            )
                .catch(() => Promise.reject(new Error('Failed to send email with reset link!')));
        }

        /**
         * Send email to user for password reset.
         * @param host
         * @param user
         */
        sendPasswordChanged(host, user) {
            return this.send('templates/base.html', host, user, 'Your password has been changed',
                'This is a confirmation that the password for your account on <a href="${host}">${greeting}</a> has just been changed.'
            )
                .catch(() => Promise.reject(new Error('Password was changed, but failed to send confirmation email!')));
        }

        /**
         * Send email to user when it was deleted.
         * @param host
         * @param user
         */
        sendAccountDeleted(host, user) {
            return this.send('templates/base.html', host, user, 'Your account was removed',
                'You are receiving this email because your account for <a href="${host}">${greeting}</a> was removed.',
                'Account was removed, but failed to send email notification to user!')
                .catch(() => Promise.reject(new Error('Password was changed, but failed to send confirmation email!')));
        }
    }

    return new MailsService();
};
