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
    implements: 'mail',
    inject: ['require(nodemailer)', 'settings']
};

module.exports.factory = function(nodemailer, settings) {
    return {
        /**
         * Send mail to user.
         * @param {Account} user
         * @param {String} subject
         * @param {String} html
         * @param {String} sendErr
         *
         * @return {Promise}
         */
        send: (user, subject, html, sendErr) => {
            const transporter = {
                service: settings.smtp.service,
                auth: {
                    user: settings.smtp.email,
                    pass: settings.smtp.password
                }
            };

            if (transporter.service === '' || transporter.auth.user === '' || transporter.auth.pass === '')
                return Promise.reject('Failed to send email. SMTP server is not configured . Please ask webmaster to setup SMTP server!');

            const mailer = nodemailer.createTransport(transporter);

            const sign = settings.smtp.sign ? `<br><br>--------------<br>${settings.smtp.sign}<br>` : '';

            const mail = {
                from: settings.smtp.address(settings.smtp.username, settings.smtp.email),
                to: settings.smtp.address(`${user.firstName} ${user.lastName}`, user.email),
                subject,
                html: html + sign
            };

            return new Promise((resolve, reject) => {
                mailer.sendMail(mail, (err) => {
                    if (err)
                        return reject(sendErr ? new Error(sendErr) : err);

                    resolve(user);
                });
            });
        }
    };
};
