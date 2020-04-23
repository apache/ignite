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
    implements: 'services/notifications',
    inject: ['mongo', 'browsers-handler']
};

/**
 * @param mongo
 * @param browsersHnd
 * @returns {NotificationsService}
 */
module.exports.factory = (mongo, browsersHnd) => {
    class NotificationsService {
        /**
         * Update notifications.
         *
         * @param {String} owner - User ID
         * @param {String} message - Message to users.
         * @param {Boolean} isShown - Whether to show message.
         * @param {Date} [date] - Optional date to save in notifications.
         * @returns {Promise.<mongo.ObjectId>} that resolve activity
         */
        static merge(owner, message, isShown = false, date = new Date()) {
            return mongo.Notifications.create({owner, message, date, isShown})
                .then(({message, date, isShown}) => browsersHnd.updateNotification({message, date, isShown}));
        }
    }

    return NotificationsService;
};
