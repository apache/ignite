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

var router = require('express').Router();
var db = require('../db');

/**
 * Get user profile page.
 */
router.get('/', function (req, res) {
    var user_id = req.currentUserId();

    db.Account.findById(user_id, function (err) {
        if (err)
            return res.status(500).send(err.message);

        res.render('settings/profile');
    });
});

function _updateUser(res, user, params) {
    if (params.userName)
        user.username = params.userName;

    if (params.email)
        user.email = params.email;

    if (params.token)
        user.token = params.token;

    if (params.userName || params.email || params.token || params.newPassword)
        user.save(function (err) {
            if (err)
                // TODO IGNITE-843 Send error to admin.
                return res.status(500).send('Failed to update profile!');

            res.json(user);
        });
    else
        res.status(200);
}

function _checkUserEmailAndUpdate(res, user, params) {
    if (params.email && user.email != params.email) {
        db.Account.findOne({email: params.email}, function(err, userForEmail) {
            // TODO send error to admin
            if (err)
                return res.status(500).send('Failed to check e-mail!');

            if (userForEmail && userForEmail._id != user._id)
                return res.status(500).send('User with this e-mail already registered!');

            _updateUser(res, user, params);
        });
    }
    else
        _updateUser(res, user, params);
}

/**
 * Save user profile.
 */
router.post('/save', function (req, res) {
    var params = req.body;

    db.Account.findById(params._id, function (err, user) {
        if (err)
        // TODO IGNITE-843 Send error to admin
            return res.status(500).send('Failed to find user!');

        if (params.newPassword) {
            var newPassword = params.newPassword;

            if (!newPassword || newPassword.length == 0)
                return res.status(500).send('Wrong value for new password!');

            user.setPassword(newPassword, function (err, user) {
                if (err)
                    return res.status(500).send(err.message);

                _checkUserEmailAndUpdate(res, user, params);
            });
        }
        else
            _checkUserEmailAndUpdate(res, user, params);
    });
});

module.exports = router;
