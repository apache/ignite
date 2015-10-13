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

var _ = require('lodash');
var router = require('express').Router();
var nodemailer = require('nodemailer');

var db = require('../db');
var config = require('../helpers/configuration-loader.js');

router.get('/', function (req, res) {
    res.render('settings/admin');
});

/**
 * Get list of user accounts.
 */
router.post('/list', function (req, res) {
    db.Account.find({}).sort('username').exec(function (err, users) {
        if (err)
            return res.status(500).send(err.message);

        res.json(users);
    });
});

// Remove user.
router.post('/remove', function (req, res) {
    var userId = req.body.userId;

    db.Account.findByIdAndRemove(userId, function (err, user) {
        if (err)
            return res.status(500).send(err.message);

        db.Space.find({owner: userId}, function(err, spaces) {
            _.forEach(spaces, function (space) {
                db.Cluster.remove({space: space._id}).exec();
                db.Cache.remove({space: space._id}).exec();
                db.CacheTypeMetadata.remove({space: space._id}).exec();
                db.DatabasePreset.remove({space: space._id}).exec();
                db.Notebook.remove({space: space._id}).exec();
                db.Space.remove({owner: space._id}).exec();
            });
        });

        var transporter = {
            service: config.get('smtp:service'),
            auth: {
                user:config.get('smtp:username'),
                pass: config.get('smtp:password')
            }
        };

        if (transporter.service != '' || transporter.auth.user != '' || transporter.auth.pass != '') {
            var mailer  = nodemailer.createTransport(transporter);

            var mailOptions = {
                from: transporter.auth.user,
                to: user.email,
                subject: 'Your account was deleted',
                text: 'You are receiving this e-mail because admin remove your account.\n\n' +
                '--------------\n' +
                'Apache Ignite Web Console http://' + req.headers.host + '\n'
            };

            mailer.sendMail(mailOptions, function(err){
                if (err)
                    return res.status(503).send('Account was removed, but failed to send e-mail notification to user!<br />' + err);

                res.sendStatus(200);
            });
        }
        else
            res.sendStatus(200);
    });
});

// Save user.
router.post('/save', function (req, res) {
    var userId = req.body.userId;
    var adminFlag = req.body.adminFlag;

    db.Account.findByIdAndUpdate(userId, {admin: adminFlag}, function (err) {
        if (err)
            return res.status(500).send(err.message);

        res.sendStatus(200);
    });
});

// Become user.
router.get('/become', function (req, res) {
    var viewedUserId = req.query.viewedUserId;

    if (!viewedUserId) {
        req.session.viewedUser = null;

        return res.redirect('/admin');
    }

    db.Account.findById(viewedUserId).exec(function (err, viewedUser) {
        if (err)
            return res.sendStatus(404);

        req.session.viewedUser = viewedUser;

        res.redirect('/');
    })
});

module.exports = router;
