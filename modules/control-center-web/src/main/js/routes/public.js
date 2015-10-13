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
var passport = require('passport');
var nodemailer = require('nodemailer');

var db = require('../db');
var config = require('../helpers/configuration-loader.js');
var $commonUtils = require('./../helpers/common-utils');

// GET dropdown-menu template.
router.get('/select', function (req, res) {
    res.render('templates/select', {});
});

// GET dropdown-menu template.
router.get('/validation-error', function (req, res) {
    res.render('templates/validation-error', {});
});

// GET confirmation dialog.
router.get('/message', function (req, res) {
    res.render('templates/message', {});
});

// GET confirmation dialog.
router.get('/confirm', function (req, res) {
    res.render('templates/confirm', {});
});

// GET batch confirmation dialog.
router.get('/confirm/batch', function (req, res) {
    res.render('templates/batch-confirm', {});
});

// GET copy dialog.
router.get('/clone', function (req, res) {
    res.render('templates/clone', {});
});

/* GET login dialog. */
router.get('/login', function (req, res) {
    res.render('login');
});

/**
 * Register new account.
 */
router.post('/register', function (req, res) {
    db.Account.count(function (err, cnt) {
        if (err)
            return res.status(401).send(err.message);

        req.body.admin = cnt == 0;

        var account = new db.Account(req.body);

        account.token = $commonUtils.randomString(20);

        db.Account.register(account, req.body.password, function (err, account) {
            if (err)
                return res.status(401).send(err.message);

            if (!account)
                return res.status(500).send('Failed to create account.');

            new db.Space({name: 'Personal space', owner: account._id}).save();

            req.logIn(account, {}, function (err) {
                if (err)
                    return res.status(401).send(err.message);

                return res.redirect('/configuration/clusters');
            });
        });
    });
});

/**
 * Login in exist account.
 */
router.post('/login', function (req, res, next) {
    passport.authenticate('local', function (err, user) {
        if (err)
            return res.status(401).send(err.message);

        if (!user)
            return res.status(401).send('Invalid email or password');

        req.logIn(user, {}, function (err) {
            if (err)
                return res.status(401).send(err.message);

            res.redirect('/configuration/clusters');
        });
    })(req, res, next);
});

/**
 * Logout.
 */
router.get('/logout', function (req, res) {
    req.logout();

    res.redirect('/');
});

/**
 * Send e-mail to user with reset token.
 */
router.post('/password/forgot', function(req, res) {
    var transporter = {
        service: config.get('smtp:service'),
        auth: {
            user:config.get('smtp:username'),
            pass: config.get('smtp:password')
        }
    };

    if (transporter.service == '' || transporter.auth.user == '' || transporter.auth.pass == '')
        return res.status(401).send('Can\'t send e-mail with instructions to reset password.<br />' +
            'Please ask webmaster to setup smtp server!');

    var token = $commonUtils.randomString(20);

    db.Account.findOne({ email: req.body.email }, function(err, user) {
        if (!user)
            return res.status(401).send('No account with that email address exists!');

        if (err)
            // TODO IGNITE-843 Send email to admin
            return res.status(401).send('Failed to reset password!');

        user.resetPasswordToken = token;

        user.save(function(err) {
            if (err)
            // TODO IGNITE-843 Send email to admin
            return res.status(401).send('Failed to reset password!');

            var mailer  = nodemailer.createTransport(transporter);

            var mailOptions = {
                from: transporter.auth.user,
                to: user.email,
                subject: 'Password Reset',
                text: 'You are receiving this because you (or someone else) have requested the reset of the password for your account.\n\n' +
                'Please click on the following link, or paste this into your browser to complete the process:\n\n' +
                'http://' + req.headers.host + '/password/reset/' + token + '\n\n' +
                'If you did not request this, please ignore this email and your password will remain unchanged.\n\n' +
                '--------------\n' +
                'Apache Ignite Web Console\n'
            };

            mailer.sendMail(mailOptions, function(err){
                if (err)
                    return res.status(401).send('Failed to send e-mail with reset link!<br />' + err);

                return res.status(403).send('An e-mail has been sent with further instructions.');
            });
        });
    });
});

/**
 * Change password with given token.
 */
router.post('/password/reset', function(req, res) {
    db.Account.findOne({ resetPasswordToken: req.body.token }, function(err, user) {
        if (!user)
            return res.status(500).send('Invalid token for password reset!');

        if (err)
            // TODO IGNITE-843 Send email to admin
            return res.status(500).send('Failed to reset password!');

        user.setPassword(req.body.password, function (err, updatedUser) {
            if (err)
                return res.status(500).send(err.message);

            updatedUser.resetPasswordToken = undefined;

            updatedUser.save(function (err) {
                if (err)
                    return res.status(500).send(err.message);

                var transporter = {
                    service: config.get('smtp:service'),
                    auth: {
                        user: config.get('smtp:username'),
                        pass: config.get('smtp:password')
                    }
                };

                var mailer = nodemailer.createTransport(transporter);

                var mailOptions = {
                    from: transporter.auth.user,
                    to: user.email,
                    subject: 'Your password has been changed',
                    text: 'Hello,\n\n' +
                    'This is a confirmation that the password for your account ' + user.email + ' has just been changed.\n\n' +
                    'Now you can login: http://' + req.headers.host + '\n\n' +
                    '--------------\n' +
                    'Apache Ignite Web Console\n'
                };

                mailer.sendMail(mailOptions, function (err) {
                    if (err)
                        return res.status(503).send('Password was changed, but failed to send confirmation e-mail!<br />' + err);

                    return res.status(200).send(user.email);
                });
            });
        });
    });
});

router.get('/password/reset', function (req, res) {
    res.render('reset');
});

/* GET reset password page. */
router.get('/password/reset/:token', function (req, res) {
    var token = req.params.token;

    var data = {token: token};

    db.Account.findOne({resetPasswordToken: token}, function (err, user) {
        if (!user)
            data.error = 'Invalid token for password reset!';
        else if (err)
            data.error = err;
        else
            data.email = user.email;

        res.render('reset', data);
    });
});

/* GET home page. */
router.get('/', function (req, res) {
    if (req.isAuthenticated())
        res.redirect('/configuration/clusters');
    else
        res.render('index');
});

module.exports = router;
