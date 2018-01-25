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

const path = require('path');
const { Selector } = require('testcafe');
const { AngularJSSelector } = require('testcafe-angular-selectors');

const igniteSignUp = async(t) => {
    await t.navigateTo(`${process.env.APP_URL || 'http://localhost:9001/'}`);

    await t.click(Selector('a').withText('Sign Up'));

    await t
        .click(Selector('#signup_email'))
        .typeText(Selector('#signup_email'), 'a@a')
        .typeText(AngularJSSelector.byModel('ui.password'), 'a')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'a')
        .typeText(AngularJSSelector.byModel('ui.firstName'), 'John')
        .typeText(AngularJSSelector.byModel('ui.lastName'), 'Doe')
        .typeText(AngularJSSelector.byModel('ui.company'), 'DevNull LTD')
        .click('#country')
        .click(Selector('span').withText('Brazil'))
        .click('#signup');

    // close modal window
    await t.click('.modal-header button.close');
};


const igniteSignIn = async(t) => {
    await t.navigateTo(`${process.env.APP_URL || 'http://localhost:9001/'}`);

    await t
        .typeText(AngularJSSelector.byModel('ui.email'), 'a@a')
        .typeText(AngularJSSelector.byModel('ui.password'), 'a')
        .click('#login');

    // close modal window
    await t.click('.modal-header button.close');
};

const signIn = process.env.IGNITE_MODULES ? require(path.join(process.env.IGNITE_MODULES, 'e2e/testcafe/roles.js')).igniteSignIn : igniteSignIn;
const signUp = process.env.IGNITE_MODULES ? require(path.join(process.env.IGNITE_MODULES, 'e2e/testcafe/roles.js')).igniteSignUp : igniteSignUp;

module.exports = { signUp, signIn };
