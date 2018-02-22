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
    await t.navigateTo(`${process.env.APP_URL || 'http://localhost:9001/'}signin`);

    await t
        .typeText(AngularJSSelector.byModel('ui_signup.email'), 'a@a')
        .typeText(AngularJSSelector.byModel('ui_signup.password'), 'a')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'a')
        .typeText(AngularJSSelector.byModel('ui_signup.firstName'), 'John')
        .typeText(AngularJSSelector.byModel('ui_signup.lastName'), 'Doe')
        .typeText(AngularJSSelector.byModel('ui_signup.company'), 'DevNull LTD')
        .click('#countryInput')
        .click(Selector('span').withText('Brazil'))
        .click('#signup_submit');

    // close modal window
    await t.click('.modal-header button.close');
};


const igniteSignIn = async(t) => {
    await t.navigateTo(`${process.env.APP_URL || 'http://localhost:9001/'}signin`);

    await t
        .typeText(AngularJSSelector.byModel('ui.email'), 'a@a')
        .typeText(AngularJSSelector.byModel('ui.password'), 'a')
        .click('#signin_submit');

    // close modal window
    await t.click('.modal-header button.close');
};

const signIn = process.env.IGNITE_MODULES ? require(path.join(process.env.IGNITE_MODULES, 'e2e/testcafe/roles.js')).igniteSignIn : igniteSignIn;
const signUp = process.env.IGNITE_MODULES ? require(path.join(process.env.IGNITE_MODULES, 'e2e/testcafe/roles.js')).igniteSignUp : igniteSignUp;

module.exports = { signUp, signIn };
