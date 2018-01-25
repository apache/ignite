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

const { Selector, Role } = require('testcafe');
const { signUp } = require('../roles');
const { AngularJSSelector } = require('testcafe-angular-selectors');
const { removeData, insertTestUser } = require('../envtools');

fixture('Checking Ignite auth screen')
    .page `${process.env.APP_URL || 'http://localhost:9001/'}`
    .beforeEach(async(t) => {
        await removeData();

        await t.setNativeDialogHandler(() => true);
        await t.useRole(Role.anonymous());
    })
    .after(async() => {
        await removeData();
    });

test('Testing Ignite signup validation and signup success', async(t) => {
    async function checkBtnDisabled() {
        const btnDisabled = await t.expect(Selector('#signup').getAttribute('disabled')).ok();

        const btnNotWorks =  await t
            .click('#signup')
            .expect(Selector('title').innerText).eql('Apache Ignite - Management Tool and Configuration Wizard – Apache Ignite Web Console');

        return btnDisabled && btnNotWorks;
    }

    await t.click(Selector('a').withText('Sign Up'));

    await t
        .click(Selector('#signup_email'))
        .typeText(Selector('#signup_email'), 'test@test.com');
    await checkBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui.password'), 'qwerty')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'qwerty');
    await checkBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui.firstName'), 'John')
        .typeText(AngularJSSelector.byModel('ui.lastName'), 'Doe');
    await checkBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui.company'), 'DevNull LTD');
    await checkBtnDisabled();

    await t
        .click('#country')
        .click(Selector('span').withText('Brazil'));

    // checking passwords confirm dismatch
    await t
        .click(AngularJSSelector.byModel('ui_exclude.confirm'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'ytrewq');
    await checkBtnDisabled();
    await t
        .click(AngularJSSelector.byModel('ui_exclude.confirm'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'qwerty');

    await t.click('#signup')
        .expect(Selector('title').innerText).eql('Basic Configuration – Apache Ignite Web Console');

});

test('Testing Ignite validation and successful sign in of existing user', async(t) => {
    async function checkSignInBtnDisabled() {
        const btnDisabled = await t.expect(await Selector('#login').getAttribute('disabled')).ok();
        const btnNotWorks =  await t
            .click('#login')
            .expect(Selector('title').innerText).eql('Apache Ignite - Management Tool and Configuration Wizard – Apache Ignite Web Console');

        return btnDisabled && btnNotWorks;
    }

    await insertTestUser();

    // checking signin validation
    await t
        .typeText(AngularJSSelector.byModel('ui.email'), 'test@test.com');
    await checkSignInBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui.password'), 'b')
        .click('#login');
    await t.expect(Selector('#popover-validation-message').withText('Invalid email or password').exists).ok();

    await t
        .click(AngularJSSelector.byModel('ui.email'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui.email'), 'testtest.com');
    await checkSignInBtnDisabled();

    // checking regular sigin in
    await t
        .click(AngularJSSelector.byModel('ui.email'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui.email'), 'a@a')
        .click(AngularJSSelector.byModel('ui.password'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui.password'), 'a')
        .click('#login')
        .expect(Selector('title').innerText).eql('Basic Configuration – Apache Ignite Web Console');

});

test('Forbid Ignite signing up of already existing user', async(t) => {
    await insertTestUser();

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

    await t.expect(Selector('#popover-validation-message').withText('A user with the given username is already registered.').exists).ok();

});

test('Test Ignite password reset', async(t) => {
    await t.click(Selector('#password-forgot-signin'));

    // testing incorrect email
    await t
        .typeText('#forgot_email', 'testtest')
        .expect(await Selector('button').withText('Send it to me').getAttribute('disabled')).ok();

    // testing handling unknown email password reset
    await t
        .click(Selector('#forgot_email'))
        .pressKey('ctrl+a delete')
        .typeText(Selector('#forgot_email'), 'nonexisting@mail.com')
        .click(Selector('button').withText('Send it to me'));

    await t.expect(Selector('#popover-validation-message').withText('Account with that email address does not exists!').exists).ok();

    // testing regular password reset
    await t
        .click(Selector('#forgot_email'))
        .pressKey('ctrl+a delete')
        .typeText(Selector('#forgot_email'), 'a@a')
        .click(Selector('button').withText('Send it to me'));

    await t.expect(Selector('#popover-validation-message').withText('Account with that email address does not exists!').exists).notOk();
});

test('Testing Ignite loguout', async(t) => {
    await signUp(t);

    await t.click(Selector('div').withAttribute('bs-dropdown', 'userbar.items'));
    await t
        .click(Selector('a').withAttribute('ui-sref', 'logout'))
        .expect(Selector('title').innerText).eql('Apache Ignite - Management Tool and Configuration Wizard – Apache Ignite Web Console');
});
