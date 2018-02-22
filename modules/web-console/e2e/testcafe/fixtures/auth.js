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
    .page `${process.env.APP_URL || 'http://localhost:9001/'}signin`
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
        const btnDisabled = await t.expect(Selector('#signup_submit').getAttribute('disabled')).ok();

        const btnNotWorks =  await t
            .click('#signup_submit')
            .expect(Selector('title').innerText).eql('Apache Ignite - Management Tool and Configuration Wizard – Apache Ignite Web Console');

        return btnDisabled && btnNotWorks;
    }

    await t
        .typeText(AngularJSSelector.byModel('ui_signup.email'), 'test@test.com');
    await checkBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui_signup.password'), 'qwerty')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'qwerty');
    await checkBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui_signup.firstName'), 'John')
        .typeText(AngularJSSelector.byModel('ui_signup.lastName'), 'Doe');
    await checkBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui_signup.company'), 'DevNull LTD');
    await checkBtnDisabled();

    await t
        .click('#countryInput')
        .click(Selector('span').withText('Brazil'));

    // Checking passwords confirm dismatch.
    await t
        .click(AngularJSSelector.byModel('ui_exclude.confirm'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'ytrewq');
    await checkBtnDisabled();

    await t
        .click(AngularJSSelector.byModel('ui_exclude.confirm'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui_exclude.confirm'), 'qwerty');

    await t.click('#signup_submit')
        .expect(Selector('title').innerText).eql('Basic Configuration – Apache Ignite Web Console');

});

test('Testing Ignite validation and successful sign in of existing user', async(t) => {
    async function checkSignInBtnDisabled() {
        const btnDisabled = await t.expect(await Selector('#signin_submit').getAttribute('disabled')).ok();
        const btnNotWorks =  await t
            .click('#signin_submit')
            .expect(Selector('title').innerText).eql('Apache Ignite - Management Tool and Configuration Wizard – Apache Ignite Web Console');

        return btnDisabled && btnNotWorks;
    }

    await insertTestUser();

    // Checking signin validation.
    await t
        .typeText(AngularJSSelector.byModel('ui.email'), 'test@test.com');
    await checkSignInBtnDisabled();

    await t
        .typeText(AngularJSSelector.byModel('ui.password'), 'b')
        .click('#signin_submit');
    await t.expect(Selector('#popover-validation-message').withText('Invalid email or password').exists).ok();

    await t
        .click(AngularJSSelector.byModel('ui.email'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui.email'), 'testtest.com');
    await checkSignInBtnDisabled();

    // Checking regular sigin in
    await t
        .click(AngularJSSelector.byModel('ui.email'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui.email'), 'a@a')
        .click(AngularJSSelector.byModel('ui.password'))
        .pressKey('ctrl+a delete')
        .typeText(AngularJSSelector.byModel('ui.password'), 'a')
        .click('#signin_submit')
        .expect(Selector('title').innerText).eql('Basic Configuration – Apache Ignite Web Console');

});

test('Forbid Ignite signing up of already existing user', async(t) => {
    await insertTestUser();

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

    await t.expect(Selector('#popover-validation-message').withText('A user with the given username is already registered').exists).ok();

});

test('Test Ignite password reset', async(t) => {
    await t.click(Selector('#forgot_show'));

    // Testing incorrect email.
    await t
        .typeText('#forgotEmailInput', 'testtest')
        .expect(await Selector('button').withText('Send it to me').getAttribute('disabled')).ok();

    // Testing handling unknown email password reset.
    await t
        .click(Selector('#forgotEmailInput'))
        .pressKey('ctrl+a delete')
        .typeText(Selector('#forgotEmailInput'), 'nonexisting@mail.com')
        .click(Selector('button').withText('Send it to me'));

    await t.expect(Selector('#popover-validation-message').withText('Account with that email address does not exists!').exists).ok();

    // Testing regular password reset.
    await t
        .click(Selector('#forgotEmailInput'))
        .pressKey('ctrl+a delete')
        .typeText(Selector('#forgotEmailInput'), 'a@a')
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
