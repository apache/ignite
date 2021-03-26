/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {AngularFormField} from '../components/FormField';
import {AngularPanelCollapsible} from '../components/PanelCollapsible';
import {Selector} from 'testcafe';

export const pageProfile = {
    firstName: new AngularFormField({id: 'firstName'}),
    lastName: new AngularFormField({id: 'lastName'}),
    email: new AngularFormField({id: 'email'}),
    phone: new AngularFormField({id: 'phone'}),
    country: new AngularFormField({id: 'country'}),
    company: new AngularFormField({id: 'company'}),
    securityToken: {
        panel: new AngularPanelCollapsible('security token'),
        generateTokenButton: Selector('a').withText('Generate Random Security Token?'),
        value: new AngularFormField({id: 'securityToken'})
    },
    password: {
        panel: new AngularPanelCollapsible('password'),
        newPassword: new AngularFormField({id: 'newPassword'}),
        confirmPassword: new AngularFormField({id: 'confirmPassword'})
    },
    saveChangesButton: Selector('.btn-ignite.btn-ignite--success').withText('Save Changes')
};
