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

import {CustomFormField} from '../components/FormField';
import {PanelCollapsible} from '../components/PanelCollapsible';
import {Selector} from 'testcafe';

export const pageProfile = {
    firstName: new CustomFormField({id: 'firstNameInput'}),
    lastName: new CustomFormField({id: 'lastNameInput'}),
    email: new CustomFormField({id: 'emailInput'}),
    phone: new CustomFormField({id: 'phoneInput'}),
    country: new CustomFormField({id: 'countryInput'}),
    company: new CustomFormField({id: 'companyInput'}),
    securityToken: {
        panel: new PanelCollapsible('security token'),
        generateTokenButton: Selector('a').withText('Generate Random Security Token?'),
        value: new CustomFormField({id: 'securityTokenInput'})
    },
    password: {
        panel: new PanelCollapsible('password'),
        newPassword: new CustomFormField({id: 'passwordInput'}),
        confirmPassword: new CustomFormField({id: 'passwordConfirmInput'})
    },
    saveChangesButton: Selector('.btn-ignite.btn-ignite--success').withText('Save Changes')
};
