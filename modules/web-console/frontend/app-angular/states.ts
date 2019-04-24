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

import {NgHybridStateDeclaration} from '@uirouter/angular-hybrid';
import {PageProfile} from '.';

export const states: NgHybridStateDeclaration[] = [
    {
        name: 'base.settings.profile',
        url: '/profile',
        component: PageProfile,
        permission: 'profile',
        tfMetaTags: {
            title: 'User profile'
        }
    }
];
