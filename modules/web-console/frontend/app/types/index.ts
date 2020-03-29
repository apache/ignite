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

import {Ng1StateDeclaration} from '@uirouter/angularjs';

interface ITfMetatagsConfig {
    title: string
}

export interface IIgniteNg1StateDeclaration extends Ng1StateDeclaration {
    /**
     * Whether to store state as last visited in local storage or not:
     * `true` - will be saved
     * `false` (default) - won't be saved
     * @type {boolean}
     */
    unsaved?: boolean,
    tfMetaTags: ITfMetatagsConfig,
    permission?: string
}

export type User = {
    _id: string,
    firstName: string,
    lastName: string,
    email: string,
    phone?: string,
    company: string,
    country: string,
    registered: string,
    lastLogin: string,
    lastActivity: string,
    admin: boolean,
    token: string,
    resetPasswordToken: string,
    // Assigned in UI
    becomeUsed?: boolean
};

export type NavigationMenuItem = {
    label: string,
    icon: string,
    order: number,
    hidden?: boolean
} & (
    {sref: string, activeSref: string} |
    {href: string}
);

export type NavigationMenu = Array<NavigationMenuItem>;

export type MenuItem <T> = {
    label: string,
    value: T
};

export type Menu <T> = MenuItem<T>[];

export interface IInputErrorNotifier {
    notifyAboutError(): void
    hideError(): void
}

export enum WellKnownOperationStatus {
    WAITING = 'WAITING',
    ERROR = 'ERROR',
    DONE = 'DONE'
}
