/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
