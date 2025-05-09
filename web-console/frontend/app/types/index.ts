

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
    id: string,
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

export type BsSelectDropdownAction = {
    click: (any) => any,
    action: string,
    available: boolean
}

export type BsSelectDropdownActions = BsSelectDropdownAction[]

export type Stacktrace = {
    message: string,
    stacktrace: string[]
};
