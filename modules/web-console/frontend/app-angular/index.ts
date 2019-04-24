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

// See https://angular.io/guide/deployment#enable-runtime-production-mode for details.
import {enableProdMode} from '@angular/core';
if (process.env.NODE_ENV === 'production') enableProdMode();

import './style.scss';

import {NgModule, Inject} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {UIRouterUpgradeModule} from '@uirouter/angular-hybrid';
import {UIRouter} from '@uirouter/angular';
import {UpgradeModule} from '@angular/upgrade/static';
import {NgxPopperModule} from 'ngx-popper';

import {ServiceBootstrapComponent} from './components/serviceBootstrap';
export {ServiceBootstrapComponent};
import {PageProfile} from './components/page-profile/component';
export {PageProfile};
import {IgniteIcon} from './components/igniteIcon.component';
import {PanelCollapsible} from './components/panelCollapsible.component';
import {CopyToClipboardButton} from './components/copyToClipboardButton.component';

import {FormFieldHint} from './components/form-field/hint.component';
import {FormFieldErrors} from './components/form-field/errors.component';
import {FormFieldError} from './components/form-field/error.component';
import {FormField} from './components/form-field/formField.component';
import {Autofocus} from './components/form-field/autofocus.directive';
import {FormFieldTooltip} from './components/form-field/tooltip.component';
import {PasswordVisibilityToggleButton} from './components/form-field/passwordVisibilityToggleButton.component';
import {ScrollToFirstInvalid} from './components/form-field/scrollToFirstInvalid.directive';

import {ReactiveFormsModule} from '@angular/forms';

import {upgradedComponents} from './upgrade';

export const declarations = [
    ServiceBootstrapComponent,
    PageProfile,
    IgniteIcon,
    PanelCollapsible,
    CopyToClipboardButton,
    FormFieldHint,
    FormFieldErrors,
    FormFieldError,
    FormField,
    Autofocus,
    FormFieldTooltip,
    PasswordVisibilityToggleButton,
    ScrollToFirstInvalid,
    ...upgradedComponents
];

export const entryComponents = [
    ServiceBootstrapComponent,
    PageProfile
];

export const upgradeService = (token: string) => ({
    provide: token,
    useFactory: (i) => i.get(token),
    deps: ['$injector']
});

export const providers = [
    'IgniteLegacyUtils',
    'Confirm',
    'IgniteCountries',
    'User',
    'IgniteIcons',
    'IgniteCopyToClipboard'
].map(upgradeService);

import {states} from './states';

@NgModule({
    imports: [
        BrowserModule,
        ReactiveFormsModule,
        UpgradeModule,
        UIRouterUpgradeModule.forRoot({states}),
        NgxPopperModule.forRoot({
            applyClass: 'ignite-popper',
            appendTo: 'body',
            boundariesElement: 'ui-view.content'
        })
    ],
    providers,
    declarations,
    entryComponents,
    exports: [
        ...declarations,
        NgxPopperModule
    ]
})
export class IgniteWebConsoleModule {
    static parameters = [[new Inject(UIRouter)]];

    constructor(private router: UIRouter) {}

    ngDoBootstrap() {}
}
