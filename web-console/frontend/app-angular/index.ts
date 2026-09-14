

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
import {TranslateModule, TranslateService} from '@ngx-translate/core';

import {upgradedComponents} from './upgrade';
import defaultLanguage from '../i18n/messages.en.json';
import messagesCn from '../i18n/messages.zh-CN.json';

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
        TranslateModule.forRoot(),
        UIRouterUpgradeModule.forRoot(),
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
    static parameters = [[new Inject(UIRouter)], [new Inject(TranslateService)]];

    constructor(router: UIRouter, translate: TranslateService) {
        translate.setTranslation('en', defaultLanguage);
        translate.setTranslation('zh-CN', messagesCn);
        
        translate.setDefaultLang('en');
        translate.use('zh-CN');
        
        states(translate).subscribe((s) => router.stateRegistry.register(s));
    }

    ngDoBootstrap() {}
}
