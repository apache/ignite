/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { APP_INITIALIZER, ApplicationInitStatus } from './application_init';
import { ApplicationRef } from './application_ref';
import { APP_ID_RANDOM_PROVIDER } from './application_tokens';
import { IterableDiffers, KeyValueDiffers, defaultIterableDiffers, defaultKeyValueDiffers } from './change_detection/change_detection';
import { Console } from './console';
import { Injector } from './di';
import { Inject, Optional, SkipSelf } from './di/metadata';
import { ErrorHandler } from './error_handler';
import { DEFAULT_LOCALE_ID } from './i18n/localization';
import { LOCALE_ID } from './i18n/tokens';
import { ivyEnabled } from './ivy_switch';
import { ComponentFactoryResolver } from './linker';
import { Compiler } from './linker/compiler';
import { NgModule } from './metadata';
import { SCHEDULER } from './render3/component_ref';
import { setLocaleId } from './render3/i18n';
import { NgZone } from './zone';
/**
 * @return {?}
 */
export function _iterableDiffersFactory() {
    return defaultIterableDiffers;
}
/**
 * @return {?}
 */
export function _keyValueDiffersFactory() {
    return defaultKeyValueDiffers;
}
/**
 * @param {?=} locale
 * @return {?}
 */
export function _localeFactory(locale) {
    if (locale) {
        if (ivyEnabled) {
            setLocaleId(locale);
        }
        return locale;
    }
    // Use `goog.LOCALE` as default value for `LOCALE_ID` token for Closure Compiler.
    // Note: default `goog.LOCALE` value is `en`, when Angular used `en-US`. In order to preserve
    // backwards compatibility, we use Angular default value over Closure Compiler's one.
    if (ngI18nClosureMode && typeof goog !== 'undefined' && goog.LOCALE !== 'en') {
        if (ivyEnabled) {
            setLocaleId(goog.LOCALE);
        }
        return goog.LOCALE;
    }
    return DEFAULT_LOCALE_ID;
}
/**
 * A built-in [dependency injection token](guide/glossary#di-token)
 * that is used to configure the root injector for bootstrapping.
 * @type {?}
 */
export const APPLICATION_MODULE_PROVIDERS = [
    {
        provide: ApplicationRef,
        useClass: ApplicationRef,
        deps: [NgZone, Console, Injector, ErrorHandler, ComponentFactoryResolver, ApplicationInitStatus]
    },
    { provide: SCHEDULER, deps: [NgZone], useFactory: zoneSchedulerFactory },
    {
        provide: ApplicationInitStatus,
        useClass: ApplicationInitStatus,
        deps: [[new Optional(), APP_INITIALIZER]]
    },
    { provide: Compiler, useClass: Compiler, deps: [] },
    APP_ID_RANDOM_PROVIDER,
    { provide: IterableDiffers, useFactory: _iterableDiffersFactory, deps: [] },
    { provide: KeyValueDiffers, useFactory: _keyValueDiffersFactory, deps: [] },
    {
        provide: LOCALE_ID,
        useFactory: _localeFactory,
        deps: [[new Inject(LOCALE_ID), new Optional(), new SkipSelf()]]
    },
];
/**
 * Schedule work at next available slot.
 *
 * In Ivy this is just `requestAnimationFrame`. For compatibility reasons when bootstrapped
 * using `platformRef.bootstrap` we need to use `NgZone.onStable` as the scheduling mechanism.
 * This overrides the scheduling mechanism in Ivy to `NgZone.onStable`.
 *
 * @param {?} ngZone NgZone to use for scheduling.
 * @return {?}
 */
export function zoneSchedulerFactory(ngZone) {
    /** @type {?} */
    let queue = [];
    ngZone.onStable.subscribe((/**
     * @return {?}
     */
    () => {
        while (queue.length) {
            (/** @type {?} */ (queue.pop()))();
        }
    }));
    return (/**
     * @param {?} fn
     * @return {?}
     */
    function (fn) { queue.push(fn); });
}
/**
 * Configures the root injector for an app with
 * providers of `\@angular/core` dependencies that `ApplicationRef` needs
 * to bootstrap components.
 *
 * Re-exported by `BrowserModule`, which is included automatically in the root
 * `AppModule` when you create a new app with the CLI `new` command.
 *
 * \@publicApi
 */
export class ApplicationModule {
    // Inject ApplicationRef to make it eager...
    /**
     * @param {?} appRef
     */
    constructor(appRef) { }
}
ApplicationModule.decorators = [
    { type: NgModule, args: [{ providers: APPLICATION_MODULE_PROVIDERS },] }
];
/** @nocollapse */
ApplicationModule.ctorParameters = () => [
    { type: ApplicationRef }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXBwbGljYXRpb25fbW9kdWxlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvYXBwbGljYXRpb25fbW9kdWxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGVBQWUsRUFBRSxxQkFBcUIsRUFBQyxNQUFNLG9CQUFvQixDQUFDO0FBQzFFLE9BQU8sRUFBQyxjQUFjLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUNqRCxPQUFPLEVBQUMsc0JBQXNCLEVBQUMsTUFBTSxzQkFBc0IsQ0FBQztBQUM1RCxPQUFPLEVBQUMsZUFBZSxFQUFFLGVBQWUsRUFBRSxzQkFBc0IsRUFBRSxzQkFBc0IsRUFBQyxNQUFNLHFDQUFxQyxDQUFDO0FBQ3JJLE9BQU8sRUFBQyxPQUFPLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFDbEMsT0FBTyxFQUFDLFFBQVEsRUFBaUIsTUFBTSxNQUFNLENBQUM7QUFDOUMsT0FBTyxFQUFDLE1BQU0sRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQ3pELE9BQU8sRUFBQyxZQUFZLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUM3QyxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUN0RCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQ3hDLE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFDeEMsT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQ2xELE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUMzQyxPQUFPLEVBQUMsUUFBUSxFQUFDLE1BQU0sWUFBWSxDQUFDO0FBQ3BDLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSx5QkFBeUIsQ0FBQztBQUNsRCxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0s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b29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtBUFBfSU5JVElBTElaRVIsIEFwcGxpY2F0aW9uSW5pdFN0YXR1c30gZnJvbSAnLi9hcHBsaWNhdGlvbl9pbml0JztcbmltcG9ydCB7QXBwbGljYXRpb25SZWZ9IGZyb20gJy4vYXBwbGljYXRpb25fcmVmJztcbmltcG9ydCB7QVBQX0lEX1JBTkRPTV9QUk9WSURFUn0gZnJvbSAnLi9hcHBsaWNhdGlvbl90b2tlbnMnO1xuaW1wb3J0IHtJdGVyYWJsZURpZmZlcnMsIEtleVZhbHVlRGlmZmVycywgZGVmYXVsdEl0ZXJhYmxlRGlmZmVycywgZGVmYXVsdEtleVZhbHVlRGlmZmVyc30gZnJvbSAnLi9jaGFuZ2VfZGV0ZWN0aW9uL2NoYW5nZV9kZXRlY3Rpb24nO1xuaW1wb3J0IHtDb25zb2xlfSBmcm9tICcuL2NvbnNvbGUnO1xuaW1wb3J0IHtJbmplY3RvciwgU3RhdGljUHJvdmlkZXJ9IGZyb20gJy4vZGknO1xuaW1wb3J0IHtJbmplY3QsIE9wdGlvbmFsLCBTa2lwU2VsZn0gZnJvbSAnLi9kaS9tZXRhZGF0YSc7XG5pbXBvcnQge0Vycm9ySGFuZGxlcn0gZnJvbSAnLi9lcnJvcl9oYW5kbGVyJztcbmltcG9ydCB7REVGQVVMVF9MT0NBTEVfSUR9IGZyb20gJy4vaTE4bi9sb2NhbGl6YXRpb24nO1xuaW1wb3J0IHtMT0NBTEVfSUR9IGZyb20gJy4vaTE4bi90b2tlbnMnO1xuaW1wb3J0IHtpdnlFbmFibGVkfSBmcm9tICcuL2l2eV9zd2l0Y2gnO1xuaW1wb3J0IHtDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJ9IGZyb20gJy4vbGlua2VyJztcbmltcG9ydCB7Q29tcGlsZXJ9IGZyb20gJy4vbGlua2VyL2NvbXBpbGVyJztcbmltcG9ydCB7TmdNb2R1bGV9IGZyb20gJy4vbWV0YWRhdGEnO1xuaW1wb3J0IHtTQ0hFRFVMRVJ9IGZyb20gJy4vcmVuZGVyMy9jb21wb25lbnRfcmVmJztcbmltcG9ydCB7c2V0TG9jYWxlSWR9IGZyb20gJy4vcmVuZGVyMy9pMThuJztcbmltcG9ydCB7Tmdab25lfSBmcm9tICcuL3pvbmUnO1xuXG5leHBvcnQgZnVuY3Rpb24gX2l0ZXJhYmxlRGlmZmVyc0ZhY3RvcnkoKSB7XG4gIHJldHVybiBkZWZhdWx0SXRlcmFibGVEaWZmZXJzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gX2tleVZhbHVlRGlmZmVyc0ZhY3RvcnkoKSB7XG4gIHJldHVybiBkZWZhdWx0S2V5VmFsdWVEaWZmZXJzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gX2xvY2FsZUZhY3RvcnkobG9jYWxlPzogc3RyaW5nKTogc3RyaW5nIHtcbiAgaWYgKGxvY2FsZSkge1xuICAgIGlmIChpdnlFbmFibGVkKSB7XG4gICAgICBzZXRMb2NhbGVJZChsb2NhbGUpO1xuICAgIH1cbiAgICByZXR1cm4gbG9jYWxlO1xuICB9XG4gIC8vIFVzZSBgZ29vZy5MT0NBTEVgIGFzIGRlZmF1bHQgdmFsdWUgZm9yIGBMT0NBTEVfSURgIHRva2VuIGZvciBDbG9zdXJlIENvbXBpbGVyLlxuICAvLyBOb3RlOiBkZWZhdWx0IGBnb29nLkxPQ0FMRWAgdmFsdWUgaXMgYGVuYCwgd2hlbiBBbmd1bGFyIHVzZWQgYGVuLVVTYC4gSW4gb3JkZXIgdG8gcHJlc2VydmVcbiAgLy8gYmFja3dhcmRzIGNvbXBhdGliaWxpdHksIHdlIHVzZSBBbmd1bGFyIGRlZmF1bHQgdmFsdWUgb3ZlciBDbG9zdXJlIENvbXBpbGVyJ3Mgb25lLlxuICBpZiAobmdJMThuQ2xvc3VyZU1vZGUgJiYgdHlwZW9mIGdvb2cgIT09ICd1bmRlZmluZWQnICYmIGdvb2cuTE9DQUxFICE9PSAnZW4nKSB7XG4gICAgaWYgKGl2eUVuYWJsZWQpIHtcbiAgICAgIHNldExvY2FsZUlkKGdvb2cuTE9DQUxFKTtcbiAgICB9XG4gICAgcmV0dXJuIGdvb2cuTE9DQUxFO1xuICB9XG4gIHJldHVybiBERUZBVUxUX0xPQ0FMRV9JRDtcbn1cblxuLyoqXG4gKiBBIGJ1aWx0LWluIFtkZXBlbmRlbmN5IGluamVjdGlvbiB0b2tlbl0oZ3VpZGUvZ2xvc3NhcnkjZGktdG9rZW4pXG4gKiB0aGF0IGlzIHVzZWQgdG8gY29uZmlndXJlIHRoZSByb290IGluamVjdG9yIGZvciBib290c3RyYXBwaW5nLlxuICovXG5leHBvcnQgY29uc3QgQVBQTElDQVRJT05fTU9EVUxFX1BST1ZJREVSUzogU3RhdGljUHJvdmlkZXJbXSA9IFtcbiAge1xuICAgIHByb3ZpZGU6IEFwcGxpY2F0aW9uUmVmLFxuICAgIHVzZUNsYXNzOiBBcHBsaWNhdGlvblJlZixcbiAgICBkZXBzOlxuICAgICAgICBbTmdab25lLCBDb25zb2xlLCBJbmplY3RvciwgRXJyb3JIYW5kbGVyLCBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIsIEFwcGxpY2F0aW9uSW5pdFN0YXR1c11cbiAgfSxcbiAge3Byb3ZpZGU6IFNDSEVEVUxFUiwgZGVwczogW05nWm9uZV0sIHVzZUZhY3Rvcnk6IHpvbmVTY2hlZHVsZXJGYWN0b3J5fSxcbiAge1xuICAgIHByb3ZpZGU6IEFwcGxpY2F0aW9uSW5pdFN0YXR1cyxcbiAgICB1c2VDbGFzczogQXBwbGljYXRpb25Jbml0U3RhdHVzLFxuICAgIGRlcHM6IFtbbmV3IE9wdGlvbmFsKCksIEFQUF9JTklUSUFMSVpFUl1dXG4gIH0sXG4gIHtwcm92aWRlOiBDb21waWxlciwgdXNlQ2xhc3M6IENvbXBpbGVyLCBkZXBzOiBbXX0sXG4gIEFQUF9JRF9SQU5ET01fUFJPVklERVIsXG4gIHtwcm92aWRlOiBJdGVyYWJsZURpZmZlcnMsIHVzZUZhY3Rvcnk6IF9pdGVyYWJsZURpZmZlcnNGYWN0b3J5LCBkZXBzOiBbXX0sXG4gIHtwcm92aWRlOiBLZXlWYWx1ZURpZmZlcnMsIHVzZUZhY3Rvcnk6IF9rZXlWYWx1ZURpZmZlcnNGYWN0b3J5LCBkZXBzOiBbXX0sXG4gIHtcbiAgICBwcm92aWRlOiBMT0NBTEVfSUQsXG4gICAgdXNlRmFjdG9yeTogX2xvY2FsZUZhY3RvcnksXG4gICAgZGVwczogW1tuZXcgSW5qZWN0KExPQ0FMRV9JRCksIG5ldyBPcHRpb25hbCgpLCBuZXcgU2tpcFNlbGYoKV1dXG4gIH0sXG5dO1xuXG4vKipcbiAqIFNjaGVkdWxlIHdvcmsgYXQgbmV4dCBhdmFpbGFibGUgc2xvdC5cbiAqXG4gKiBJbiBJdnkgdGhpcyBpcyBqdXN0IGByZXF1ZXN0QW5pbWF0aW9uRnJhbWVgLiBGb3IgY29tcGF0aWJpbGl0eSByZWFzb25zIHdoZW4gYm9vdHN0cmFwcGVkXG4gKiB1c2luZyBgcGxhdGZvcm1SZWYuYm9vdHN0cmFwYCB3ZSBuZWVkIHRvIHVzZSBgTmdab25lLm9uU3RhYmxlYCBhcyB0aGUgc2NoZWR1bGluZyBtZWNoYW5pc20uXG4gKiBUaGlzIG92ZXJyaWRlcyB0aGUgc2NoZWR1bGluZyBtZWNoYW5pc20gaW4gSXZ5IHRvIGBOZ1pvbmUub25TdGFibGVgLlxuICpcbiAqIEBwYXJhbSBuZ1pvbmUgTmdab25lIHRvIHVzZSBmb3Igc2NoZWR1bGluZy5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHpvbmVTY2hlZHVsZXJGYWN0b3J5KG5nWm9uZTogTmdab25lKTogKGZuOiAoKSA9PiB2b2lkKSA9PiB2b2lkIHtcbiAgbGV0IHF1ZXVlOiAoKCkgPT4gdm9pZClbXSA9IFtdO1xuICBuZ1pvbmUub25TdGFibGUuc3Vic2NyaWJlKCgpID0+IHtcbiAgICB3aGlsZSAocXVldWUubGVuZ3RoKSB7XG4gICAgICBxdWV1ZS5wb3AoKSAhKCk7XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIGZ1bmN0aW9uKGZuOiAoKSA9PiB2b2lkKSB7IHF1ZXVlLnB1c2goZm4pOyB9O1xufVxuXG4vKipcbiAqIENvbmZpZ3VyZXMgdGhlIHJvb3QgaW5qZWN0b3IgZm9yIGFuIGFwcCB3aXRoXG4gKiBwcm92aWRlcnMgb2YgYEBhbmd1bGFyL2NvcmVgIGRlcGVuZGVuY2llcyB0aGF0IGBBcHBsaWNhdGlvblJlZmAgbmVlZHNcbiAqIHRvIGJvb3RzdHJhcCBjb21wb25lbnRzLlxuICpcbiAqIFJlLWV4cG9ydGVkIGJ5IGBCcm93c2VyTW9kdWxlYCwgd2hpY2ggaXMgaW5jbHVkZWQgYXV0b21hdGljYWxseSBpbiB0aGUgcm9vdFxuICogYEFwcE1vZHVsZWAgd2hlbiB5b3UgY3JlYXRlIGEgbmV3IGFwcCB3aXRoIHRoZSBDTEkgYG5ld2AgY29tbWFuZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBOZ01vZHVsZSh7cHJvdmlkZXJzOiBBUFBMSUNBVElPTl9NT0RVTEVfUFJPVklERVJTfSlcbmV4cG9ydCBjbGFzcyBBcHBsaWNhdGlvbk1vZHVsZSB7XG4gIC8vIEluamVjdCBBcHBsaWNhdGlvblJlZiB0byBtYWtlIGl0IGVhZ2VyLi4uXG4gIGNvbnN0cnVjdG9yKGFwcFJlZjogQXBwbGljYXRpb25SZWYpIHt9XG59XG4iXX0=