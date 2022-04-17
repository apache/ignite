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
import { InjectionToken } from '../di/injection_token';
/**
 * Provide this token to set the locale of your application.
 * It is used for i18n extraction, by i18n pipes (DatePipe, I18nPluralPipe, CurrencyPipe,
 * DecimalPipe and PercentPipe) and by ICU expressions.
 *
 * See the [i18n guide](guide/i18n#setting-up-locale) for more information.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * import { LOCALE_ID } from '\@angular/core';
 * import { platformBrowserDynamic } from '\@angular/platform-browser-dynamic';
 * import { AppModule } from './app/app.module';
 *
 * platformBrowserDynamic().bootstrapModule(AppModule, {
 *   providers: [{provide: LOCALE_ID, useValue: 'en-US' }]
 * });
 * ```
 *
 * \@publicApi
 * @type {?}
 */
export const LOCALE_ID = new InjectionToken('LocaleId');
/**
 * Use this token at bootstrap to provide the content of your translation file (`xtb`,
 * `xlf` or `xlf2`) when you want to translate your application in another language.
 *
 * See the [i18n guide](guide/i18n#merge) for more information.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * import { TRANSLATIONS } from '\@angular/core';
 * import { platformBrowserDynamic } from '\@angular/platform-browser-dynamic';
 * import { AppModule } from './app/app.module';
 *
 * // content of your translation file
 * const translations = '....';
 *
 * platformBrowserDynamic().bootstrapModule(AppModule, {
 *   providers: [{provide: TRANSLATIONS, useValue: translations }]
 * });
 * ```
 *
 * \@publicApi
 * @type {?}
 */
export const TRANSLATIONS = new InjectionToken('Translations');
/**
 * Provide this token at bootstrap to set the format of your {\@link TRANSLATIONS}: `xtb`,
 * `xlf` or `xlf2`.
 *
 * See the [i18n guide](guide/i18n#merge) for more information.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * import { TRANSLATIONS_FORMAT } from '\@angular/core';
 * import { platformBrowserDynamic } from '\@angular/platform-browser-dynamic';
 * import { AppModule } from './app/app.module';
 *
 * platformBrowserDynamic().bootstrapModule(AppModule, {
 *   providers: [{provide: TRANSLATIONS_FORMAT, useValue: 'xlf' }]
 * });
 * ```
 *
 * \@publicApi
 * @type {?}
 */
export const TRANSLATIONS_FORMAT = new InjectionToken('TranslationsFormat');
/** @enum {number} */
const MissingTranslationStrategy = {
    Error: 0,
    Warning: 1,
    Ignore: 2,
};
export { MissingTranslationStrategy };
MissingTranslationStrategy[MissingTranslationStrategy.Error] = 'Error';
MissingTranslationStrategy[MissingTranslationStrategy.Warning] = 'Warning';
MissingTranslationStrategy[MissingTranslationStrategy.Ignore] = 'Ignore';
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidG9rZW5zLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvaTE4bi90b2tlbnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sdUJBQXVCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQXdCckQsTUFBTSxPQUFPLFNBQVMsR0FBRyxJQUFJLGNBQWMsQ0FBUyxVQUFVLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBMEIvRCxNQUFNLE9BQU8sWUFBWSxHQUFHLElBQUksY0FBYyxDQUFTLGNBQWMsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUF1QnRFLE1BQU0sT0FBTyxtQkFBbUIsR0FBRyxJQUFJLGNBQWMsQ0FBUyxvQkFBb0IsQ0FBQzs7O0lBMEJqRixRQUFTO0lBQ1QsVUFBVztJQUNYLFNBQVUiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0aW9uVG9rZW59IGZyb20gJy4uL2RpL2luamVjdGlvbl90b2tlbic7XG5cbi8qKlxuICogUHJvdmlkZSB0aGlzIHRva2VuIHRvIHNldCB0aGUgbG9jYWxlIG9mIHlvdXIgYXBwbGljYXRpb24uXG4gKiBJdCBpcyB1c2VkIGZvciBpMThuIGV4dHJhY3Rpb24sIGJ5IGkxOG4gcGlwZXMgKERhdGVQaXBlLCBJMThuUGx1cmFsUGlwZSwgQ3VycmVuY3lQaXBlLFxuICogRGVjaW1hbFBpcGUgYW5kIFBlcmNlbnRQaXBlKSBhbmQgYnkgSUNVIGV4cHJlc3Npb25zLlxuICpcbiAqIFNlZSB0aGUgW2kxOG4gZ3VpZGVdKGd1aWRlL2kxOG4jc2V0dGluZy11cC1sb2NhbGUpIGZvciBtb3JlIGluZm9ybWF0aW9uLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIGltcG9ydCB7IExPQ0FMRV9JRCB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuICogaW1wb3J0IHsgcGxhdGZvcm1Ccm93c2VyRHluYW1pYyB9IGZyb20gJ0Bhbmd1bGFyL3BsYXRmb3JtLWJyb3dzZXItZHluYW1pYyc7XG4gKiBpbXBvcnQgeyBBcHBNb2R1bGUgfSBmcm9tICcuL2FwcC9hcHAubW9kdWxlJztcbiAqXG4gKiBwbGF0Zm9ybUJyb3dzZXJEeW5hbWljKCkuYm9vdHN0cmFwTW9kdWxlKEFwcE1vZHVsZSwge1xuICogICBwcm92aWRlcnM6IFt7cHJvdmlkZTogTE9DQUxFX0lELCB1c2VWYWx1ZTogJ2VuLVVTJyB9XVxuICogfSk7XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBMT0NBTEVfSUQgPSBuZXcgSW5qZWN0aW9uVG9rZW48c3RyaW5nPignTG9jYWxlSWQnKTtcblxuLyoqXG4gKiBVc2UgdGhpcyB0b2tlbiBhdCBib290c3RyYXAgdG8gcHJvdmlkZSB0aGUgY29udGVudCBvZiB5b3VyIHRyYW5zbGF0aW9uIGZpbGUgKGB4dGJgLFxuICogYHhsZmAgb3IgYHhsZjJgKSB3aGVuIHlvdSB3YW50IHRvIHRyYW5zbGF0ZSB5b3VyIGFwcGxpY2F0aW9uIGluIGFub3RoZXIgbGFuZ3VhZ2UuXG4gKlxuICogU2VlIHRoZSBbaTE4biBndWlkZV0oZ3VpZGUvaTE4biNtZXJnZSkgZm9yIG1vcmUgaW5mb3JtYXRpb24uXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogYGBgdHlwZXNjcmlwdFxuICogaW1wb3J0IHsgVFJBTlNMQVRJT05TIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG4gKiBpbXBvcnQgeyBwbGF0Zm9ybUJyb3dzZXJEeW5hbWljIH0gZnJvbSAnQGFuZ3VsYXIvcGxhdGZvcm0tYnJvd3Nlci1keW5hbWljJztcbiAqIGltcG9ydCB7IEFwcE1vZHVsZSB9IGZyb20gJy4vYXBwL2FwcC5tb2R1bGUnO1xuICpcbiAqIC8vIGNvbnRlbnQgb2YgeW91ciB0cmFuc2xhdGlvbiBmaWxlXG4gKiBjb25zdCB0cmFuc2xhdGlvbnMgPSAnLi4uLic7XG4gKlxuICogcGxhdGZvcm1Ccm93c2VyRHluYW1pYygpLmJvb3RzdHJhcE1vZHVsZShBcHBNb2R1bGUsIHtcbiAqICAgcHJvdmlkZXJzOiBbe3Byb3ZpZGU6IFRSQU5TTEFUSU9OUywgdXNlVmFsdWU6IHRyYW5zbGF0aW9ucyB9XVxuICogfSk7XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBUUkFOU0xBVElPTlMgPSBuZXcgSW5qZWN0aW9uVG9rZW48c3RyaW5nPignVHJhbnNsYXRpb25zJyk7XG5cbi8qKlxuICogUHJvdmlkZSB0aGlzIHRva2VuIGF0IGJvb3RzdHJhcCB0byBzZXQgdGhlIGZvcm1hdCBvZiB5b3VyIHtAbGluayBUUkFOU0xBVElPTlN9OiBgeHRiYCxcbiAqIGB4bGZgIG9yIGB4bGYyYC5cbiAqXG4gKiBTZWUgdGhlIFtpMThuIGd1aWRlXShndWlkZS9pMThuI21lcmdlKSBmb3IgbW9yZSBpbmZvcm1hdGlvbi5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBpbXBvcnQgeyBUUkFOU0xBVElPTlNfRk9STUFUIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG4gKiBpbXBvcnQgeyBwbGF0Zm9ybUJyb3dzZXJEeW5hbWljIH0gZnJvbSAnQGFuZ3VsYXIvcGxhdGZvcm0tYnJvd3Nlci1keW5hbWljJztcbiAqIGltcG9ydCB7IEFwcE1vZHVsZSB9IGZyb20gJy4vYXBwL2FwcC5tb2R1bGUnO1xuICpcbiAqIHBsYXRmb3JtQnJvd3NlckR5bmFtaWMoKS5ib290c3RyYXBNb2R1bGUoQXBwTW9kdWxlLCB7XG4gKiAgIHByb3ZpZGVyczogW3twcm92aWRlOiBUUkFOU0xBVElPTlNfRk9STUFULCB1c2VWYWx1ZTogJ3hsZicgfV1cbiAqIH0pO1xuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgVFJBTlNMQVRJT05TX0ZPUk1BVCA9IG5ldyBJbmplY3Rpb25Ub2tlbjxzdHJpbmc+KCdUcmFuc2xhdGlvbnNGb3JtYXQnKTtcblxuLyoqXG4gKiBVc2UgdGhpcyBlbnVtIGF0IGJvb3RzdHJhcCBhcyBhbiBvcHRpb24gb2YgYGJvb3RzdHJhcE1vZHVsZWAgdG8gZGVmaW5lIHRoZSBzdHJhdGVneVxuICogdGhhdCB0aGUgY29tcGlsZXIgc2hvdWxkIHVzZSBpbiBjYXNlIG9mIG1pc3NpbmcgdHJhbnNsYXRpb25zOlxuICogLSBFcnJvcjogdGhyb3cgaWYgeW91IGhhdmUgbWlzc2luZyB0cmFuc2xhdGlvbnMuXG4gKiAtIFdhcm5pbmcgKGRlZmF1bHQpOiBzaG93IGEgd2FybmluZyBpbiB0aGUgY29uc29sZSBhbmQvb3Igc2hlbGwuXG4gKiAtIElnbm9yZTogZG8gbm90aGluZy5cbiAqXG4gKiBTZWUgdGhlIFtpMThuIGd1aWRlXShndWlkZS9pMThuI21pc3NpbmctdHJhbnNsYXRpb24pIGZvciBtb3JlIGluZm9ybWF0aW9uLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICogYGBgdHlwZXNjcmlwdFxuICogaW1wb3J0IHsgTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbiAqIGltcG9ydCB7IHBsYXRmb3JtQnJvd3NlckR5bmFtaWMgfSBmcm9tICdAYW5ndWxhci9wbGF0Zm9ybS1icm93c2VyLWR5bmFtaWMnO1xuICogaW1wb3J0IHsgQXBwTW9kdWxlIH0gZnJvbSAnLi9hcHAvYXBwLm1vZHVsZSc7XG4gKlxuICogcGxhdGZvcm1Ccm93c2VyRHluYW1pYygpLmJvb3RzdHJhcE1vZHVsZShBcHBNb2R1bGUsIHtcbiAqICAgbWlzc2luZ1RyYW5zbGF0aW9uOiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneS5FcnJvclxuICogfSk7XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBlbnVtIE1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5IHtcbiAgRXJyb3IgPSAwLFxuICBXYXJuaW5nID0gMSxcbiAgSWdub3JlID0gMixcbn1cbiJdfQ==