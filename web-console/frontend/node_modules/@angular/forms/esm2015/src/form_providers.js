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
import { NgModule } from '@angular/core';
import { InternalFormsSharedModule, NG_FORM_SELECTOR_WARNING, NG_MODEL_WITH_FORM_CONTROL_WARNING, REACTIVE_DRIVEN_DIRECTIVES, TEMPLATE_DRIVEN_DIRECTIVES } from './directives';
import { RadioControlRegistry } from './directives/radio_control_value_accessor';
import { FormBuilder } from './form_builder';
/**
 * Exports the required providers and directives for template-driven forms,
 * making them available for import by NgModules that import this module.
 *
 * @see [Forms Guide](/guide/forms)
 *
 * \@publicApi
 */
export class FormsModule {
    /**
     * \@description
     * Provides options for configuring the template-driven forms module.
     *
     * @param {?} opts An object of configuration options
     * * `warnOnDeprecatedNgFormSelector` Configures when to emit a warning when the deprecated
     * `ngForm` selector is used.
     * @return {?}
     */
    static withConfig(opts) {
        return {
            ngModule: FormsModule,
            providers: [{ provide: NG_FORM_SELECTOR_WARNING, useValue: opts.warnOnDeprecatedNgFormSelector }]
        };
    }
}
FormsModule.decorators = [
    { type: NgModule, args: [{
                declarations: TEMPLATE_DRIVEN_DIRECTIVES,
                providers: [RadioControlRegistry],
                exports: [InternalFormsSharedModule, TEMPLATE_DRIVEN_DIRECTIVES]
            },] }
];
/**
 * Exports the required infrastructure and directives for reactive forms,
 * making them available for import by NgModules that import this module.
 * @see [Forms](guide/reactive-forms)
 *
 * @see [Reactive Forms Guide](/guide/reactive-forms)
 *
 * \@publicApi
 */
export class ReactiveFormsModule {
    /**
     * \@description
     * Provides options for configuring the reactive forms module.
     *
     * @param {?} opts An object of configuration options
     * * `warnOnNgModelWithFormControl` Configures when to emit a warning when an `ngModel`
     * binding is used with reactive form directives.
     * @return {?}
     */
    static withConfig(opts) {
        return {
            ngModule: ReactiveFormsModule,
            providers: [{
                    provide: NG_MODEL_WITH_FORM_CONTROL_WARNING,
                    useValue: opts.warnOnNgModelWithFormControl
                }]
        };
    }
}
ReactiveFormsModule.decorators = [
    { type: NgModule, args: [{
                declarations: [REACTIVE_DRIVEN_DIRECTIVES],
                providers: [FormBuilder, RadioControlRegistry],
                exports: [InternalFormsSharedModule, REACTIVE_DRIVEN_DIRECTIVES]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZm9ybV9wcm92aWRlcnMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9mb3Jtcy9zcmMvZm9ybV9wcm92aWRlcnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQXNCLFFBQVEsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUU1RCxPQUFPLEVBQUMseUJBQXlCLEVBQUUsd0JBQXdCLEVBQUUsa0NBQWtDLEVBQUUsMEJBQTBCLEVBQUUsMEJBQTBCLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFDN0ssT0FBTyxFQUFDLG9CQUFvQixFQUFDLE1BQU0sMkNBQTJDLENBQUM7QUFDL0UsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLGdCQUFnQixDQUFDOzs7Ozs7Ozs7QUFlM0MsTUFBTSxPQUFPLFdBQVc7Ozs7Ozs7Ozs7SUFTdEIsTUFBTSxDQUFDLFVBQVUsQ0FBQyxJQUVqQjtRQUNDLE9BQU87WUFDTCxRQUFRLEVBQUUsV0FBVztZQUNyQixTQUFTLEVBQ0wsQ0FBQyxFQUFDLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxRQUFRLEVBQUUsSUFBSSxDQUFDLDhCQUE4QixFQUFDLENBQUM7U0FDekYsQ0FBQztJQUNKLENBQUM7OztZQXRCRixRQUFRLFNBQUM7Z0JBQ1IsWUFBWSxFQUFFLDBCQUEwQjtnQkFDeEMsU0FBUyxFQUFFLENBQUMsb0JBQW9CLENBQUM7Z0JBQ2pDLE9BQU8sRUFBRSxDQUFDLHlCQUF5QixFQUFFLDBCQUEwQixDQUFDO2FBQ2pFOzs7Ozs7Ozs7OztBQW1DRCxNQUFNLE9BQU8sbUJBQW1COzs7Ozs7Ozs7O0lBUzlCLE1BQU0sQ0FBQyxVQUFVLENBQUMsSUFFakI7UUFDQyxPQUFPO1lBQ0wsUUFBUSxFQUFFLG1CQUFtQjtZQUM3QixTQUFTLEVBQUUsQ0FBQztvQkFDVixPQUFPLEVBQUUsa0NBQWtDO29CQUMzQyxRQUFRLEVBQUUsSUFBSSxDQUFDLDRCQUE0QjtpQkFDNUMsQ0FBQztTQUNILENBQUM7SUFDSixDQUFDOzs7WUF4QkYsUUFBUSxTQUFDO2dCQUNSLFlBQVksRUFBRSxDQUFDLDBCQUEwQixDQUFDO2dCQUMxQyxTQUFTLEVBQUUsQ0FBQyxXQUFXLEVBQUUsb0JBQW9CLENBQUM7Z0JBQzlDLE9BQU8sRUFBRSxDQUFDLHlCQUF5QixFQUFFLDBCQUEwQixDQUFDO2FBQ2pFIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge01vZHVsZVdpdGhQcm92aWRlcnMsIE5nTW9kdWxlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtJbnRlcm5hbEZvcm1zU2hhcmVkTW9kdWxlLCBOR19GT1JNX1NFTEVDVE9SX1dBUk5JTkcsIE5HX01PREVMX1dJVEhfRk9STV9DT05UUk9MX1dBUk5JTkcsIFJFQUNUSVZFX0RSSVZFTl9ESVJFQ1RJVkVTLCBURU1QTEFURV9EUklWRU5fRElSRUNUSVZFU30gZnJvbSAnLi9kaXJlY3RpdmVzJztcbmltcG9ydCB7UmFkaW9Db250cm9sUmVnaXN0cnl9IGZyb20gJy4vZGlyZWN0aXZlcy9yYWRpb19jb250cm9sX3ZhbHVlX2FjY2Vzc29yJztcbmltcG9ydCB7Rm9ybUJ1aWxkZXJ9IGZyb20gJy4vZm9ybV9idWlsZGVyJztcblxuLyoqXG4gKiBFeHBvcnRzIHRoZSByZXF1aXJlZCBwcm92aWRlcnMgYW5kIGRpcmVjdGl2ZXMgZm9yIHRlbXBsYXRlLWRyaXZlbiBmb3JtcyxcbiAqIG1ha2luZyB0aGVtIGF2YWlsYWJsZSBmb3IgaW1wb3J0IGJ5IE5nTW9kdWxlcyB0aGF0IGltcG9ydCB0aGlzIG1vZHVsZS5cbiAqXG4gKiBAc2VlIFtGb3JtcyBHdWlkZV0oL2d1aWRlL2Zvcm1zKVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQE5nTW9kdWxlKHtcbiAgZGVjbGFyYXRpb25zOiBURU1QTEFURV9EUklWRU5fRElSRUNUSVZFUyxcbiAgcHJvdmlkZXJzOiBbUmFkaW9Db250cm9sUmVnaXN0cnldLFxuICBleHBvcnRzOiBbSW50ZXJuYWxGb3Jtc1NoYXJlZE1vZHVsZSwgVEVNUExBVEVfRFJJVkVOX0RJUkVDVElWRVNdXG59KVxuZXhwb3J0IGNsYXNzIEZvcm1zTW9kdWxlIHtcbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBQcm92aWRlcyBvcHRpb25zIGZvciBjb25maWd1cmluZyB0aGUgdGVtcGxhdGUtZHJpdmVuIGZvcm1zIG1vZHVsZS5cbiAgICpcbiAgICogQHBhcmFtIG9wdHMgQW4gb2JqZWN0IG9mIGNvbmZpZ3VyYXRpb24gb3B0aW9uc1xuICAgKiAqIGB3YXJuT25EZXByZWNhdGVkTmdGb3JtU2VsZWN0b3JgIENvbmZpZ3VyZXMgd2hlbiB0byBlbWl0IGEgd2FybmluZyB3aGVuIHRoZSBkZXByZWNhdGVkXG4gICAqIGBuZ0Zvcm1gIHNlbGVjdG9yIGlzIHVzZWQuXG4gICAqL1xuICBzdGF0aWMgd2l0aENvbmZpZyhvcHRzOiB7XG4gICAgLyoqIEBkZXByZWNhdGVkIGFzIG9mIHY2ICovIHdhcm5PbkRlcHJlY2F0ZWROZ0Zvcm1TZWxlY3Rvcj86ICduZXZlcicgfCAnb25jZScgfCAnYWx3YXlzJyxcbiAgfSk6IE1vZHVsZVdpdGhQcm92aWRlcnM8Rm9ybXNNb2R1bGU+IHtcbiAgICByZXR1cm4ge1xuICAgICAgbmdNb2R1bGU6IEZvcm1zTW9kdWxlLFxuICAgICAgcHJvdmlkZXJzOlxuICAgICAgICAgIFt7cHJvdmlkZTogTkdfRk9STV9TRUxFQ1RPUl9XQVJOSU5HLCB1c2VWYWx1ZTogb3B0cy53YXJuT25EZXByZWNhdGVkTmdGb3JtU2VsZWN0b3J9XVxuICAgIH07XG4gIH1cbn1cblxuLyoqXG4gKiBFeHBvcnRzIHRoZSByZXF1aXJlZCBpbmZyYXN0cnVjdHVyZSBhbmQgZGlyZWN0aXZlcyBmb3IgcmVhY3RpdmUgZm9ybXMsXG4gKiBtYWtpbmcgdGhlbSBhdmFpbGFibGUgZm9yIGltcG9ydCBieSBOZ01vZHVsZXMgdGhhdCBpbXBvcnQgdGhpcyBtb2R1bGUuXG4gKiBAc2VlIFtGb3Jtc10oZ3VpZGUvcmVhY3RpdmUtZm9ybXMpXG4gKlxuICogQHNlZSBbUmVhY3RpdmUgRm9ybXMgR3VpZGVdKC9ndWlkZS9yZWFjdGl2ZS1mb3JtcylcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBOZ01vZHVsZSh7XG4gIGRlY2xhcmF0aW9uczogW1JFQUNUSVZFX0RSSVZFTl9ESVJFQ1RJVkVTXSxcbiAgcHJvdmlkZXJzOiBbRm9ybUJ1aWxkZXIsIFJhZGlvQ29udHJvbFJlZ2lzdHJ5XSxcbiAgZXhwb3J0czogW0ludGVybmFsRm9ybXNTaGFyZWRNb2R1bGUsIFJFQUNUSVZFX0RSSVZFTl9ESVJFQ1RJVkVTXVxufSlcbmV4cG9ydCBjbGFzcyBSZWFjdGl2ZUZvcm1zTW9kdWxlIHtcbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBQcm92aWRlcyBvcHRpb25zIGZvciBjb25maWd1cmluZyB0aGUgcmVhY3RpdmUgZm9ybXMgbW9kdWxlLlxuICAgKlxuICAgKiBAcGFyYW0gb3B0cyBBbiBvYmplY3Qgb2YgY29uZmlndXJhdGlvbiBvcHRpb25zXG4gICAqICogYHdhcm5Pbk5nTW9kZWxXaXRoRm9ybUNvbnRyb2xgIENvbmZpZ3VyZXMgd2hlbiB0byBlbWl0IGEgd2FybmluZyB3aGVuIGFuIGBuZ01vZGVsYFxuICAgKiBiaW5kaW5nIGlzIHVzZWQgd2l0aCByZWFjdGl2ZSBmb3JtIGRpcmVjdGl2ZXMuXG4gICAqL1xuICBzdGF0aWMgd2l0aENvbmZpZyhvcHRzOiB7XG4gICAgLyoqIEBkZXByZWNhdGVkIGFzIG9mIHY2ICovIHdhcm5Pbk5nTW9kZWxXaXRoRm9ybUNvbnRyb2w6ICduZXZlcicgfCAnb25jZScgfCAnYWx3YXlzJ1xuICB9KTogTW9kdWxlV2l0aFByb3ZpZGVyczxSZWFjdGl2ZUZvcm1zTW9kdWxlPiB7XG4gICAgcmV0dXJuIHtcbiAgICAgIG5nTW9kdWxlOiBSZWFjdGl2ZUZvcm1zTW9kdWxlLFxuICAgICAgcHJvdmlkZXJzOiBbe1xuICAgICAgICBwcm92aWRlOiBOR19NT0RFTF9XSVRIX0ZPUk1fQ09OVFJPTF9XQVJOSU5HLFxuICAgICAgICB1c2VWYWx1ZTogb3B0cy53YXJuT25OZ01vZGVsV2l0aEZvcm1Db250cm9sXG4gICAgICB9XVxuICAgIH07XG4gIH1cbn1cbiJdfQ==