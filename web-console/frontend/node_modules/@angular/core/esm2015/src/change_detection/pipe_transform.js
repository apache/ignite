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
/**
 * An interface that is implemented by pipes in order to perform a transformation.
 * Angular invokes the `transform` method with the value of a binding
 * as the first argument, and any parameters as the second argument in list form.
 *
 * \@usageNotes
 *
 * In the following example, `RepeatPipe` repeats a given value a given number of times.
 *
 * ```ts
 * import {Pipe, PipeTransform} from '\@angular/core';
 *
 * \@Pipe({name: 'repeat'})
 * export class RepeatPipe implements PipeTransform {
 *   transform(value: any, times: number) {
 *     return value.repeat(times);
 *   }
 * }
 * ```
 *
 * Invoking `{{ 'ok' | repeat:3 }}` in a template produces `okokok`.
 *
 * \@publicApi
 * @record
 */
export function PipeTransform() { }
if (false) {
    /**
     * @param {?} value
     * @param {...?} args
     * @return {?}
     */
    PipeTransform.prototype.transform = function (value, args) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZV90cmFuc2Zvcm0uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9jaGFuZ2VfZGV0ZWN0aW9uL3BpcGVfdHJhbnNmb3JtLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQWdDQSxtQ0FBOEU7Ozs7Ozs7SUFBN0MsK0RBQTJDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIEFuIGludGVyZmFjZSB0aGF0IGlzIGltcGxlbWVudGVkIGJ5IHBpcGVzIGluIG9yZGVyIHRvIHBlcmZvcm0gYSB0cmFuc2Zvcm1hdGlvbi5cbiAqIEFuZ3VsYXIgaW52b2tlcyB0aGUgYHRyYW5zZm9ybWAgbWV0aG9kIHdpdGggdGhlIHZhbHVlIG9mIGEgYmluZGluZ1xuICogYXMgdGhlIGZpcnN0IGFyZ3VtZW50LCBhbmQgYW55IHBhcmFtZXRlcnMgYXMgdGhlIHNlY29uZCBhcmd1bWVudCBpbiBsaXN0IGZvcm0uXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqXG4gKiBJbiB0aGUgZm9sbG93aW5nIGV4YW1wbGUsIGBSZXBlYXRQaXBlYCByZXBlYXRzIGEgZ2l2ZW4gdmFsdWUgYSBnaXZlbiBudW1iZXIgb2YgdGltZXMuXG4gKlxuICogYGBgdHNcbiAqIGltcG9ydCB7UGlwZSwgUGlwZVRyYW5zZm9ybX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG4gKlxuICogQFBpcGUoe25hbWU6ICdyZXBlYXQnfSlcbiAqIGV4cG9ydCBjbGFzcyBSZXBlYXRQaXBlIGltcGxlbWVudHMgUGlwZVRyYW5zZm9ybSB7XG4gKiAgIHRyYW5zZm9ybSh2YWx1ZTogYW55LCB0aW1lczogbnVtYmVyKSB7XG4gKiAgICAgcmV0dXJuIHZhbHVlLnJlcGVhdCh0aW1lcyk7XG4gKiAgIH1cbiAqIH1cbiAqIGBgYFxuICpcbiAqIEludm9raW5nIGB7eyAnb2snIHwgcmVwZWF0OjMgfX1gIGluIGEgdGVtcGxhdGUgcHJvZHVjZXMgYG9rb2tva2AuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgaW50ZXJmYWNlIFBpcGVUcmFuc2Zvcm0geyB0cmFuc2Zvcm0odmFsdWU6IGFueSwgLi4uYXJnczogYW55W10pOiBhbnk7IH1cbiJdfQ==