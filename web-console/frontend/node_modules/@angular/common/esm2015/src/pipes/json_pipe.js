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
import { Injectable, Pipe } from '@angular/core';
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Converts a value into its JSON-format representation.  Useful for debugging.
 *
 * \@usageNotes
 *
 * The following component uses a JSON pipe to convert an object
 * to JSON format, and displays the string in both formats for comparison.
 *
 * {\@example common/pipes/ts/json_pipe.ts region='JsonPipe'}
 *
 * \@publicApi
 */
export class JsonPipe {
    /**
     * @param {?} value A value of any type to convert into a JSON-format string.
     * @return {?}
     */
    transform(value) { return JSON.stringify(value, null, 2); }
}
JsonPipe.decorators = [
    { type: Injectable },
    { type: Pipe, args: [{ name: 'json', pure: false },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoianNvbl9waXBlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9waXBlcy9qc29uX3BpcGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsVUFBVSxFQUFFLElBQUksRUFBZ0IsTUFBTSxlQUFlLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7QUFtQjlELE1BQU0sT0FBTyxRQUFROzs7OztJQUluQixTQUFTLENBQUMsS0FBVSxJQUFZLE9BQU8sSUFBSSxDQUFDLFNBQVMsQ0FBQyxLQUFLLEVBQUUsSUFBSSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQzs7O1lBTnpFLFVBQVU7WUFDVixJQUFJLFNBQUMsRUFBQyxJQUFJLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxLQUFLLEVBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0YWJsZSwgUGlwZSwgUGlwZVRyYW5zZm9ybX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogQ29udmVydHMgYSB2YWx1ZSBpbnRvIGl0cyBKU09OLWZvcm1hdCByZXByZXNlbnRhdGlvbi4gIFVzZWZ1bCBmb3IgZGVidWdnaW5nLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogVGhlIGZvbGxvd2luZyBjb21wb25lbnQgdXNlcyBhIEpTT04gcGlwZSB0byBjb252ZXJ0IGFuIG9iamVjdFxuICogdG8gSlNPTiBmb3JtYXQsIGFuZCBkaXNwbGF5cyB0aGUgc3RyaW5nIGluIGJvdGggZm9ybWF0cyBmb3IgY29tcGFyaXNvbi5cbiAqXG4gKiB7QGV4YW1wbGUgY29tbW9uL3BpcGVzL3RzL2pzb25fcGlwZS50cyByZWdpb249J0pzb25QaXBlJ31cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbkBQaXBlKHtuYW1lOiAnanNvbicsIHB1cmU6IGZhbHNlfSlcbmV4cG9ydCBjbGFzcyBKc29uUGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICAvKipcbiAgICogQHBhcmFtIHZhbHVlIEEgdmFsdWUgb2YgYW55IHR5cGUgdG8gY29udmVydCBpbnRvIGEgSlNPTi1mb3JtYXQgc3RyaW5nLlxuICAgKi9cbiAgdHJhbnNmb3JtKHZhbHVlOiBhbnkpOiBzdHJpbmcgeyByZXR1cm4gSlNPTi5zdHJpbmdpZnkodmFsdWUsIG51bGwsIDIpOyB9XG59XG4iXX0=