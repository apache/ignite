/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Injectable, KeyValueDiffers, Pipe } from '@angular/core';
function makeKeyValuePair(key, value) {
    return { key: key, value: value };
}
/**
 * @ngModule CommonModule
 * @description
 *
 * Transforms Object or Map into an array of key value pairs.
 *
 * The output array will be ordered by keys.
 * By default the comparator will be by Unicode point value.
 * You can optionally pass a compareFn if your keys are complex types.
 *
 * @usageNotes
 * ### Examples
 *
 * This examples show how an Object or a Map can be iterated by ngFor with the use of this keyvalue
 * pipe.
 *
 * {@example common/pipes/ts/keyvalue_pipe.ts region='KeyValuePipe'}
 *
 * @publicApi
 */
var KeyValuePipe = /** @class */ (function () {
    function KeyValuePipe(differs) {
        this.differs = differs;
        this.keyValues = [];
    }
    KeyValuePipe.prototype.transform = function (input, compareFn) {
        var _this = this;
        if (compareFn === void 0) { compareFn = defaultComparator; }
        if (!input || (!(input instanceof Map) && typeof input !== 'object')) {
            return null;
        }
        if (!this.differ) {
            // make a differ for whatever type we've been passed in
            this.differ = this.differs.find(input).create();
        }
        var differChanges = this.differ.diff(input);
        if (differChanges) {
            this.keyValues = [];
            differChanges.forEachItem(function (r) {
                _this.keyValues.push(makeKeyValuePair(r.key, r.currentValue));
            });
            this.keyValues.sort(compareFn);
        }
        return this.keyValues;
    };
    KeyValuePipe = tslib_1.__decorate([
        Injectable(),
        Pipe({ name: 'keyvalue', pure: false }),
        tslib_1.__metadata("design:paramtypes", [KeyValueDiffers])
    ], KeyValuePipe);
    return KeyValuePipe;
}());
export { KeyValuePipe };
export function defaultComparator(keyValueA, keyValueB) {
    var a = keyValueA.key;
    var b = keyValueB.key;
    // if same exit with 0;
    if (a === b)
        return 0;
    // make sure that undefined are at the end of the sort.
    if (a === undefined)
        return 1;
    if (b === undefined)
        return -1;
    // make sure that nulls are at the end of the sort.
    if (a === null)
        return 1;
    if (b === null)
        return -1;
    if (typeof a == 'string' && typeof b == 'string') {
        return a < b ? -1 : 1;
    }
    if (typeof a == 'number' && typeof b == 'number') {
        return a - b;
    }
    if (typeof a == 'boolean' && typeof b == 'boolean') {
        return a < b ? -1 : 1;
    }
    // `a` and `b` are of different types. Compare their string values.
    var aString = String(a);
    var bString = String(b);
    return aString == bString ? 0 : aString < bString ? -1 : 1;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoia2V5dmFsdWVfcGlwZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvcGlwZXMva2V5dmFsdWVfcGlwZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLFVBQVUsRUFBeUQsZUFBZSxFQUFFLElBQUksRUFBZ0IsTUFBTSxlQUFlLENBQUM7QUFFdEksU0FBUyxnQkFBZ0IsQ0FBTyxHQUFNLEVBQUUsS0FBUTtJQUM5QyxPQUFPLEVBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFDLENBQUM7QUFDbEMsQ0FBQztBQWFEOzs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBbUJHO0FBR0g7SUFDRSxzQkFBNkIsT0FBd0I7UUFBeEIsWUFBTyxHQUFQLE9BQU8sQ0FBaUI7UUFHN0MsY0FBUyxHQUE4QixFQUFFLENBQUM7SUFITSxDQUFDO0lBZ0J6RCxnQ0FBUyxHQUFULFVBQ0ksS0FBMEQsRUFDMUQsU0FBK0U7UUFGbkYsaUJBdUJDO1FBckJHLDBCQUFBLEVBQUEsNkJBQStFO1FBRWpGLElBQUksQ0FBQyxLQUFLLElBQUksQ0FBQyxDQUFDLENBQUMsS0FBSyxZQUFZLEdBQUcsQ0FBQyxJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsQ0FBQyxFQUFFO1lBQ3BFLE9BQU8sSUFBSSxDQUFDO1NBQ2I7UUFFRCxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sRUFBRTtZQUNoQix1REFBdUQ7WUFDdkQsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxNQUFNLEVBQUUsQ0FBQztTQUNqRDtRQUVELElBQU0sYUFBYSxHQUErQixJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxLQUFZLENBQUMsQ0FBQztRQUVqRixJQUFJLGFBQWEsRUFBRTtZQUNqQixJQUFJLENBQUMsU0FBUyxHQUFHLEVBQUUsQ0FBQztZQUNwQixhQUFhLENBQUMsV0FBVyxDQUFDLFVBQUMsQ0FBNkI7Z0JBQ3RELEtBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDLFlBQWMsQ0FBQyxDQUFDLENBQUM7WUFDakUsQ0FBQyxDQUFDLENBQUM7WUFDSCxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUNoQztRQUNELE9BQU8sSUFBSSxDQUFDLFNBQVMsQ0FBQztJQUN4QixDQUFDO0lBeENVLFlBQVk7UUFGeEIsVUFBVSxFQUFFO1FBQ1osSUFBSSxDQUFDLEVBQUMsSUFBSSxFQUFFLFVBQVUsRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFDLENBQUM7aURBRUUsZUFBZTtPQUQxQyxZQUFZLENBeUN4QjtJQUFELG1CQUFDO0NBQUEsQUF6Q0QsSUF5Q0M7U0F6Q1ksWUFBWTtBQTJDekIsTUFBTSxVQUFVLGlCQUFpQixDQUM3QixTQUF5QixFQUFFLFNBQXlCO0lBQ3RELElBQU0sQ0FBQyxHQUFHLFNBQVMsQ0FBQyxHQUFHLENBQUM7SUFDeEIsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDLEdBQUcsQ0FBQztJQUN4Qix1QkFBdUI7SUFDdkIsSUFBSSxDQUFDLEtBQUssQ0FBQztRQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQ3RCLHVEQUF1RDtJQUN2RCxJQUFJLENBQUMsS0FBSyxTQUFTO1FBQUUsT0FBTyxDQUFDLENBQUM7SUFDOUIsSUFBSSxDQUFDLEtBQUssU0FBUztRQUFFLE9BQU8sQ0FBQyxDQUFDLENBQUM7SUFDL0IsbURBQW1EO0lBQ25ELElBQUksQ0FBQyxLQUFLLElBQUk7UUFBRSxPQUFPLENBQUMsQ0FBQztJQUN6QixJQUFJLENBQUMsS0FBSyxJQUFJO1FBQUUsT0FBTyxDQUFDLENBQUMsQ0FBQztJQUMxQixJQUFJLE9BQU8sQ0FBQyxJQUFJLFFBQVEsSUFBSSxPQUFPLENBQUMsSUFBSSxRQUFRLEVBQUU7UUFDaEQsT0FBTyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO0tBQ3ZCO0lBQ0QsSUFBSSxPQUFPLENBQUMsSUFBSSxRQUFRLElBQUksT0FBTyxDQUFDLElBQUksUUFBUSxFQUFFO1FBQ2hELE9BQU8sQ0FBQyxHQUFHLENBQUMsQ0FBQztLQUNkO0lBQ0QsSUFBSSxPQUFPLENBQUMsSUFBSSxTQUFTLElBQUksT0FBTyxDQUFDLElBQUksU0FBUyxFQUFFO1FBQ2xELE9BQU8sQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztLQUN2QjtJQUNELG1FQUFtRTtJQUNuRSxJQUFNLE9BQU8sR0FBRyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDMUIsSUFBTSxPQUFPLEdBQUcsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQzFCLE9BQU8sT0FBTyxJQUFJLE9BQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxPQUFPLEdBQUcsT0FBTyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO0FBQzdELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0YWJsZSwgS2V5VmFsdWVDaGFuZ2VSZWNvcmQsIEtleVZhbHVlQ2hhbmdlcywgS2V5VmFsdWVEaWZmZXIsIEtleVZhbHVlRGlmZmVycywgUGlwZSwgUGlwZVRyYW5zZm9ybX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmZ1bmN0aW9uIG1ha2VLZXlWYWx1ZVBhaXI8SywgVj4oa2V5OiBLLCB2YWx1ZTogVik6IEtleVZhbHVlPEssIFY+IHtcbiAgcmV0dXJuIHtrZXk6IGtleSwgdmFsdWU6IHZhbHVlfTtcbn1cblxuLyoqXG4gKiBBIGtleSB2YWx1ZSBwYWlyLlxuICogVXN1YWxseSB1c2VkIHRvIHJlcHJlc2VudCB0aGUga2V5IHZhbHVlIHBhaXJzIGZyb20gYSBNYXAgb3IgT2JqZWN0LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBLZXlWYWx1ZTxLLCBWPiB7XG4gIGtleTogSztcbiAgdmFsdWU6IFY7XG59XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogVHJhbnNmb3JtcyBPYmplY3Qgb3IgTWFwIGludG8gYW4gYXJyYXkgb2Yga2V5IHZhbHVlIHBhaXJzLlxuICpcbiAqIFRoZSBvdXRwdXQgYXJyYXkgd2lsbCBiZSBvcmRlcmVkIGJ5IGtleXMuXG4gKiBCeSBkZWZhdWx0IHRoZSBjb21wYXJhdG9yIHdpbGwgYmUgYnkgVW5pY29kZSBwb2ludCB2YWx1ZS5cbiAqIFlvdSBjYW4gb3B0aW9uYWxseSBwYXNzIGEgY29tcGFyZUZuIGlmIHlvdXIga2V5cyBhcmUgY29tcGxleCB0eXBlcy5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVzXG4gKlxuICogVGhpcyBleGFtcGxlcyBzaG93IGhvdyBhbiBPYmplY3Qgb3IgYSBNYXAgY2FuIGJlIGl0ZXJhdGVkIGJ5IG5nRm9yIHdpdGggdGhlIHVzZSBvZiB0aGlzIGtleXZhbHVlXG4gKiBwaXBlLlxuICpcbiAqIHtAZXhhbXBsZSBjb21tb24vcGlwZXMvdHMva2V5dmFsdWVfcGlwZS50cyByZWdpb249J0tleVZhbHVlUGlwZSd9XG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5ASW5qZWN0YWJsZSgpXG5AUGlwZSh7bmFtZTogJ2tleXZhbHVlJywgcHVyZTogZmFsc2V9KVxuZXhwb3J0IGNsYXNzIEtleVZhbHVlUGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHJlYWRvbmx5IGRpZmZlcnM6IEtleVZhbHVlRGlmZmVycykge31cblxuICBwcml2YXRlIGRpZmZlciAhOiBLZXlWYWx1ZURpZmZlcjxhbnksIGFueT47XG4gIHByaXZhdGUga2V5VmFsdWVzOiBBcnJheTxLZXlWYWx1ZTxhbnksIGFueT4+ID0gW107XG5cbiAgdHJhbnNmb3JtPEssIFY+KGlucHV0OiBudWxsLCBjb21wYXJlRm4/OiAoYTogS2V5VmFsdWU8SywgVj4sIGI6IEtleVZhbHVlPEssIFY+KSA9PiBudW1iZXIpOiBudWxsO1xuICB0cmFuc2Zvcm08Vj4oXG4gICAgICBpbnB1dDoge1trZXk6IHN0cmluZ106IFZ9fE1hcDxzdHJpbmcsIFY+LFxuICAgICAgY29tcGFyZUZuPzogKGE6IEtleVZhbHVlPHN0cmluZywgVj4sIGI6IEtleVZhbHVlPHN0cmluZywgVj4pID0+IG51bWJlcik6XG4gICAgICBBcnJheTxLZXlWYWx1ZTxzdHJpbmcsIFY+PjtcbiAgdHJhbnNmb3JtPFY+KFxuICAgICAgaW5wdXQ6IHtba2V5OiBudW1iZXJdOiBWfXxNYXA8bnVtYmVyLCBWPixcbiAgICAgIGNvbXBhcmVGbj86IChhOiBLZXlWYWx1ZTxudW1iZXIsIFY+LCBiOiBLZXlWYWx1ZTxudW1iZXIsIFY+KSA9PiBudW1iZXIpOlxuICAgICAgQXJyYXk8S2V5VmFsdWU8bnVtYmVyLCBWPj47XG4gIHRyYW5zZm9ybTxLLCBWPihpbnB1dDogTWFwPEssIFY+LCBjb21wYXJlRm4/OiAoYTogS2V5VmFsdWU8SywgVj4sIGI6IEtleVZhbHVlPEssIFY+KSA9PiBudW1iZXIpOlxuICAgICAgQXJyYXk8S2V5VmFsdWU8SywgVj4+O1xuICB0cmFuc2Zvcm08SywgVj4oXG4gICAgICBpbnB1dDogbnVsbHx7W2tleTogc3RyaW5nXTogViwgW2tleTogbnVtYmVyXTogVn18TWFwPEssIFY+LFxuICAgICAgY29tcGFyZUZuOiAoYTogS2V5VmFsdWU8SywgVj4sIGI6IEtleVZhbHVlPEssIFY+KSA9PiBudW1iZXIgPSBkZWZhdWx0Q29tcGFyYXRvcik6XG4gICAgICBBcnJheTxLZXlWYWx1ZTxLLCBWPj58bnVsbCB7XG4gICAgaWYgKCFpbnB1dCB8fCAoIShpbnB1dCBpbnN0YW5jZW9mIE1hcCkgJiYgdHlwZW9mIGlucHV0ICE9PSAnb2JqZWN0JykpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGlmICghdGhpcy5kaWZmZXIpIHtcbiAgICAgIC8vIG1ha2UgYSBkaWZmZXIgZm9yIHdoYXRldmVyIHR5cGUgd2UndmUgYmVlbiBwYXNzZWQgaW5cbiAgICAgIHRoaXMuZGlmZmVyID0gdGhpcy5kaWZmZXJzLmZpbmQoaW5wdXQpLmNyZWF0ZSgpO1xuICAgIH1cblxuICAgIGNvbnN0IGRpZmZlckNoYW5nZXM6IEtleVZhbHVlQ2hhbmdlczxLLCBWPnxudWxsID0gdGhpcy5kaWZmZXIuZGlmZihpbnB1dCBhcyBhbnkpO1xuXG4gICAgaWYgKGRpZmZlckNoYW5nZXMpIHtcbiAgICAgIHRoaXMua2V5VmFsdWVzID0gW107XG4gICAgICBkaWZmZXJDaGFuZ2VzLmZvckVhY2hJdGVtKChyOiBLZXlWYWx1ZUNoYW5nZVJlY29yZDxLLCBWPikgPT4ge1xuICAgICAgICB0aGlzLmtleVZhbHVlcy5wdXNoKG1ha2VLZXlWYWx1ZVBhaXIoci5rZXksIHIuY3VycmVudFZhbHVlICEpKTtcbiAgICAgIH0pO1xuICAgICAgdGhpcy5rZXlWYWx1ZXMuc29ydChjb21wYXJlRm4pO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5rZXlWYWx1ZXM7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlZmF1bHRDb21wYXJhdG9yPEssIFY+KFxuICAgIGtleVZhbHVlQTogS2V5VmFsdWU8SywgVj4sIGtleVZhbHVlQjogS2V5VmFsdWU8SywgVj4pOiBudW1iZXIge1xuICBjb25zdCBhID0ga2V5VmFsdWVBLmtleTtcbiAgY29uc3QgYiA9IGtleVZhbHVlQi5rZXk7XG4gIC8vIGlmIHNhbWUgZXhpdCB3aXRoIDA7XG4gIGlmIChhID09PSBiKSByZXR1cm4gMDtcbiAgLy8gbWFrZSBzdXJlIHRoYXQgdW5kZWZpbmVkIGFyZSBhdCB0aGUgZW5kIG9mIHRoZSBzb3J0LlxuICBpZiAoYSA9PT0gdW5kZWZpbmVkKSByZXR1cm4gMTtcbiAgaWYgKGIgPT09IHVuZGVmaW5lZCkgcmV0dXJuIC0xO1xuICAvLyBtYWtlIHN1cmUgdGhhdCBudWxscyBhcmUgYXQgdGhlIGVuZCBvZiB0aGUgc29ydC5cbiAgaWYgKGEgPT09IG51bGwpIHJldHVybiAxO1xuICBpZiAoYiA9PT0gbnVsbCkgcmV0dXJuIC0xO1xuICBpZiAodHlwZW9mIGEgPT0gJ3N0cmluZycgJiYgdHlwZW9mIGIgPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gYSA8IGIgPyAtMSA6IDE7XG4gIH1cbiAgaWYgKHR5cGVvZiBhID09ICdudW1iZXInICYmIHR5cGVvZiBiID09ICdudW1iZXInKSB7XG4gICAgcmV0dXJuIGEgLSBiO1xuICB9XG4gIGlmICh0eXBlb2YgYSA9PSAnYm9vbGVhbicgJiYgdHlwZW9mIGIgPT0gJ2Jvb2xlYW4nKSB7XG4gICAgcmV0dXJuIGEgPCBiID8gLTEgOiAxO1xuICB9XG4gIC8vIGBhYCBhbmQgYGJgIGFyZSBvZiBkaWZmZXJlbnQgdHlwZXMuIENvbXBhcmUgdGhlaXIgc3RyaW5nIHZhbHVlcy5cbiAgY29uc3QgYVN0cmluZyA9IFN0cmluZyhhKTtcbiAgY29uc3QgYlN0cmluZyA9IFN0cmluZyhiKTtcbiAgcmV0dXJuIGFTdHJpbmcgPT0gYlN0cmluZyA/IDAgOiBhU3RyaW5nIDwgYlN0cmluZyA/IC0xIDogMTtcbn1cbiJdfQ==