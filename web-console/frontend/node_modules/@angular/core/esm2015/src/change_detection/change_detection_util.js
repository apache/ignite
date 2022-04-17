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
import { looseIdentical } from '../util/comparison';
import { getSymbolIterator } from '../util/symbol';
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
export function devModeEqual(a, b) {
    /** @type {?} */
    const isListLikeIterableA = isListLikeIterable(a);
    /** @type {?} */
    const isListLikeIterableB = isListLikeIterable(b);
    if (isListLikeIterableA && isListLikeIterableB) {
        return areIterablesEqual(a, b, devModeEqual);
    }
    else {
        /** @type {?} */
        const isAObject = a && (typeof a === 'object' || typeof a === 'function');
        /** @type {?} */
        const isBObject = b && (typeof b === 'object' || typeof b === 'function');
        if (!isListLikeIterableA && isAObject && !isListLikeIterableB && isBObject) {
            return true;
        }
        else {
            return looseIdentical(a, b);
        }
    }
}
/**
 * Indicates that the result of a {\@link Pipe} transformation has changed even though the
 * reference has not changed.
 *
 * Wrapped values are unwrapped automatically during the change detection, and the unwrapped value
 * is stored.
 *
 * Example:
 *
 * ```
 * if (this._latestValue === this._latestReturnedValue) {
 *    return this._latestReturnedValue;
 *  } else {
 *    this._latestReturnedValue = this._latestValue;
 *    return WrappedValue.wrap(this._latestValue); // this will force update
 *  }
 * ```
 *
 * \@publicApi
 */
export class WrappedValue {
    /**
     * @param {?} value
     */
    constructor(value) { this.wrapped = value; }
    /**
     * Creates a wrapped value.
     * @param {?} value
     * @return {?}
     */
    static wrap(value) { return new WrappedValue(value); }
    /**
     * Returns the underlying value of a wrapped value.
     * Returns the given `value` when it is not wrapped.
     *
     * @param {?} value
     * @return {?}
     */
    static unwrap(value) { return WrappedValue.isWrapped(value) ? value.wrapped : value; }
    /**
     * Returns true if `value` is a wrapped value.
     * @param {?} value
     * @return {?}
     */
    static isWrapped(value) { return value instanceof WrappedValue; }
}
if (false) {
    /**
     * @deprecated from 5.3, use `unwrap()` instead - will switch to protected
     * @type {?}
     */
    WrappedValue.prototype.wrapped;
}
/**
 * @param {?} obj
 * @return {?}
 */
export function isListLikeIterable(obj) {
    if (!isJsObject(obj))
        return false;
    return Array.isArray(obj) ||
        (!(obj instanceof Map) && // JS Map are iterables but return entries as [k, v]
            getSymbolIterator() in obj); // JS Iterable have a Symbol.iterator prop
}
/**
 * @param {?} a
 * @param {?} b
 * @param {?} comparator
 * @return {?}
 */
export function areIterablesEqual(a, b, comparator) {
    /** @type {?} */
    const iterator1 = a[getSymbolIterator()]();
    /** @type {?} */
    const iterator2 = b[getSymbolIterator()]();
    while (true) {
        /** @type {?} */
        const item1 = iterator1.next();
        /** @type {?} */
        const item2 = iterator2.next();
        if (item1.done && item2.done)
            return true;
        if (item1.done || item2.done)
            return false;
        if (!comparator(item1.value, item2.value))
            return false;
    }
}
/**
 * @param {?} obj
 * @param {?} fn
 * @return {?}
 */
export function iterateListLike(obj, fn) {
    if (Array.isArray(obj)) {
        for (let i = 0; i < obj.length; i++) {
            fn(obj[i]);
        }
    }
    else {
        /** @type {?} */
        const iterator = obj[getSymbolIterator()]();
        /** @type {?} */
        let item;
        while (!((item = iterator.next()).done)) {
            fn(item.value);
        }
    }
}
/**
 * @param {?} o
 * @return {?}
 */
export function isJsObject(o) {
    return o !== null && (typeof o === 'function' || typeof o === 'object');
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2hhbmdlX2RldGVjdGlvbl91dGlsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvY2hhbmdlX2RldGVjdGlvbi9jaGFuZ2VfZGV0ZWN0aW9uX3V0aWwudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFDbEQsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sZ0JBQWdCLENBQUM7Ozs7OztBQUVqRCxNQUFNLFVBQVUsWUFBWSxDQUFDLENBQU0sRUFBRSxDQUFNOztVQUNuQyxtQkFBbUIsR0FBRyxrQkFBa0IsQ0FBQyxDQUFDLENBQUM7O1VBQzNDLG1CQUFtQixHQUFHLGtCQUFrQixDQUFDLENBQUMsQ0FBQztJQUNqRCxJQUFJLG1CQUFtQixJQUFJLG1CQUFtQixFQUFFO1FBQzlDLE9BQU8saUJBQWlCLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxZQUFZLENBQUMsQ0FBQztLQUM5QztTQUFNOztjQUNDLFNBQVMsR0FBRyxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsS0FBSyxRQUFRLElBQUksT0FBTyxDQUFDLEtBQUssVUFBVSxDQUFDOztjQUNuRSxTQUFTLEdBQUcsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssUUFBUSxJQUFJLE9BQU8sQ0FBQyxLQUFLLFVBQVUsQ0FBQztRQUN6RSxJQUFJLENBQUMsbUJBQW1CLElBQUksU0FBUyxJQUFJLENBQUMsbUJBQW1CLElBQUksU0FBUyxFQUFFO1lBQzFFLE9BQU8sSUFBSSxDQUFDO1NBQ2I7YUFBTTtZQUNMLE9BQU8sY0FBYyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQztTQUM3QjtLQUNGO0FBQ0gsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBc0JELE1BQU0sT0FBTyxZQUFZOzs7O0lBSXZCLFlBQVksS0FBVSxJQUFJLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDLENBQUMsQ0FBQzs7Ozs7O0lBR2pELE1BQU0sQ0FBQyxJQUFJLENBQUMsS0FBVSxJQUFrQixPQUFPLElBQUksWUFBWSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQzs7Ozs7Ozs7SUFNekUsTUFBTSxDQUFDLE1BQU0sQ0FBQyxLQUFVLElBQVMsT0FBTyxZQUFZLENBQUMsU0FBUyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFHaEcsTUFBTSxDQUFDLFNBQVMsQ0FBQyxLQUFVLElBQTJCLE9BQU8sS0FBSyxZQUFZLFlBQVksQ0FBQyxDQUFDLENBQUM7Q0FDOUY7Ozs7OztJQWZDLCtCQUFhOzs7Ozs7QUFpQmYsTUFBTSxVQUFVLGtCQUFrQixDQUFDLEdBQVE7SUFDekMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUM7UUFBRSxPQUFPLEtBQUssQ0FBQztJQUNuQyxPQUFPLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDO1FBQ3JCLENBQUMsQ0FBQyxDQUFDLEdBQUcsWUFBWSxHQUFHLENBQUMsSUFBUyxvREFBb0Q7WUFDbEYsaUJBQWlCLEVBQUUsSUFBSSxHQUFHLENBQUMsQ0FBQyxDQUFFLDBDQUEwQztBQUMvRSxDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLGlCQUFpQixDQUM3QixDQUFNLEVBQUUsQ0FBTSxFQUFFLFVBQXVDOztVQUNuRCxTQUFTLEdBQUcsQ0FBQyxDQUFDLGlCQUFpQixFQUFFLENBQUMsRUFBRTs7VUFDcEMsU0FBUyxHQUFHLENBQUMsQ0FBQyxpQkFBaUIsRUFBRSxDQUFDLEVBQUU7SUFFMUMsT0FBTyxJQUFJLEVBQUU7O2NBQ0wsS0FBSyxHQUFHLFNBQVMsQ0FBQyxJQUFJLEVBQUU7O2NBQ3hCLEtBQUssR0FBRyxTQUFTLENBQUMsSUFBSSxFQUFFO1FBQzlCLElBQUksS0FBSyxDQUFDLElBQUksSUFBSSxLQUFLLENBQUMsSUFBSTtZQUFFLE9BQU8sSUFBSSxDQUFDO1FBQzFDLElBQUksS0FBSyxDQUFDLElBQUksSUFBSSxLQUFLLENBQUMsSUFBSTtZQUFFLE9BQU8sS0FBSyxDQUFDO1FBQzNDLElBQUksQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsS0FBSyxDQUFDO1lBQUUsT0FBTyxLQUFLLENBQUM7S0FDekQ7QUFDSCxDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsZUFBZSxDQUFDLEdBQVEsRUFBRSxFQUFtQjtJQUMzRCxJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDLEVBQUU7UUFDdEIsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLEdBQUcsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7WUFDbkMsRUFBRSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQ1o7S0FDRjtTQUFNOztjQUNDLFFBQVEsR0FBRyxHQUFHLENBQUMsaUJBQWlCLEVBQUUsQ0FBQyxFQUFFOztZQUN2QyxJQUFTO1FBQ2IsT0FBTyxDQUFDLENBQUMsQ0FBQyxJQUFJLEdBQUcsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDdkMsRUFBRSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztTQUNoQjtLQUNGO0FBQ0gsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsVUFBVSxDQUFDLENBQU07SUFDL0IsT0FBTyxDQUFDLEtBQUssSUFBSSxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssVUFBVSxJQUFJLE9BQU8sQ0FBQyxLQUFLLFFBQVEsQ0FBQyxDQUFDO0FBQzFFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7bG9vc2VJZGVudGljYWx9IGZyb20gJy4uL3V0aWwvY29tcGFyaXNvbic7XG5pbXBvcnQge2dldFN5bWJvbEl0ZXJhdG9yfSBmcm9tICcuLi91dGlsL3N5bWJvbCc7XG5cbmV4cG9ydCBmdW5jdGlvbiBkZXZNb2RlRXF1YWwoYTogYW55LCBiOiBhbnkpOiBib29sZWFuIHtcbiAgY29uc3QgaXNMaXN0TGlrZUl0ZXJhYmxlQSA9IGlzTGlzdExpa2VJdGVyYWJsZShhKTtcbiAgY29uc3QgaXNMaXN0TGlrZUl0ZXJhYmxlQiA9IGlzTGlzdExpa2VJdGVyYWJsZShiKTtcbiAgaWYgKGlzTGlzdExpa2VJdGVyYWJsZUEgJiYgaXNMaXN0TGlrZUl0ZXJhYmxlQikge1xuICAgIHJldHVybiBhcmVJdGVyYWJsZXNFcXVhbChhLCBiLCBkZXZNb2RlRXF1YWwpO1xuICB9IGVsc2Uge1xuICAgIGNvbnN0IGlzQU9iamVjdCA9IGEgJiYgKHR5cGVvZiBhID09PSAnb2JqZWN0JyB8fCB0eXBlb2YgYSA9PT0gJ2Z1bmN0aW9uJyk7XG4gICAgY29uc3QgaXNCT2JqZWN0ID0gYiAmJiAodHlwZW9mIGIgPT09ICdvYmplY3QnIHx8IHR5cGVvZiBiID09PSAnZnVuY3Rpb24nKTtcbiAgICBpZiAoIWlzTGlzdExpa2VJdGVyYWJsZUEgJiYgaXNBT2JqZWN0ICYmICFpc0xpc3RMaWtlSXRlcmFibGVCICYmIGlzQk9iamVjdCkge1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiBsb29zZUlkZW50aWNhbChhLCBiKTtcbiAgICB9XG4gIH1cbn1cblxuLyoqXG4gKiBJbmRpY2F0ZXMgdGhhdCB0aGUgcmVzdWx0IG9mIGEge0BsaW5rIFBpcGV9IHRyYW5zZm9ybWF0aW9uIGhhcyBjaGFuZ2VkIGV2ZW4gdGhvdWdoIHRoZVxuICogcmVmZXJlbmNlIGhhcyBub3QgY2hhbmdlZC5cbiAqXG4gKiBXcmFwcGVkIHZhbHVlcyBhcmUgdW53cmFwcGVkIGF1dG9tYXRpY2FsbHkgZHVyaW5nIHRoZSBjaGFuZ2UgZGV0ZWN0aW9uLCBhbmQgdGhlIHVud3JhcHBlZCB2YWx1ZVxuICogaXMgc3RvcmVkLlxuICpcbiAqIEV4YW1wbGU6XG4gKlxuICogYGBgXG4gKiBpZiAodGhpcy5fbGF0ZXN0VmFsdWUgPT09IHRoaXMuX2xhdGVzdFJldHVybmVkVmFsdWUpIHtcbiAqICAgIHJldHVybiB0aGlzLl9sYXRlc3RSZXR1cm5lZFZhbHVlO1xuICogIH0gZWxzZSB7XG4gKiAgICB0aGlzLl9sYXRlc3RSZXR1cm5lZFZhbHVlID0gdGhpcy5fbGF0ZXN0VmFsdWU7XG4gKiAgICByZXR1cm4gV3JhcHBlZFZhbHVlLndyYXAodGhpcy5fbGF0ZXN0VmFsdWUpOyAvLyB0aGlzIHdpbGwgZm9yY2UgdXBkYXRlXG4gKiAgfVxuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgV3JhcHBlZFZhbHVlIHtcbiAgLyoqIEBkZXByZWNhdGVkIGZyb20gNS4zLCB1c2UgYHVud3JhcCgpYCBpbnN0ZWFkIC0gd2lsbCBzd2l0Y2ggdG8gcHJvdGVjdGVkICovXG4gIHdyYXBwZWQ6IGFueTtcblxuICBjb25zdHJ1Y3Rvcih2YWx1ZTogYW55KSB7IHRoaXMud3JhcHBlZCA9IHZhbHVlOyB9XG5cbiAgLyoqIENyZWF0ZXMgYSB3cmFwcGVkIHZhbHVlLiAqL1xuICBzdGF0aWMgd3JhcCh2YWx1ZTogYW55KTogV3JhcHBlZFZhbHVlIHsgcmV0dXJuIG5ldyBXcmFwcGVkVmFsdWUodmFsdWUpOyB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgdGhlIHVuZGVybHlpbmcgdmFsdWUgb2YgYSB3cmFwcGVkIHZhbHVlLlxuICAgKiBSZXR1cm5zIHRoZSBnaXZlbiBgdmFsdWVgIHdoZW4gaXQgaXMgbm90IHdyYXBwZWQuXG4gICAqKi9cbiAgc3RhdGljIHVud3JhcCh2YWx1ZTogYW55KTogYW55IHsgcmV0dXJuIFdyYXBwZWRWYWx1ZS5pc1dyYXBwZWQodmFsdWUpID8gdmFsdWUud3JhcHBlZCA6IHZhbHVlOyB9XG5cbiAgLyoqIFJldHVybnMgdHJ1ZSBpZiBgdmFsdWVgIGlzIGEgd3JhcHBlZCB2YWx1ZS4gKi9cbiAgc3RhdGljIGlzV3JhcHBlZCh2YWx1ZTogYW55KTogdmFsdWUgaXMgV3JhcHBlZFZhbHVlIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgV3JhcHBlZFZhbHVlOyB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0xpc3RMaWtlSXRlcmFibGUob2JqOiBhbnkpOiBib29sZWFuIHtcbiAgaWYgKCFpc0pzT2JqZWN0KG9iaikpIHJldHVybiBmYWxzZTtcbiAgcmV0dXJuIEFycmF5LmlzQXJyYXkob2JqKSB8fFxuICAgICAgKCEob2JqIGluc3RhbmNlb2YgTWFwKSAmJiAgICAgIC8vIEpTIE1hcCBhcmUgaXRlcmFibGVzIGJ1dCByZXR1cm4gZW50cmllcyBhcyBbaywgdl1cbiAgICAgICBnZXRTeW1ib2xJdGVyYXRvcigpIGluIG9iaik7ICAvLyBKUyBJdGVyYWJsZSBoYXZlIGEgU3ltYm9sLml0ZXJhdG9yIHByb3Bcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFyZUl0ZXJhYmxlc0VxdWFsKFxuICAgIGE6IGFueSwgYjogYW55LCBjb21wYXJhdG9yOiAoYTogYW55LCBiOiBhbnkpID0+IGJvb2xlYW4pOiBib29sZWFuIHtcbiAgY29uc3QgaXRlcmF0b3IxID0gYVtnZXRTeW1ib2xJdGVyYXRvcigpXSgpO1xuICBjb25zdCBpdGVyYXRvcjIgPSBiW2dldFN5bWJvbEl0ZXJhdG9yKCldKCk7XG5cbiAgd2hpbGUgKHRydWUpIHtcbiAgICBjb25zdCBpdGVtMSA9IGl0ZXJhdG9yMS5uZXh0KCk7XG4gICAgY29uc3QgaXRlbTIgPSBpdGVyYXRvcjIubmV4dCgpO1xuICAgIGlmIChpdGVtMS5kb25lICYmIGl0ZW0yLmRvbmUpIHJldHVybiB0cnVlO1xuICAgIGlmIChpdGVtMS5kb25lIHx8IGl0ZW0yLmRvbmUpIHJldHVybiBmYWxzZTtcbiAgICBpZiAoIWNvbXBhcmF0b3IoaXRlbTEudmFsdWUsIGl0ZW0yLnZhbHVlKSkgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpdGVyYXRlTGlzdExpa2Uob2JqOiBhbnksIGZuOiAocDogYW55KSA9PiBhbnkpIHtcbiAgaWYgKEFycmF5LmlzQXJyYXkob2JqKSkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgb2JqLmxlbmd0aDsgaSsrKSB7XG4gICAgICBmbihvYmpbaV0pO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBjb25zdCBpdGVyYXRvciA9IG9ialtnZXRTeW1ib2xJdGVyYXRvcigpXSgpO1xuICAgIGxldCBpdGVtOiBhbnk7XG4gICAgd2hpbGUgKCEoKGl0ZW0gPSBpdGVyYXRvci5uZXh0KCkpLmRvbmUpKSB7XG4gICAgICBmbihpdGVtLnZhbHVlKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzSnNPYmplY3QobzogYW55KTogYm9vbGVhbiB7XG4gIHJldHVybiBvICE9PSBudWxsICYmICh0eXBlb2YgbyA9PT0gJ2Z1bmN0aW9uJyB8fCB0eXBlb2YgbyA9PT0gJ29iamVjdCcpO1xufVxuIl19