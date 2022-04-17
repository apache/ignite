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
import { stringify } from '../util/stringify';
import { resolveForwardRef } from './forward_ref';
/**
 * A unique object used for retrieving items from the {\@link ReflectiveInjector}.
 *
 * Keys have:
 * - a system-wide unique `id`.
 * - a `token`.
 *
 * `Key` is used internally by {\@link ReflectiveInjector} because its system-wide unique `id` allows
 * the
 * injector to store created objects in a more efficient way.
 *
 * `Key` should not be created directly. {\@link ReflectiveInjector} creates keys automatically when
 * resolving
 * providers.
 *
 * @deprecated No replacement
 * \@publicApi
 */
export class ReflectiveKey {
    /**
     * Private
     * @param {?} token
     * @param {?} id
     */
    constructor(token, id) {
        this.token = token;
        this.id = id;
        if (!token) {
            throw new Error('Token must be defined!');
        }
        this.displayName = stringify(this.token);
    }
    /**
     * Retrieves a `Key` for a token.
     * @param {?} token
     * @return {?}
     */
    static get(token) {
        return _globalKeyRegistry.get(resolveForwardRef(token));
    }
    /**
     * @return {?} the number of keys registered in the system.
     */
    static get numberOfKeys() { return _globalKeyRegistry.numberOfKeys; }
}
if (false) {
    /** @type {?} */
    ReflectiveKey.prototype.displayName;
    /** @type {?} */
    ReflectiveKey.prototype.token;
    /** @type {?} */
    ReflectiveKey.prototype.id;
}
export class KeyRegistry {
    constructor() {
        this._allKeys = new Map();
    }
    /**
     * @param {?} token
     * @return {?}
     */
    get(token) {
        if (token instanceof ReflectiveKey)
            return token;
        if (this._allKeys.has(token)) {
            return (/** @type {?} */ (this._allKeys.get(token)));
        }
        /** @type {?} */
        const newKey = new ReflectiveKey(token, ReflectiveKey.numberOfKeys);
        this._allKeys.set(token, newKey);
        return newKey;
    }
    /**
     * @return {?}
     */
    get numberOfKeys() { return this._allKeys.size; }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    KeyRegistry.prototype._allKeys;
}
/** @type {?} */
const _globalKeyRegistry = new KeyRegistry();
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmbGVjdGl2ZV9rZXkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9kaS9yZWZsZWN0aXZlX2tleS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUM1QyxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxlQUFlLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFxQmhELE1BQU0sT0FBTyxhQUFhOzs7Ozs7SUFLeEIsWUFBbUIsS0FBYSxFQUFTLEVBQVU7UUFBaEMsVUFBSyxHQUFMLEtBQUssQ0FBUTtRQUFTLE9BQUUsR0FBRixFQUFFLENBQVE7UUFDakQsSUFBSSxDQUFDLEtBQUssRUFBRTtZQUNWLE1BQU0sSUFBSSxLQUFLLENBQUMsd0JBQXdCLENBQUMsQ0FBQztTQUMzQztRQUNELElBQUksQ0FBQyxXQUFXLEdBQUcsU0FBUyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUMzQyxDQUFDOzs7Ozs7SUFLRCxNQUFNLENBQUMsR0FBRyxDQUFDLEtBQWE7UUFDdEIsT0FBTyxrQkFBa0IsQ0FBQyxHQUFHLENBQUMsaUJBQWlCLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztJQUMxRCxDQUFDOzs7O0lBS0QsTUFBTSxLQUFLLFlBQVksS0FBYSxPQUFPLGtCQUFrQixDQUFDLFlBQVksQ0FBQyxDQUFDLENBQUM7Q0FDOUU7OztJQXRCQyxvQ0FBb0M7O0lBSXhCLDhCQUFvQjs7SUFBRSwyQkFBaUI7O0FBb0JyRCxNQUFNLE9BQU8sV0FBVztJQUF4QjtRQUNVLGFBQVEsR0FBRyxJQUFJLEdBQUcsRUFBeUIsQ0FBQztJQWV0RCxDQUFDOzs7OztJQWJDLEdBQUcsQ0FBQyxLQUFhO1FBQ2YsSUFBSSxLQUFLLFlBQVksYUFBYTtZQUFFLE9BQU8sS0FBSyxDQUFDO1FBRWpELElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLEVBQUU7WUFDNUIsT0FBTyxtQkFBQSxJQUFJLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxLQUFLLENBQUMsRUFBRSxDQUFDO1NBQ25DOztjQUVLLE1BQU0sR0FBRyxJQUFJLGFBQWEsQ0FBQyxLQUFLLEVBQUUsYUFBYSxDQUFDLFlBQVksQ0FBQztRQUNuRSxJQUFJLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxLQUFLLEVBQUUsTUFBTSxDQUFDLENBQUM7UUFDakMsT0FBTyxNQUFNLENBQUM7SUFDaEIsQ0FBQzs7OztJQUVELElBQUksWUFBWSxLQUFhLE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO0NBQzFEOzs7Ozs7SUFmQywrQkFBb0Q7OztNQWlCaEQsa0JBQWtCLEdBQUcsSUFBSSxXQUFXLEVBQUUiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7c3RyaW5naWZ5fSBmcm9tICcuLi91dGlsL3N0cmluZ2lmeSc7XG5pbXBvcnQge3Jlc29sdmVGb3J3YXJkUmVmfSBmcm9tICcuL2ZvcndhcmRfcmVmJztcblxuXG4vKipcbiAqIEEgdW5pcXVlIG9iamVjdCB1c2VkIGZvciByZXRyaWV2aW5nIGl0ZW1zIGZyb20gdGhlIHtAbGluayBSZWZsZWN0aXZlSW5qZWN0b3J9LlxuICpcbiAqIEtleXMgaGF2ZTpcbiAqIC0gYSBzeXN0ZW0td2lkZSB1bmlxdWUgYGlkYC5cbiAqIC0gYSBgdG9rZW5gLlxuICpcbiAqIGBLZXlgIGlzIHVzZWQgaW50ZXJuYWxseSBieSB7QGxpbmsgUmVmbGVjdGl2ZUluamVjdG9yfSBiZWNhdXNlIGl0cyBzeXN0ZW0td2lkZSB1bmlxdWUgYGlkYCBhbGxvd3NcbiAqIHRoZVxuICogaW5qZWN0b3IgdG8gc3RvcmUgY3JlYXRlZCBvYmplY3RzIGluIGEgbW9yZSBlZmZpY2llbnQgd2F5LlxuICpcbiAqIGBLZXlgIHNob3VsZCBub3QgYmUgY3JlYXRlZCBkaXJlY3RseS4ge0BsaW5rIFJlZmxlY3RpdmVJbmplY3Rvcn0gY3JlYXRlcyBrZXlzIGF1dG9tYXRpY2FsbHkgd2hlblxuICogcmVzb2x2aW5nXG4gKiBwcm92aWRlcnMuXG4gKlxuICogQGRlcHJlY2F0ZWQgTm8gcmVwbGFjZW1lbnRcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIFJlZmxlY3RpdmVLZXkge1xuICBwdWJsaWMgcmVhZG9ubHkgZGlzcGxheU5hbWU6IHN0cmluZztcbiAgLyoqXG4gICAqIFByaXZhdGVcbiAgICovXG4gIGNvbnN0cnVjdG9yKHB1YmxpYyB0b2tlbjogT2JqZWN0LCBwdWJsaWMgaWQ6IG51bWJlcikge1xuICAgIGlmICghdG9rZW4pIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignVG9rZW4gbXVzdCBiZSBkZWZpbmVkIScpO1xuICAgIH1cbiAgICB0aGlzLmRpc3BsYXlOYW1lID0gc3RyaW5naWZ5KHRoaXMudG9rZW4pO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyBhIGBLZXlgIGZvciBhIHRva2VuLlxuICAgKi9cbiAgc3RhdGljIGdldCh0b2tlbjogT2JqZWN0KTogUmVmbGVjdGl2ZUtleSB7XG4gICAgcmV0dXJuIF9nbG9iYWxLZXlSZWdpc3RyeS5nZXQocmVzb2x2ZUZvcndhcmRSZWYodG9rZW4pKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAcmV0dXJucyB0aGUgbnVtYmVyIG9mIGtleXMgcmVnaXN0ZXJlZCBpbiB0aGUgc3lzdGVtLlxuICAgKi9cbiAgc3RhdGljIGdldCBudW1iZXJPZktleXMoKTogbnVtYmVyIHsgcmV0dXJuIF9nbG9iYWxLZXlSZWdpc3RyeS5udW1iZXJPZktleXM7IH1cbn1cblxuZXhwb3J0IGNsYXNzIEtleVJlZ2lzdHJ5IHtcbiAgcHJpdmF0ZSBfYWxsS2V5cyA9IG5ldyBNYXA8T2JqZWN0LCBSZWZsZWN0aXZlS2V5PigpO1xuXG4gIGdldCh0b2tlbjogT2JqZWN0KTogUmVmbGVjdGl2ZUtleSB7XG4gICAgaWYgKHRva2VuIGluc3RhbmNlb2YgUmVmbGVjdGl2ZUtleSkgcmV0dXJuIHRva2VuO1xuXG4gICAgaWYgKHRoaXMuX2FsbEtleXMuaGFzKHRva2VuKSkge1xuICAgICAgcmV0dXJuIHRoaXMuX2FsbEtleXMuZ2V0KHRva2VuKSAhO1xuICAgIH1cblxuICAgIGNvbnN0IG5ld0tleSA9IG5ldyBSZWZsZWN0aXZlS2V5KHRva2VuLCBSZWZsZWN0aXZlS2V5Lm51bWJlck9mS2V5cyk7XG4gICAgdGhpcy5fYWxsS2V5cy5zZXQodG9rZW4sIG5ld0tleSk7XG4gICAgcmV0dXJuIG5ld0tleTtcbiAgfVxuXG4gIGdldCBudW1iZXJPZktleXMoKTogbnVtYmVyIHsgcmV0dXJuIHRoaXMuX2FsbEtleXMuc2l6ZTsgfVxufVxuXG5jb25zdCBfZ2xvYmFsS2V5UmVnaXN0cnkgPSBuZXcgS2V5UmVnaXN0cnkoKTtcbiJdfQ==