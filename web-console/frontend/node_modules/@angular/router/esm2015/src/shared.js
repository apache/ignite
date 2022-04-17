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
 * The primary routing outlet.
 *
 * \@publicApi
 * @type {?}
 */
export const PRIMARY_OUTLET = 'primary';
/**
 * A map that provides access to the required and optional parameters
 * specific to a route.
 * The map supports retrieving a single value with `get()`
 * or multiple values with `getAll()`.
 *
 * @see [URLSearchParams](https://developer.mozilla.org/en-US/docs/Web/API/URLSearchParams)
 *
 * \@publicApi
 * @record
 */
export function ParamMap() { }
if (false) {
    /**
     * Names of the parameters in the map.
     * @type {?}
     */
    ParamMap.prototype.keys;
    /**
     * Reports whether the map contains a given parameter.
     * @param {?} name The parameter name.
     * @return {?} True if the map contains the given parameter, false otherwise.
     */
    ParamMap.prototype.has = function (name) { };
    /**
     * Retrieves a single value for a parameter.
     * @param {?} name The parameter name.
     * @return {?} The parameter's single value,
     * or the first value if the parameter has multiple values,
     * or `null` when there is no such parameter.
     */
    ParamMap.prototype.get = function (name) { };
    /**
     * Retrieves multiple values for a parameter.
     * @param {?} name The parameter name.
     * @return {?} An array containing one or more values,
     * or an empty array if there is no such parameter.
     *
     */
    ParamMap.prototype.getAll = function (name) { };
}
class ParamsAsMap {
    /**
     * @param {?} params
     */
    constructor(params) { this.params = params || {}; }
    /**
     * @param {?} name
     * @return {?}
     */
    has(name) { return this.params.hasOwnProperty(name); }
    /**
     * @param {?} name
     * @return {?}
     */
    get(name) {
        if (this.has(name)) {
            /** @type {?} */
            const v = this.params[name];
            return Array.isArray(v) ? v[0] : v;
        }
        return null;
    }
    /**
     * @param {?} name
     * @return {?}
     */
    getAll(name) {
        if (this.has(name)) {
            /** @type {?} */
            const v = this.params[name];
            return Array.isArray(v) ? v : [v];
        }
        return [];
    }
    /**
     * @return {?}
     */
    get keys() { return Object.keys(this.params); }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ParamsAsMap.prototype.params;
}
/**
 * Converts a `Params` instance to a `ParamMap`.
 * \@publicApi
 * @param {?} params The instance to convert.
 * @return {?} The new map instance.
 *
 */
export function convertToParamMap(params) {
    return new ParamsAsMap(params);
}
/** @type {?} */
const NAVIGATION_CANCELING_ERROR = 'ngNavigationCancelingError';
/**
 * @param {?} message
 * @return {?}
 */
export function navigationCancelingError(message) {
    /** @type {?} */
    const error = Error('NavigationCancelingError: ' + message);
    ((/** @type {?} */ (error)))[NAVIGATION_CANCELING_ERROR] = true;
    return error;
}
/**
 * @param {?} error
 * @return {?}
 */
export function isNavigationCancelingError(error) {
    return error && ((/** @type {?} */ (error)))[NAVIGATION_CANCELING_ERROR];
}
// Matches the route configuration (`route`) against the actual URL (`segments`).
/**
 * @param {?} segments
 * @param {?} segmentGroup
 * @param {?} route
 * @return {?}
 */
export function defaultUrlMatcher(segments, segmentGroup, route) {
    /** @type {?} */
    const parts = (/** @type {?} */ (route.path)).split('/');
    if (parts.length > segments.length) {
        // The actual URL is shorter than the config, no match
        return null;
    }
    if (route.pathMatch === 'full' &&
        (segmentGroup.hasChildren() || parts.length < segments.length)) {
        // The config is longer than the actual URL but we are looking for a full match, return null
        return null;
    }
    /** @type {?} */
    const posParams = {};
    // Check each config part against the actual URL
    for (let index = 0; index < parts.length; index++) {
        /** @type {?} */
        const part = parts[index];
        /** @type {?} */
        const segment = segments[index];
        /** @type {?} */
        const isParameter = part.startsWith(':');
        if (isParameter) {
            posParams[part.substring(1)] = segment;
        }
        else if (part !== segment.path) {
            // The actual URL part does not match the config, no match
            return null;
        }
    }
    return { consumed: segments.slice(0, parts.length), posParams };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2hhcmVkLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9zaGFyZWQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFpQkEsTUFBTSxPQUFPLGNBQWMsR0FBRyxTQUFTOzs7Ozs7Ozs7Ozs7QUF1QnZDLDhCQTBCQzs7Ozs7O0lBREMsd0JBQXdCOzs7Ozs7SUFuQnhCLDZDQUEyQjs7Ozs7Ozs7SUFRM0IsNkNBQStCOzs7Ozs7OztJQVEvQixnREFBK0I7O0FBTWpDLE1BQU0sV0FBVzs7OztJQUdmLFlBQVksTUFBYyxJQUFJLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxJQUFJLEVBQUUsQ0FBQyxDQUFDLENBQUM7Ozs7O0lBRTNELEdBQUcsQ0FBQyxJQUFZLElBQWEsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7O0lBRXZFLEdBQUcsQ0FBQyxJQUFZO1FBQ2QsSUFBSSxJQUFJLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxFQUFFOztrQkFDWixDQUFDLEdBQUcsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUM7WUFDM0IsT0FBTyxLQUFLLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztTQUNwQztRQUVELE9BQU8sSUFBSSxDQUFDO0lBQ2QsQ0FBQzs7Ozs7SUFFRCxNQUFNLENBQUMsSUFBWTtRQUNqQixJQUFJLElBQUksQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLEVBQUU7O2tCQUNaLENBQUMsR0FBRyxJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQztZQUMzQixPQUFPLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztTQUNuQztRQUVELE9BQU8sRUFBRSxDQUFDO0lBQ1osQ0FBQzs7OztJQUVELElBQUksSUFBSSxLQUFlLE9BQU8sTUFBTSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDO0NBQzFEOzs7Ozs7SUF6QkMsNkJBQXVCOzs7Ozs7Ozs7QUFrQ3pCLE1BQU0sVUFBVSxpQkFBaUIsQ0FBQyxNQUFjO0lBQzlDLE9BQU8sSUFBSSxXQUFXLENBQUMsTUFBTSxDQUFDLENBQUM7QUFDakMsQ0FBQzs7TUFFSywwQkFBMEIsR0FBRyw0QkFBNEI7Ozs7O0FBRS9ELE1BQU0sVUFBVSx3QkFBd0IsQ0FBQyxPQUFlOztVQUNoRCxLQUFLLEdBQUcsS0FBSyxDQUFDLDRCQUE0QixHQUFHLE9BQU8sQ0FBQztJQUMzRCxDQUFDLG1CQUFBLEtBQUssRUFBTyxDQUFDLENBQUMsMEJBQTBCLENBQUMsR0FBRyxJQUFJLENBQUM7SUFDbEQsT0FBTyxLQUFLLENBQUM7QUFDZixDQUFDOzs7OztBQUVELE1BQU0sVUFBVSwwQkFBMEIsQ0FBQyxLQUFZO0lBQ3JELE9BQU8sS0FBSyxJQUFJLENBQUMsbUJBQUEsS0FBSyxFQUFPLENBQUMsQ0FBQywwQkFBMEIsQ0FBQyxDQUFDO0FBQzdELENBQUM7Ozs7Ozs7O0FBR0QsTUFBTSxVQUFVLGlCQUFpQixDQUM3QixRQUFzQixFQUFFLFlBQTZCLEVBQUUsS0FBWTs7VUFDL0QsS0FBSyxHQUFHLG1CQUFBLEtBQUssQ0FBQyxJQUFJLEVBQUUsQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDO0lBRXJDLElBQUksS0FBSyxDQUFDLE1BQU0sR0FBRyxRQUFRLENBQUMsTUFBTSxFQUFFO1FBQ2xDLHNEQUFzRDtRQUN0RCxPQUFPLElBQUksQ0FBQztLQUNiO0lBRUQsSUFBSSxLQUFLLENBQUMsU0FBUyxLQUFLLE1BQU07UUFDMUIsQ0FBQyxZQUFZLENBQUMsV0FBVyxFQUFFLElBQUksS0FBSyxDQUFDLE1BQU0sR0FBRyxRQUFRLENBQUMsTUFBTSxDQUFDLEVBQUU7UUFDbEUsNEZBQTRGO1FBQzVGLE9BQU8sSUFBSSxDQUFDO0tBQ2I7O1VBRUssU0FBUyxHQUFnQyxFQUFFO0lBRWpELGdEQUFnRDtJQUNoRCxLQUFLLElBQUksS0FBSyxHQUFHLENBQUMsRUFBRSxLQUFLLEdBQUcsS0FBSyxDQUFDLE1BQU0sRUFBRSxLQUFLLEVBQUUsRUFBRTs7Y0FDM0MsSUFBSSxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUM7O2NBQ25CLE9BQU8sR0FBRyxRQUFRLENBQUMsS0FBSyxDQUFDOztjQUN6QixXQUFXLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUM7UUFDeEMsSUFBSSxXQUFXLEVBQUU7WUFDZixTQUFTLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxHQUFHLE9BQU8sQ0FBQztTQUN4QzthQUFNLElBQUksSUFBSSxLQUFLLE9BQU8sQ0FBQyxJQUFJLEVBQUU7WUFDaEMsMERBQTBEO1lBQzFELE9BQU8sSUFBSSxDQUFDO1NBQ2I7S0FDRjtJQUVELE9BQU8sRUFBQyxRQUFRLEVBQUUsUUFBUSxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUUsS0FBSyxDQUFDLE1BQU0sQ0FBQyxFQUFFLFNBQVMsRUFBQyxDQUFDO0FBQ2hFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Um91dGUsIFVybE1hdGNoUmVzdWx0fSBmcm9tICcuL2NvbmZpZyc7XG5pbXBvcnQge1VybFNlZ21lbnQsIFVybFNlZ21lbnRHcm91cH0gZnJvbSAnLi91cmxfdHJlZSc7XG5cblxuLyoqXG4gKiBUaGUgcHJpbWFyeSByb3V0aW5nIG91dGxldC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBQUklNQVJZX09VVExFVCA9ICdwcmltYXJ5JztcblxuLyoqXG4gKiBBIGNvbGxlY3Rpb24gb2YgbWF0cml4IGFuZCBxdWVyeSBVUkwgcGFyYW1ldGVycy5cbiAqIEBzZWUgYGNvbnZlcnRUb1BhcmFtTWFwKClgXG4gKiBAc2VlIGBQYXJhbU1hcGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCB0eXBlIFBhcmFtcyA9IHtcbiAgW2tleTogc3RyaW5nXTogYW55XG59O1xuXG4vKipcbiAqIEEgbWFwIHRoYXQgcHJvdmlkZXMgYWNjZXNzIHRvIHRoZSByZXF1aXJlZCBhbmQgb3B0aW9uYWwgcGFyYW1ldGVyc1xuICogc3BlY2lmaWMgdG8gYSByb3V0ZS5cbiAqIFRoZSBtYXAgc3VwcG9ydHMgcmV0cmlldmluZyBhIHNpbmdsZSB2YWx1ZSB3aXRoIGBnZXQoKWBcbiAqIG9yIG11bHRpcGxlIHZhbHVlcyB3aXRoIGBnZXRBbGwoKWAuXG4gKlxuICogQHNlZSBbVVJMU2VhcmNoUGFyYW1zXShodHRwczovL2RldmVsb3Blci5tb3ppbGxhLm9yZy9lbi1VUy9kb2NzL1dlYi9BUEkvVVJMU2VhcmNoUGFyYW1zKVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBQYXJhbU1hcCB7XG4gIC8qKlxuICAgKiBSZXBvcnRzIHdoZXRoZXIgdGhlIG1hcCBjb250YWlucyBhIGdpdmVuIHBhcmFtZXRlci5cbiAgICogQHBhcmFtIG5hbWUgVGhlIHBhcmFtZXRlciBuYW1lLlxuICAgKiBAcmV0dXJucyBUcnVlIGlmIHRoZSBtYXAgY29udGFpbnMgdGhlIGdpdmVuIHBhcmFtZXRlciwgZmFsc2Ugb3RoZXJ3aXNlLlxuICAgKi9cbiAgaGFzKG5hbWU6IHN0cmluZyk6IGJvb2xlYW47XG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgYSBzaW5nbGUgdmFsdWUgZm9yIGEgcGFyYW1ldGVyLlxuICAgKiBAcGFyYW0gbmFtZSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEByZXR1cm4gVGhlIHBhcmFtZXRlcidzIHNpbmdsZSB2YWx1ZSxcbiAgICogb3IgdGhlIGZpcnN0IHZhbHVlIGlmIHRoZSBwYXJhbWV0ZXIgaGFzIG11bHRpcGxlIHZhbHVlcyxcbiAgICogb3IgYG51bGxgIHdoZW4gdGhlcmUgaXMgbm8gc3VjaCBwYXJhbWV0ZXIuXG4gICAqL1xuICBnZXQobmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGw7XG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgbXVsdGlwbGUgdmFsdWVzIGZvciBhIHBhcmFtZXRlci5cbiAgICogQHBhcmFtIG5hbWUgVGhlIHBhcmFtZXRlciBuYW1lLlxuICAgKiBAcmV0dXJuIEFuIGFycmF5IGNvbnRhaW5pbmcgb25lIG9yIG1vcmUgdmFsdWVzLFxuICAgKiBvciBhbiBlbXB0eSBhcnJheSBpZiB0aGVyZSBpcyBubyBzdWNoIHBhcmFtZXRlci5cbiAgICpcbiAgICovXG4gIGdldEFsbChuYW1lOiBzdHJpbmcpOiBzdHJpbmdbXTtcblxuICAvKiogTmFtZXMgb2YgdGhlIHBhcmFtZXRlcnMgaW4gdGhlIG1hcC4gKi9cbiAgcmVhZG9ubHkga2V5czogc3RyaW5nW107XG59XG5cbmNsYXNzIFBhcmFtc0FzTWFwIGltcGxlbWVudHMgUGFyYW1NYXAge1xuICBwcml2YXRlIHBhcmFtczogUGFyYW1zO1xuXG4gIGNvbnN0cnVjdG9yKHBhcmFtczogUGFyYW1zKSB7IHRoaXMucGFyYW1zID0gcGFyYW1zIHx8IHt9OyB9XG5cbiAgaGFzKG5hbWU6IHN0cmluZyk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5wYXJhbXMuaGFzT3duUHJvcGVydHkobmFtZSk7IH1cblxuICBnZXQobmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIGlmICh0aGlzLmhhcyhuYW1lKSkge1xuICAgICAgY29uc3QgdiA9IHRoaXMucGFyYW1zW25hbWVdO1xuICAgICAgcmV0dXJuIEFycmF5LmlzQXJyYXkodikgPyB2WzBdIDogdjtcbiAgICB9XG5cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGdldEFsbChuYW1lOiBzdHJpbmcpOiBzdHJpbmdbXSB7XG4gICAgaWYgKHRoaXMuaGFzKG5hbWUpKSB7XG4gICAgICBjb25zdCB2ID0gdGhpcy5wYXJhbXNbbmFtZV07XG4gICAgICByZXR1cm4gQXJyYXkuaXNBcnJheSh2KSA/IHYgOiBbdl07XG4gICAgfVxuXG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgZ2V0IGtleXMoKTogc3RyaW5nW10geyByZXR1cm4gT2JqZWN0LmtleXModGhpcy5wYXJhbXMpOyB9XG59XG5cbi8qKlxuICogQ29udmVydHMgYSBgUGFyYW1zYCBpbnN0YW5jZSB0byBhIGBQYXJhbU1hcGAuXG4gKiBAcGFyYW0gcGFyYW1zIFRoZSBpbnN0YW5jZSB0byBjb252ZXJ0LlxuICogQHJldHVybnMgVGhlIG5ldyBtYXAgaW5zdGFuY2UuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gY29udmVydFRvUGFyYW1NYXAocGFyYW1zOiBQYXJhbXMpOiBQYXJhbU1hcCB7XG4gIHJldHVybiBuZXcgUGFyYW1zQXNNYXAocGFyYW1zKTtcbn1cblxuY29uc3QgTkFWSUdBVElPTl9DQU5DRUxJTkdfRVJST1IgPSAnbmdOYXZpZ2F0aW9uQ2FuY2VsaW5nRXJyb3InO1xuXG5leHBvcnQgZnVuY3Rpb24gbmF2aWdhdGlvbkNhbmNlbGluZ0Vycm9yKG1lc3NhZ2U6IHN0cmluZykge1xuICBjb25zdCBlcnJvciA9IEVycm9yKCdOYXZpZ2F0aW9uQ2FuY2VsaW5nRXJyb3I6ICcgKyBtZXNzYWdlKTtcbiAgKGVycm9yIGFzIGFueSlbTkFWSUdBVElPTl9DQU5DRUxJTkdfRVJST1JdID0gdHJ1ZTtcbiAgcmV0dXJuIGVycm9yO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNOYXZpZ2F0aW9uQ2FuY2VsaW5nRXJyb3IoZXJyb3I6IEVycm9yKSB7XG4gIHJldHVybiBlcnJvciAmJiAoZXJyb3IgYXMgYW55KVtOQVZJR0FUSU9OX0NBTkNFTElOR19FUlJPUl07XG59XG5cbi8vIE1hdGNoZXMgdGhlIHJvdXRlIGNvbmZpZ3VyYXRpb24gKGByb3V0ZWApIGFnYWluc3QgdGhlIGFjdHVhbCBVUkwgKGBzZWdtZW50c2ApLlxuZXhwb3J0IGZ1bmN0aW9uIGRlZmF1bHRVcmxNYXRjaGVyKFxuICAgIHNlZ21lbnRzOiBVcmxTZWdtZW50W10sIHNlZ21lbnRHcm91cDogVXJsU2VnbWVudEdyb3VwLCByb3V0ZTogUm91dGUpOiBVcmxNYXRjaFJlc3VsdHxudWxsIHtcbiAgY29uc3QgcGFydHMgPSByb3V0ZS5wYXRoICEuc3BsaXQoJy8nKTtcblxuICBpZiAocGFydHMubGVuZ3RoID4gc2VnbWVudHMubGVuZ3RoKSB7XG4gICAgLy8gVGhlIGFjdHVhbCBVUkwgaXMgc2hvcnRlciB0aGFuIHRoZSBjb25maWcsIG5vIG1hdGNoXG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBpZiAocm91dGUucGF0aE1hdGNoID09PSAnZnVsbCcgJiZcbiAgICAgIChzZWdtZW50R3JvdXAuaGFzQ2hpbGRyZW4oKSB8fCBwYXJ0cy5sZW5ndGggPCBzZWdtZW50cy5sZW5ndGgpKSB7XG4gICAgLy8gVGhlIGNvbmZpZyBpcyBsb25nZXIgdGhhbiB0aGUgYWN0dWFsIFVSTCBidXQgd2UgYXJlIGxvb2tpbmcgZm9yIGEgZnVsbCBtYXRjaCwgcmV0dXJuIG51bGxcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGNvbnN0IHBvc1BhcmFtczoge1trZXk6IHN0cmluZ106IFVybFNlZ21lbnR9ID0ge307XG5cbiAgLy8gQ2hlY2sgZWFjaCBjb25maWcgcGFydCBhZ2FpbnN0IHRoZSBhY3R1YWwgVVJMXG4gIGZvciAobGV0IGluZGV4ID0gMDsgaW5kZXggPCBwYXJ0cy5sZW5ndGg7IGluZGV4KyspIHtcbiAgICBjb25zdCBwYXJ0ID0gcGFydHNbaW5kZXhdO1xuICAgIGNvbnN0IHNlZ21lbnQgPSBzZWdtZW50c1tpbmRleF07XG4gICAgY29uc3QgaXNQYXJhbWV0ZXIgPSBwYXJ0LnN0YXJ0c1dpdGgoJzonKTtcbiAgICBpZiAoaXNQYXJhbWV0ZXIpIHtcbiAgICAgIHBvc1BhcmFtc1twYXJ0LnN1YnN0cmluZygxKV0gPSBzZWdtZW50O1xuICAgIH0gZWxzZSBpZiAocGFydCAhPT0gc2VnbWVudC5wYXRoKSB7XG4gICAgICAvLyBUaGUgYWN0dWFsIFVSTCBwYXJ0IGRvZXMgbm90IG1hdGNoIHRoZSBjb25maWcsIG5vIG1hdGNoXG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gIH1cblxuICByZXR1cm4ge2NvbnN1bWVkOiBzZWdtZW50cy5zbGljZSgwLCBwYXJ0cy5sZW5ndGgpLCBwb3NQYXJhbXN9O1xufVxuIl19