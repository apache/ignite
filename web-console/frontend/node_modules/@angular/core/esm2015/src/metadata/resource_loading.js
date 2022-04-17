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
 * Used to resolve resource URLs on `\@Component` when used with JIT compilation.
 *
 * Example:
 * ```
 * \@Component({
 *   selector: 'my-comp',
 *   templateUrl: 'my-comp.html', // This requires asynchronous resolution
 * })
 * class MyComponent{
 * }
 *
 * // Calling `renderComponent` will fail because `renderComponent` is a synchronous process
 * // and `MyComponent`'s `\@Component.templateUrl` needs to be resolved asynchronously.
 *
 * // Calling `resolveComponentResources()` will resolve `\@Component.templateUrl` into
 * // `\@Component.template`, which allows `renderComponent` to proceed in a synchronous manner.
 *
 * // Use browser's `fetch()` function as the default resource resolution strategy.
 * resolveComponentResources(fetch).then(() => {
 *   // After resolution all URLs have been converted into `template` strings.
 *   renderComponent(MyComponent);
 * });
 *
 * ```
 *
 * NOTE: In AOT the resolution happens during compilation, and so there should be no need
 * to call this method outside JIT mode.
 *
 * @param {?} resourceResolver a function which is responsible for returning a `Promise` to the
 * contents of the resolved URL. Browser's `fetch()` method is a good default implementation.
 * @return {?}
 */
export function resolveComponentResources(resourceResolver) {
    // Store all promises which are fetching the resources.
    /** @type {?} */
    const componentResolved = [];
    // Cache so that we don't fetch the same resource more than once.
    /** @type {?} */
    const urlMap = new Map();
    /**
     * @param {?} url
     * @return {?}
     */
    function cachedResourceResolve(url) {
        /** @type {?} */
        let promise = urlMap.get(url);
        if (!promise) {
            /** @type {?} */
            const resp = resourceResolver(url);
            urlMap.set(url, promise = resp.then(unwrapResponse));
        }
        return promise;
    }
    componentResourceResolutionQueue.forEach((/**
     * @param {?} component
     * @param {?} type
     * @return {?}
     */
    (component, type) => {
        /** @type {?} */
        const promises = [];
        if (component.templateUrl) {
            promises.push(cachedResourceResolve(component.templateUrl).then((/**
             * @param {?} template
             * @return {?}
             */
            (template) => {
                component.template = template;
            })));
        }
        /** @type {?} */
        const styleUrls = component.styleUrls;
        /** @type {?} */
        const styles = component.styles || (component.styles = []);
        /** @type {?} */
        const styleOffset = component.styles.length;
        styleUrls && styleUrls.forEach((/**
         * @param {?} styleUrl
         * @param {?} index
         * @return {?}
         */
        (styleUrl, index) => {
            styles.push(''); // pre-allocate array.
            promises.push(cachedResourceResolve(styleUrl).then((/**
             * @param {?} style
             * @return {?}
             */
            (style) => {
                styles[styleOffset + index] = style;
                styleUrls.splice(styleUrls.indexOf(styleUrl), 1);
                if (styleUrls.length == 0) {
                    component.styleUrls = undefined;
                }
            })));
        }));
        /** @type {?} */
        const fullyResolved = Promise.all(promises).then((/**
         * @return {?}
         */
        () => componentDefResolved(type)));
        componentResolved.push(fullyResolved);
    }));
    clearResolutionOfComponentResourcesQueue();
    return Promise.all(componentResolved).then((/**
     * @return {?}
     */
    () => undefined));
}
/** @type {?} */
let componentResourceResolutionQueue = new Map();
// Track when existing ngComponentDef for a Type is waiting on resources.
/** @type {?} */
const componentDefPendingResolution = new Set();
/**
 * @param {?} type
 * @param {?} metadata
 * @return {?}
 */
export function maybeQueueResolutionOfComponentResources(type, metadata) {
    if (componentNeedsResolution(metadata)) {
        componentResourceResolutionQueue.set(type, metadata);
        componentDefPendingResolution.add(type);
    }
}
/**
 * @param {?} type
 * @return {?}
 */
export function isComponentDefPendingResolution(type) {
    return componentDefPendingResolution.has(type);
}
/**
 * @param {?} component
 * @return {?}
 */
export function componentNeedsResolution(component) {
    return !!((component.templateUrl && !component.hasOwnProperty('template')) ||
        component.styleUrls && component.styleUrls.length);
}
/**
 * @return {?}
 */
export function clearResolutionOfComponentResourcesQueue() {
    /** @type {?} */
    const old = componentResourceResolutionQueue;
    componentResourceResolutionQueue = new Map();
    return old;
}
/**
 * @param {?} queue
 * @return {?}
 */
export function restoreComponentResolutionQueue(queue) {
    componentDefPendingResolution.clear();
    queue.forEach((/**
     * @param {?} _
     * @param {?} type
     * @return {?}
     */
    (_, type) => componentDefPendingResolution.add(type)));
    componentResourceResolutionQueue = queue;
}
/**
 * @return {?}
 */
export function isComponentResourceResolutionQueueEmpty() {
    return componentResourceResolutionQueue.size === 0;
}
/**
 * @param {?} response
 * @return {?}
 */
function unwrapResponse(response) {
    return typeof response == 'string' ? response : response.text();
}
/**
 * @param {?} type
 * @return {?}
 */
function componentDefResolved(type) {
    componentDefPendingResolution.delete(type);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb3VyY2VfbG9hZGluZy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL21ldGFkYXRhL3Jlc291cmNlX2xvYWRpbmcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUE0Q0EsTUFBTSxVQUFVLHlCQUF5QixDQUNyQyxnQkFBOEU7OztVQUUxRSxpQkFBaUIsR0FBb0IsRUFBRTs7O1VBR3ZDLE1BQU0sR0FBRyxJQUFJLEdBQUcsRUFBMkI7Ozs7O0lBQ2pELFNBQVMscUJBQXFCLENBQUMsR0FBVzs7WUFDcEMsT0FBTyxHQUFHLE1BQU0sQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDO1FBQzdCLElBQUksQ0FBQyxPQUFPLEVBQUU7O2tCQUNOLElBQUksR0FBRyxnQkFBZ0IsQ0FBQyxHQUFHLENBQUM7WUFDbEMsTUFBTSxDQUFDLEdBQUcsQ0FBQyxHQUFHLEVBQUUsT0FBTyxHQUFHLElBQUksQ0FBQyxJQUFJLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQztTQUN0RDtRQUNELE9BQU8sT0FBTyxDQUFDO0lBQ2pCLENBQUM7SUFFRCxnQ0FBZ0MsQ0FBQyxPQUFPOzs7OztJQUFDLENBQUMsU0FBb0IsRUFBRSxJQUFlLEVBQUUsRUFBRTs7Y0FDM0UsUUFBUSxHQUFvQixFQUFFO1FBQ3BDLElBQUksU0FBUyxDQUFDLFdBQVcsRUFBRTtZQUN6QixRQUFRLENBQUMsSUFBSSxDQUFDLHFCQUFxQixDQUFDLFNBQVMsQ0FBQyxXQUFXLENBQUMsQ0FBQyxJQUFJOzs7O1lBQUMsQ0FBQyxRQUFRLEVBQUUsRUFBRTtnQkFDM0UsU0FBUyxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUM7WUFDaEMsQ0FBQyxFQUFDLENBQUMsQ0FBQztTQUNMOztjQUNLLFNBQVMsR0FBRyxTQUFTLENBQUMsU0FBUzs7Y0FDL0IsTUFBTSxHQUFHLFNB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LElBQUksS0FBSyxDQUFDLENBQUM7QUFDckQsQ0FBQzs7Ozs7QUFFRCxTQUFTLGNBQWMsQ0FBQyxRQUE0QztJQUNsRSxPQUFPLE9BQU8sUUFBUSxJQUFJLFFBQVEsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsSUFBSSxFQUFFLENBQUM7QUFDbEUsQ0FBQzs7Ozs7QUFFRCxTQUFTLG9CQUFvQixDQUFDLElBQWU7SUFDM0MsNkJBQTZCLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxDQUFDO0FBQzdDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtDb21wb25lbnR9IGZyb20gJy4vZGlyZWN0aXZlcyc7XG5cblxuLyoqXG4gKiBVc2VkIHRvIHJlc29sdmUgcmVzb3VyY2UgVVJMcyBvbiBgQENvbXBvbmVudGAgd2hlbiB1c2VkIHdpdGggSklUIGNvbXBpbGF0aW9uLlxuICpcbiAqIEV4YW1wbGU6XG4gKiBgYGBcbiAqIEBDb21wb25lbnQoe1xuICogICBzZWxlY3RvcjogJ215LWNvbXAnLFxuICogICB0ZW1wbGF0ZVVybDogJ215LWNvbXAuaHRtbCcsIC8vIFRoaXMgcmVxdWlyZXMgYXN5bmNocm9ub3VzIHJlc29sdXRpb25cbiAqIH0pXG4gKiBjbGFzcyBNeUNvbXBvbmVudHtcbiAqIH1cbiAqXG4gKiAvLyBDYWxsaW5nIGByZW5kZXJDb21wb25lbnRgIHdpbGwgZmFpbCBiZWNhdXNlIGByZW5kZXJDb21wb25lbnRgIGlzIGEgc3luY2hyb25vdXMgcHJvY2Vzc1xuICogLy8gYW5kIGBNeUNvbXBvbmVudGAncyBgQENvbXBvbmVudC50ZW1wbGF0ZVVybGAgbmVlZHMgdG8gYmUgcmVzb2x2ZWQgYXN5bmNocm9ub3VzbHkuXG4gKlxuICogLy8gQ2FsbGluZyBgcmVzb2x2ZUNvbXBvbmVudFJlc291cmNlcygpYCB3aWxsIHJlc29sdmUgYEBDb21wb25lbnQudGVtcGxhdGVVcmxgIGludG9cbiAqIC8vIGBAQ29tcG9uZW50LnRlbXBsYXRlYCwgd2hpY2ggYWxsb3dzIGByZW5kZXJDb21wb25lbnRgIHRvIHByb2NlZWQgaW4gYSBzeW5jaHJvbm91cyBtYW5uZXIuXG4gKlxuICogLy8gVXNlIGJyb3dzZXIncyBgZmV0Y2goKWAgZnVuY3Rpb24gYXMgdGhlIGRlZmF1bHQgcmVzb3VyY2UgcmVzb2x1dGlvbiBzdHJhdGVneS5cbiAqIHJlc29sdmVDb21wb25lbnRSZXNvdXJjZXMoZmV0Y2gpLnRoZW4oKCkgPT4ge1xuICogICAvLyBBZnRlciByZXNvbHV0aW9uIGFsbCBVUkxzIGhhdmUgYmVlbiBjb252ZXJ0ZWQgaW50byBgdGVtcGxhdGVgIHN0cmluZ3MuXG4gKiAgIHJlbmRlckNvbXBvbmVudChNeUNvbXBvbmVudCk7XG4gKiB9KTtcbiAqXG4gKiBgYGBcbiAqXG4gKiBOT1RFOiBJbiBBT1QgdGhlIHJlc29sdXRpb24gaGFwcGVucyBkdXJpbmcgY29tcGlsYXRpb24sIGFuZCBzbyB0aGVyZSBzaG91bGQgYmUgbm8gbmVlZFxuICogdG8gY2FsbCB0aGlzIG1ldGhvZCBvdXRzaWRlIEpJVCBtb2RlLlxuICpcbiAqIEBwYXJhbSByZXNvdXJjZVJlc29sdmVyIGEgZnVuY3Rpb24gd2hpY2ggaXMgcmVzcG9uc2libGUgZm9yIHJldHVybmluZyBhIGBQcm9taXNlYCB0byB0aGVcbiAqIGNvbnRlbnRzIG9mIHRoZSByZXNvbHZlZCBVUkwuIEJyb3dzZXIncyBgZmV0Y2goKWAgbWV0aG9kIGlzIGEgZ29vZCBkZWZhdWx0IGltcGxlbWVudGF0aW9uLlxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVzb2x2ZUNvbXBvbmVudFJlc291cmNlcyhcbiAgICByZXNvdXJjZVJlc29sdmVyOiAodXJsOiBzdHJpbmcpID0+IChQcm9taXNlPHN0cmluZ3x7dGV4dCgpOiBQcm9taXNlPHN0cmluZz59PikpOiBQcm9taXNlPHZvaWQ+IHtcbiAgLy8gU3RvcmUgYWxsIHByb21pc2VzIHdoaWNoIGFyZSBmZXRjaGluZyB0aGUgcmVzb3VyY2VzLlxuICBjb25zdCBjb21wb25lbnRSZXNvbHZlZDogUHJvbWlzZTx2b2lkPltdID0gW107XG5cbiAgLy8gQ2FjaGUgc28gdGhhdCB3ZSBkb24ndCBmZXRjaCB0aGUgc2FtZSByZXNvdXJjZSBtb3JlIHRoYW4gb25jZS5cbiAgY29uc3QgdXJsTWFwID0gbmV3IE1hcDxzdHJpbmcsIFByb21pc2U8c3RyaW5nPj4oKTtcbiAgZnVuY3Rpb24gY2FjaGVkUmVzb3VyY2VSZXNvbHZlKHVybDogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICBsZXQgcHJvbWlzZSA9IHVybE1hcC5nZXQodXJsKTtcbiAgICBpZiAoIXByb21pc2UpIHtcbiAgICAgIGNvbnN0IHJlc3AgPSByZXNvdXJjZVJlc29sdmVyKHVybCk7XG4gICAgICB1cmxNYXAuc2V0KHVybCwgcHJvbWlzZSA9IHJlc3AudGhlbih1bndyYXBSZXNwb25zZSkpO1xuICAgIH1cbiAgICByZXR1cm4gcHJvbWlzZTtcbiAgfVxuXG4gIGNvbXBvbmVudFJlc291cmNlUmVzb2x1dGlvblF1ZXVlLmZvckVhY2goKGNvbXBvbmVudDogQ29tcG9uZW50LCB0eXBlOiBUeXBlPGFueT4pID0+IHtcbiAgICBjb25zdCBwcm9taXNlczogUHJvbWlzZTx2b2lkPltdID0gW107XG4gICAgaWYgKGNvbXBvbmVudC50ZW1wbGF0ZVVybCkge1xuICAgICAgcHJvbWlzZXMucHVzaChjYWNoZWRSZXNvdXJjZVJlc29sdmUoY29tcG9uZW50LnRlbXBsYXRlVXJsKS50aGVuKCh0ZW1wbGF0ZSkgPT4ge1xuICAgICAgICBjb21wb25lbnQudGVtcGxhdGUgPSB0ZW1wbGF0ZTtcbiAgICAgIH0pKTtcbiAgICB9XG4gICAgY29uc3Qgc3R5bGVVcmxzID0gY29tcG9uZW50LnN0eWxlVXJscztcbiAgICBjb25zdCBzdHlsZXMgPSBjb21wb25lbnQuc3R5bGVzIHx8IChjb21wb25lbnQuc3R5bGVzID0gW10pO1xuICAgIGNvbnN0IHN0eWxlT2Zmc2V0ID0gY29tcG9uZW50LnN0eWxlcy5sZW5ndGg7XG4gICAgc3R5bGVVcmxzICYmIHN0eWxlVXJscy5mb3JFYWNoKChzdHlsZVVybCwgaW5kZXgpID0+IHtcbiAgICAgIHN0eWxlcy5wdXNoKCcnKTsgIC8vIHByZS1hbGxvY2F0ZSBhcnJheS5cbiAgICAgIHByb21pc2VzLnB1c2goY2FjaGVkUmVzb3VyY2VSZXNvbHZlKHN0eWxlVXJsKS50aGVuKChzdHlsZSkgPT4ge1xuICAgICAgICBzdHlsZXNbc3R5bGVPZmZzZXQgKyBpbmRleF0gPSBzdHlsZTtcbiAgICAgICAgc3R5bGVVcmxzLnNwbGljZShzdHlsZVVybHMuaW5kZXhPZihzdHlsZVVybCksIDEpO1xuICAgICAgICBpZiAoc3R5bGVVcmxzLmxlbmd0aCA9PSAwKSB7XG4gICAgICAgICAgY29tcG9uZW50LnN0eWxlVXJscyA9IHVuZGVmaW5lZDtcbiAgICAgICAgfVxuICAgICAgfSkpO1xuICAgIH0pO1xuICAgIGNvbnN0IGZ1bGx5UmVzb2x2ZWQgPSBQcm9taXNlLmFsbChwcm9taXNlcykudGhlbigoKSA9PiBjb21wb25lbnREZWZSZXNvbHZlZCh0eXBlKSk7XG4gICAgY29tcG9uZW50UmVzb2x2ZWQucHVzaChmdWxseVJlc29sdmVkKTtcbiAgfSk7XG4gIGNsZWFyUmVzb2x1dGlvbk9mQ29tcG9uZW50UmVzb3VyY2VzUXVldWUoKTtcbiAgcmV0dXJuIFByb21pc2UuYWxsKGNvbXBvbmVudFJlc29sdmVkKS50aGVuKCgpID0+IHVuZGVmaW5lZCk7XG59XG5cbmxldCBjb21wb25lbnRSZXNvdXJjZVJlc29sdXRpb25RdWV1ZSA9IG5ldyBNYXA8VHlwZTxhbnk+LCBDb21wb25lbnQ+KCk7XG5cbi8vIFRyYWNrIHdoZW4gZXhpc3RpbmcgbmdDb21wb25lbnREZWYgZm9yIGEgVHlwZSBpcyB3YWl0aW5nIG9uIHJlc291cmNlcy5cbmNvbnN0IGNvbXBvbmVudERlZlBlbmRpbmdSZXNvbHV0aW9uID0gbmV3IFNldDxUeXBlPGFueT4+KCk7XG5cbmV4cG9ydCBmdW5jdGlvbiBtYXliZVF1ZXVlUmVzb2x1dGlvbk9mQ29tcG9uZW50UmVzb3VyY2VzKHR5cGU6IFR5cGU8YW55PiwgbWV0YWRhdGE6IENvbXBvbmVudCkge1xuICBpZiAoY29tcG9uZW50TmVlZHNSZXNvbHV0aW9uKG1ldGFkYXRhKSkge1xuICAgIGNvbXBvbmVudFJlc291cmNlUmVzb2x1dGlvblF1ZXVlLnNldCh0eXBlLCBtZXRhZGF0YSk7XG4gICAgY29tcG9uZW50RGVmUGVuZGluZ1Jlc29sdXRpb24uYWRkKHR5cGUpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0NvbXBvbmVudERlZlBlbmRpbmdSZXNvbHV0aW9uKHR5cGU6IFR5cGU8YW55Pik6IGJvb2xlYW4ge1xuICByZXR1cm4gY29tcG9uZW50RGVmUGVuZGluZ1Jlc29sdXRpb24uaGFzKHR5cGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY29tcG9uZW50TmVlZHNSZXNvbHV0aW9uKGNvbXBvbmVudDogQ29tcG9uZW50KTogYm9vbGVhbiB7XG4gIHJldHVybiAhIShcbiAgICAgIChjb21wb25lbnQudGVtcGxhdGVVcmwgJiYgIWNvbXBvbmVudC5oYXNPd25Qcm9wZXJ0eSgndGVtcGxhdGUnKSkgfHxcbiAgICAgIGNvbXBvbmVudC5zdHlsZVVybHMgJiYgY29tcG9uZW50LnN0eWxlVXJscy5sZW5ndGgpO1xufVxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFyUmVzb2x1dGlvbk9mQ29tcG9uZW50UmVzb3VyY2VzUXVldWUoKTogTWFwPFR5cGU8YW55PiwgQ29tcG9uZW50PiB7XG4gIGNvbnN0IG9sZCA9IGNvbXBvbmVudFJlc291cmNlUmVzb2x1dGlvblF1ZXVlO1xuICBjb21wb25lbnRSZXNvdXJjZVJlc29sdXRpb25RdWV1ZSA9IG5ldyBNYXAoKTtcbiAgcmV0dXJuIG9sZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlc3RvcmVDb21wb25lbnRSZXNvbHV0aW9uUXVldWUocXVldWU6IE1hcDxUeXBlPGFueT4sIENvbXBvbmVudD4pOiB2b2lkIHtcbiAgY29tcG9uZW50RGVmUGVuZGluZ1Jlc29sdXRpb24uY2xlYXIoKTtcbiAgcXVldWUuZm9yRWFjaCgoXywgdHlwZSkgPT4gY29tcG9uZW50RGVmUGVuZGluZ1Jlc29sdXRpb24uYWRkKHR5cGUpKTtcbiAgY29tcG9uZW50UmVzb3VyY2VSZXNvbHV0aW9uUXVldWUgPSBxdWV1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQ29tcG9uZW50UmVzb3VyY2VSZXNvbHV0aW9uUXVldWVFbXB0eSgpIHtcbiAgcmV0dXJuIGNvbXBvbmVudFJlc291cmNlUmVzb2x1dGlvblF1ZXVlLnNpemUgPT09IDA7XG59XG5cbmZ1bmN0aW9uIHVud3JhcFJlc3BvbnNlKHJlc3BvbnNlOiBzdHJpbmcgfCB7dGV4dCgpOiBQcm9taXNlPHN0cmluZz59KTogc3RyaW5nfFByb21pc2U8c3RyaW5nPiB7XG4gIHJldHVybiB0eXBlb2YgcmVzcG9uc2UgPT0gJ3N0cmluZycgPyByZXNwb25zZSA6IHJlc3BvbnNlLnRleHQoKTtcbn1cblxuZnVuY3Rpb24gY29tcG9uZW50RGVmUmVzb2x2ZWQodHlwZTogVHlwZTxhbnk+KTogdm9pZCB7XG4gIGNvbXBvbmVudERlZlBlbmRpbmdSZXNvbHV0aW9uLmRlbGV0ZSh0eXBlKTtcbn0iXX0=