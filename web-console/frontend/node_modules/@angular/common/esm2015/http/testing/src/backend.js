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
import { HttpEventType } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { TestRequest } from './request';
/**
 * A testing backend for `HttpClient` which both acts as an `HttpBackend`
 * and as the `HttpTestingController`.
 *
 * `HttpClientTestingBackend` works by keeping a list of all open requests.
 * As requests come in, they're added to the list. Users can assert that specific
 * requests were made and then flush them. In the end, a verify() method asserts
 * that no unexpected requests were made.
 *
 *
 */
export class HttpClientTestingBackend {
    constructor() {
        /**
         * List of pending requests which have not yet been expected.
         */
        this.open = [];
    }
    /**
     * Handle an incoming request by queueing it in the list of open requests.
     * @param {?} req
     * @return {?}
     */
    handle(req) {
        return new Observable((/**
         * @param {?} observer
         * @return {?}
         */
        (observer) => {
            /** @type {?} */
            const testReq = new TestRequest(req, observer);
            this.open.push(testReq);
            observer.next((/** @type {?} */ ({ type: HttpEventType.Sent })));
            return (/**
             * @return {?}
             */
            () => { testReq._cancelled = true; });
        }));
    }
    /**
     * Helper function to search for requests in the list of open requests.
     * @private
     * @param {?} match
     * @return {?}
     */
    _match(match) {
        if (typeof match === 'string') {
            return this.open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => testReq.request.urlWithParams === match));
        }
        else if (typeof match === 'function') {
            return this.open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => match(testReq.request)));
        }
        else {
            return this.open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => (!match.method || testReq.request.method === match.method.toUpperCase()) &&
                (!match.url || testReq.request.urlWithParams === match.url)));
        }
    }
    /**
     * Search for requests in the list of open requests, and return all that match
     * without asserting anything about the number of matches.
     * @param {?} match
     * @return {?}
     */
    match(match) {
        /** @type {?} */
        const results = this._match(match);
        results.forEach((/**
         * @param {?} result
         * @return {?}
         */
        result => {
            /** @type {?} */
            const index = this.open.indexOf(result);
            if (index !== -1) {
                this.open.splice(index, 1);
            }
        }));
        return results;
    }
    /**
     * Expect that a single outstanding request matches the given matcher, and return
     * it.
     *
     * Requests returned through this API will no longer be in the list of open requests,
     * and thus will not match twice.
     * @param {?} match
     * @param {?=} description
     * @return {?}
     */
    expectOne(match, description) {
        description = description || this.descriptionFromMatcher(match);
        /** @type {?} */
        const matches = this.match(match);
        if (matches.length > 1) {
            throw new Error(`Expected one matching request for criteria "${description}", found ${matches.length} requests.`);
        }
        if (matches.length === 0) {
            throw new Error(`Expected one matching request for criteria "${description}", found none.`);
        }
        return matches[0];
    }
    /**
     * Expect that no outstanding requests match the given matcher, and throw an error
     * if any do.
     * @param {?} match
     * @param {?=} description
     * @return {?}
     */
    expectNone(match, description) {
        description = description || this.descriptionFromMatcher(match);
        /** @type {?} */
        const matches = this.match(match);
        if (matches.length > 0) {
            throw new Error(`Expected zero matching requests for criteria "${description}", found ${matches.length}.`);
        }
    }
    /**
     * Validate that there are no outstanding requests.
     * @param {?=} opts
     * @return {?}
     */
    verify(opts = {}) {
        /** @type {?} */
        let open = this.open;
        // It's possible that some requests may be cancelled, and this is expected.
        // The user can ask to ignore open requests which have been cancelled.
        if (opts.ignoreCancelled) {
            open = open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => !testReq.cancelled));
        }
        if (open.length > 0) {
            // Show the methods and URLs of open requests in the error, for convenience.
            /** @type {?} */
            const requests = open.map((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => {
                /** @type {?} */
                const url = testReq.request.urlWithParams.split('?')[0];
                /** @type {?} */
                const method = testReq.request.method;
                return `${method} ${url}`;
            }))
                .join(', ');
            throw new Error(`Expected no open requests, found ${open.length}: ${requests}`);
        }
    }
    /**
     * @private
     * @param {?} matcher
     * @return {?}
     */
    descriptionFromMatcher(matcher) {
        if (typeof matcher === 'string') {
            return `Match URL: ${matcher}`;
        }
        else if (typeof matcher === 'object') {
            /** @type {?} */
            const method = matcher.method || '(any)';
            /** @type {?} */
            const url = matcher.url || '(any)';
            return `Match method: ${method}, URL: ${url}`;
        }
        else {
            return `Match by function: ${matcher.name}`;
        }
    }
}
HttpClientTestingBackend.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * List of pending requests which have not yet been expected.
     * @type {?}
     * @private
     */
    HttpClientTestingBackend.prototype.open;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYmFja2VuZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9odHRwL3Rlc3Rpbmcvc3JjL2JhY2tlbmQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQXlCLGFBQWEsRUFBYyxNQUFNLHNCQUFzQixDQUFDO0FBQ3hGLE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFDekMsT0FBTyxFQUFDLFVBQVUsRUFBVyxNQUFNLE1BQU0sQ0FBQztBQUcxQyxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0sV0FBVyxDQUFDOzs7Ozs7Ozs7Ozs7QUFldEMsTUFBTSxPQUFPLHdCQUF3QjtJQURyQzs7OztRQUtVLFNBQUksR0FBa0IsRUFBRSxDQUFDO0lBaUhuQyxDQUFDOzs7Ozs7SUE1R0MsTUFBTSxDQUFDLEdBQXFCO1FBQzFCLE9BQU8sSUFBSSxVQUFVOzs7O1FBQUMsQ0FBQyxRQUF1QixFQUFFLEVBQUU7O2tCQUMxQyxPQUFPLEdBQUcsSUFBSSxXQUFXLENBQUMsR0FBRyxFQUFFLFFBQVEsQ0FBQztZQUM5QyxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQztZQUN4QixRQUFRLENBQUMsSUFBSSxDQUFDLG1CQUFBLEVBQUUsSUFBSSxFQUFFLGFBQWEsQ0FBQyxJQUFJLEVBQUUsRUFBa0IsQ0FBQyxDQUFDO1lBQzlEOzs7WUFBTyxHQUFHLEVBQUUsR0FBRyxPQUFPLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsRUFBQztRQUM5QyxDQUFDLEVBQUMsQ0FBQztJQUNMLENBQUM7Ozs7Ozs7SUFLTyxNQUFNLENBQUMsS0FBK0Q7UUFDNUUsSUFBSSxPQUFPLEtBQUssS0FBSyxRQUFRLEVBQUU7WUFDN0IsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU07Ozs7WUFBQyxPQUFPLENBQUMsRUFB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WUFBQyxPQUFPLENBQUMsRUFBRTs7c0JBQ1AsR0FBRyxHQUFHLE9BQU8sQ0FBQyxPQUFPLENBQUMsYUFBYSxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUM7O3NCQUNqRCxNQUFNLEdBQUcsT0FBTyxDQUFDLE9BQU8sQ0FBQyxNQUFNO2dCQUNyQyxPQUFPLEdBQUcsTUFBTSxJQUFJLEdBQUcsRUFBRSxDQUFDO1lBQzVCLENBQUMsRUFBQztpQkFDRCxJQUFJLENBQUMsSUFBSSxDQUFDO1lBQ2hDLE1BQU0sSUFBSSxLQUFLLENBQUMsb0NBQW9DLElBQUksQ0FBQyxNQUFNLEtBQUssUUFBUSxFQUFFLENBQUMsQ0FBQztTQUNqRjtJQUNILENBQUM7Ozs7OztJQUVPLHNCQUFzQixDQUFDLE9BQ29DO1FBQ2pFLElBQUksT0FBTyxPQUFPLEtBQUssUUFBUSxFQUFFO1lBQy9CLE9BQU8sY0FBYyxPQUFPLEVBQUUsQ0FBQztTQUNoQzthQUFNLElBQUksT0FBTyxPQUFPLEtBQUssUUFBUSxFQUFFOztrQkFDaEMsTUFBTSxHQUFHLE9BQU8sQ0FBQyxNQUFNLElBQUksT0FBTzs7a0JBQ2xDLEdBQUcsR0FBRyxPQUFPLENBQUMsR0FBRyxJQUFJLE9BQU87WUFDbEMsT0FBTyxpQkFBaUIsTUFBTSxVQUFVLEdBQUcsRUFBRSxDQUFDO1NBQy9DO2FBQU07WUFDTCxPQUFPLHNCQUFzQixPQUFPLENBQUMsSUFBSSxFQUFFLENBQUM7U0FDN0M7SUFDSCxDQUFDOzs7WUFySEYsVUFBVTs7Ozs7Ozs7SUFLVCx3Q0FBaUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SHR0cEJhY2tlbmQsIEh0dHBFdmVudCwgSHR0cEV2ZW50VHlwZSwgSHR0cFJlcXVlc3R9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbi9odHRwJztcbmltcG9ydCB7SW5qZWN0YWJsZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge09ic2VydmFibGUsIE9ic2VydmVyfSBmcm9tICdyeGpzJztcblxuaW1wb3J0IHtIdHRwVGVzdGluZ0NvbnRyb2xsZXIsIFJlcXVlc3RNYXRjaH0gZnJvbSAnLi9hcGknO1xuaW1wb3J0IHtUZXN0UmVxdWVzdH0gZnJvbSAnLi9yZXF1ZXN0JztcblxuXG4vKipcbiAqIEEgdGVzdGluZyBiYWNrZW5kIGZvciBgSHR0cENsaWVudGAgd2hpY2ggYm90aCBhY3RzIGFzIGFuIGBIdHRwQmFja2VuZGBcbiAqIGFuZCBhcyB0aGUgYEh0dHBUZXN0aW5nQ29udHJvbGxlcmAuXG4gKlxuICogYEh0dHBDbGllbnRUZXN0aW5nQmFja2VuZGAgd29ya3MgYnkga2VlcGluZyBhIGxpc3Qgb2YgYWxsIG9wZW4gcmVxdWVzdHMuXG4gKiBBcyByZXF1ZXN0cyBjb21lIGluLCB0aGV5J3JlIGFkZGVkIHRvIHRoZSBsaXN0LiBVc2VycyBjYW4gYXNzZXJ0IHRoYXQgc3BlY2lmaWNcbiAqIHJlcXVlc3RzIHdlcmUgbWFkZSBhbmQgdGhlbiBmbHVzaCB0aGVtLiBJbiB0aGUgZW5kLCBhIHZlcmlmeSgpIG1ldGhvZCBhc3NlcnRzXG4gKiB0aGF0IG5vIHVuZXhwZWN0ZWQgcmVxdWVzdHMgd2VyZSBtYWRlLlxuICpcbiAqXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBIdHRwQ2xpZW50VGVzdGluZ0JhY2tlbmQgaW1wbGVtZW50cyBIdHRwQmFja2VuZCwgSHR0cFRlc3RpbmdDb250cm9sbGVyIHtcbiAgLyoqXG4gICAqIExpc3Qgb2YgcGVuZGluZyByZXF1ZXN0cyB3aGljaCBoYXZlIG5vdCB5ZXQgYmVlbiBleHBlY3RlZC5cbiAgICovXG4gIHByaXZhdGUgb3BlbjogVGVzdFJlcXVlc3RbXSA9IFtdO1xuXG4gIC8qKlxuICAgKiBIYW5kbGUgYW4gaW5jb21pbmcgcmVxdWVzdCBieSBxdWV1ZWluZyBpdCBpbiB0aGUgbGlzdCBvZiBvcGVuIHJlcXVlc3RzLlxuICAgKi9cbiAgaGFuZGxlKHJlcTogSHR0cFJlcXVlc3Q8YW55Pik6IE9ic2VydmFibGU8SHR0cEV2ZW50PGFueT4+IHtcbiAgICByZXR1cm4gbmV3IE9ic2VydmFibGUoKG9ic2VydmVyOiBPYnNlcnZlcjxhbnk+KSA9PiB7XG4gICAgICBjb25zdCB0ZXN0UmVxID0gbmV3IFRlc3RSZXF1ZXN0KHJlcSwgb2JzZXJ2ZXIpO1xuICAgICAgdGhpcy5vcGVuLnB1c2godGVzdFJlcSk7XG4gICAgICBvYnNlcnZlci5uZXh0KHsgdHlwZTogSHR0cEV2ZW50VHlwZS5TZW50IH0gYXMgSHR0cEV2ZW50PGFueT4pO1xuICAgICAgcmV0dXJuICgpID0+IHsgdGVzdFJlcS5fY2FuY2VsbGVkID0gdHJ1ZTsgfTtcbiAgICB9KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBIZWxwZXIgZnVuY3Rpb24gdG8gc2VhcmNoIGZvciByZXF1ZXN0cyBpbiB0aGUgbGlzdCBvZiBvcGVuIHJlcXVlc3RzLlxuICAgKi9cbiAgcHJpdmF0ZSBfbWF0Y2gobWF0Y2g6IHN0cmluZ3xSZXF1ZXN0TWF0Y2h8KChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pKTogVGVzdFJlcXVlc3RbXSB7XG4gICAgaWYgKHR5cGVvZiBtYXRjaCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHJldHVybiB0aGlzLm9wZW4uZmlsdGVyKHRlc3RSZXEgPT4gdGVzdFJlcS5yZXF1ZXN0LnVybFdpdGhQYXJhbXMgPT09IG1hdGNoKTtcbiAgICB9IGVsc2UgaWYgKHR5cGVvZiBtYXRjaCA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgcmV0dXJuIHRoaXMub3Blbi5maWx0ZXIodGVzdFJlcSA9PiBtYXRjaCh0ZXN0UmVxLnJlcXVlc3QpKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMub3Blbi5maWx0ZXIoXG4gICAgICAgICAgdGVzdFJlcSA9PiAoIW1hdGNoLm1ldGhvZCB8fCB0ZXN0UmVxLnJlcXVlc3QubWV0aG9kID09PSBtYXRjaC5tZXRob2QudG9VcHBlckNhc2UoKSkgJiZcbiAgICAgICAgICAgICAgKCFtYXRjaC51cmwgfHwgdGVzdFJlcS5yZXF1ZXN0LnVybFdpdGhQYXJhbXMgPT09IG1hdGNoLnVybCkpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBTZWFyY2ggZm9yIHJlcXVlc3RzIGluIHRoZSBsaXN0IG9mIG9wZW4gcmVxdWVzdHMsIGFuZCByZXR1cm4gYWxsIHRoYXQgbWF0Y2hcbiAgICogd2l0aG91dCBhc3NlcnRpbmcgYW55dGhpbmcgYWJvdXQgdGhlIG51bWJlciBvZiBtYXRjaGVzLlxuICAgKi9cbiAgbWF0Y2gobWF0Y2g6IHN0cmluZ3xSZXF1ZXN0TWF0Y2h8KChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pKTogVGVzdFJlcXVlc3RbXSB7XG4gICAgY29uc3QgcmVzdWx0cyA9IHRoaXMuX21hdGNoKG1hdGNoKTtcbiAgICByZXN1bHRzLmZvckVhY2gocmVzdWx0ID0+IHtcbiAgICAgIGNvbnN0IGluZGV4ID0gdGhpcy5vcGVuLmluZGV4T2YocmVzdWx0KTtcbiAgICAgIGlmIChpbmRleCAhPT0gLTEpIHtcbiAgICAgICAgdGhpcy5vcGVuLnNwbGljZShpbmRleCwgMSk7XG4gICAgICB9XG4gICAgfSk7XG4gICAgcmV0dXJuIHJlc3VsdHM7XG4gIH1cblxuICAvKipcbiAgICogRXhwZWN0IHRoYXQgYSBzaW5nbGUgb3V0c3RhbmRpbmcgcmVxdWVzdCBtYXRjaGVzIHRoZSBnaXZlbiBtYXRjaGVyLCBhbmQgcmV0dXJuXG4gICAqIGl0LlxuICAgKlxuICAgKiBSZXF1ZXN0cyByZXR1cm5lZCB0aHJvdWdoIHRoaXMgQVBJIHdpbGwgbm8gbG9uZ2VyIGJlIGluIHRoZSBsaXN0IG9mIG9wZW4gcmVxdWVzdHMsXG4gICAqIGFuZCB0aHVzIHdpbGwgbm90IG1hdGNoIHR3aWNlLlxuICAgKi9cbiAgZXhwZWN0T25lKG1hdGNoOiBzdHJpbmd8UmVxdWVzdE1hdGNofCgocmVxOiBIdHRwUmVxdWVzdDxhbnk+KSA9PiBib29sZWFuKSwgZGVzY3JpcHRpb24/OiBzdHJpbmcpOlxuICAgICAgVGVzdFJlcXVlc3Qge1xuICAgIGRlc2NyaXB0aW9uID0gZGVzY3JpcHRpb24gfHwgdGhpcy5kZXNjcmlwdGlvbkZyb21NYXRjaGVyKG1hdGNoKTtcbiAgICBjb25zdCBtYXRjaGVzID0gdGhpcy5tYXRjaChtYXRjaCk7XG4gICAgaWYgKG1hdGNoZXMubGVuZ3RoID4gMSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBFeHBlY3RlZCBvbmUgbWF0Y2hpbmcgcmVxdWVzdCBmb3IgY3JpdGVyaWEgXCIke2Rlc2NyaXB0aW9ufVwiLCBmb3VuZCAke21hdGNoZXMubGVuZ3RofSByZXF1ZXN0cy5gKTtcbiAgICB9XG4gICAgaWYgKG1hdGNoZXMubGVuZ3RoID09PSAwKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYEV4cGVjdGVkIG9uZSBtYXRjaGluZyByZXF1ZXN0IGZvciBjcml0ZXJpYSBcIiR7ZGVzY3JpcHRpb259XCIsIGZvdW5kIG5vbmUuYCk7XG4gICAgfVxuICAgIHJldHVybiBtYXRjaGVzWzBdO1xuICB9XG5cbiAgLyoqXG4gICAqIEV4cGVjdCB0aGF0IG5vIG91dHN0YW5kaW5nIHJlcXVlc3RzIG1hdGNoIHRoZSBnaXZlbiBtYXRjaGVyLCBhbmQgdGhyb3cgYW4gZXJyb3JcbiAgICogaWYgYW55IGRvLlxuICAgKi9cbiAgZXhwZWN0Tm9uZShtYXRjaDogc3RyaW5nfFJlcXVlc3RNYXRjaHwoKHJlcTogSHR0cFJlcXVlc3Q8YW55PikgPT4gYm9vbGVhbiksIGRlc2NyaXB0aW9uPzogc3RyaW5nKTpcbiAgICAgIHZvaWQge1xuICAgIGRlc2NyaXB0aW9uID0gZGVzY3JpcHRpb24gfHwgdGhpcy5kZXNjcmlwdGlvbkZyb21NYXRjaGVyKG1hdGNoKTtcbiAgICBjb25zdCBtYXRjaGVzID0gdGhpcy5tYXRjaChtYXRjaCk7XG4gICAgaWYgKG1hdGNoZXMubGVuZ3RoID4gMCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBFeHBlY3RlZCB6ZXJvIG1hdGNoaW5nIHJlcXVlc3RzIGZvciBjcml0ZXJpYSBcIiR7ZGVzY3JpcHRpb259XCIsIGZvdW5kICR7bWF0Y2hlcy5sZW5ndGh9LmApO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBWYWxpZGF0ZSB0aGF0IHRoZXJlIGFyZSBubyBvdXRzdGFuZGluZyByZXF1ZXN0cy5cbiAgICovXG4gIHZlcmlmeShvcHRzOiB7aWdub3JlQ2FuY2VsbGVkPzogYm9vbGVhbn0gPSB7fSk6IHZvaWQge1xuICAgIGxldCBvcGVuID0gdGhpcy5vcGVuO1xuICAgIC8vIEl0J3MgcG9zc2libGUgdGhhdCBzb21lIHJlcXVlc3RzIG1heSBiZSBjYW5jZWxsZWQsIGFuZCB0aGlzIGlzIGV4cGVjdGVkLlxuICAgIC8vIFRoZSB1c2VyIGNhbiBhc2sgdG8gaWdub3JlIG9wZW4gcmVxdWVzdHMgd2hpY2ggaGF2ZSBiZWVuIGNhbmNlbGxlZC5cbiAgICBpZiAob3B0cy5pZ25vcmVDYW5jZWxsZWQpIHtcbiAgICAgIG9wZW4gPSBvcGVuLmZpbHRlcih0ZXN0UmVxID0+ICF0ZXN0UmVxLmNhbmNlbGxlZCk7XG4gICAgfVxuICAgIGlmIChvcGVuLmxlbmd0aCA+IDApIHtcbiAgICAgIC8vIFNob3cgdGhlIG1ldGhvZHMgYW5kIFVSTHMgb2Ygb3BlbiByZXF1ZXN0cyBpbiB0aGUgZXJyb3IsIGZvciBjb252ZW5pZW5jZS5cbiAgICAgIGNvbnN0IHJlcXVlc3RzID0gb3Blbi5tYXAodGVzdFJlcSA9PiB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnN0IHVybCA9IHRlc3RSZXEucmVxdWVzdC51cmxXaXRoUGFyYW1zLnNwbGl0KCc/JylbMF07XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnN0IG1ldGhvZCA9IHRlc3RSZXEucmVxdWVzdC5tZXRob2Q7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBgJHttZXRob2R9ICR7dXJsfWA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICB9KVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgLmpvaW4oJywgJyk7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYEV4cGVjdGVkIG5vIG9wZW4gcmVxdWVzdHMsIGZvdW5kICR7b3Blbi5sZW5ndGh9OiAke3JlcXVlc3RzfWApO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZGVzY3JpcHRpb25Gcm9tTWF0Y2hlcihtYXRjaGVyOiBzdHJpbmd8UmVxdWVzdE1hdGNofFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgKChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pKTogc3RyaW5nIHtcbiAgICBpZiAodHlwZW9mIG1hdGNoZXIgPT09ICdzdHJpbmcnKSB7XG4gICAgICByZXR1cm4gYE1hdGNoIFVSTDogJHttYXRjaGVyfWA7XG4gICAgfSBlbHNlIGlmICh0eXBlb2YgbWF0Y2hlciA9PT0gJ29iamVjdCcpIHtcbiAgICAgIGNvbnN0IG1ldGhvZCA9IG1hdGNoZXIubWV0aG9kIHx8ICcoYW55KSc7XG4gICAgICBjb25zdCB1cmwgPSBtYXRjaGVyLnVybCB8fCAnKGFueSknO1xuICAgICAgcmV0dXJuIGBNYXRjaCBtZXRob2Q6ICR7bWV0aG9kfSwgVVJMOiAke3VybH1gO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gYE1hdGNoIGJ5IGZ1bmN0aW9uOiAke21hdGNoZXIubmFtZX1gO1xuICAgIH1cbiAgfVxufVxuIl19