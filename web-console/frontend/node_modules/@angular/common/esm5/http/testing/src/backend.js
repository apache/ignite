/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
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
var HttpClientTestingBackend = /** @class */ (function () {
    function HttpClientTestingBackend() {
        /**
         * List of pending requests which have not yet been expected.
         */
        this.open = [];
    }
    /**
     * Handle an incoming request by queueing it in the list of open requests.
     */
    HttpClientTestingBackend.prototype.handle = function (req) {
        var _this = this;
        return new Observable(function (observer) {
            var testReq = new TestRequest(req, observer);
            _this.open.push(testReq);
            observer.next({ type: HttpEventType.Sent });
            return function () { testReq._cancelled = true; };
        });
    };
    /**
     * Helper function to search for requests in the list of open requests.
     */
    HttpClientTestingBackend.prototype._match = function (match) {
        if (typeof match === 'string') {
            return this.open.filter(function (testReq) { return testReq.request.urlWithParams === match; });
        }
        else if (typeof match === 'function') {
            return this.open.filter(function (testReq) { return match(testReq.request); });
        }
        else {
            return this.open.filter(function (testReq) { return (!match.method || testReq.request.method === match.method.toUpperCase()) &&
                (!match.url || testReq.request.urlWithParams === match.url); });
        }
    };
    /**
     * Search for requests in the list of open requests, and return all that match
     * without asserting anything about the number of matches.
     */
    HttpClientTestingBackend.prototype.match = function (match) {
        var _this = this;
        var results = this._match(match);
        results.forEach(function (result) {
            var index = _this.open.indexOf(result);
            if (index !== -1) {
                _this.open.splice(index, 1);
            }
        });
        return results;
    };
    /**
     * Expect that a single outstanding request matches the given matcher, and return
     * it.
     *
     * Requests returned through this API will no longer be in the list of open requests,
     * and thus will not match twice.
     */
    HttpClientTestingBackend.prototype.expectOne = function (match, description) {
        description = description || this.descriptionFromMatcher(match);
        var matches = this.match(match);
        if (matches.length > 1) {
            throw new Error("Expected one matching request for criteria \"" + description + "\", found " + matches.length + " requests.");
        }
        if (matches.length === 0) {
            throw new Error("Expected one matching request for criteria \"" + description + "\", found none.");
        }
        return matches[0];
    };
    /**
     * Expect that no outstanding requests match the given matcher, and throw an error
     * if any do.
     */
    HttpClientTestingBackend.prototype.expectNone = function (match, description) {
        description = description || this.descriptionFromMatcher(match);
        var matches = this.match(match);
        if (matches.length > 0) {
            throw new Error("Expected zero matching requests for criteria \"" + description + "\", found " + matches.length + ".");
        }
    };
    /**
     * Validate that there are no outstanding requests.
     */
    HttpClientTestingBackend.prototype.verify = function (opts) {
        if (opts === void 0) { opts = {}; }
        var open = this.open;
        // It's possible that some requests may be cancelled, and this is expected.
        // The user can ask to ignore open requests which have been cancelled.
        if (opts.ignoreCancelled) {
            open = open.filter(function (testReq) { return !testReq.cancelled; });
        }
        if (open.length > 0) {
            // Show the methods and URLs of open requests in the error, for convenience.
            var requests = open.map(function (testReq) {
                var url = testReq.request.urlWithParams.split('?')[0];
                var method = testReq.request.method;
                return method + " " + url;
            })
                .join(', ');
            throw new Error("Expected no open requests, found " + open.length + ": " + requests);
        }
    };
    HttpClientTestingBackend.prototype.descriptionFromMatcher = function (matcher) {
        if (typeof matcher === 'string') {
            return "Match URL: " + matcher;
        }
        else if (typeof matcher === 'object') {
            var method = matcher.method || '(any)';
            var url = matcher.url || '(any)';
            return "Match method: " + method + ", URL: " + url;
        }
        else {
            return "Match by function: " + matcher.name;
        }
    };
    HttpClientTestingBackend = tslib_1.__decorate([
        Injectable()
    ], HttpClientTestingBackend);
    return HttpClientTestingBackend;
}());
export { HttpClientTestingBackend };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYmFja2VuZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9odHRwL3Rlc3Rpbmcvc3JjL2JhY2tlbmQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBeUIsYUFBYSxFQUFjLE1BQU0sc0JBQXNCLENBQUM7QUFDeEYsT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUN6QyxPQUFPLEVBQUMsVUFBVSxFQUFXLE1BQU0sTUFBTSxDQUFDO0FBRzFDLE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFHdEM7Ozs7Ozs7Ozs7R0FVRztBQUVIO0lBREE7UUFFRTs7V0FFRztRQUNLLFNBQUksR0FBa0IsRUFBRSxDQUFDO0lBaUhuQyxDQUFDO0lBL0dDOztPQUVHO0lBQ0gseUNBQU0sR0FBTixVQUFPLEdBQXFCO1FBQTVCLGlCQU9DO1FBTkMsT0FBTyxJQUFJLFVBQVUsQ0FBQyxVQUFDLFFBQXVCO1lBQzVDLElBQU0sT0FBTyxHQUFHLElBQUksV0FBVyxDQUFDLEdBQUcsRUFBRSxRQUFRLENBQUMsQ0FBQztZQUMvQyxLQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQztZQUN4QixRQUFRLENBQUMsSUFBSSxDQUFDLEVBQUUsSUFBSSxFQUFFLGFBQWEsQ0FBQyxJQUFJLEVBQW9CLENBQUMsQ0FBQztZQUM5RCxPQUFPLGNBQVEsT0FBTyxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDOUMsQ0FBQyxDQUFDLENBQUM7SUFDTCxDQUFDO0lBRUQ7O09BRUc7SUFDSyx5Q0FBTSxHQUFkLFVBQWUsS0FBK0Q7UUFDNUUsSUFBSSxPQUFPLEtBQUssS0FBSyxRQUFRLEVBQUU7WUFDN0IsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLE1B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QUFDO0NBQUEsQUFySEQsSUFxSEM7U0FySFksd0JBQXdCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0h0dHBCYWNrZW5kLCBIdHRwRXZlbnQsIEh0dHBFdmVudFR5cGUsIEh0dHBSZXF1ZXN0fSBmcm9tICdAYW5ndWxhci9jb21tb24vaHR0cCc7XG5pbXBvcnQge0luamVjdGFibGV9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtPYnNlcnZhYmxlLCBPYnNlcnZlcn0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7SHR0cFRlc3RpbmdDb250cm9sbGVyLCBSZXF1ZXN0TWF0Y2h9IGZyb20gJy4vYXBpJztcbmltcG9ydCB7VGVzdFJlcXVlc3R9IGZyb20gJy4vcmVxdWVzdCc7XG5cblxuLyoqXG4gKiBBIHRlc3RpbmcgYmFja2VuZCBmb3IgYEh0dHBDbGllbnRgIHdoaWNoIGJvdGggYWN0cyBhcyBhbiBgSHR0cEJhY2tlbmRgXG4gKiBhbmQgYXMgdGhlIGBIdHRwVGVzdGluZ0NvbnRyb2xsZXJgLlxuICpcbiAqIGBIdHRwQ2xpZW50VGVzdGluZ0JhY2tlbmRgIHdvcmtzIGJ5IGtlZXBpbmcgYSBsaXN0IG9mIGFsbCBvcGVuIHJlcXVlc3RzLlxuICogQXMgcmVxdWVzdHMgY29tZSBpbiwgdGhleSdyZSBhZGRlZCB0byB0aGUgbGlzdC4gVXNlcnMgY2FuIGFzc2VydCB0aGF0IHNwZWNpZmljXG4gKiByZXF1ZXN0cyB3ZXJlIG1hZGUgYW5kIHRoZW4gZmx1c2ggdGhlbS4gSW4gdGhlIGVuZCwgYSB2ZXJpZnkoKSBtZXRob2QgYXNzZXJ0c1xuICogdGhhdCBubyB1bmV4cGVjdGVkIHJlcXVlc3RzIHdlcmUgbWFkZS5cbiAqXG4gKlxuICovXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgSHR0cENsaWVudFRlc3RpbmdCYWNrZW5kIGltcGxlbWVudHMgSHR0cEJhY2tlbmQsIEh0dHBUZXN0aW5nQ29udHJvbGxlciB7XG4gIC8qKlxuICAgKiBMaXN0IG9mIHBlbmRpbmcgcmVxdWVzdHMgd2hpY2ggaGF2ZSBub3QgeWV0IGJlZW4gZXhwZWN0ZWQuXG4gICAqL1xuICBwcml2YXRlIG9wZW46IFRlc3RSZXF1ZXN0W10gPSBbXTtcblxuICAvKipcbiAgICogSGFuZGxlIGFuIGluY29taW5nIHJlcXVlc3QgYnkgcXVldWVpbmcgaXQgaW4gdGhlIGxpc3Qgb2Ygb3BlbiByZXF1ZXN0cy5cbiAgICovXG4gIGhhbmRsZShyZXE6IEh0dHBSZXF1ZXN0PGFueT4pOiBPYnNlcnZhYmxlPEh0dHBFdmVudDxhbnk+PiB7XG4gICAgcmV0dXJuIG5ldyBPYnNlcnZhYmxlKChvYnNlcnZlcjogT2JzZXJ2ZXI8YW55PikgPT4ge1xuICAgICAgY29uc3QgdGVzdFJlcSA9IG5ldyBUZXN0UmVxdWVzdChyZXEsIG9ic2VydmVyKTtcbiAgICAgIHRoaXMub3Blbi5wdXNoKHRlc3RSZXEpO1xuICAgICAgb2JzZXJ2ZXIubmV4dCh7IHR5cGU6IEh0dHBFdmVudFR5cGUuU2VudCB9IGFzIEh0dHBFdmVudDxhbnk+KTtcbiAgICAgIHJldHVybiAoKSA9PiB7IHRlc3RSZXEuX2NhbmNlbGxlZCA9IHRydWU7IH07XG4gICAgfSk7XG4gIH1cblxuICAvKipcbiAgICogSGVscGVyIGZ1bmN0aW9uIHRvIHNlYXJjaCBmb3IgcmVxdWVzdHMgaW4gdGhlIGxpc3Qgb2Ygb3BlbiByZXF1ZXN0cy5cbiAgICovXG4gIHByaXZhdGUgX21hdGNoKG1hdGNoOiBzdHJpbmd8UmVxdWVzdE1hdGNofCgocmVxOiBIdHRwUmVxdWVzdDxhbnk+KSA9PiBib29sZWFuKSk6IFRlc3RSZXF1ZXN0W10ge1xuICAgIGlmICh0eXBlb2YgbWF0Y2ggPT09ICdzdHJpbmcnKSB7XG4gICAgICByZXR1cm4gdGhpcy5vcGVuLmZpbHRlcih0ZXN0UmVxID0+IHRlc3RSZXEucmVxdWVzdC51cmxXaXRoUGFyYW1zID09PSBtYXRjaCk7XG4gICAgfSBlbHNlIGlmICh0eXBlb2YgbWF0Y2ggPT09ICdmdW5jdGlvbicpIHtcbiAgICAgIHJldHVybiB0aGlzLm9wZW4uZmlsdGVyKHRlc3RSZXEgPT4gbWF0Y2godGVzdFJlcS5yZXF1ZXN0KSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB0aGlzLm9wZW4uZmlsdGVyKFxuICAgICAgICAgIHRlc3RSZXEgPT4gKCFtYXRjaC5tZXRob2QgfHwgdGVzdFJlcS5yZXF1ZXN0Lm1ldGhvZCA9PT0gbWF0Y2gubWV0aG9kLnRvVXBwZXJDYXNlKCkpICYmXG4gICAgICAgICAgICAgICghbWF0Y2gudXJsIHx8IHRlc3RSZXEucmVxdWVzdC51cmxXaXRoUGFyYW1zID09PSBtYXRjaC51cmwpKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogU2VhcmNoIGZvciByZXF1ZXN0cyBpbiB0aGUgbGlzdCBvZiBvcGVuIHJlcXVlc3RzLCBhbmQgcmV0dXJuIGFsbCB0aGF0IG1hdGNoXG4gICAqIHdpdGhvdXQgYXNzZXJ0aW5nIGFueXRoaW5nIGFib3V0IHRoZSBudW1iZXIgb2YgbWF0Y2hlcy5cbiAgICovXG4gIG1hdGNoKG1hdGNoOiBzdHJpbmd8UmVxdWVzdE1hdGNofCgocmVxOiBIdHRwUmVxdWVzdDxhbnk+KSA9PiBib29sZWFuKSk6IFRlc3RSZXF1ZXN0W10ge1xuICAgIGNvbnN0IHJlc3VsdHMgPSB0aGlzLl9tYXRjaChtYXRjaCk7XG4gICAgcmVzdWx0cy5mb3JFYWNoKHJlc3VsdCA9PiB7XG4gICAgICBjb25zdCBpbmRleCA9IHRoaXMub3Blbi5pbmRleE9mKHJlc3VsdCk7XG4gICAgICBpZiAoaW5kZXggIT09IC0xKSB7XG4gICAgICAgIHRoaXMub3Blbi5zcGxpY2UoaW5kZXgsIDEpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIHJldHVybiByZXN1bHRzO1xuICB9XG5cbiAgLyoqXG4gICAqIEV4cGVjdCB0aGF0IGEgc2luZ2xlIG91dHN0YW5kaW5nIHJlcXVlc3QgbWF0Y2hlcyB0aGUgZ2l2ZW4gbWF0Y2hlciwgYW5kIHJldHVyblxuICAgKiBpdC5cbiAgICpcbiAgICogUmVxdWVzdHMgcmV0dXJuZWQgdGhyb3VnaCB0aGlzIEFQSSB3aWxsIG5vIGxvbmdlciBiZSBpbiB0aGUgbGlzdCBvZiBvcGVuIHJlcXVlc3RzLFxuICAgKiBhbmQgdGh1cyB3aWxsIG5vdCBtYXRjaCB0d2ljZS5cbiAgICovXG4gIGV4cGVjdE9uZShtYXRjaDogc3RyaW5nfFJlcXVlc3RNYXRjaHwoKHJlcTogSHR0cFJlcXVlc3Q8YW55PikgPT4gYm9vbGVhbiksIGRlc2NyaXB0aW9uPzogc3RyaW5nKTpcbiAgICAgIFRlc3RSZXF1ZXN0IHtcbiAgICBkZXNjcmlwdGlvbiA9IGRlc2NyaXB0aW9uIHx8IHRoaXMuZGVzY3JpcHRpb25Gcm9tTWF0Y2hlcihtYXRjaCk7XG4gICAgY29uc3QgbWF0Y2hlcyA9IHRoaXMubWF0Y2gobWF0Y2gpO1xuICAgIGlmIChtYXRjaGVzLmxlbmd0aCA+IDEpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgRXhwZWN0ZWQgb25lIG1hdGNoaW5nIHJlcXVlc3QgZm9yIGNyaXRlcmlhIFwiJHtkZXNjcmlwdGlvbn1cIiwgZm91bmQgJHttYXRjaGVzLmxlbmd0aH0gcmVxdWVzdHMuYCk7XG4gICAgfVxuICAgIGlmIChtYXRjaGVzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBFeHBlY3RlZCBvbmUgbWF0Y2hpbmcgcmVxdWVzdCBmb3IgY3JpdGVyaWEgXCIke2Rlc2NyaXB0aW9ufVwiLCBmb3VuZCBub25lLmApO1xuICAgIH1cbiAgICByZXR1cm4gbWF0Y2hlc1swXTtcbiAgfVxuXG4gIC8qKlxuICAgKiBFeHBlY3QgdGhhdCBubyBvdXRzdGFuZGluZyByZXF1ZXN0cyBtYXRjaCB0aGUgZ2l2ZW4gbWF0Y2hlciwgYW5kIHRocm93IGFuIGVycm9yXG4gICAqIGlmIGFueSBkby5cbiAgICovXG4gIGV4cGVjdE5vbmUobWF0Y2g6IHN0cmluZ3xSZXF1ZXN0TWF0Y2h8KChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pLCBkZXNjcmlwdGlvbj86IHN0cmluZyk6XG4gICAgICB2b2lkIHtcbiAgICBkZXNjcmlwdGlvbiA9IGRlc2NyaXB0aW9uIHx8IHRoaXMuZGVzY3JpcHRpb25Gcm9tTWF0Y2hlcihtYXRjaCk7XG4gICAgY29uc3QgbWF0Y2hlcyA9IHRoaXMubWF0Y2gobWF0Y2gpO1xuICAgIGlmIChtYXRjaGVzLmxlbmd0aCA+IDApIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgRXhwZWN0ZWQgemVybyBtYXRjaGluZyByZXF1ZXN0cyBmb3IgY3JpdGVyaWEgXCIke2Rlc2NyaXB0aW9ufVwiLCBmb3VuZCAke21hdGNoZXMubGVuZ3RofS5gKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogVmFsaWRhdGUgdGhhdCB0aGVyZSBhcmUgbm8gb3V0c3RhbmRpbmcgcmVxdWVzdHMuXG4gICAqL1xuICB2ZXJpZnkob3B0czoge2lnbm9yZUNhbmNlbGxlZD86IGJvb2xlYW59ID0ge30pOiB2b2lkIHtcbiAgICBsZXQgb3BlbiA9IHRoaXMub3BlbjtcbiAgICAvLyBJdCdzIHBvc3NpYmxlIHRoYXQgc29tZSByZXF1ZXN0cyBtYXkgYmUgY2FuY2VsbGVkLCBhbmQgdGhpcyBpcyBleHBlY3RlZC5cbiAgICAvLyBUaGUgdXNlciBjYW4gYXNrIHRvIGlnbm9yZSBvcGVuIHJlcXVlc3RzIHdoaWNoIGhhdmUgYmVlbiBjYW5jZWxsZWQuXG4gICAgaWYgKG9wdHMuaWdub3JlQ2FuY2VsbGVkKSB7XG4gICAgICBvcGVuID0gb3Blbi5maWx0ZXIodGVzdFJlcSA9PiAhdGVzdFJlcS5jYW5jZWxsZWQpO1xuICAgIH1cbiAgICBpZiAob3Blbi5sZW5ndGggPiAwKSB7XG4gICAgICAvLyBTaG93IHRoZSBtZXRob2RzIGFuZCBVUkxzIG9mIG9wZW4gcmVxdWVzdHMgaW4gdGhlIGVycm9yLCBmb3IgY29udmVuaWVuY2UuXG4gICAgICBjb25zdCByZXF1ZXN0cyA9IG9wZW4ubWFwKHRlc3RSZXEgPT4ge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb25zdCB1cmwgPSB0ZXN0UmVxLnJlcXVlc3QudXJsV2l0aFBhcmFtcy5zcGxpdCgnPycpWzBdO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb25zdCBtZXRob2QgPSB0ZXN0UmVxLnJlcXVlc3QubWV0aG9kO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gYCR7bWV0aG9kfSAke3VybH1gO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgfSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC5qb2luKCcsICcpO1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBFeHBlY3RlZCBubyBvcGVuIHJlcXVlc3RzLCBmb3VuZCAke29wZW4ubGVuZ3RofTogJHtyZXF1ZXN0c31gKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGRlc2NyaXB0aW9uRnJvbU1hdGNoZXIobWF0Y2hlcjogc3RyaW5nfFJlcXVlc3RNYXRjaHxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICgocmVxOiBIdHRwUmVxdWVzdDxhbnk+KSA9PiBib29sZWFuKSk6IHN0cmluZyB7XG4gICAgaWYgKHR5cGVvZiBtYXRjaGVyID09PSAnc3RyaW5nJykge1xuICAgICAgcmV0dXJuIGBNYXRjaCBVUkw6ICR7bWF0Y2hlcn1gO1xuICAgIH0gZWxzZSBpZiAodHlwZW9mIG1hdGNoZXIgPT09ICdvYmplY3QnKSB7XG4gICAgICBjb25zdCBtZXRob2QgPSBtYXRjaGVyLm1ldGhvZCB8fCAnKGFueSknO1xuICAgICAgY29uc3QgdXJsID0gbWF0Y2hlci51cmwgfHwgJyhhbnkpJztcbiAgICAgIHJldHVybiBgTWF0Y2ggbWV0aG9kOiAke21ldGhvZH0sIFVSTDogJHt1cmx9YDtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGBNYXRjaCBieSBmdW5jdGlvbjogJHttYXRjaGVyLm5hbWV9YDtcbiAgICB9XG4gIH1cbn1cbiJdfQ==