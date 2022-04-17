/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { HttpHeaders } from './headers';
/**
 * Type enumeration for the different kinds of `HttpEvent`.
 *
 * @publicApi
 */
export var HttpEventType;
(function (HttpEventType) {
    /**
     * The request was sent out over the wire.
     */
    HttpEventType[HttpEventType["Sent"] = 0] = "Sent";
    /**
     * An upload progress event was received.
     */
    HttpEventType[HttpEventType["UploadProgress"] = 1] = "UploadProgress";
    /**
     * The response status code and headers were received.
     */
    HttpEventType[HttpEventType["ResponseHeader"] = 2] = "ResponseHeader";
    /**
     * A download progress event was received.
     */
    HttpEventType[HttpEventType["DownloadProgress"] = 3] = "DownloadProgress";
    /**
     * The full response including the body was received.
     */
    HttpEventType[HttpEventType["Response"] = 4] = "Response";
    /**
     * A custom event from an interceptor or a backend.
     */
    HttpEventType[HttpEventType["User"] = 5] = "User";
})(HttpEventType || (HttpEventType = {}));
/**
 * Base class for both `HttpResponse` and `HttpHeaderResponse`.
 *
 * @publicApi
 */
var HttpResponseBase = /** @class */ (function () {
    /**
     * Super-constructor for all responses.
     *
     * The single parameter accepted is an initialization hash. Any properties
     * of the response passed there will override the default values.
     */
    function HttpResponseBase(init, defaultStatus, defaultStatusText) {
        if (defaultStatus === void 0) { defaultStatus = 200; }
        if (defaultStatusText === void 0) { defaultStatusText = 'OK'; }
        // If the hash has values passed, use them to initialize the response.
        // Otherwise use the default values.
        this.headers = init.headers || new HttpHeaders();
        this.status = init.status !== undefined ? init.status : defaultStatus;
        this.statusText = init.statusText || defaultStatusText;
        this.url = init.url || null;
        // Cache the ok value to avoid defining a getter.
        this.ok = this.status >= 200 && this.status < 300;
    }
    return HttpResponseBase;
}());
export { HttpResponseBase };
/**
 * A partial HTTP response which only includes the status and header data,
 * but no response body.
 *
 * `HttpHeaderResponse` is a `HttpEvent` available on the response
 * event stream, only when progress events are requested.
 *
 * @publicApi
 */
var HttpHeaderResponse = /** @class */ (function (_super) {
    tslib_1.__extends(HttpHeaderResponse, _super);
    /**
     * Create a new `HttpHeaderResponse` with the given parameters.
     */
    function HttpHeaderResponse(init) {
        if (init === void 0) { init = {}; }
        var _this = _super.call(this, init) || this;
        _this.type = HttpEventType.ResponseHeader;
        return _this;
    }
    /**
     * Copy this `HttpHeaderResponse`, overriding its contents with the
     * given parameter hash.
     */
    HttpHeaderResponse.prototype.clone = function (update) {
        if (update === void 0) { update = {}; }
        // Perform a straightforward initialization of the new HttpHeaderResponse,
        // overriding the current parameters with new ones if given.
        return new HttpHeaderResponse({
            headers: update.headers || this.headers,
            status: update.status !== undefined ? update.status : this.status,
            statusText: update.statusText || this.statusText,
            url: update.url || this.url || undefined,
        });
    };
    return HttpHeaderResponse;
}(HttpResponseBase));
export { HttpHeaderResponse };
/**
 * A full HTTP response, including a typed response body (which may be `null`
 * if one was not returned).
 *
 * `HttpResponse` is a `HttpEvent` available on the response event
 * stream.
 *
 * @publicApi
 */
var HttpResponse = /** @class */ (function (_super) {
    tslib_1.__extends(HttpResponse, _super);
    /**
     * Construct a new `HttpResponse`.
     */
    function HttpResponse(init) {
        if (init === void 0) { init = {}; }
        var _this = _super.call(this, init) || this;
        _this.type = HttpEventType.Response;
        _this.body = init.body !== undefined ? init.body : null;
        return _this;
    }
    HttpResponse.prototype.clone = function (update) {
        if (update === void 0) { update = {}; }
        return new HttpResponse({
            body: (update.body !== undefined) ? update.body : this.body,
            headers: update.headers || this.headers,
            status: (update.status !== undefined) ? update.status : this.status,
            statusText: update.statusText || this.statusText,
            url: update.url || this.url || undefined,
        });
    };
    return HttpResponse;
}(HttpResponseBase));
export { HttpResponse };
/**
 * A response that represents an error or failure, either from a
 * non-successful HTTP status, an error while executing the request,
 * or some other failure which occurred during the parsing of the response.
 *
 * Any error returned on the `Observable` response stream will be
 * wrapped in an `HttpErrorResponse` to provide additional context about
 * the state of the HTTP layer when the error occurred. The error property
 * will contain either a wrapped Error object or the error response returned
 * from the server.
 *
 * @publicApi
 */
var HttpErrorResponse = /** @class */ (function (_super) {
    tslib_1.__extends(HttpErrorResponse, _super);
    function HttpErrorResponse(init) {
        var _this = 
        // Initialize with a default status of 0 / Unknown Error.
        _super.call(this, init, 0, 'Unknown Error') || this;
        _this.name = 'HttpErrorResponse';
        /**
         * Errors are never okay, even when the status code is in the 2xx success range.
         */
        _this.ok = false;
        // If the response was successful, then this was a parse error. Otherwise, it was
        // a protocol-level failure of some sort. Either the request failed in transit
        // or the server returned an unsuccessful status code.
        if (_this.status >= 200 && _this.status < 300) {
            _this.message = "Http failure during parsing for " + (init.url || '(unknown url)');
        }
        else {
            _this.message =
                "Http failure response for " + (init.url || '(unknown url)') + ": " + init.status + " " + init.statusText;
        }
        _this.error = init.error || null;
        return _this;
    }
    return HttpErrorResponse;
}(HttpResponseBase));
export { HttpErrorResponse };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzcG9uc2UuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vaHR0cC9zcmMvcmVzcG9uc2UudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFFdEM7Ozs7R0FJRztBQUNILE1BQU0sQ0FBTixJQUFZLGFBOEJYO0FBOUJELFdBQVksYUFBYTtJQUN2Qjs7T0FFRztJQUNILGlEQUFJLENBQUE7SUFFSjs7T0FFRztJQUNILHFFQUFjLENBQUE7SUFFZDs7T0FFRztJQUNILHFFQUFjLENBQUE7SUFFZDs7T0FFRztJQUNILHlFQUFnQixDQUFBO0lBRWhCOztPQUVHO0lBQ0gseURBQVEsQ0FBQTtJQUVSOztPQUVHO0lBQ0gsaURBQUksQ0FBQTtBQUNOLENBQUMsRUE5QlcsYUFBYSxLQUFiLGFBQWEsUUE4QnhCO0FBNEZEOzs7O0dBSUc7QUFDSDtJQWtDRTs7Ozs7T0FLRztJQUNILDBCQUNJLElBS0MsRUFDRCxhQUEyQixFQUFFLGlCQUFnQztRQUE3RCw4QkFBQSxFQUFBLG1CQUEyQjtRQUFFLGtDQUFBLEVBQUEsd0JBQWdDO1FBQy9ELHNFQUFzRTtRQUN0RSxvQ0FBb0M7UUFDcEMsSUFBSSxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUMsT0FBTyxJQUFJLElBQUksV0FBVyxFQUFFLENBQUM7UUFDakQsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLENBQUMsTUFBTSxLQUFLLFNBQVMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsYUFBYSxDQUFDO1FBQ3RFLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLFVBQVUsSUFBSSxpQkFBaUIsQ0FBQztRQUN2RCxJQUFJLENBQUMsR0FBRyxHQUFHLElBQUksQ0FBQyxHQUFHLElBQUksSUFBSSxD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LEtBQUksQ0FBQyxNQUFNLElBQUksR0FBRyxJQUFJLEtBQUksQ0FBQyxNQUFNLEdBQUcsR0FBRyxFQUFFO1lBQzNDLEtBQUksQ0FBQyxPQUFPLEdBQUcsc0NBQW1DLElBQUksQ0FBQyxHQUFHLElBQUksZUFBZSxDQUFFLENBQUM7U0FDakY7YUFBTTtZQUNMLEtBQUksQ0FBQyxPQUFPO2dCQUNSLGdDQUE2QixJQUFJLENBQUMsR0FBRyxJQUFJLGVBQWUsV0FBSyxJQUFJLENBQUMsTUFBTSxTQUFJLElBQUksQ0FBQyxVQUFZLENBQUM7U0FDbkc7UUFDRCxLQUFJLENBQUMsS0FBSyxHQUFHLElBQUksQ0FBQyxLQUFLLElBQUksSUFBSSxDQUFDOztJQUNsQyxDQUFDO0lBQ0gsd0JBQUM7QUFBRCxDQUFDLEFBM0JELENBQXVDLGdCQUFnQixHQTJCdEQiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SHR0cEhlYWRlcnN9IGZyb20gJy4vaGVhZGVycyc7XG5cbi8qKlxuICogVHlwZSBlbnVtZXJhdGlvbiBmb3IgdGhlIGRpZmZlcmVudCBraW5kcyBvZiBgSHR0cEV2ZW50YC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBlbnVtIEh0dHBFdmVudFR5cGUge1xuICAvKipcbiAgICogVGhlIHJlcXVlc3Qgd2FzIHNlbnQgb3V0IG92ZXIgdGhlIHdpcmUuXG4gICAqL1xuICBTZW50LFxuXG4gIC8qKlxuICAgKiBBbiB1cGxvYWQgcHJvZ3Jlc3MgZXZlbnQgd2FzIHJlY2VpdmVkLlxuICAgKi9cbiAgVXBsb2FkUHJvZ3Jlc3MsXG5cbiAgLyoqXG4gICAqIFRoZSByZXNwb25zZSBzdGF0dXMgY29kZSBhbmQgaGVhZGVycyB3ZXJlIHJlY2VpdmVkLlxuICAgKi9cbiAgUmVzcG9uc2VIZWFkZXIsXG5cbiAgLyoqXG4gICAqIEEgZG93bmxvYWQgcHJvZ3Jlc3MgZXZlbnQgd2FzIHJlY2VpdmVkLlxuICAgKi9cbiAgRG93bmxvYWRQcm9ncmVzcyxcblxuICAvKipcbiAgICogVGhlIGZ1bGwgcmVzcG9uc2UgaW5jbHVkaW5nIHRoZSBib2R5IHdhcyByZWNlaXZlZC5cbiAgICovXG4gIFJlc3BvbnNlLFxuXG4gIC8qKlxuICAgKiBBIGN1c3RvbSBldmVudCBmcm9tIGFuIGludGVyY2VwdG9yIG9yIGEgYmFja2VuZC5cbiAgICovXG4gIFVzZXIsXG59XG5cbi8qKlxuICogQmFzZSBpbnRlcmZhY2UgZm9yIHByb2dyZXNzIGV2ZW50cy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgSHR0cFByb2dyZXNzRXZlbnQge1xuICAvKipcbiAgICogUHJvZ3Jlc3MgZXZlbnQgdHlwZSBpcyBlaXRoZXIgdXBsb2FkIG9yIGRvd25sb2FkLlxuICAgKi9cbiAgdHlwZTogSHR0cEV2ZW50VHlwZS5Eb3dubG9hZFByb2dyZXNzfEh0dHBFdmVudFR5cGUuVXBsb2FkUHJvZ3Jlc3M7XG5cbiAgLyoqXG4gICAqIE51bWJlciBvZiBieXRlcyB1cGxvYWRlZCBvciBkb3dubG9hZGVkLlxuICAgKi9cbiAgbG9hZGVkOiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIFRvdGFsIG51bWJlciBvZiBieXRlcyB0byB1cGxvYWQgb3IgZG93bmxvYWQuIERlcGVuZGluZyBvbiB0aGUgcmVxdWVzdCBvclxuICAgKiByZXNwb25zZSwgdGhpcyBtYXkgbm90IGJlIGNvbXB1dGFibGUgYW5kIHRodXMgbWF5IG5vdCBiZSBwcmVzZW50LlxuICAgKi9cbiAgdG90YWw/OiBudW1iZXI7XG59XG5cbi8qKlxuICogQSBkb3dubG9hZCBwcm9ncmVzcyBldmVudC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgSHR0cERvd25sb2FkUHJvZ3Jlc3NFdmVudCBleHRlbmRzIEh0dHBQcm9ncmVzc0V2ZW50IHtcbiAgdHlwZTogSHR0cEV2ZW50VHlwZS5Eb3dubG9hZFByb2dyZXNzO1xuXG4gIC8qKlxuICAgKiBUaGUgcGFydGlhbCByZXNwb25zZSBib2R5IGFzIGRvd25sb2FkZWQgc28gZmFyLlxuICAgKlxuICAgKiBPbmx5IHByZXNlbnQgaWYgdGhlIHJlc3BvbnNlVHlwZSB3YXMgYHRleHRgLlxuICAgKi9cbiAgcGFydGlhbFRleHQ/OiBzdHJpbmc7XG59XG5cbi8qKlxuICogQW4gdXBsb2FkIHByb2dyZXNzIGV2ZW50LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBIdHRwVXBsb2FkUHJvZ3Jlc3NFdmVudCBleHRlbmRzIEh0dHBQcm9ncmVzc0V2ZW50IHtcbiAgdHlwZTogSHR0cEV2ZW50VHlwZS5VcGxvYWRQcm9ncmVzcztcbn1cblxuLyoqXG4gKiBBbiBldmVudCBpbmRpY2F0aW5nIHRoYXQgdGhlIHJlcXVlc3Qgd2FzIHNlbnQgdG8gdGhlIHNlcnZlci4gVXNlZnVsXG4gKiB3aGVuIGEgcmVxdWVzdCBtYXkgYmUgcmV0cmllZCBtdWx0aXBsZSB0aW1lcywgdG8gZGlzdGluZ3Vpc2ggYmV0d2VlblxuICogcmV0cmllcyBvbiB0aGUgZmluYWwgZXZlbnQgc3RyZWFtLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBIdHRwU2VudEV2ZW50IHsgdHlwZTogSHR0cEV2ZW50VHlwZS5TZW50OyB9XG5cbi8qKlxuICogQSB1c2VyLWRlZmluZWQgZXZlbnQuXG4gKlxuICogR3JvdXBpbmcgYWxsIGN1c3RvbSBldmVudHMgdW5kZXIgdGhpcyB0eXBlIGVuc3VyZXMgdGhleSB3aWxsIGJlIGhhbmRsZWRcbiAqIGFuZCBmb3J3YXJkZWQgYnkgYWxsIGltcGxlbWVudGF0aW9ucyBvZiBpbnRlcmNlcHRvcnMuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgaW50ZXJmYWNlIEh0dHBVc2VyRXZlbnQ8VD4geyB0eXBlOiBIdHRwRXZlbnRUeXBlLlVzZXI7IH1cblxuLyoqXG4gKiBBbiBlcnJvciB0aGF0IHJlcHJlc2VudHMgYSBmYWlsZWQgYXR0ZW1wdCB0byBKU09OLnBhcnNlIHRleHQgY29taW5nIGJhY2tcbiAqIGZyb20gdGhlIHNlcnZlci5cbiAqXG4gKiBJdCBidW5kbGVzIHRoZSBFcnJvciBvYmplY3Qgd2l0aCB0aGUgYWN0dWFsIHJlc3BvbnNlIGJvZHkgdGhhdCBmYWlsZWQgdG8gcGFyc2UuXG4gKlxuICpcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBIdHRwSnNvblBhcnNlRXJyb3Ige1xuICBlcnJvcjogRXJyb3I7XG4gIHRleHQ6IHN0cmluZztcbn1cblxuLyoqXG4gKiBVbmlvbiB0eXBlIGZvciBhbGwgcG9zc2libGUgZXZlbnRzIG9uIHRoZSByZXNwb25zZSBzdHJlYW0uXG4gKlxuICogVHlwZWQgYWNjb3JkaW5nIHRvIHRoZSBleHBlY3RlZCB0eXBlIG9mIHRoZSByZXNwb25zZS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCB0eXBlIEh0dHBFdmVudDxUPiA9XG4gICAgSHR0cFNlbnRFdmVudCB8IEh0dHBIZWFkZXJSZXNwb25zZSB8IEh0dHBSZXNwb25zZTxUPnwgSHR0cFByb2dyZXNzRXZlbnQgfCBIdHRwVXNlckV2ZW50PFQ+O1xuXG4vKipcbiAqIEJhc2UgY2xhc3MgZm9yIGJvdGggYEh0dHBSZXNwb25zZWAgYW5kIGBIdHRwSGVhZGVyUmVzcG9uc2VgLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIEh0dHBSZXNwb25zZUJhc2Uge1xuICAvKipcbiAgICogQWxsIHJlc3BvbnNlIGhlYWRlcnMuXG4gICAqL1xuICByZWFkb25seSBoZWFkZXJzOiBIdHRwSGVhZGVycztcblxuICAvKipcbiAgICogUmVzcG9uc2Ugc3RhdHVzIGNvZGUuXG4gICAqL1xuICByZWFkb25seSBzdGF0dXM6IG51bWJlcjtcblxuICAvKipcbiAgICogVGV4dHVhbCBkZXNjcmlwdGlvbiBvZiByZXNwb25zZSBzdGF0dXMgY29kZS5cbiAgICpcbiAgICogRG8gbm90IGRlcGVuZCBvbiB0aGlzLlxuICAgKi9cbiAgcmVhZG9ubHkgc3RhdHVzVGV4dDogc3RyaW5nO1xuXG4gIC8qKlxuICAgKiBVUkwgb2YgdGhlIHJlc291cmNlIHJldHJpZXZlZCwgb3IgbnVsbCBpZiBub3QgYXZhaWxhYmxlLlxuICAgKi9cbiAgcmVhZG9ubHkgdXJsOiBzdHJpbmd8bnVsbDtcblxuICAvKipcbiAgICogV2hldGhlciB0aGUgc3RhdHVzIGNvZGUgZmFsbHMgaW4gdGhlIDJ4eCByYW5nZS5cbiAgICovXG4gIHJlYWRvbmx5IG9rOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBUeXBlIG9mIHRoZSByZXNwb25zZSwgbmFycm93ZWQgdG8gZWl0aGVyIHRoZSBmdWxsIHJlc3BvbnNlIG9yIHRoZSBoZWFkZXIuXG4gICAqL1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcmVhZG9ubHkgdHlwZSAhOiBIdHRwRXZlbnRUeXBlLlJlc3BvbnNlIHwgSHR0cEV2ZW50VHlwZS5SZXNwb25zZUhlYWRlcjtcblxuICAvKipcbiAgICogU3VwZXItY29uc3RydWN0b3IgZm9yIGFsbCByZXNwb25zZXMuXG4gICAqXG4gICAqIFRoZSBzaW5nbGUgcGFyYW1ldGVyIGFjY2VwdGVkIGlzIGFuIGluaXRpYWxpemF0aW9uIGhhc2guIEFueSBwcm9wZXJ0aWVzXG4gICAqIG9mIHRoZSByZXNwb25zZSBwYXNzZWQgdGhlcmUgd2lsbCBvdmVycmlkZSB0aGUgZGVmYXVsdCB2YWx1ZXMuXG4gICAqL1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIGluaXQ6IHtcbiAgICAgICAgaGVhZGVycz86IEh0dHBIZWFkZXJzLFxuICAgICAgICBzdGF0dXM/OiBudW1iZXIsXG4gICAgICAgIHN0YXR1c1RleHQ/OiBzdHJpbmcsXG4gICAgICAgIHVybD86IHN0cmluZyxcbiAgICAgIH0sXG4gICAgICBkZWZhdWx0U3RhdHVzOiBudW1iZXIgPSAyMDAsIGRlZmF1bHRTdGF0dXNUZXh0OiBzdHJpbmcgPSAnT0snKSB7XG4gICAgLy8gSWYgdGhlIGhhc2ggaGFzIHZhbHVlcyBwYXNzZWQsIHVzZSB0aGVtIHRvIGluaXRpYWxpemUgdGhlIHJlc3BvbnNlLlxuICAgIC8vIE90aGVyd2lzZSB1c2UgdGhlIGRlZmF1bHQgdmFsdWVzLlxuICAgIHRoaXMuaGVhZGVycyA9IGluaXQuaGVhZGVycyB8fCBuZXcgSHR0cEhlYWRlcnMoKTtcbiAgICB0aGlzLnN0YXR1cyA9IGluaXQuc3RhdHVzICE9PSB1bmRlZmluZWQgPyBpbml0LnN0YXR1cyA6IGRlZmF1bHRTdGF0dXM7XG4gICAgdGhpcy5zdGF0dXNUZXh0ID0gaW5pdC5zdGF0dXNUZXh0IHx8IGRlZmF1bHRTdGF0dXNUZXh0O1xuICAgIHRoaXMudXJsID0gaW5pdC51cmwgfHwgbnVsbDtcblxuICAgIC8vIENhY2hlIHRoZSBvayB2YWx1ZSB0byBhdm9pZCBkZWZpbmluZyBhIGdldHRlci5cbiAgICB0aGlzLm9rID0gdGhpcy5zdGF0dXMgPj0gMjAwICYmIHRoaXMuc3RhdHVzIDwgMzAwO1xuICB9XG59XG5cbi8qKlxuICogQSBwYXJ0aWFsIEhUVFAgcmVzcG9uc2Ugd2hpY2ggb25seSBpbmNsdWRlcyB0aGUgc3RhdHVzIGFuZCBoZWFkZXIgZGF0YSxcbiAqIGJ1dCBubyByZXNwb25zZSBib2R5LlxuICpcbiAqIGBIdHRwSGVhZGVyUmVzcG9uc2VgIGlzIGEgYEh0dHBFdmVudGAgYXZhaWxhYmxlIG9uIHRoZSByZXNwb25zZVxuICogZXZlbnQgc3RyZWFtLCBvbmx5IHdoZW4gcHJvZ3Jlc3MgZXZlbnRzIGFyZSByZXF1ZXN0ZWQuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgSHR0cEhlYWRlclJlc3BvbnNlIGV4dGVuZHMgSHR0cFJlc3BvbnNlQmFzZSB7XG4gIC8qKlxuICAgKiBDcmVhdGUgYSBuZXcgYEh0dHBIZWFkZXJSZXNwb25zZWAgd2l0aCB0aGUgZ2l2ZW4gcGFyYW1ldGVycy5cbiAgICovXG4gIGNvbnN0cnVjdG9yKGluaXQ6IHtcbiAgICBoZWFkZXJzPzogSHR0cEhlYWRlcnMsXG4gICAgc3RhdHVzPzogbnVtYmVyLFxuICAgIHN0YXR1c1RleHQ/OiBzdHJpbmcsXG4gICAgdXJsPzogc3RyaW5nLFxuICB9ID0ge30pIHtcbiAgICBzdXBlcihpbml0KTtcbiAgfVxuXG4gIHJlYWRvbmx5IHR5cGU6IEh0dHBFdmVudFR5cGUuUmVzcG9uc2VIZWFkZXIgPSBIdHRwRXZlbnRUeXBlLlJlc3BvbnNlSGVhZGVyO1xuXG4gIC8qKlxuICAgKiBDb3B5IHRoaXMgYEh0dHBIZWFkZXJSZXNwb25zZWAsIG92ZXJyaWRpbmcgaXRzIGNvbnRlbnRzIHdpdGggdGhlXG4gICAqIGdpdmVuIHBhcmFtZXRlciBoYXNoLlxuICAgKi9cbiAgY2xvbmUodXBkYXRlOiB7aGVhZGVycz86IEh0dHBIZWFkZXJzOyBzdGF0dXM/OiBudW1iZXI7IHN0YXR1c1RleHQ/OiBzdHJpbmc7IHVybD86IHN0cmluZzt9ID0ge30pOlxuICAgICAgSHR0cEhlYWRlclJlc3BvbnNlIHtcbiAgICAvLyBQZXJmb3JtIGEgc3RyYWlnaHRmb3J3YXJkIGluaXRpYWxpemF0aW9uIG9mIHRoZSBuZXcgSHR0cEhlYWRlclJlc3BvbnNlLFxuICAgIC8vIG92ZXJyaWRpbmcgdGhlIGN1cnJlbnQgcGFyYW1ldGVycyB3aXRoIG5ldyBvbmVzIGlmIGdpdmVuLlxuICAgIHJldHVybiBuZXcgSHR0cEhlYWRlclJlc3BvbnNlKHtcbiAgICAgIGhlYWRlcnM6IHVwZGF0ZS5oZWFkZXJzIHx8IHRoaXMuaGVhZGVycyxcbiAgICAgIHN0YXR1czogdXBkYXRlLnN0YXR1cyAhPT0gdW5kZWZpbmVkID8gdXBkYXRlLnN0YXR1cyA6IHRoaXMuc3RhdHVzLFxuICAgICAgc3RhdHVzVGV4dDogdXBkYXRlLnN0YXR1c1RleHQgfHwgdGhpcy5zdGF0dXNUZXh0LFxuICAgICAgdXJsOiB1cGRhdGUudXJsIHx8IHRoaXMudXJsIHx8IHVuZGVmaW5lZCxcbiAgICB9KTtcbiAgfVxufVxuXG4vKipcbiAqIEEgZnVsbCBIVFRQIHJlc3BvbnNlLCBpbmNsdWRpbmcgYSB0eXBlZCByZXNwb25zZSBib2R5ICh3aGljaCBtYXkgYmUgYG51bGxgXG4gKiBpZiBvbmUgd2FzIG5vdCByZXR1cm5lZCkuXG4gKlxuICogYEh0dHBSZXNwb25zZWAgaXMgYSBgSHR0cEV2ZW50YCBhdmFpbGFibGUgb24gdGhlIHJlc3BvbnNlIGV2ZW50XG4gKiBzdHJlYW0uXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgSHR0cFJlc3BvbnNlPFQ+IGV4dGVuZHMgSHR0cFJlc3BvbnNlQmFzZSB7XG4gIC8qKlxuICAgKiBUaGUgcmVzcG9uc2UgYm9keSwgb3IgYG51bGxgIGlmIG9uZSB3YXMgbm90IHJldHVybmVkLlxuICAgKi9cbiAgcmVhZG9ubHkgYm9keTogVHxudWxsO1xuXG4gIC8qKlxuICAgKiBDb25zdHJ1Y3QgYSBuZXcgYEh0dHBSZXNwb25zZWAuXG4gICAqL1xuICBjb25zdHJ1Y3Rvcihpbml0OiB7XG4gICAgYm9keT86IFQgfCBudWxsLCBoZWFkZXJzPzogSHR0cEhlYWRlcnM7IHN0YXR1cz86IG51bWJlcjsgc3RhdHVzVGV4dD86IHN0cmluZzsgdXJsPzogc3RyaW5nO1xuICB9ID0ge30pIHtcbiAgICBzdXBlcihpbml0KTtcbiAgICB0aGlzLmJvZHkgPSBpbml0LmJvZHkgIT09IHVuZGVmaW5lZCA/IGluaXQuYm9keSA6IG51bGw7XG4gIH1cblxuICByZWFkb25seSB0eXBlOiBIdHRwRXZlbnRUeXBlLlJlc3BvbnNlID0gSHR0cEV2ZW50VHlwZS5SZXNwb25zZTtcblxuICBjbG9uZSgpOiBIdHRwUmVzcG9uc2U8VD47XG4gIGNsb25lKHVwZGF0ZToge2hlYWRlcnM/OiBIdHRwSGVhZGVyczsgc3RhdHVzPzogbnVtYmVyOyBzdGF0dXNUZXh0Pzogc3RyaW5nOyB1cmw/OiBzdHJpbmc7fSk6XG4gICAgICBIdHRwUmVzcG9uc2U8VD47XG4gIGNsb25lPFY+KHVwZGF0ZToge1xuICAgIGJvZHk/OiBWIHwgbnVsbCwgaGVhZGVycz86IEh0dHBIZWFkZXJzOyBzdGF0dXM/OiBudW1iZXI7IHN0YXR1c1RleHQ/OiBzdHJpbmc7IHVybD86IHN0cmluZztcbiAgfSk6IEh0dHBSZXNwb25zZTxWPjtcbiAgY2xvbmUodXBkYXRlOiB7XG4gICAgYm9keT86IGFueSB8IG51bGw7IGhlYWRlcnM/OiBIdHRwSGVhZGVyczsgc3RhdHVzPzogbnVtYmVyOyBzdGF0dXNUZXh0Pzogc3RyaW5nOyB1cmw/OiBzdHJpbmc7XG4gIH0gPSB7fSk6IEh0dHBSZXNwb25zZTxhbnk+IHtcbiAgICByZXR1cm4gbmV3IEh0dHBSZXNwb25zZTxhbnk+KHtcbiAgICAgIGJvZHk6ICh1cGRhdGUuYm9keSAhPT0gdW5kZWZpbmVkKSA/IHVwZGF0ZS5ib2R5IDogdGhpcy5ib2R5LFxuICAgICAgaGVhZGVyczogdXBkYXRlLmhlYWRlcnMgfHwgdGhpcy5oZWFkZXJzLFxuICAgICAgc3RhdHVzOiAodXBkYXRlLnN0YXR1cyAhPT0gdW5kZWZpbmVkKSA/IHVwZGF0ZS5zdGF0dXMgOiB0aGlzLnN0YXR1cyxcbiAgICAgIHN0YXR1c1RleHQ6IHVwZGF0ZS5zdGF0dXNUZXh0IHx8IHRoaXMuc3RhdHVzVGV4dCxcbiAgICAgIHVybDogdXBkYXRlLnVybCB8fCB0aGlzLnVybCB8fCB1bmRlZmluZWQsXG4gICAgfSk7XG4gIH1cbn1cblxuLyoqXG4gKiBBIHJlc3BvbnNlIHRoYXQgcmVwcmVzZW50cyBhbiBlcnJvciBvciBmYWlsdXJlLCBlaXRoZXIgZnJvbSBhXG4gKiBub24tc3VjY2Vzc2Z1bCBIVFRQIHN0YXR1cywgYW4gZXJyb3Igd2hpbGUgZXhlY3V0aW5nIHRoZSByZXF1ZXN0LFxuICogb3Igc29tZSBvdGhlciBmYWlsdXJlIHdoaWNoIG9jY3VycmVkIGR1cmluZyB0aGUgcGFyc2luZyBvZiB0aGUgcmVzcG9uc2UuXG4gKlxuICogQW55IGVycm9yIHJldHVybmVkIG9uIHRoZSBgT2JzZXJ2YWJsZWAgcmVzcG9uc2Ugc3RyZWFtIHdpbGwgYmVcbiAqIHdyYXBwZWQgaW4gYW4gYEh0dHBFcnJvclJlc3BvbnNlYCB0byBwcm92aWRlIGFkZGl0aW9uYWwgY29udGV4dCBhYm91dFxuICogdGhlIHN0YXRlIG9mIHRoZSBIVFRQIGxheWVyIHdoZW4gdGhlIGVycm9yIG9jY3VycmVkLiBUaGUgZXJyb3IgcHJvcGVydHlcbiAqIHdpbGwgY29udGFpbiBlaXRoZXIgYSB3cmFwcGVkIEVycm9yIG9iamVjdCBvciB0aGUgZXJyb3IgcmVzcG9uc2UgcmV0dXJuZWRcbiAqIGZyb20gdGhlIHNlcnZlci5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBIdHRwRXJyb3JSZXNwb25zZSBleHRlbmRzIEh0dHBSZXNwb25zZUJhc2UgaW1wbGVtZW50cyBFcnJvciB7XG4gIHJlYWRvbmx5IG5hbWUgPSAnSHR0cEVycm9yUmVzcG9uc2UnO1xuICByZWFkb25seSBtZXNzYWdlOiBzdHJpbmc7XG4gIHJlYWRvbmx5IGVycm9yOiBhbnl8bnVsbDtcblxuICAvKipcbiAgICogRXJyb3JzIGFyZSBuZXZlciBva2F5LCBldmVuIHdoZW4gdGhlIHN0YXR1cyBjb2RlIGlzIGluIHRoZSAyeHggc3VjY2VzcyByYW5nZS5cbiAgICovXG4gIHJlYWRvbmx5IG9rID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IoaW5pdDoge1xuICAgIGVycm9yPzogYW55OyBoZWFkZXJzPzogSHR0cEhlYWRlcnM7IHN0YXR1cz86IG51bWJlcjsgc3RhdHVzVGV4dD86IHN0cmluZzsgdXJsPzogc3RyaW5nO1xuICB9KSB7XG4gICAgLy8gSW5pdGlhbGl6ZSB3aXRoIGEgZGVmYXVsdCBzdGF0dXMgb2YgMCAvIFVua25vd24gRXJyb3IuXG4gICAgc3VwZXIoaW5pdCwgMCwgJ1Vua25vd24gRXJyb3InKTtcblxuICAgIC8vIElmIHRoZSByZXNwb25zZSB3YXMgc3VjY2Vzc2Z1bCwgdGhlbiB0aGlzIHdhcyBhIHBhcnNlIGVycm9yLiBPdGhlcndpc2UsIGl0IHdhc1xuICAgIC8vIGEgcHJvdG9jb2wtbGV2ZWwgZmFpbHVyZSBvZiBzb21lIHNvcnQuIEVpdGhlciB0aGUgcmVxdWVzdCBmYWlsZWQgaW4gdHJhbnNpdFxuICAgIC8vIG9yIHRoZSBzZXJ2ZXIgcmV0dXJuZWQgYW4gdW5zdWNjZXNzZnVsIHN0YXR1cyBjb2RlLlxuICAgIGlmICh0aGlzLnN0YXR1cyA+PSAyMDAgJiYgdGhpcy5zdGF0dXMgPCAzMDApIHtcbiAgICAgIHRoaXMubWVzc2FnZSA9IGBIdHRwIGZhaWx1cmUgZHVyaW5nIHBhcnNpbmcgZm9yICR7aW5pdC51cmwgfHwgJyh1bmtub3duIHVybCknfWA7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMubWVzc2FnZSA9XG4gICAgICAgICAgYEh0dHAgZmFpbHVyZSByZXNwb25zZSBmb3IgJHtpbml0LnVybCB8fCAnKHVua25vd24gdXJsKSd9OiAke2luaXQuc3RhdHVzfSAke2luaXQuc3RhdHVzVGV4dH1gO1xuICAgIH1cbiAgICB0aGlzLmVycm9yID0gaW5pdC5lcnJvciB8fCBudWxsO1xuICB9XG59XG4iXX0=