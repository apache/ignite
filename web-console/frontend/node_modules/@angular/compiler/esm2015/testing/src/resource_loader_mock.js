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
import { ResourceLoader } from '@angular/compiler';
/**
 * A mock implementation of {\@link ResourceLoader} that allows outgoing requests to be mocked
 * and responded to within a single test, without going to the network.
 */
export class MockResourceLoader extends ResourceLoader {
    constructor() {
        super(...arguments);
        this._expectations = [];
        this._definitions = new Map();
        this._requests = [];
    }
    /**
     * @param {?} url
     * @return {?}
     */
    get(url) {
        /** @type {?} */
        const request = new _PendingRequest(url);
        this._requests.push(request);
        return request.getPromise();
    }
    /**
     * @return {?}
     */
    hasPendingRequests() { return !!this._requests.length; }
    /**
     * Add an expectation for the given URL. Incoming requests will be checked against
     * the next expectation (in FIFO order). The `verifyNoOutstandingExpectations` method
     * can be used to check if any expectations have not yet been met.
     *
     * The response given will be returned if the expectation matches.
     * @param {?} url
     * @param {?} response
     * @return {?}
     */
    expect(url, response) {
        /** @type {?} */
        const expectation = new _Expectation(url, response);
        this._expectations.push(expectation);
    }
    /**
     * Add a definition for the given URL to return the given response. Unlike expectations,
     * definitions have no order and will satisfy any matching request at any time. Also
     * unlike expectations, unused definitions do not cause `verifyNoOutstandingExpectations`
     * to return an error.
     * @param {?} url
     * @param {?} response
     * @return {?}
     */
    when(url, response) { this._definitions.set(url, response); }
    /**
     * Process pending requests and verify there are no outstanding expectations. Also fails
     * if no requests are pending.
     * @return {?}
     */
    flush() {
        if (this._requests.length === 0) {
            throw new Error('No pending requests to flush');
        }
        do {
            this._processRequest((/** @type {?} */ (this._requests.shift())));
        } while (this._requests.length > 0);
        this.verifyNoOutstandingExpectations();
    }
    /**
     * Throw an exception if any expectations have not been satisfied.
     * @return {?}
     */
    verifyNoOutstandingExpectations() {
        if (this._expectations.length === 0)
            return;
        /** @type {?} */
        const urls = [];
        for (let i = 0; i < this._expectations.length; i++) {
            /** @type {?} */
            const expectation = this._expectations[i];
            urls.push(expectation.url);
        }
        throw new Error(`Unsatisfied requests: ${urls.join(', ')}`);
    }
    /**
     * @private
     * @param {?} request
     * @return {?}
     */
    _processRequest(request) {
        /** @type {?} */
        const url = request.url;
        if (this._expectations.length > 0) {
            /** @type {?} */
            const expectation = this._expectations[0];
            if (expectation.url == url) {
                remove(this._expectations, expectation);
                request.complete(expectation.response);
                return;
            }
        }
        if (this._definitions.has(url)) {
            /** @type {?} */
            const response = this._definitions.get(url);
            request.complete(response == null ? null : response);
            return;
        }
        throw new Error(`Unexpected request ${url}`);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockResourceLoader.prototype._expectations;
    /**
     * @type {?}
     * @private
     */
    MockResourceLoader.prototype._definitions;
    /**
     * @type {?}
     * @private
     */
    MockResourceLoader.prototype._requests;
}
class _PendingRequest {
    /**
     * @param {?} url
     */
    constructor(url) {
        this.url = url;
        this.promise = new Promise((/**
         * @param {?} res
         * @param {?} rej
         * @return {?}
         */
        (res, rej) => {
            this.resolve = res;
            this.reject = rej;
        }));
    }
    /**
     * @param {?} response
     * @return {?}
     */
    complete(response) {
        if (response == null) {
            this.reject(`Failed to load ${this.url}`);
        }
        else {
            this.resolve(response);
        }
    }
    /**
     * @return {?}
     */
    getPromise() { return this.promise; }
}
if (false) {
    /** @type {?} */
    _PendingRequest.prototype.resolve;
    /** @type {?} */
    _PendingRequest.prototype.reject;
    /** @type {?} */
    _PendingRequest.prototype.promise;
    /** @type {?} */
    _PendingRequest.prototype.url;
}
class _Expectation {
    /**
     * @param {?} url
     * @param {?} response
     */
    constructor(url, response) {
        this.url = url;
        this.response = response;
    }
}
if (false) {
    /** @type {?} */
    _Expectation.prototype.url;
    /** @type {?} */
    _Expectation.prototype.response;
}
/**
 * @template T
 * @param {?} list
 * @param {?} el
 * @return {?}
 */
function remove(list, el) {
    /** @type {?} */
    const index = list.indexOf(el);
    if (index > -1) {
        list.splice(index, 1);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb3VyY2VfbG9hZGVyX21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9yZXNvdXJjZV9sb2FkZXJfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxjQUFjLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQzs7Ozs7QUFNakQsTUFBTSxPQUFPLGtCQUFtQixTQUFRLGNBQWM7SUFBdEQ7O1FBQ1Usa0JBQWEsR0FBbUIsRUFBRSxDQUFDO1FBQ25DLGlCQUFZLEdBQUcsSUFBSSxHQUFHLEVBQWtCLENBQUM7UUFDekMsY0FBUyxHQUFzQixFQUFFLENBQUM7SUFpRjVDLENBQUM7Ozs7O0lBL0VDLEdBQUcsQ0FBQyxHQUFXOztjQUNQLE9BQU8sR0FBRyxJQUFJLGVBQWUsQ0FBQyxHQUFHLENBQUM7UUFDeEMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7UUFDN0IsT0FBTyxPQUFPLENBQUMsVUFBVSxFQUFFLENBQUM7SUFDOUIsQ0FBQzs7OztJQUVELGtCQUFrQixLQUFLLE9BQU8sQ0FBQyxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQzs7Ozs7Ozs7Ozs7SUFTeEQsTUFBTSxDQUFDLEdBQVcsRUFBRSxRQUFnQjs7Y0FDNUIsV0FBVyxHQUFHLElBQUksWUFBWSxDQUFDLEdBQUcsRUFBRSxRQUFRLENBQUM7UUFDbkQsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLENBQUM7SUFDdkMsQ0FBQzs7Ozs7Ozs7OztJQVFELElBQUksQ0FBQyxHQUFXLEVBQUUsUUFBZ0IsSUFBSSxJQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsQ0FBQyxHQUFHLEVBQUUsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFNN0UsS0FBSztRQUNI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QUN2RDs7O0lBckJDLGtDQUFvQzs7SUFFcEMsaUNBQStCOztJQUMvQixrQ0FBeUI7O0lBRWIsOEJBQWtCOztBQWtCaEMsTUFBTSxZQUFZOzs7OztJQUdoQixZQUFZLEdBQVcsRUFBRSxRQUFnQjtRQUN2QyxJQUFJLENBQUMsR0FBRyxHQUFHLEdBQUcsQ0FBQztRQUNmLElBQUksQ0FBQyxRQUFRLEdBQUcsUUFBUSxDQUFDO0lBQzNCLENBQUM7Q0FDRjs7O0lBTkMsMkJBQVk7O0lBQ1osZ0NBQWlCOzs7Ozs7OztBQU9uQixTQUFTLE1BQU0sQ0FBSSxJQUFTLEVBQUUsRUFBSzs7VUFDM0IsS0FBSyxHQUFHLElBQUksQ0FBQyxPQUFPLENBQUMsRUFBRSxDQUFDO0lBQzlCLElBQUksS0FBSyxHQUFHLENBQUMsQ0FBQyxFQUFFO1FBQ2QsSUFBSSxDQUFDLE1BQU0sQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLENBQUM7S0FDdkI7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1Jlc291cmNlTG9hZGVyfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5cbi8qKlxuICogQSBtb2NrIGltcGxlbWVudGF0aW9uIG9mIHtAbGluayBSZXNvdXJjZUxvYWRlcn0gdGhhdCBhbGxvd3Mgb3V0Z29pbmcgcmVxdWVzdHMgdG8gYmUgbW9ja2VkXG4gKiBhbmQgcmVzcG9uZGVkIHRvIHdpdGhpbiBhIHNpbmdsZSB0ZXN0LCB3aXRob3V0IGdvaW5nIHRvIHRoZSBuZXR3b3JrLlxuICovXG5leHBvcnQgY2xhc3MgTW9ja1Jlc291cmNlTG9hZGVyIGV4dGVuZHMgUmVzb3VyY2VMb2FkZXIge1xuICBwcml2YXRlIF9leHBlY3RhdGlvbnM6IF9FeHBlY3RhdGlvbltdID0gW107XG4gIHByaXZhdGUgX2RlZmluaXRpb25zID0gbmV3IE1hcDxzdHJpbmcsIHN0cmluZz4oKTtcbiAgcHJpdmF0ZSBfcmVxdWVzdHM6IF9QZW5kaW5nUmVxdWVzdFtdID0gW107XG5cbiAgZ2V0KHVybDogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICBjb25zdCByZXF1ZXN0ID0gbmV3IF9QZW5kaW5nUmVxdWVzdCh1cmwpO1xuICAgIHRoaXMuX3JlcXVlc3RzLnB1c2gocmVxdWVzdCk7XG4gICAgcmV0dXJuIHJlcXVlc3QuZ2V0UHJvbWlzZSgpO1xuICB9XG5cbiAgaGFzUGVuZGluZ1JlcXVlc3RzKCkgeyByZXR1cm4gISF0aGlzLl9yZXF1ZXN0cy5sZW5ndGg7IH1cblxuICAvKipcbiAgICogQWRkIGFuIGV4cGVjdGF0aW9uIGZvciB0aGUgZ2l2ZW4gVVJMLiBJbmNvbWluZyByZXF1ZXN0cyB3aWxsIGJlIGNoZWNrZWQgYWdhaW5zdFxuICAgKiB0aGUgbmV4dCBleHBlY3RhdGlvbiAoaW4gRklGTyBvcmRlcikuIFRoZSBgdmVyaWZ5Tm9PdXRzdGFuZGluZ0V4cGVjdGF0aW9uc2AgbWV0aG9kXG4gICAqIGNhbiBiZSB1c2VkIHRvIGNoZWNrIGlmIGFueSBleHBlY3RhdGlvbnMgaGF2ZSBub3QgeWV0IGJlZW4gbWV0LlxuICAgKlxuICAgKiBUaGUgcmVzcG9uc2UgZ2l2ZW4gd2lsbCBiZSByZXR1cm5lZCBpZiB0aGUgZXhwZWN0YXRpb24gbWF0Y2hlcy5cbiAgICovXG4gIGV4cGVjdCh1cmw6IHN0cmluZywgcmVzcG9uc2U6IHN0cmluZykge1xuICAgIGNvbnN0IGV4cGVjdGF0aW9uID0gbmV3IF9FeHBlY3RhdGlvbih1cmwsIHJlc3BvbnNlKTtcbiAgICB0aGlzLl9leHBlY3RhdGlvbnMucHVzaChleHBlY3RhdGlvbik7XG4gIH1cblxuICAvKipcbiAgICogQWRkIGEgZGVmaW5pdGlvbiBmb3IgdGhlIGdpdmVuIFVSTCB0byByZXR1cm4gdGhlIGdpdmVuIHJlc3BvbnNlLiBVbmxpa2UgZXhwZWN0YXRpb25zLFxuICAgKiBkZWZpbml0aW9ucyBoYXZlIG5vIG9yZGVyIGFuZCB3aWxsIHNhdGlzZnkgYW55IG1hdGNoaW5nIHJlcXVlc3QgYXQgYW55IHRpbWUuIEFsc29cbiAgICogdW5saWtlIGV4cGVjdGF0aW9ucywgdW51c2VkIGRlZmluaXRpb25zIGRvIG5vdCBjYXVzZSBgdmVyaWZ5Tm9PdXRzdGFuZGluZ0V4cGVjdGF0aW9uc2BcbiAgICogdG8gcmV0dXJuIGFuIGVycm9yLlxuICAgKi9cbiAgd2hlbih1cmw6IHN0cmluZywgcmVzcG9uc2U6IHN0cmluZykgeyB0aGlzLl9kZWZpbml0aW9ucy5zZXQodXJsLCByZXNwb25zZSk7IH1cblxuICAvKipcbiAgICogUHJvY2VzcyBwZW5kaW5nIHJlcXVlc3RzIGFuZCB2ZXJpZnkgdGhlcmUgYXJlIG5vIG91dHN0YW5kaW5nIGV4cGVjdGF0aW9ucy4gQWxzbyBmYWlsc1xuICAgKiBpZiBubyByZXF1ZXN0cyBhcmUgcGVuZGluZy5cbiAgICovXG4gIGZsdXNoKCkge1xuICAgIGlmICh0aGlzLl9yZXF1ZXN0cy5sZW5ndGggPT09IDApIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignTm8gcGVuZGluZyByZXF1ZXN0cyB0byBmbHVzaCcpO1xuICAgIH1cblxuICAgIGRvIHtcbiAgICAgIHRoaXMuX3Byb2Nlc3NSZXF1ZXN0KHRoaXMuX3JlcXVlc3RzLnNoaWZ0KCkgISk7XG4gICAgfSB3aGlsZSAodGhpcy5fcmVxdWVzdHMubGVuZ3RoID4gMCk7XG5cbiAgICB0aGlzLnZlcmlmeU5vT3V0c3RhbmRpbmdFeHBlY3RhdGlvbnMoKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBUaHJvdyBhbiBleGNlcHRpb24gaWYgYW55IGV4cGVjdGF0aW9ucyBoYXZlIG5vdCBiZWVuIHNhdGlzZmllZC5cbiAgICovXG4gIHZlcmlmeU5vT3V0c3RhbmRpbmdFeHBlY3RhdGlvbnMoKSB7XG4gICAgaWYgKHRoaXMuX2V4cGVjdGF0aW9ucy5sZW5ndGggPT09IDApIHJldHVybjtcblxuICAgIGNvbnN0IHVybHM6IHN0cmluZ1tdID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLl9leHBlY3RhdGlvbnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IGV4cGVjdGF0aW9uID0gdGhpcy5fZXhwZWN0YXRpb25zW2ldO1xuICAgICAgdXJscy5wdXNoKGV4cGVjdGF0aW9uLnVybCk7XG4gICAgfVxuXG4gICAgdGhyb3cgbmV3IEVycm9yKGBVbnNhdGlzZmllZCByZXF1ZXN0czogJHt1cmxzLmpvaW4oJywgJyl9YCk7XG4gIH1cblxuICBwcml2YXRlIF9wcm9jZXNzUmVxdWVzdChyZXF1ZXN0OiBfUGVuZGluZ1JlcXVlc3QpIHtcbiAgICBjb25zdCB1cmwgPSByZXF1ZXN0LnVybDtcblxuICAgIGlmICh0aGlzLl9leHBlY3RhdGlvbnMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgZXhwZWN0YXRpb24gPSB0aGlzLl9leHBlY3RhdGlvbnNbMF07XG4gICAgICBpZiAoZXhwZWN0YXRpb24udXJsID09IHVybCkge1xuICAgICAgICByZW1vdmUodGhpcy5fZXhwZWN0YXRpb25zLCBleHBlY3RhdGlvbik7XG4gICAgICAgIHJlcXVlc3QuY29tcGxldGUoZXhwZWN0YXRpb24ucmVzcG9uc2UpO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHRoaXMuX2RlZmluaXRpb25zLmhhcyh1cmwpKSB7XG4gICAgICBjb25zdCByZXNwb25zZSA9IHRoaXMuX2RlZmluaXRpb25zLmdldCh1cmwpO1xuICAgICAgcmVxdWVzdC5jb21wbGV0ZShyZXNwb25zZSA9PSBudWxsID8gbnVsbCA6IHJlc3BvbnNlKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRXJyb3IoYFVuZXhwZWN0ZWQgcmVxdWVzdCAke3VybH1gKTtcbiAgfVxufVxuXG5jbGFzcyBfUGVuZGluZ1JlcXVlc3Qge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcmVzb2x2ZSAhOiAocmVzdWx0OiBzdHJpbmcpID0+IHZvaWQ7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICByZWplY3QgITogKGVycm9yOiBhbnkpID0+IHZvaWQ7XG4gIHByb21pc2U6IFByb21pc2U8c3RyaW5nPjtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgdXJsOiBzdHJpbmcpIHtcbiAgICB0aGlzLnByb21pc2UgPSBuZXcgUHJvbWlzZSgocmVzLCByZWopID0+IHtcbiAgICAgIHRoaXMucmVzb2x2ZSA9IHJlcztcbiAgICAgIHRoaXMucmVqZWN0ID0gcmVqO1xuICAgIH0pO1xuICB9XG5cbiAgY29tcGxldGUocmVzcG9uc2U6IHN0cmluZ3xudWxsKSB7XG4gICAgaWYgKHJlc3BvbnNlID09IG51bGwpIHtcbiAgICAgIHRoaXMucmVqZWN0KGBGYWlsZWQgdG8gbG9hZCAke3RoaXMudXJsfWApO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnJlc29sdmUocmVzcG9uc2UpO1xuICAgIH1cbiAgfVxuXG4gIGdldFByb21pc2UoKTogUHJvbWlzZTxzdHJpbmc+IHsgcmV0dXJuIHRoaXMucHJvbWlzZTsgfVxufVxuXG5jbGFzcyBfRXhwZWN0YXRpb24ge1xuICB1cmw6IHN0cmluZztcbiAgcmVzcG9uc2U6IHN0cmluZztcbiAgY29uc3RydWN0b3IodXJsOiBzdHJpbmcsIHJlc3BvbnNlOiBzdHJpbmcpIHtcbiAgICB0aGlzLnVybCA9IHVybDtcbiAgICB0aGlzLnJlc3BvbnNlID0gcmVzcG9uc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gcmVtb3ZlPFQ+KGxpc3Q6IFRbXSwgZWw6IFQpOiB2b2lkIHtcbiAgY29uc3QgaW5kZXggPSBsaXN0LmluZGV4T2YoZWwpO1xuICBpZiAoaW5kZXggPiAtMSkge1xuICAgIGxpc3Quc3BsaWNlKGluZGV4LCAxKTtcbiAgfVxufVxuIl19