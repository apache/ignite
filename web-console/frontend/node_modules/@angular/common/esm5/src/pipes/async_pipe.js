/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { ChangeDetectorRef, Injectable, Pipe, WrappedValue, ɵisObservable, ɵisPromise, ɵlooseIdentical } from '@angular/core';
import { invalidPipeArgumentError } from './invalid_pipe_argument_error';
var ObservableStrategy = /** @class */ (function () {
    function ObservableStrategy() {
    }
    ObservableStrategy.prototype.createSubscription = function (async, updateLatestValue) {
        return async.subscribe({ next: updateLatestValue, error: function (e) { throw e; } });
    };
    ObservableStrategy.prototype.dispose = function (subscription) { subscription.unsubscribe(); };
    ObservableStrategy.prototype.onDestroy = function (subscription) { subscription.unsubscribe(); };
    return ObservableStrategy;
}());
var PromiseStrategy = /** @class */ (function () {
    function PromiseStrategy() {
    }
    PromiseStrategy.prototype.createSubscription = function (async, updateLatestValue) {
        return async.then(updateLatestValue, function (e) { throw e; });
    };
    PromiseStrategy.prototype.dispose = function (subscription) { };
    PromiseStrategy.prototype.onDestroy = function (subscription) { };
    return PromiseStrategy;
}());
var _promiseStrategy = new PromiseStrategy();
var _observableStrategy = new ObservableStrategy();
/**
 * @ngModule CommonModule
 * @description
 *
 * Unwraps a value from an asynchronous primitive.
 *
 * The `async` pipe subscribes to an `Observable` or `Promise` and returns the latest value it has
 * emitted. When a new value is emitted, the `async` pipe marks the component to be checked for
 * changes. When the component gets destroyed, the `async` pipe unsubscribes automatically to avoid
 * potential memory leaks.
 *
 * @usageNotes
 *
 * ### Examples
 *
 * This example binds a `Promise` to the view. Clicking the `Resolve` button resolves the
 * promise.
 *
 * {@example common/pipes/ts/async_pipe.ts region='AsyncPipePromise'}
 *
 * It's also possible to use `async` with Observables. The example below binds the `time` Observable
 * to the view. The Observable continuously updates the view with the current time.
 *
 * {@example common/pipes/ts/async_pipe.ts region='AsyncPipeObservable'}
 *
 * @publicApi
 */
var AsyncPipe = /** @class */ (function () {
    function AsyncPipe(_ref) {
        this._ref = _ref;
        this._latestValue = null;
        this._latestReturnedValue = null;
        this._subscription = null;
        this._obj = null;
        this._strategy = null;
    }
    AsyncPipe_1 = AsyncPipe;
    AsyncPipe.prototype.ngOnDestroy = function () {
        if (this._subscription) {
            this._dispose();
        }
    };
    AsyncPipe.prototype.transform = function (obj) {
        if (!this._obj) {
            if (obj) {
                this._subscribe(obj);
            }
            this._latestReturnedValue = this._latestValue;
            return this._latestValue;
        }
        if (obj !== this._obj) {
            this._dispose();
            return this.transform(obj);
        }
        if (ɵlooseIdentical(this._latestValue, this._latestReturnedValue)) {
            return this._latestReturnedValue;
        }
        this._latestReturnedValue = this._latestValue;
        return WrappedValue.wrap(this._latestValue);
    };
    AsyncPipe.prototype._subscribe = function (obj) {
        var _this = this;
        this._obj = obj;
        this._strategy = this._selectStrategy(obj);
        this._subscription = this._strategy.createSubscription(obj, function (value) { return _this._updateLatestValue(obj, value); });
    };
    AsyncPipe.prototype._selectStrategy = function (obj) {
        if (ɵisPromise(obj)) {
            return _promiseStrategy;
        }
        if (ɵisObservable(obj)) {
            return _observableStrategy;
        }
        throw invalidPipeArgumentError(AsyncPipe_1, obj);
    };
    AsyncPipe.prototype._dispose = function () {
        this._strategy.dispose(this._subscription);
        this._latestValue = null;
        this._latestReturnedValue = null;
        this._subscription = null;
        this._obj = null;
    };
    AsyncPipe.prototype._updateLatestValue = function (async, value) {
        if (async === this._obj) {
            this._latestValue = value;
            this._ref.markForCheck();
        }
    };
    var AsyncPipe_1;
    AsyncPipe = AsyncPipe_1 = tslib_1.__decorate([
        Injectable(),
        Pipe({ name: 'async', pure: false }),
        tslib_1.__metadata("design:paramtypes", [ChangeDetectorRef])
    ], AsyncPipe);
    return AsyncPipe;
}());
export { AsyncPipe };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXN5bmNfcGlwZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvcGlwZXMvYXN5bmNfcGlwZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLGlCQUFpQixFQUFnQixVQUFVLEVBQWEsSUFBSSxFQUFpQixZQUFZLEVBQUUsYUFBYSxFQUFFLFVBQVUsRUFBRSxlQUFlLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFcEssT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0sK0JBQStCLENBQUM7QUFTdkU7SUFBQTtJQVFBLENBQUM7SUFQQywrQ0FBa0IsR0FBbEIsVUFBbUIsS0FBc0IsRUFBRSxpQkFBc0I7UUFDL0QsT0FBTyxLQUFLLENBQUMsU0FBUyxDQUFDLEVBQUMsSUFBSSxFQUFFLGlCQUFpQixFQUFFLEtBQUssRUFBRSxVQUFDLENBQU0sSUFBTyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBQyxDQUFDLENBQUM7SUFDckYsQ0FBQztJQUVELG9DQUFPLEdBQVAsVUFBUSxZQUE4QixJQUFVLFlBQVksQ0FBQyxXQUFXLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFFN0Usc0NBQVMsR0FBVCxVQUFVLFlBQThCLElBQVUsWUFBWSxDQUFDLFdBQVcsRUFBRSxDQUFDLENBQUMsQ0FBQztJQUNqRix5QkFBQztBQUFELENBQUMsQUFSRCxJQVFDO0FBRUQ7SUFBQTtJQVFBLENBQUM7SUFQQyw0Q0FBa0IsR0FBbEIsVUFBbUIsS0FBbUIsRUFBRSxpQkFBa0M7UUFDeEUsT0FBTyxLQUFLLENBQUMsSUFBSSxDQUFDLGlCQUFpQixFQUFFLFVBQUEsQ0FBQyxJQUFNLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDMUQsQ0FBQztJQUVELGlDQUFPLEdBQVAsVUFBUSxZQUEwQixJQUFTLENBQUM7SUFFNUMsbUNB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LHNDQUFrQixHQUExQixVQUEyQixLQUFVLEVBQUUsS0FBYTtRQUNsRCxJQUFJLEtBQUssS0FBSyxJQUFJLENBQUMsSUFBSSxFQUFFO1lBQ3ZCLElBQUksQ0FBQyxZQUFZLEdBQUcsS0FBSyxDQUFDO1lBQzFCLElBQUksQ0FBQyxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7U0FDMUI7SUFDSCxDQUFDOztJQTFFVSxTQUFTO1FBRnJCLFVBQVUsRUFBRTtRQUNaLElBQUksQ0FBQyxFQUFDLElBQUksRUFBRSxPQUFPLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBQyxDQUFDO2lEQVNQLGlCQUFpQjtPQVJoQyxTQUFTLENBMkVyQjtJQUFELGdCQUFDO0NBQUEsQUEzRUQsSUEyRUM7U0EzRVksU0FBUyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDaGFuZ2VEZXRlY3RvclJlZiwgRXZlbnRFbWl0dGVyLCBJbmplY3RhYmxlLCBPbkRlc3Ryb3ksIFBpcGUsIFBpcGVUcmFuc2Zvcm0sIFdyYXBwZWRWYWx1ZSwgybVpc09ic2VydmFibGUsIMm1aXNQcm9taXNlLCDJtWxvb3NlSWRlbnRpY2FsfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7T2JzZXJ2YWJsZSwgU3Vic2NyaXB0aW9uTGlrZX0gZnJvbSAncnhqcyc7XG5pbXBvcnQge2ludmFsaWRQaXBlQXJndW1lbnRFcnJvcn0gZnJvbSAnLi9pbnZhbGlkX3BpcGVfYXJndW1lbnRfZXJyb3InO1xuXG5pbnRlcmZhY2UgU3Vic2NyaXB0aW9uU3RyYXRlZ3kge1xuICBjcmVhdGVTdWJzY3JpcHRpb24oYXN5bmM6IE9ic2VydmFibGU8YW55PnxQcm9taXNlPGFueT4sIHVwZGF0ZUxhdGVzdFZhbHVlOiBhbnkpOiBTdWJzY3JpcHRpb25MaWtlXG4gICAgICB8UHJvbWlzZTxhbnk+O1xuICBkaXNwb3NlKHN1YnNjcmlwdGlvbjogU3Vic2NyaXB0aW9uTGlrZXxQcm9taXNlPGFueT4pOiB2b2lkO1xuICBvbkRlc3Ryb3koc3Vic2NyaXB0aW9uOiBTdWJzY3JpcHRpb25MaWtlfFByb21pc2U8YW55Pik6IHZvaWQ7XG59XG5cbmNsYXNzIE9ic2VydmFibGVTdHJhdGVneSBpbXBsZW1lbnRzIFN1YnNjcmlwdGlvblN0cmF0ZWd5IHtcbiAgY3JlYXRlU3Vic2NyaXB0aW9uKGFzeW5jOiBPYnNlcnZhYmxlPGFueT4sIHVwZGF0ZUxhdGVzdFZhbHVlOiBhbnkpOiBTdWJzY3JpcHRpb25MaWtlIHtcbiAgICByZXR1cm4gYXN5bmMuc3Vic2NyaWJlKHtuZXh0OiB1cGRhdGVMYXRlc3RWYWx1ZSwgZXJyb3I6IChlOiBhbnkpID0+IHsgdGhyb3cgZTsgfX0pO1xuICB9XG5cbiAgZGlzcG9zZShzdWJzY3JpcHRpb246IFN1YnNjcmlwdGlvbkxpa2UpOiB2b2lkIHsgc3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7IH1cblxuICBvbkRlc3Ryb3koc3Vic2NyaXB0aW9uOiBTdWJzY3JpcHRpb25MaWtlKTogdm9pZCB7IHN1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpOyB9XG59XG5cbmNsYXNzIFByb21pc2VTdHJhdGVneSBpbXBsZW1lbnRzIFN1YnNjcmlwdGlvblN0cmF0ZWd5IHtcbiAgY3JlYXRlU3Vic2NyaXB0aW9uKGFzeW5jOiBQcm9taXNlPGFueT4sIHVwZGF0ZUxhdGVzdFZhbHVlOiAodjogYW55KSA9PiBhbnkpOiBQcm9taXNlPGFueT4ge1xuICAgIHJldHVybiBhc3luYy50aGVuKHVwZGF0ZUxhdGVzdFZhbHVlLCBlID0+IHsgdGhyb3cgZTsgfSk7XG4gIH1cblxuICBkaXNwb3NlKHN1YnNjcmlwdGlvbjogUHJvbWlzZTxhbnk+KTogdm9pZCB7fVxuXG4gIG9uRGVzdHJveShzdWJzY3JpcHRpb246IFByb21pc2U8YW55Pik6IHZvaWQge31cbn1cblxuY29uc3QgX3Byb21pc2VTdHJhdGVneSA9IG5ldyBQcm9taXNlU3RyYXRlZ3koKTtcbmNvbnN0IF9vYnNlcnZhYmxlU3RyYXRlZ3kgPSBuZXcgT2JzZXJ2YWJsZVN0cmF0ZWd5KCk7XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogVW53cmFwcyBhIHZhbHVlIGZyb20gYW4gYXN5bmNocm9ub3VzIHByaW1pdGl2ZS5cbiAqXG4gKiBUaGUgYGFzeW5jYCBwaXBlIHN1YnNjcmliZXMgdG8gYW4gYE9ic2VydmFibGVgIG9yIGBQcm9taXNlYCBhbmQgcmV0dXJucyB0aGUgbGF0ZXN0IHZhbHVlIGl0IGhhc1xuICogZW1pdHRlZC4gV2hlbiBhIG5ldyB2YWx1ZSBpcyBlbWl0dGVkLCB0aGUgYGFzeW5jYCBwaXBlIG1hcmtzIHRoZSBjb21wb25lbnQgdG8gYmUgY2hlY2tlZCBmb3JcbiAqIGNoYW5nZXMuIFdoZW4gdGhlIGNvbXBvbmVudCBnZXRzIGRlc3Ryb3llZCwgdGhlIGBhc3luY2AgcGlwZSB1bnN1YnNjcmliZXMgYXV0b21hdGljYWxseSB0byBhdm9pZFxuICogcG90ZW50aWFsIG1lbW9yeSBsZWFrcy5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICpcbiAqICMjIyBFeGFtcGxlc1xuICpcbiAqIFRoaXMgZXhhbXBsZSBiaW5kcyBhIGBQcm9taXNlYCB0byB0aGUgdmlldy4gQ2xpY2tpbmcgdGhlIGBSZXNvbHZlYCBidXR0b24gcmVzb2x2ZXMgdGhlXG4gKiBwcm9taXNlLlxuICpcbiAqIHtAZXhhbXBsZSBjb21tb24vcGlwZXMvdHMvYXN5bmNfcGlwZS50cyByZWdpb249J0FzeW5jUGlwZVByb21pc2UnfVxuICpcbiAqIEl0J3MgYWxzbyBwb3NzaWJsZSB0byB1c2UgYGFzeW5jYCB3aXRoIE9ic2VydmFibGVzLiBUaGUgZXhhbXBsZSBiZWxvdyBiaW5kcyB0aGUgYHRpbWVgIE9ic2VydmFibGVcbiAqIHRvIHRoZSB2aWV3LiBUaGUgT2JzZXJ2YWJsZSBjb250aW51b3VzbHkgdXBkYXRlcyB0aGUgdmlldyB3aXRoIHRoZSBjdXJyZW50IHRpbWUuXG4gKlxuICoge0BleGFtcGxlIGNvbW1vbi9waXBlcy90cy9hc3luY19waXBlLnRzIHJlZ2lvbj0nQXN5bmNQaXBlT2JzZXJ2YWJsZSd9XG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5ASW5qZWN0YWJsZSgpXG5AUGlwZSh7bmFtZTogJ2FzeW5jJywgcHVyZTogZmFsc2V9KVxuZXhwb3J0IGNsYXNzIEFzeW5jUGlwZSBpbXBsZW1lbnRzIE9uRGVzdHJveSwgUGlwZVRyYW5zZm9ybSB7XG4gIHByaXZhdGUgX2xhdGVzdFZhbHVlOiBhbnkgPSBudWxsO1xuICBwcml2YXRlIF9sYXRlc3RSZXR1cm5lZFZhbHVlOiBhbnkgPSBudWxsO1xuXG4gIHByaXZhdGUgX3N1YnNjcmlwdGlvbjogU3Vic2NyaXB0aW9uTGlrZXxQcm9taXNlPGFueT58bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX29iajogT2JzZXJ2YWJsZTxhbnk+fFByb21pc2U8YW55PnxFdmVudEVtaXR0ZXI8YW55PnxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfc3RyYXRlZ3k6IFN1YnNjcmlwdGlvblN0cmF0ZWd5ID0gbnVsbCAhO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3JlZjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHt9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX3N1YnNjcmlwdGlvbikge1xuICAgICAgdGhpcy5fZGlzcG9zZSgpO1xuICAgIH1cbiAgfVxuXG4gIHRyYW5zZm9ybTxUPihvYmo6IG51bGwpOiBudWxsO1xuICB0cmFuc2Zvcm08VD4ob2JqOiB1bmRlZmluZWQpOiB1bmRlZmluZWQ7XG4gIHRyYW5zZm9ybTxUPihvYmo6IE9ic2VydmFibGU8VD58bnVsbHx1bmRlZmluZWQpOiBUfG51bGw7XG4gIHRyYW5zZm9ybTxUPihvYmo6IFByb21pc2U8VD58bnVsbHx1bmRlZmluZWQpOiBUfG51bGw7XG4gIHRyYW5zZm9ybShvYmo6IE9ic2VydmFibGU8YW55PnxQcm9taXNlPGFueT58bnVsbHx1bmRlZmluZWQpOiBhbnkge1xuICAgIGlmICghdGhpcy5fb2JqKSB7XG4gICAgICBpZiAob2JqKSB7XG4gICAgICAgIHRoaXMuX3N1YnNjcmliZShvYmopO1xuICAgICAgfVxuICAgICAgdGhpcy5fbGF0ZXN0UmV0dXJuZWRWYWx1ZSA9IHRoaXMuX2xhdGVzdFZhbHVlO1xuICAgICAgcmV0dXJuIHRoaXMuX2xhdGVzdFZhbHVlO1xuICAgIH1cblxuICAgIGlmIChvYmogIT09IHRoaXMuX29iaikge1xuICAgICAgdGhpcy5fZGlzcG9zZSgpO1xuICAgICAgcmV0dXJuIHRoaXMudHJhbnNmb3JtKG9iaiBhcyBhbnkpO1xuICAgIH1cblxuICAgIGlmICjJtWxvb3NlSWRlbnRpY2FsKHRoaXMuX2xhdGVzdFZhbHVlLCB0aGlzLl9sYXRlc3RSZXR1cm5lZFZhbHVlKSkge1xuICAgICAgcmV0dXJuIHRoaXMuX2xhdGVzdFJldHVybmVkVmFsdWU7XG4gICAgfVxuXG4gICAgdGhpcy5fbGF0ZXN0UmV0dXJuZWRWYWx1ZSA9IHRoaXMuX2xhdGVzdFZhbHVlO1xuICAgIHJldHVybiBXcmFwcGVkVmFsdWUud3JhcCh0aGlzLl9sYXRlc3RWYWx1ZSk7XG4gIH1cblxuICBwcml2YXRlIF9zdWJzY3JpYmUob2JqOiBPYnNlcnZhYmxlPGFueT58UHJvbWlzZTxhbnk+fEV2ZW50RW1pdHRlcjxhbnk+KTogdm9pZCB7XG4gICAgdGhpcy5fb2JqID0gb2JqO1xuICAgIHRoaXMuX3N0cmF0ZWd5ID0gdGhpcy5fc2VsZWN0U3RyYXRlZ3kob2JqKTtcbiAgICB0aGlzLl9zdWJzY3JpcHRpb24gPSB0aGlzLl9zdHJhdGVneS5jcmVhdGVTdWJzY3JpcHRpb24oXG4gICAgICAgIG9iaiwgKHZhbHVlOiBPYmplY3QpID0+IHRoaXMuX3VwZGF0ZUxhdGVzdFZhbHVlKG9iaiwgdmFsdWUpKTtcbiAgfVxuXG4gIHByaXZhdGUgX3NlbGVjdFN0cmF0ZWd5KG9iajogT2JzZXJ2YWJsZTxhbnk+fFByb21pc2U8YW55PnxFdmVudEVtaXR0ZXI8YW55Pik6IGFueSB7XG4gICAgaWYgKMm1aXNQcm9taXNlKG9iaikpIHtcbiAgICAgIHJldHVybiBfcHJvbWlzZVN0cmF0ZWd5O1xuICAgIH1cblxuICAgIGlmICjJtWlzT2JzZXJ2YWJsZShvYmopKSB7XG4gICAgICByZXR1cm4gX29ic2VydmFibGVTdHJhdGVneTtcbiAgICB9XG5cbiAgICB0aHJvdyBpbnZhbGlkUGlwZUFyZ3VtZW50RXJyb3IoQXN5bmNQaXBlLCBvYmopO1xuICB9XG5cbiAgcHJpdmF0ZSBfZGlzcG9zZSgpOiB2b2lkIHtcbiAgICB0aGlzLl9zdHJhdGVneS5kaXNwb3NlKHRoaXMuX3N1YnNjcmlwdGlvbiAhKTtcbiAgICB0aGlzLl9sYXRlc3RWYWx1ZSA9IG51bGw7XG4gICAgdGhpcy5fbGF0ZXN0UmV0dXJuZWRWYWx1ZSA9IG51bGw7XG4gICAgdGhpcy5fc3Vic2NyaXB0aW9uID0gbnVsbDtcbiAgICB0aGlzLl9vYmogPSBudWxsO1xuICB9XG5cbiAgcHJpdmF0ZSBfdXBkYXRlTGF0ZXN0VmFsdWUoYXN5bmM6IGFueSwgdmFsdWU6IE9iamVjdCk6IHZvaWQge1xuICAgIGlmIChhc3luYyA9PT0gdGhpcy5fb2JqKSB7XG4gICAgICB0aGlzLl9sYXRlc3RWYWx1ZSA9IHZhbHVlO1xuICAgICAgdGhpcy5fcmVmLm1hcmtGb3JDaGVjaygpO1xuICAgIH1cbiAgfVxufVxuIl19