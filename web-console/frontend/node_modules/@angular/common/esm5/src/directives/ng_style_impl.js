import * as tslib_1 from "tslib";
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ElementRef, Injectable, KeyValueDiffers, Renderer2 } from '@angular/core';
import { StylingDiffer } from './styling_differ';
/**
 * Used as a token for an injected service within the NgStyle directive.
 *
 * NgStyle behaves differenly whether or not VE is being used or not. If
 * present then the legacy ngClass diffing algorithm will be used as an
 * injected service. Otherwise the new diffing algorithm (which delegates
 * to the `[style]` binding) will be used. This toggle behavior is done so
 * via the ivy_switch mechanism.
 */
var NgStyleImpl = /** @class */ (function () {
    function NgStyleImpl() {
    }
    return NgStyleImpl;
}());
export { NgStyleImpl };
var NgStyleR2Impl = /** @class */ (function () {
    function NgStyleR2Impl(_ngEl, _differs, _renderer) {
        this._ngEl = _ngEl;
        this._differs = _differs;
        this._renderer = _renderer;
    }
    NgStyleR2Impl.prototype.getValue = function () { return null; };
    /**
     * A map of style properties, specified as colon-separated
     * key-value pairs.
     * * The key is a style name, with an optional `.<unit>` suffix
     *    (such as 'top.px', 'font-style.em').
     * * The value is an expression to be evaluated.
     */
    NgStyleR2Impl.prototype.setNgStyle = function (values) {
        this._ngStyle = values;
        if (!this._differ && values) {
            this._differ = this._differs.find(values).create();
        }
    };
    /**
     * Applies the new styles if needed.
     */
    NgStyleR2Impl.prototype.applyChanges = function () {
        if (this._differ) {
            var changes = this._differ.diff(this._ngStyle);
            if (changes) {
                this._applyChanges(changes);
            }
        }
    };
    NgStyleR2Impl.prototype._applyChanges = function (changes) {
        var _this = this;
        changes.forEachRemovedItem(function (record) { return _this._setStyle(record.key, null); });
        changes.forEachAddedItem(function (record) { return _this._setStyle(record.key, record.currentValue); });
        changes.forEachChangedItem(function (record) { return _this._setStyle(record.key, record.currentValue); });
    };
    NgStyleR2Impl.prototype._setStyle = function (nameAndUnit, value) {
        var _a = tslib_1.__read(nameAndUnit.split('.'), 2), name = _a[0], unit = _a[1];
        value = value != null && unit ? "" + value + unit : value;
        if (value != null) {
            this._renderer.setStyle(this._ngEl.nativeElement, name, value);
        }
        else {
            this._renderer.removeStyle(this._ngEl.nativeElement, name);
        }
    };
    NgStyleR2Impl = tslib_1.__decorate([
        Injectable(),
        tslib_1.__metadata("design:paramtypes", [ElementRef, KeyValueDiffers, Renderer2])
    ], NgStyleR2Impl);
    return NgStyleR2Impl;
}());
export { NgStyleR2Impl };
var NgStyleR3Impl = /** @class */ (function () {
    function NgStyleR3Impl() {
        this._differ = new StylingDiffer('NgStyle', 8 /* AllowUnits */);
        this._value = null;
    }
    NgStyleR3Impl.prototype.getValue = function () { return this._value; };
    NgStyleR3Impl.prototype.setNgStyle = function (value) { this._differ.setValue(value); };
    NgStyleR3Impl.prototype.applyChanges = function () {
        if (this._differ.hasValueChanged()) {
            this._value = this._differ.value;
        }
    };
    NgStyleR3Impl = tslib_1.__decorate([
        Injectable()
    ], NgStyleR3Impl);
    return NgStyleR3Impl;
}());
export { NgStyleR3Impl };
// the implementation for both NgClassR2Impl and NgClassR3Impl are
// not ivy_switch'd away, instead they are only hooked up into the
// DI via NgStyle's directive's provider property.
export var NgStyleImplProvider__PRE_R3__ = {
    provide: NgStyleImpl,
    useClass: NgStyleR2Impl
};
export var NgStyleImplProvider__POST_R3__ = {
    provide: NgStyleImpl,
    useClass: NgStyleR3Impl
};
export var NgStyleImplProvider = NgStyleImplProvider__PRE_R3__;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfc3R5bGVfaW1wbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvZGlyZWN0aXZlcy9uZ19zdHlsZV9pbXBsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7QUFBQTs7Ozs7O0dBTUc7QUFDSCxPQUFPLEVBQUMsVUFBVSxFQUFFLFVBQVUsRUFBbUMsZUFBZSxFQUFFLFNBQVMsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUVsSCxPQUFPLEVBQUMsYUFBYSxFQUF1QixNQUFNLGtCQUFrQixDQUFDO0FBRXJFOzs7Ozs7OztHQVFHO0FBQ0g7SUFBQTtJQUlBLENBQUM7SUFBRCxrQkFBQztBQUFELENBQUMsQUFKRCxJQUlDOztBQUdEO0lBTUUsdUJBQ1ksS0FBaUIsRUFBVSxRQUF5QixFQUFVLFNBQW9CO1FBQWxGLFVBQUssR0FBTCxLQUFLLENBQVk7UUFBVSxhQUFRLEdBQVIsUUFBUSxDQUFpQjtRQUFVLGNBQVMsR0FBVCxTQUFTLENBQVc7SUFBRyxDQUFDO0lBRWxHLGdDQUFRLEdBQVIsY0FBYSxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7SUFFM0I7Ozs7OztPQU1HO0lBQ0gsa0NBQVUsR0FBVixVQUFXLE1BQStCO1FBQ3hDLElBQUksQ0FBQyxRQUFRLEdBQUcsTUFBTSxDQUFDO1FBQ3ZCLElBQUksQ0FBQyxJQUFJLENBQUMsT0FBTyxJQUFJLE1BQU0sRUFBRTtZQUMzQixJQUFJLENBQUMsT0FBTyxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLE1BQU0sRUFBRSxDQUFDO1NBQ3BEO0lBQ0gsQ0FBQztJQUVEOztPQUVHO0lBQ0gsb0NBQVksR0FBWjtRQUNFLElBQUksSUFBSSxDQUFDLE9BQU8sRUFBRTtZQUNoQixJQUFNLE9BQU8sR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxD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XG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5pbXBvcnQge0VsZW1lbnRSZWYsIEluamVjdGFibGUsIEtleVZhbHVlQ2hhbmdlcywgS2V5VmFsdWVEaWZmZXIsIEtleVZhbHVlRGlmZmVycywgUmVuZGVyZXIyfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtTdHlsaW5nRGlmZmVyLCBTdHlsaW5nRGlmZmVyT3B0aW9uc30gZnJvbSAnLi9zdHlsaW5nX2RpZmZlcic7XG5cbi8qKlxuICogVXNlZCBhcyBhIHRva2VuIGZvciBhbiBpbmplY3RlZCBzZXJ2aWNlIHdpdGhpbiB0aGUgTmdTdHlsZSBkaXJlY3RpdmUuXG4gKlxuICogTmdTdHlsZSBiZWhhdmVzIGRpZmZlcmVubHkgd2hldGhlciBvciBub3QgVkUgaXMgYmVpbmcgdXNlZCBvciBub3QuIElmXG4gKiBwcmVzZW50IHRoZW4gdGhlIGxlZ2FjeSBuZ0NsYXNzIGRpZmZpbmcgYWxnb3JpdGhtIHdpbGwgYmUgdXNlZCBhcyBhblxuICogaW5qZWN0ZWQgc2VydmljZS4gT3RoZXJ3aXNlIHRoZSBuZXcgZGlmZmluZyBhbGdvcml0aG0gKHdoaWNoIGRlbGVnYXRlc1xuICogdG8gdGhlIGBbc3R5bGVdYCBiaW5kaW5nKSB3aWxsIGJlIHVzZWQuIFRoaXMgdG9nZ2xlIGJlaGF2aW9yIGlzIGRvbmUgc29cbiAqIHZpYSB0aGUgaXZ5X3N3aXRjaCBtZWNoYW5pc20uXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBOZ1N0eWxlSW1wbCB7XG4gIGFic3RyYWN0IGdldFZhbHVlKCk6IHtba2V5OiBzdHJpbmddOiBhbnl9fG51bGw7XG4gIGFic3RyYWN0IHNldE5nU3R5bGUodmFsdWU6IHtba2V5OiBzdHJpbmddOiBhbnl9fG51bGwpOiB2b2lkO1xuICBhYnN0cmFjdCBhcHBseUNoYW5nZXMoKTogdm9pZDtcbn1cblxuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE5nU3R5bGVSMkltcGwgaW1wbGVtZW50cyBOZ1N0eWxlSW1wbCB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9uZ1N0eWxlICE6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfZGlmZmVyICE6IEtleVZhbHVlRGlmZmVyPHN0cmluZywgc3RyaW5nfG51bWJlcj47XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9uZ0VsOiBFbGVtZW50UmVmLCBwcml2YXRlIF9kaWZmZXJzOiBLZXlWYWx1ZURpZmZlcnMsIHByaXZhdGUgX3JlbmRlcmVyOiBSZW5kZXJlcjIpIHt9XG5cbiAgZ2V0VmFsdWUoKSB7IHJldHVybiBudWxsOyB9XG5cbiAgLyoqXG4gICAqIEEgbWFwIG9mIHN0eWxlIHByb3BlcnRpZXMsIHNwZWNpZmllZCBhcyBjb2xvbi1zZXBhcmF0ZWRcbiAgICoga2V5LXZhbHVlIHBhaXJzLlxuICAgKiAqIFRoZSBrZXkgaXMgYSBzdHlsZSBuYW1lLCB3aXRoIGFuIG9wdGlvbmFsIGAuPHVuaXQ+YCBzdWZmaXhcbiAgICogICAgKHN1Y2ggYXMgJ3RvcC5weCcsICdmb250LXN0eWxlLmVtJykuXG4gICAqICogVGhlIHZhbHVlIGlzIGFuIGV4cHJlc3Npb24gdG8gYmUgZXZhbHVhdGVkLlxuICAgKi9cbiAgc2V0TmdTdHlsZSh2YWx1ZXM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9KSB7XG4gICAgdGhpcy5fbmdTdHlsZSA9IHZhbHVlcztcbiAgICBpZiAoIXRoaXMuX2RpZmZlciAmJiB2YWx1ZXMpIHtcbiAgICAgIHRoaXMuX2RpZmZlciA9IHRoaXMuX2RpZmZlcnMuZmluZCh2YWx1ZXMpLmNyZWF0ZSgpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBBcHBsaWVzIHRoZSBuZXcgc3R5bGVzIGlmIG5lZWRlZC5cbiAgICovXG4gIGFwcGx5Q2hhbmdlcygpIHtcbiAgICBpZiAodGhpcy5fZGlmZmVyKSB7XG4gICAgICBjb25zdCBjaGFuZ2VzID0gdGhpcy5fZGlmZmVyLmRpZmYodGhpcy5fbmdTdHlsZSk7XG4gICAgICBpZiAoY2hhbmdlcykge1xuICAgICAgICB0aGlzLl9hcHBseUNoYW5nZXMoY2hhbmdlcyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfYXBwbHlDaGFuZ2VzKGNoYW5nZXM6IEtleVZhbHVlQ2hhbmdlczxzdHJpbmcsIHN0cmluZ3xudW1iZXI+KTogdm9pZCB7XG4gICAgY2hhbmdlcy5mb3JFYWNoUmVtb3ZlZEl0ZW0oKHJlY29yZCkgPT4gdGhpcy5fc2V0U3R5bGUocmVjb3JkLmtleSwgbnVsbCkpO1xuICAgIGNoYW5nZXMuZm9yRWFjaEFkZGVkSXRlbSgocmVjb3JkKSA9PiB0aGlzLl9zZXRTdHlsZShyZWNvcmQua2V5LCByZWNvcmQuY3VycmVudFZhbHVlKSk7XG4gICAgY2hhbmdlcy5mb3JFYWNoQ2hhbmdlZEl0ZW0oKHJlY29yZCkgPT4gdGhpcy5fc2V0U3R5bGUocmVjb3JkLmtleSwgcmVjb3JkLmN1cnJlbnRWYWx1ZSkpO1xuICB9XG5cbiAgcHJpdmF0ZSBfc2V0U3R5bGUobmFtZUFuZFVuaXQ6IHN0cmluZywgdmFsdWU6IHN0cmluZ3xudW1iZXJ8bnVsbHx1bmRlZmluZWQpOiB2b2lkIHtcbiAgICBjb25zdCBbbmFtZSwgdW5pdF0gPSBuYW1lQW5kVW5pdC5zcGxpdCgnLicpO1xuICAgIHZhbHVlID0gdmFsdWUgIT0gbnVsbCAmJiB1bml0ID8gYCR7dmFsdWV9JHt1bml0fWAgOiB2YWx1ZTtcblxuICAgIGlmICh2YWx1ZSAhPSBudWxsKSB7XG4gICAgICB0aGlzLl9yZW5kZXJlci5zZXRTdHlsZSh0aGlzLl9uZ0VsLm5hdGl2ZUVsZW1lbnQsIG5hbWUsIHZhbHVlIGFzIHN0cmluZyk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX3JlbmRlcmVyLnJlbW92ZVN0eWxlKHRoaXMuX25nRWwubmF0aXZlRWxlbWVudCwgbmFtZSk7XG4gICAgfVxuICB9XG59XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBOZ1N0eWxlUjNJbXBsIGltcGxlbWVudHMgTmdTdHlsZUltcGwge1xuICBwcml2YXRlIF9kaWZmZXIgPVxuICAgICAgbmV3IFN0eWxpbmdEaWZmZXI8e1trZXk6IHN0cmluZ106IGFueX18bnVsbD4oJ05nU3R5bGUnLCBTdHlsaW5nRGlmZmVyT3B0aW9ucy5BbGxvd1VuaXRzKTtcblxuICBwcml2YXRlIF92YWx1ZToge1trZXk6IHN0cmluZ106IGFueX18bnVsbCA9IG51bGw7XG5cbiAgZ2V0VmFsdWUoKSB7IHJldHVybiB0aGlzLl92YWx1ZTsgfVxuXG4gIHNldE5nU3R5bGUodmFsdWU6IHtba2V5OiBzdHJpbmddOiBhbnl9fG51bGwpIHsgdGhpcy5fZGlmZmVyLnNldFZhbHVlKHZhbHVlKTsgfVxuXG4gIGFwcGx5Q2hhbmdlcygpIHtcbiAgICBpZiAodGhpcy5fZGlmZmVyLmhhc1ZhbHVlQ2hhbmdlZCgpKSB7XG4gICAgICB0aGlzLl92YWx1ZSA9IHRoaXMuX2RpZmZlci52YWx1ZTtcbiAgICB9XG4gIH1cbn1cblxuLy8gdGhlIGltcGxlbWVudGF0aW9uIGZvciBib3RoIE5nQ2xhc3NSMkltcGwgYW5kIE5nQ2xhc3NSM0ltcGwgYXJlXG4vLyBub3QgaXZ5X3N3aXRjaCdkIGF3YXksIGluc3RlYWQgdGhleSBhcmUgb25seSBob29rZWQgdXAgaW50byB0aGVcbi8vIERJIHZpYSBOZ1N0eWxlJ3MgZGlyZWN0aXZlJ3MgcHJvdmlkZXIgcHJvcGVydHkuXG5leHBvcnQgY29uc3QgTmdTdHlsZUltcGxQcm92aWRlcl9fUFJFX1IzX18gPSB7XG4gIHByb3ZpZGU6IE5nU3R5bGVJbXBsLFxuICB1c2VDbGFzczogTmdTdHlsZVIySW1wbFxufTtcblxuZXhwb3J0IGNvbnN0IE5nU3R5bGVJbXBsUHJvdmlkZXJfX1BPU1RfUjNfXyA9IHtcbiAgcHJvdmlkZTogTmdTdHlsZUltcGwsXG4gIHVzZUNsYXNzOiBOZ1N0eWxlUjNJbXBsXG59O1xuXG5leHBvcnQgY29uc3QgTmdTdHlsZUltcGxQcm92aWRlciA9IE5nU3R5bGVJbXBsUHJvdmlkZXJfX1BSRV9SM19fO1xuIl19