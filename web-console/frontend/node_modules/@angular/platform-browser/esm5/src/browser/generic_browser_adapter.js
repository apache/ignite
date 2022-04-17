/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { DomAdapter } from '../dom/dom_adapter';
/**
 * Provides DOM operations in any browser environment.
 *
 * @security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 */
var GenericBrowserDomAdapter = /** @class */ (function (_super) {
    tslib_1.__extends(GenericBrowserDomAdapter, _super);
    function GenericBrowserDomAdapter() {
        var _this = _super.call(this) || this;
        _this._animationPrefix = null;
        _this._transitionEnd = null;
        try {
            var element_1 = _this.createElement('div', document);
            if (_this.getStyle(element_1, 'animationName') != null) {
                _this._animationPrefix = '';
            }
            else {
                var domPrefixes = ['Webkit', 'Moz', 'O', 'ms'];
                for (var i = 0; i < domPrefixes.length; i++) {
                    if (_this.getStyle(element_1, domPrefixes[i] + 'AnimationName') != null) {
                        _this._animationPrefix = '-' + domPrefixes[i].toLowerCase() + '-';
                        break;
                    }
                }
            }
            var transEndEventNames_1 = {
                WebkitTransition: 'webkitTransitionEnd',
                MozTransition: 'transitionend',
                OTransition: 'oTransitionEnd otransitionend',
                transition: 'transitionend'
            };
            Object.keys(transEndEventNames_1).forEach(function (key) {
                if (_this.getStyle(element_1, key) != null) {
                    _this._transitionEnd = transEndEventNames_1[key];
                }
            });
        }
        catch (_a) {
            _this._animationPrefix = null;
            _this._transitionEnd = null;
        }
        return _this;
    }
    GenericBrowserDomAdapter.prototype.getDistributedNodes = function (el) { return el.getDistributedNodes(); };
    GenericBrowserDomAdapter.prototype.resolveAndSetHref = function (el, baseUrl, href) {
        el.href = href == null ? baseUrl : baseUrl + '/../' + href;
    };
    GenericBrowserDomAdapter.prototype.supportsDOMEvents = function () { return true; };
    GenericBrowserDomAdapter.prototype.supportsNativeShadowDOM = function () {
        return typeof document.body.createShadowRoot === 'function';
    };
    GenericBrowserDomAdapter.prototype.getAnimationPrefix = function () { return this._animationPrefix ? this._animationPrefix : ''; };
    GenericBrowserDomAdapter.prototype.getTransitionEnd = function () { return this._transitionEnd ? this._transitionEnd : ''; };
    GenericBrowserDomAdapter.prototype.supportsAnimation = function () {
        return this._animationPrefix != null && this._transitionEnd != null;
    };
    return GenericBrowserDomAdapter;
}(DomAdapter));
export { GenericBrowserDomAdapter };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2VuZXJpY19icm93c2VyX2FkYXB0ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9wbGF0Zm9ybS1icm93c2VyL3NyYy9icm93c2VyL2dlbmVyaWNfYnJvd3Nlcl9hZGFwdGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFJOUM7Ozs7O0dBS0c7QUFDSDtJQUF1RCxvREFBVTtJQUcvRDtRQUFBLFlBQ0UsaUJBQU8sU0FnQ1I7UUFuQ08sc0JBQWdCLEdBQWdCLElBQUksQ0FBQztRQUNyQyxvQkFBYyxHQUFnQixJQUFJLENBQUM7UUFHekMsSUFBSTtZQUNGLElBQU0sU0FBTyxHQUFHLEtBQUksQ0FBQyxhQUFhLENBQUMsS0FBSyxFQUFFLFFBQVEsQ0FBQyxDQUFDO1lBQ3BELElBQUksS0FBSSxDQUFDLFFBQVEsQ0FBQyxTQUFPLEVBQUUsZUFBZSxDQUFDLElBQUksSUFBSSxFQUFFO2dCQUNuRCxLQUFJLENBQUMsZ0JBQWdCLEdBQUcsRUFBRSxDQUFDO2FBQzVCO2lCQUFNO2dCQUNMLElBQU0sV0FBVyxHQUFHLENBQUMsUUFBUSxFQUFFLEtBQUssRUFBRSxHQUFHLEVBQUUsSUFBSSxDQUFDLENBQUM7Z0JBRWpELEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxXQUFXLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO29CQUMzQyxJQUFJLEtBQUksQ0FBQyxRQUFRLENBQUMsU0FBTyxFQUFFLFdBQVcsQ0FBQyxDQUFDLENBQUMsR0FBRyxlQUFlLENBQUMsSUFBSSxJQUFJLEVBQUU7d0JBQ3BFLEtBQUksQ0FBQyxnQkFBZ0IsR0FBRyxHQUFHLEdBQUcsV0FBVyxDQUFDLENBQUMsQ0FBQyxDQUFDLFdBQVcsRUFBRSxHQUFHLEdBQUcsQ0FBQzt3QkFDakUsTUFBTTtxQkFDUDtpQkFDRjthQUNGO1lBRUQsSUFBTSxvQkFBa0IsR0FBNEI7Z0JBQ2xELGdCQUFnQixFQUFFLHFCQUFxQjtnQkFDdkMsYUFBYSxFQUFFLGVBQWU7Z0JBQzlCLFdBQVcsRUFBRSwrQkFBK0I7Z0JBQzVDLFVBQVUsRUFBRSxlQUFlO2FBQzVCLENBQUM7WUFFRixNQUFNLENBQUMsSUFBSSxDQUFDLG9CQUFrQixDQUFDLENBQUMsT0FBTyxDQUFDLFVBQUMsR0FBVztnQkFDbEQsSUFBSSxLQUFJLENBQUMsUUFBUSxDQUFDLFNBQU8sRUFBRSxHQUFHLENBQUMsSUFBSSxJQUFJLEVBQUU7b0JBQ3ZDLEtBQUksQ0FBQyxjQUFjLEdBQUcsb0JBQWtCLENBQUMsR0FBRyxDQUFDLENBQUM7aUJBQy9DO1lBQ0gsQ0FBQyxDQUFDLENBQUM7U0FDSjtRQUFDLFdBQU07WUFDTixLQUFJLENBQUMsZ0JBQWdCLEdBQUcsSUFBSSxDQUFDO1lBQzdCLEtBQUksQ0FBQyxjQUFjLEdBQUcsSUFBSSxDQUFDO1NBQzVCOztJQUNILENBQUM7SUFFRCxzREFBbUIsR0FBbkIsVUFBb0IsRUFBZSxJQUFZLE9BQWEsRUFBRyxDQUFDLG1CQUFtQixFQUFFLENBQUMsQ0FBQyxDQUFDO0lBQ3hGLG9EQUFpQixHQUFqQixVQUFrQixFQUFxQixFQUFFLE9BQWUsRUFBRSxJQUFZO1FBQ3BFLEVBQUUsQ0FBQyxJQUFJLEdBQUcsSUFBSSxJQUFJLElBQUksQ0FBQyxDQUFDLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQyxPQUFPLEdBQUcsTUFBTSxHQUFHLElBQUksQ0FBQztJQUM3RCxDQUFDO0lBQ0Qsb0RBQWlCLEdBQWpCLGNBQStCLE9BQU8sSUFBSSxDQUFDLENBQUMsQ0FBQztJQUM3QywwREFBdUIsR0FBdkI7UUFDRSxPQUFPLE9BQVksUUFBUSxDQUFDLElBQUssQ0FBQyxnQkFBZ0IsS0FBSyxVQUFVLENBQUM7SUFDcEUsQ0FBQztJQUNELHFEQUFrQixHQUFsQixjQUErQixPQUFPLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDO0lBQzNGLG1EQUFnQixHQUFoQixjQUE2QixPQUFPLElBQUksQ0FBQyxjQUFjLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxjQUFjLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFDckYsb0RBQWlCLEdBQWpCO1FBQ0UsT0FBTyxJQUFJLENBQUMsZ0JBQWdCLElBQUksSUFBSSxJQUFJLElBQUksQ0FBQyxjQUFjLElBQUksSUFBSSxDQUFDO0lBQ3RFLENBQUM7SUFDSCwrQkFBQztBQUFELENBQUMsQUFuREQsQ0FBdUQsVUFBVSxHQW1EaEUiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RG9tQWRhcHRlcn0gZnJvbSAnLi4vZG9tL2RvbV9hZGFwdGVyJztcblxuXG5cbi8qKlxuICogUHJvdmlkZXMgRE9NIG9wZXJhdGlvbnMgaW4gYW55IGJyb3dzZXIgZW52aXJvbm1lbnQuXG4gKlxuICogQHNlY3VyaXR5IFRyZWFkIGNhcmVmdWxseSEgSW50ZXJhY3Rpbmcgd2l0aCB0aGUgRE9NIGRpcmVjdGx5IGlzIGRhbmdlcm91cyBhbmRcbiAqIGNhbiBpbnRyb2R1Y2UgWFNTIHJpc2tzLlxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgR2VuZXJpY0Jyb3dzZXJEb21BZGFwdGVyIGV4dGVuZHMgRG9tQWRhcHRlciB7XG4gIHByaXZhdGUgX2FuaW1hdGlvblByZWZpeDogc3RyaW5nfG51bGwgPSBudWxsO1xuICBwcml2YXRlIF90cmFuc2l0aW9uRW5kOiBzdHJpbmd8bnVsbCA9IG51bGw7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCk7XG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGVsZW1lbnQgPSB0aGlzLmNyZWF0ZUVsZW1lbnQoJ2RpdicsIGRvY3VtZW50KTtcbiAgICAgIGlmICh0aGlzLmdldFN0eWxlKGVsZW1lbnQsICdhbmltYXRpb25OYW1lJykgIT0gbnVsbCkge1xuICAgICAgICB0aGlzLl9hbmltYXRpb25QcmVmaXggPSAnJztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnN0IGRvbVByZWZpeGVzID0gWydXZWJraXQnLCAnTW96JywgJ08nLCAnbXMnXTtcblxuICAgICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGRvbVByZWZpeGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICAgICAgaWYgKHRoaXMuZ2V0U3R5bGUoZWxlbWVudCwgZG9tUHJlZml4ZXNbaV0gKyAnQW5pbWF0aW9uTmFtZScpICE9IG51bGwpIHtcbiAgICAgICAgICAgIHRoaXMuX2FuaW1hdGlvblByZWZpeCA9ICctJyArIGRvbVByZWZpeGVzW2ldLnRvTG93ZXJDYXNlKCkgKyAnLSc7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgY29uc3QgdHJhbnNFbmRFdmVudE5hbWVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHtcbiAgICAgICAgV2Via2l0VHJhbnNpdGlvbjogJ3dlYmtpdFRyYW5zaXRpb25FbmQnLFxuICAgICAgICBNb3pUcmFuc2l0aW9uOiAndHJhbnNpdGlvbmVuZCcsXG4gICAgICAgIE9UcmFuc2l0aW9uOiAnb1RyYW5zaXRpb25FbmQgb3RyYW5zaXRpb25lbmQnLFxuICAgICAgICB0cmFuc2l0aW9uOiAndHJhbnNpdGlvbmVuZCdcbiAgICAgIH07XG5cbiAgICAgIE9iamVjdC5rZXlzKHRyYW5zRW5kRXZlbnROYW1lcykuZm9yRWFjaCgoa2V5OiBzdHJpbmcpID0+IHtcbiAgICAgICAgaWYgKHRoaXMuZ2V0U3R5bGUoZWxlbWVudCwga2V5KSAhPSBudWxsKSB7XG4gICAgICAgICAgdGhpcy5fdHJhbnNpdGlvbkVuZCA9IHRyYW5zRW5kRXZlbnROYW1lc1trZXldO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9IGNhdGNoIHtcbiAgICAgIHRoaXMuX2FuaW1hdGlvblByZWZpeCA9IG51bGw7XG4gICAgICB0aGlzLl90cmFuc2l0aW9uRW5kID0gbnVsbDtcbiAgICB9XG4gIH1cblxuICBnZXREaXN0cmlidXRlZE5vZGVzKGVsOiBIVE1MRWxlbWVudCk6IE5vZGVbXSB7IHJldHVybiAoPGFueT5lbCkuZ2V0RGlzdHJpYnV0ZWROb2RlcygpOyB9XG4gIHJlc29sdmVBbmRTZXRIcmVmKGVsOiBIVE1MQW5jaG9yRWxlbWVudCwgYmFzZVVybDogc3RyaW5nLCBocmVmOiBzdHJpbmcpIHtcbiAgICBlbC5ocmVmID0gaHJlZiA9PSBudWxsID8gYmFzZVVybCA6IGJhc2VVcmwgKyAnLy4uLycgKyBocmVmO1xuICB9XG4gIHN1cHBvcnRzRE9NRXZlbnRzKCk6IGJvb2xlYW4geyByZXR1cm4gdHJ1ZTsgfVxuICBzdXBwb3J0c05hdGl2ZVNoYWRvd0RPTSgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdHlwZW9mKDxhbnk+ZG9jdW1lbnQuYm9keSkuY3JlYXRlU2hhZG93Um9vdCA9PT0gJ2Z1bmN0aW9uJztcbiAgfVxuICBnZXRBbmltYXRpb25QcmVmaXgoKTogc3RyaW5nIHsgcmV0dXJuIHRoaXMuX2FuaW1hdGlvblByZWZpeCA/IHRoaXMuX2FuaW1hdGlvblByZWZpeCA6ICcnOyB9XG4gIGdldFRyYW5zaXRpb25FbmQoKTogc3RyaW5nIHsgcmV0dXJuIHRoaXMuX3RyYW5zaXRpb25FbmQgPyB0aGlzLl90cmFuc2l0aW9uRW5kIDogJyc7IH1cbiAgc3VwcG9ydHNBbmltYXRpb24oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2FuaW1hdGlvblByZWZpeCAhPSBudWxsICYmIHRoaXMuX3RyYW5zaXRpb25FbmQgIT0gbnVsbDtcbiAgfVxufVxuIl19