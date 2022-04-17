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
import { DomAdapter } from '../dom/dom_adapter';
/**
 * Provides DOM operations in any browser environment.
 *
 * \@security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 * @abstract
 */
export class GenericBrowserDomAdapter extends DomAdapter {
    constructor() {
        super();
        this._animationPrefix = null;
        this._transitionEnd = null;
        try {
            /** @type {?} */
            const element = this.createElement('div', document);
            if (this.getStyle(element, 'animationName') != null) {
                this._animationPrefix = '';
            }
            else {
                /** @type {?} */
                const domPrefixes = ['Webkit', 'Moz', 'O', 'ms'];
                for (let i = 0; i < domPrefixes.length; i++) {
                    if (this.getStyle(element, domPrefixes[i] + 'AnimationName') != null) {
                        this._animationPrefix = '-' + domPrefixes[i].toLowerCase() + '-';
                        break;
                    }
                }
            }
            /** @type {?} */
            const transEndEventNames = {
                WebkitTransition: 'webkitTransitionEnd',
                MozTransition: 'transitionend',
                OTransition: 'oTransitionEnd otransitionend',
                transition: 'transitionend'
            };
            Object.keys(transEndEventNames).forEach((/**
             * @param {?} key
             * @return {?}
             */
            (key) => {
                if (this.getStyle(element, key) != null) {
                    this._transitionEnd = transEndEventNames[key];
                }
            }));
        }
        catch (_a) {
            this._animationPrefix = null;
            this._transitionEnd = null;
        }
    }
    /**
     * @param {?} el
     * @return {?}
     */
    getDistributedNodes(el) { return ((/** @type {?} */ (el))).getDistributedNodes(); }
    /**
     * @param {?} el
     * @param {?} baseUrl
     * @param {?} href
     * @return {?}
     */
    resolveAndSetHref(el, baseUrl, href) {
        el.href = href == null ? baseUrl : baseUrl + '/../' + href;
    }
    /**
     * @return {?}
     */
    supportsDOMEvents() { return true; }
    /**
     * @return {?}
     */
    supportsNativeShadowDOM() {
        return typeof ((/** @type {?} */ (document.body))).createShadowRoot === 'function';
    }
    /**
     * @return {?}
     */
    getAnimationPrefix() { return this._animationPrefix ? this._animationPrefix : ''; }
    /**
     * @return {?}
     */
    getTransitionEnd() { return this._transitionEnd ? this._transitionEnd : ''; }
    /**
     * @return {?}
     */
    supportsAnimation() {
        return this._animationPrefix != null && this._transitionEnd != null;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    GenericBrowserDomAdapter.prototype._animationPrefix;
    /**
     * @type {?}
     * @private
     */
    GenericBrowserDomAdapter.prototype._transitionEnd;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2VuZXJpY19icm93c2VyX2FkYXB0ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9wbGF0Zm9ybS1icm93c2VyL3NyYy9icm93c2VyL2dlbmVyaWNfYnJvd3Nlcl9hZGFwdGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLG9CQUFvQixDQUFDOzs7Ozs7OztBQVU5QyxNQUFNLE9BQWdCLHdCQUF5QixTQUFRLFVBQVU7SUFHL0Q7UUFDRSxLQUFLLEVBQUUsQ0FBQztRQUhGLHFCQUFnQixHQUFnQixJQUFJLENBQUM7UUFDckMsbUJBQWMsR0FBZ0IsSUFBSSxDQUFDO1FBR3pDLElBQUk7O2tCQUNJLE9BQU8sR0FBRyxJQUFJLENBQUMsYUFBYSxDQUFDLEtBQUssRUFBRSxRQUFRLENBQUM7WUFDbkQsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLE9BQU8sRUFBRSxlQUFlLENBQUMsSUFBSSxJQUFJLEVBQUU7Z0JBQ25ELElBQUksQ0FBQyxnQkFBZ0IsR0FBRyxFQUFFLENBQUM7YUFDNUI7aUJBQU07O3NCQUNDLFdBQVcsR0FBRyxDQUFDLFFBQVEsRUFBRSxLQUFLLEVBQUUsR0FBRyxFQUFFLElBQUksQ0FBQztnQkFFaEQsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLFdBQVcsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7b0JBQzNDLElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxPQUFPLEVBQUUsV0FBVyxDQUFDLENBQUMsQ0FBQyxHQUFHLGVBQWUsQ0FBQyxJQUFJLElBQUksRUFBRTt3QkFDcEUsSUFBSSxDQUFDLGdCQUFnQixHQUFHLEdBQUcsR0FBRyxXQUFXLENBQUMsQ0FBQyxDQUFDLENBQUMsV0FBVyxFQUFFLEdBQUcsR0FBRyxDQUFDO3dCQUNqRSxNQUFNO3FCQUNQO2lCQUNGO2FBQ0Y7O2tCQUVLLGtCQUFrQixHQUE0QjtnQkFDbEQsZ0JBQWdCLEVBQUUscUJBQXFCO2dCQUN2QyxhQUFhLEVBQUUsZUFBZTtnQkFDOUIsV0FBVyxFQUFFLCtCQUErQjtnQkFDNUMsVUFBVSxFQUFFLGVBQWU7YUFDNUI7WUFFRCxNQUFNLENBQUMsSUFBSSxDQUFDLGtCQUFrQixDQUFDLENBQUMsT0FBTzs7OztZQUFDLENBQUMsR0FBVyxFQUFFLEVBQUU7Z0JBQ3RELElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxPQUFPLEVBQUUsR0FBRyxDQUFDLElBQUksSUFBSSxFQUFFO29CQUN2QyxJQUFJLENBQUMsY0FBYyxHQUFHLGtCQUFrQixDQUFDLEdBQUcsQ0FBQyxDQUFDO2lCQUMvQztZQUNILENBQUMsRUFBQyxDQUFDO1NBQ0o7UUFBQyxXQUFNO1lBQ04sSUFBSSxDQUFDLGdCQUFnQixHQUFHLElBQUksQ0FBQztZQUM3QixJQUFJLENBQUMsY0FBYyxHQUFHLElBQUksQ0FBQztTQUM1QjtJQUNILENBQUM7Ozs7O0lBRUQsbUJBQW1CLENBQUMsRUFBZSxJQUFZLE9BQU8sQ0FBQyxtQkFBSyxFQUFFLEVBQUEsQ0FBQyxDQUFDLG1CQUFtQixFQUFFLENBQUMsQ0FBQyxDQUFDOzs7Ozs7O0lBQ3hGLGlCQUFpQixDQUFDLEVBQXFCLEVBQUUsT0FBZSxFQUFFLElBQVk7UUFDcEUsRUFBRSxDQUFDLElBQUksR0FBRyxJQUFJLElBQUksSUFBSSxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLE9BQU8sR0FBRyxNQUFNLEdBQUcsSUFBSSxDQUFDO0lBQzdELENBQUM7Ozs7SUFDRCxpQkFBaUIsS0FBYyxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7Ozs7SUFDN0MsdUJBQXVCO1FBQ3JCLE9BQU8sT0FBTSxDQUFDLG1CQUFLLFFBQVEsQ0FBQyxJQUFJLEVBQUEsQ0FBQyxDQUFDLGdCQUFnQixLQUFLLFVBQVUsQ0FBQztJQUNwRSxDQUFDOzs7O0lBQ0Qsa0JBQWtCLEtBQWEsT0FBTyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQzs7OztJQUMzRixnQkFBZ0IsS0FBYSxPQUFPLElBQUksQ0FBQyxjQUFjLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxjQUFjLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7Ozs7SUFDckYsaUJBQWlCO1FBQ2YsT0FBTyxJQUFJLENBQUMsZ0JBQWdCLElBQUksSUFBSSxJQUFJLElBQUksQ0FBQyxjQUFjLElBQUksSUFBSSxDQUFDO0lBQ3RFLENBQUM7Q0FDRjs7Ozs7O0lBbERDLG9EQUE2Qzs7Ozs7SUFDN0Msa0RBQTJDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0RvbUFkYXB0ZXJ9IGZyb20gJy4uL2RvbS9kb21fYWRhcHRlcic7XG5cblxuXG4vKipcbiAqIFByb3ZpZGVzIERPTSBvcGVyYXRpb25zIGluIGFueSBicm93c2VyIGVudmlyb25tZW50LlxuICpcbiAqIEBzZWN1cml0eSBUcmVhZCBjYXJlZnVsbHkhIEludGVyYWN0aW5nIHdpdGggdGhlIERPTSBkaXJlY3RseSBpcyBkYW5nZXJvdXMgYW5kXG4gKiBjYW4gaW50cm9kdWNlIFhTUyByaXNrcy5cbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIEdlbmVyaWNCcm93c2VyRG9tQWRhcHRlciBleHRlbmRzIERvbUFkYXB0ZXIge1xuICBwcml2YXRlIF9hbmltYXRpb25QcmVmaXg6IHN0cmluZ3xudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfdHJhbnNpdGlvbkVuZDogc3RyaW5nfG51bGwgPSBudWxsO1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcigpO1xuICAgIHRyeSB7XG4gICAgICBjb25zdCBlbGVtZW50ID0gdGhpcy5jcmVhdGVFbGVtZW50KCdkaXYnLCBkb2N1bWVudCk7XG4gICAgICBpZiAodGhpcy5nZXRTdHlsZShlbGVtZW50LCAnYW5pbWF0aW9uTmFtZScpICE9IG51bGwpIHtcbiAgICAgICAgdGhpcy5fYW5pbWF0aW9uUHJlZml4ID0gJyc7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zdCBkb21QcmVmaXhlcyA9IFsnV2Via2l0JywgJ01veicsICdPJywgJ21zJ107XG5cbiAgICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBkb21QcmVmaXhlcy5sZW5ndGg7IGkrKykge1xuICAgICAgICAgIGlmICh0aGlzLmdldFN0eWxlKGVsZW1lbnQsIGRvbVByZWZpeGVzW2ldICsgJ0FuaW1hdGlvbk5hbWUnKSAhPSBudWxsKSB7XG4gICAgICAgICAgICB0aGlzLl9hbmltYXRpb25QcmVmaXggPSAnLScgKyBkb21QcmVmaXhlc1tpXS50b0xvd2VyQ2FzZSgpICsgJy0nO1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHRyYW5zRW5kRXZlbnROYW1lczoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSB7XG4gICAgICAgIFdlYmtpdFRyYW5zaXRpb246ICd3ZWJraXRUcmFuc2l0aW9uRW5kJyxcbiAgICAgICAgTW96VHJhbnNpdGlvbjogJ3RyYW5zaXRpb25lbmQnLFxuICAgICAgICBPVHJhbnNpdGlvbjogJ29UcmFuc2l0aW9uRW5kIG90cmFuc2l0aW9uZW5kJyxcbiAgICAgICAgdHJhbnNpdGlvbjogJ3RyYW5zaXRpb25lbmQnXG4gICAgICB9O1xuXG4gICAgICBPYmplY3Qua2V5cyh0cmFuc0VuZEV2ZW50TmFtZXMpLmZvckVhY2goKGtleTogc3RyaW5nKSA9PiB7XG4gICAgICAgIGlmICh0aGlzLmdldFN0eWxlKGVsZW1lbnQsIGtleSkgIT0gbnVsbCkge1xuICAgICAgICAgIHRoaXMuX3RyYW5zaXRpb25FbmQgPSB0cmFuc0VuZEV2ZW50TmFtZXNba2V5XTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfSBjYXRjaCB7XG4gICAgICB0aGlzLl9hbmltYXRpb25QcmVmaXggPSBudWxsO1xuICAgICAgdGhpcy5fdHJhbnNpdGlvbkVuZCA9IG51bGw7XG4gICAgfVxuICB9XG5cbiAgZ2V0RGlzdHJpYnV0ZWROb2RlcyhlbDogSFRNTEVsZW1lbnQpOiBOb2RlW10geyByZXR1cm4gKDxhbnk+ZWwpLmdldERpc3RyaWJ1dGVkTm9kZXMoKTsgfVxuICByZXNvbHZlQW5kU2V0SHJlZihlbDogSFRNTEFuY2hvckVsZW1lbnQsIGJhc2VVcmw6IHN0cmluZywgaHJlZjogc3RyaW5nKSB7XG4gICAgZWwuaHJlZiA9IGhyZWYgPT0gbnVsbCA/IGJhc2VVcmwgOiBiYXNlVXJsICsgJy8uLi8nICsgaHJlZjtcbiAgfVxuICBzdXBwb3J0c0RPTUV2ZW50cygpOiBib29sZWFuIHsgcmV0dXJuIHRydWU7IH1cbiAgc3VwcG9ydHNOYXRpdmVTaGFkb3dET00oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHR5cGVvZig8YW55PmRvY3VtZW50LmJvZHkpLmNyZWF0ZVNoYWRvd1Jvb3QgPT09ICdmdW5jdGlvbic7XG4gIH1cbiAgZ2V0QW5pbWF0aW9uUHJlZml4KCk6IHN0cmluZyB7IHJldHVybiB0aGlzLl9hbmltYXRpb25QcmVmaXggPyB0aGlzLl9hbmltYXRpb25QcmVmaXggOiAnJzsgfVxuICBnZXRUcmFuc2l0aW9uRW5kKCk6IHN0cmluZyB7IHJldHVybiB0aGlzLl90cmFuc2l0aW9uRW5kID8gdGhpcy5fdHJhbnNpdGlvbkVuZCA6ICcnOyB9XG4gIHN1cHBvcnRzQW5pbWF0aW9uKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9hbmltYXRpb25QcmVmaXggIT0gbnVsbCAmJiB0aGlzLl90cmFuc2l0aW9uRW5kICE9IG51bGw7XG4gIH1cbn1cbiJdfQ==