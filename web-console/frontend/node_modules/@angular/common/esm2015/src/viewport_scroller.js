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
import { ErrorHandler, ɵɵdefineInjectable, ɵɵinject } from '@angular/core';
import { DOCUMENT } from './dom_tokens';
/**
 * Defines a scroll position manager. Implemented by `BrowserViewportScroller`.
 *
 * \@publicApi
 * @abstract
 */
export class ViewportScroller {
}
// De-sugared tree-shakable injection
// See #23917
/** @nocollapse */
/** @nocollapse */ ViewportScroller.ngInjectableDef = ɵɵdefineInjectable({
    token: ViewportScroller,
    providedIn: 'root',
    factory: (/**
     * @nocollapse @return {?}
     */
    () => new BrowserViewportScroller(ɵɵinject(DOCUMENT), window, ɵɵinject(ErrorHandler)))
});
if (false) {
    /**
     * @nocollapse
     * @type {?}
     */
    ViewportScroller.ngInjectableDef;
    /**
     * Configures the top offset used when scrolling to an anchor.
     * @abstract
     * @param {?} offset A position in screen coordinates (a tuple with x and y values)
     * or a function that returns the top offset position.
     *
     * @return {?}
     */
    ViewportScroller.prototype.setOffset = function (offset) { };
    /**
     * Retrieves the current scroll position.
     * @abstract
     * @return {?} A position in screen coordinates (a tuple with x and y values).
     */
    ViewportScroller.prototype.getScrollPosition = function () { };
    /**
     * Scrolls to a specified position.
     * @abstract
     * @param {?} position A position in screen coordinates (a tuple with x and y values).
     * @return {?}
     */
    ViewportScroller.prototype.scrollToPosition = function (position) { };
    /**
     * Scrolls to an anchor element.
     * @abstract
     * @param {?} anchor The ID of the anchor element.
     * @return {?}
     */
    ViewportScroller.prototype.scrollToAnchor = function (anchor) { };
    /**
     * Disables automatic scroll restoration provided by the browser.
     * See also [window.history.scrollRestoration
     * info](https://developers.google.com/web/updates/2015/09/history-api-scroll-restoration).
     * @abstract
     * @param {?} scrollRestoration
     * @return {?}
     */
    ViewportScroller.prototype.setHistoryScrollRestoration = function (scrollRestoration) { };
}
/**
 * Manages the scroll position for a browser window.
 */
export class BrowserViewportScroller {
    /**
     * @param {?} document
     * @param {?} window
     * @param {?} errorHandler
     */
    constructor(document, window, errorHandler) {
        this.document = document;
        this.window = window;
        this.errorHandler = errorHandler;
        this.offset = (/**
         * @return {?}
         */
        () => [0, 0]);
    }
    /**
     * Configures the top offset used when scrolling to an anchor.
     * @param {?} offset A position in screen coordinates (a tuple with x and y values)
     * or a function that returns the top offset position.
     *
     * @return {?}
     */
    setOffset(offset) {
        if (Array.isArray(offset)) {
            this.offset = (/**
             * @return {?}
             */
            () => offset);
        }
        else {
            this.offset = offset;
        }
    }
    /**
     * Retrieves the current scroll position.
     * @return {?} The position in screen coordinates.
     */
    getScrollPosition() {
        if (this.supportScrollRestoration()) {
            return [this.window.scrollX, this.window.scrollY];
        }
        else {
            return [0, 0];
        }
    }
    /**
     * Sets the scroll position.
     * @param {?} position The new position in screen coordinates.
     * @return {?}
     */
    scrollToPosition(position) {
        if (this.supportScrollRestoration()) {
            this.window.scrollTo(position[0], position[1]);
        }
    }
    /**
     * Scrolls to an anchor element.
     * @param {?} anchor The ID of the anchor element.
     * @return {?}
     */
    scrollToAnchor(anchor) {
        if (this.supportScrollRestoration()) {
            // Escape anything passed to `querySelector` as it can throw errors and stop the application
            // from working if invalid values are passed.
            if (this.window.CSS && this.window.CSS.escape) {
                anchor = this.window.CSS.escape(anchor);
            }
            else {
                anchor = anchor.replace(/(\"|\'\ |:|\.|\[|\]|,|=)/g, '\\$1');
            }
            try {
                /** @type {?} */
                const elSelectedById = this.document.querySelector(`#${anchor}`);
                if (elSelectedById) {
                    this.scrollToElement(elSelectedById);
                    return;
                }
                /** @type {?} */
                const elSelectedByName = this.document.querySelector(`[name='${anchor}']`);
                if (elSelectedByName) {
                    this.scrollToElement(elSelectedByName);
                    return;
                }
            }
            catch (e) {
                this.errorHandler.handleError(e);
            }
        }
    }
    /**
     * Disables automatic scroll restoration provided by the browser.
     * @param {?} scrollRestoration
     * @return {?}
     */
    setHistoryScrollRestoration(scrollRestoration) {
        if (this.supportScrollRestoration()) {
            /** @type {?} */
            const history = this.window.history;
            if (history && history.scrollRestoration) {
                history.scrollRestoration = scrollRestoration;
            }
        }
    }
    /**
     * @private
     * @param {?} el
     * @return {?}
     */
    scrollToElement(el) {
        /** @type {?} */
        const rect = el.getBoundingClientRect();
        /** @type {?} */
        const left = rect.left + this.window.pageXOffset;
        /** @type {?} */
        const top = rect.top + this.window.pageYOffset;
        /** @type {?} */
        const offset = this.offset();
        this.window.scrollTo(left - offset[0], top - offset[1]);
    }
    /**
     * We only support scroll restoration when we can get a hold of window.
     * This means that we do not support this behavior when running in a web worker.
     *
     * Lifting this restriction right now would require more changes in the dom adapter.
     * Since webworkers aren't widely used, we will lift it once RouterScroller is
     * battle-tested.
     * @private
     * @return {?}
     */
    supportScrollRestoration() {
        try {
            return !!this.window && !!this.window.scrollTo;
        }
        catch (_a) {
            return false;
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    BrowserViewportScroller.prototype.offset;
    /**
     * @type {?}
     * @private
     */
    BrowserViewportScroller.prototype.document;
    /**
     * @type {?}
     * @private
     */
    BrowserViewportScroller.prototype.window;
    /**
     * @type {?}
     * @private
     */
    BrowserViewportScroller.prototype.errorHandler;
}
/**
 * Provides an empty implementation of the viewport scroller. This will
 * live in \@angular/common as it will be used by both platform-server and platform-webworker.
 */
export class NullViewportScroller {
    /**
     * Empty implementation
     * @param {?} offset
     * @return {?}
     */
    setOffset(offset) { }
    /**
     * Empty implementation
     * @return {?}
     */
    getScrollPosition() { return [0, 0]; }
    /**
     * Empty implementation
     * @param {?} position
     * @return {?}
     */
    scrollToPosition(position) { }
    /**
     * Empty implementation
     * @param {?} anchor
     * @return {?}
     */
    scrollToAnchor(anchor) { }
    /**
     * Empty implementation
     * @param {?} scrollRestoration
     * @return {?}
     */
    setHistoryScrollRestoration(scrollRestoration) { }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlld3BvcnRfc2Nyb2xsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vc3JjL3ZpZXdwb3J0X3Njcm9sbGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFlBQVksRUFBRSxrQkFBa0IsRUFBRSxRQUFRLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFekUsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGNBQWMsQ0FBQzs7Ozs7OztBQVN0QyxNQUFNLE9BQWdCLGdCQUFnQjs7Ozs7QUFJN0IsZ0NBQWUsR0FBRyxrQkFBa0IsQ0FBQztJQUMxQyxLQUFLLEVBQUUsZ0JBQWdCO0lBQ3ZCLFVBQVUsRUFBRSxNQUFNO0lBQ2xCLE9BQU87OztJQUFFLEdBQUcsRUFBRSxDQUFDLElBQUksdUJBQXVCLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxFQUFFLE1BQU0sRUFBRSxRQUFRLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQTtDQUMvRixDQUFDLENBQUM7Ozs7OztJQUpILGlDQUlHOzs7Ozs7Ozs7SUFRSCw2REFBNEU7Ozs7OztJQU01RSwrREFBK0M7Ozs7Ozs7SUFNL0Msc0VBQTREOzs7Ozs7O0lBTTVELGtFQUE4Qzs7Ozs7Ozs7O0lBTzlDLDBGQUErRTs7Ozs7QUFNakYsTUFBTSxPQUFPLHVCQUF1Qjs7Ozs7O0lBR2xDLFlBQW9CLFFBQWEsRUFBVSxNQUFXLEVBQVUsWUFBMEI7UUFBdEUsYUFBUSxHQUFSLFFBQVEsQ0FBSztRQUFVLFdBQU0sR0FBTixNQUFNLENBQUs7UUFBVSxpQkFBWSxHQUFaLFlBQVksQ0FBYztRQUZsRixXQUFNOzs7UUFBMkIsR0FBRyxFQUFFLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUM7SUFFdUMsQ0FBQzs7Ozs7Ozs7SUFROUYsU0FBUyxDQUFDLE1BQWlEO1FBQ3pELElBQUksS0FBSyxDQUFDLE9BQU8sQ0FB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LENBQUM7Ozs7Ozs7Ozs7O0lBVU8sd0JBQXdCO1FBQzlCLElBQUk7WUFDRixPQUFPLENBQUMsQ0FBQyxJQUFJLENBQUMsTUFBTSxJQUFJLENBQUMsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLFFBQVEsQ0FBQztTQUNoRDtRQUFDLFdBQU07WUFDTixPQUFPLEtBQUssQ0FBQztTQUNkO0lBQ0gsQ0FBQztDQUNGOzs7Ozs7SUF6R0MseUNBQXNEOzs7OztJQUUxQywyQ0FBcUI7Ozs7O0lBQUUseUNBQW1COzs7OztJQUFFLCtDQUFrQzs7Ozs7O0FBOEc1RixNQUFNLE9BQU8sb0JBQW9COzs7Ozs7SUFJL0IsU0FBUyxDQUFDLE1BQWlELElBQVMsQ0FBQzs7Ozs7SUFLckUsaUJBQWlCLEtBQXVCLE9BQU8sQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFLeEQsZ0JBQWdCLENBQUMsUUFBMEIsSUFBUyxDQUFDOzs7Ozs7SUFLckQsY0FBYyxDQUFDLE1BQWMsSUFBUyxDQUFDOzs7Ozs7SUFLdkMsMkJBQTJCLENBQUMsaUJBQWtDLElBQVMsQ0FBQztDQUN6RSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtFcnJvckhhbmRsZXIsIMm1ybVkZWZpbmVJbmplY3RhYmxlLCDJtcm1aW5qZWN0fSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtET0NVTUVOVH0gZnJvbSAnLi9kb21fdG9rZW5zJztcblxuXG5cbi8qKlxuICogRGVmaW5lcyBhIHNjcm9sbCBwb3NpdGlvbiBtYW5hZ2VyLiBJbXBsZW1lbnRlZCBieSBgQnJvd3NlclZpZXdwb3J0U2Nyb2xsZXJgLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFZpZXdwb3J0U2Nyb2xsZXIge1xuICAvLyBEZS1zdWdhcmVkIHRyZWUtc2hha2FibGUgaW5qZWN0aW9uXG4gIC8vIFNlZSAjMjM5MTdcbiAgLyoqIEBub2NvbGxhcHNlICovXG4gIHN0YXRpYyBuZ0luamVjdGFibGVEZWYgPSDJtcm1ZGVmaW5lSW5qZWN0YWJsZSh7XG4gICAgdG9rZW46IFZpZXdwb3J0U2Nyb2xsZXIsXG4gICAgcHJvdmlkZWRJbjogJ3Jvb3QnLFxuICAgIGZhY3Rvcnk6ICgpID0+IG5ldyBCcm93c2VyVmlld3BvcnRTY3JvbGxlcijJtcm1aW5qZWN0KERPQ1VNRU5UKSwgd2luZG93LCDJtcm1aW5qZWN0KEVycm9ySGFuZGxlcikpXG4gIH0pO1xuXG4gIC8qKlxuICAgKiBDb25maWd1cmVzIHRoZSB0b3Agb2Zmc2V0IHVzZWQgd2hlbiBzY3JvbGxpbmcgdG8gYW4gYW5jaG9yLlxuICAgKiBAcGFyYW0gb2Zmc2V0IEEgcG9zaXRpb24gaW4gc2NyZWVuIGNvb3JkaW5hdGVzIChhIHR1cGxlIHdpdGggeCBhbmQgeSB2YWx1ZXMpXG4gICAqIG9yIGEgZnVuY3Rpb24gdGhhdCByZXR1cm5zIHRoZSB0b3Agb2Zmc2V0IHBvc2l0aW9uLlxuICAgKlxuICAgKi9cbiAgYWJzdHJhY3Qgc2V0T2Zmc2V0KG9mZnNldDogW251bWJlciwgbnVtYmVyXXwoKCkgPT4gW251bWJlciwgbnVtYmVyXSkpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgdGhlIGN1cnJlbnQgc2Nyb2xsIHBvc2l0aW9uLlxuICAgKiBAcmV0dXJucyBBIHBvc2l0aW9uIGluIHNjcmVlbiBjb29yZGluYXRlcyAoYSB0dXBsZSB3aXRoIHggYW5kIHkgdmFsdWVzKS5cbiAgICovXG4gIGFic3RyYWN0IGdldFNjcm9sbFBvc2l0aW9uKCk6IFtudW1iZXIsIG51bWJlcl07XG5cbiAgLyoqXG4gICAqIFNjcm9sbHMgdG8gYSBzcGVjaWZpZWQgcG9zaXRpb24uXG4gICAqIEBwYXJhbSBwb3NpdGlvbiBBIHBvc2l0aW9uIGluIHNjcmVlbiBjb29yZGluYXRlcyAoYSB0dXBsZSB3aXRoIHggYW5kIHkgdmFsdWVzKS5cbiAgICovXG4gIGFic3RyYWN0IHNjcm9sbFRvUG9zaXRpb24ocG9zaXRpb246IFtudW1iZXIsIG51bWJlcl0pOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBTY3JvbGxzIHRvIGFuIGFuY2hvciBlbGVtZW50LlxuICAgKiBAcGFyYW0gYW5jaG9yIFRoZSBJRCBvZiB0aGUgYW5jaG9yIGVsZW1lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBzY3JvbGxUb0FuY2hvcihhbmNob3I6IHN0cmluZyk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIERpc2FibGVzIGF1dG9tYXRpYyBzY3JvbGwgcmVzdG9yYXRpb24gcHJvdmlkZWQgYnkgdGhlIGJyb3dzZXIuXG4gICAqIFNlZSBhbHNvIFt3aW5kb3cuaGlzdG9yeS5zY3JvbGxSZXN0b3JhdGlvblxuICAgKiBpbmZvXShodHRwczovL2RldmVsb3BlcnMuZ29vZ2xlLmNvbS93ZWIvdXBkYXRlcy8yMDE1LzA5L2hpc3RvcnktYXBpLXNjcm9sbC1yZXN0b3JhdGlvbikuXG4gICAqL1xuICBhYnN0cmFjdCBzZXRIaXN0b3J5U2Nyb2xsUmVzdG9yYXRpb24oc2Nyb2xsUmVzdG9yYXRpb246ICdhdXRvJ3wnbWFudWFsJyk6IHZvaWQ7XG59XG5cbi8qKlxuICogTWFuYWdlcyB0aGUgc2Nyb2xsIHBvc2l0aW9uIGZvciBhIGJyb3dzZXIgd2luZG93LlxuICovXG5leHBvcnQgY2xhc3MgQnJvd3NlclZpZXdwb3J0U2Nyb2xsZXIgaW1wbGVtZW50cyBWaWV3cG9ydFNjcm9sbGVyIHtcbiAgcHJpdmF0ZSBvZmZzZXQ6ICgpID0+IFtudW1iZXIsIG51bWJlcl0gPSAoKSA9PiBbMCwgMF07XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBkb2N1bWVudDogYW55LCBwcml2YXRlIHdpbmRvdzogYW55LCBwcml2YXRlIGVycm9ySGFuZGxlcjogRXJyb3JIYW5kbGVyKSB7fVxuXG4gIC8qKlxuICAgKiBDb25maWd1cmVzIHRoZSB0b3Agb2Zmc2V0IHVzZWQgd2hlbiBzY3JvbGxpbmcgdG8gYW4gYW5jaG9yLlxuICAgKiBAcGFyYW0gb2Zmc2V0IEEgcG9zaXRpb24gaW4gc2NyZWVuIGNvb3JkaW5hdGVzIChhIHR1cGxlIHdpdGggeCBhbmQgeSB2YWx1ZXMpXG4gICAqIG9yIGEgZnVuY3Rpb24gdGhhdCByZXR1cm5zIHRoZSB0b3Agb2Zmc2V0IHBvc2l0aW9uLlxuICAgKlxuICAgKi9cbiAgc2V0T2Zmc2V0KG9mZnNldDogW251bWJlciwgbnVtYmVyXXwoKCkgPT4gW251bWJlciwgbnVtYmVyXSkpOiB2b2lkIHtcbiAgICBpZiAoQXJyYXkuaXNBcnJheShvZmZzZXQpKSB7XG4gICAgICB0aGlzLm9mZnNldCA9ICgpID0+IG9mZnNldDtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5vZmZzZXQgPSBvZmZzZXQ7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyB0aGUgY3VycmVudCBzY3JvbGwgcG9zaXRpb24uXG4gICAqIEByZXR1cm5zIFRoZSBwb3NpdGlvbiBpbiBzY3JlZW4gY29vcmRpbmF0ZXMuXG4gICAqL1xuICBnZXRTY3JvbGxQb3NpdGlvbigpOiBbbnVtYmVyLCBudW1iZXJdIHtcbiAgICBpZiAodGhpcy5zdXBwb3J0U2Nyb2xsUmVzdG9yYXRpb24oKSkge1xuICAgICAgcmV0dXJuIFt0aGlzLndpbmRvdy5zY3JvbGxYLCB0aGlzLndpbmRvdy5zY3JvbGxZXTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIFswLCAwXTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogU2V0cyB0aGUgc2Nyb2xsIHBvc2l0aW9uLlxuICAgKiBAcGFyYW0gcG9zaXRpb24gVGhlIG5ldyBwb3NpdGlvbiBpbiBzY3JlZW4gY29vcmRpbmF0ZXMuXG4gICAqL1xuICBzY3JvbGxUb1Bvc2l0aW9uKHBvc2l0aW9uOiBbbnVtYmVyLCBudW1iZXJdKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuc3VwcG9ydFNjcm9sbFJlc3RvcmF0aW9uKCkpIHtcbiAgICAgIHRoaXMud2luZG93LnNjcm9sbFRvKHBvc2l0aW9uWzBdLCBwb3NpdGlvblsxXSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFNjcm9sbHMgdG8gYW4gYW5jaG9yIGVsZW1lbnQuXG4gICAqIEBwYXJhbSBhbmNob3IgVGhlIElEIG9mIHRoZSBhbmNob3IgZWxlbWVudC5cbiAgICovXG4gIHNjcm9sbFRvQW5jaG9yKGFuY2hvcjogc3RyaW5nKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuc3VwcG9ydFNjcm9sbFJlc3RvcmF0aW9uKCkpIHtcbiAgICAgIC8vIEVzY2FwZSBhbnl0aGluZyBwYXNzZWQgdG8gYHF1ZXJ5U2VsZWN0b3JgIGFzIGl0IGNhbiB0aHJvdyBlcnJvcnMgYW5kIHN0b3AgdGhlIGFwcGxpY2F0aW9uXG4gICAgICAvLyBmcm9tIHdvcmtpbmcgaWYgaW52YWxpZCB2YWx1ZXMgYXJlIHBhc3NlZC5cbiAgICAgIGlmICh0aGlzLndpbmRvdy5DU1MgJiYgdGhpcy53aW5kb3cuQ1NTLmVzY2FwZSkge1xuICAgICAgICBhbmNob3IgPSB0aGlzLndpbmRvdy5DU1MuZXNjYXBlKGFuY2hvcik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBhbmNob3IgPSBhbmNob3IucmVwbGFjZSgvKFxcXCJ8XFwnXFwgfDp8XFwufFxcW3xcXF18LHw9KS9nLCAnXFxcXCQxJyk7XG4gICAgICB9XG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCBlbFNlbGVjdGVkQnlJZCA9IHRoaXMuZG9jdW1lbnQucXVlcnlTZWxlY3RvcihgIyR7YW5jaG9yfWApO1xuICAgICAgICBpZiAoZWxTZWxlY3RlZEJ5SWQpIHtcbiAgICAgICAgICB0aGlzLnNjcm9sbFRvRWxlbWVudChlbFNlbGVjdGVkQnlJZCk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IGVsU2VsZWN0ZWRCeU5hbWUgPSB0aGlzLmRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoYFtuYW1lPScke2FuY2hvcn0nXWApO1xuICAgICAgICBpZiAoZWxTZWxlY3RlZEJ5TmFtZSkge1xuICAgICAgICAgIHRoaXMuc2Nyb2xsVG9FbGVtZW50KGVsU2VsZWN0ZWRCeU5hbWUpO1xuICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICB0aGlzLmVycm9ySGFuZGxlci5oYW5kbGVFcnJvcihlKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogRGlzYWJsZXMgYXV0b21hdGljIHNjcm9sbCByZXN0b3JhdGlvbiBwcm92aWRlZCBieSB0aGUgYnJvd3Nlci5cbiAgICovXG4gIHNldEhpc3RvcnlTY3JvbGxSZXN0b3JhdGlvbihzY3JvbGxSZXN0b3JhdGlvbjogJ2F1dG8nfCdtYW51YWwnKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuc3VwcG9ydFNjcm9sbFJlc3RvcmF0aW9uKCkpIHtcbiAgICAgIGNvbnN0IGhpc3RvcnkgPSB0aGlzLndpbmRvdy5oaXN0b3J5O1xuICAgICAgaWYgKGhpc3RvcnkgJiYgaGlzdG9yeS5zY3JvbGxSZXN0b3JhdGlvbikge1xuICAgICAgICBoaXN0b3J5LnNjcm9sbFJlc3RvcmF0aW9uID0gc2Nyb2xsUmVzdG9yYXRpb247XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBzY3JvbGxUb0VsZW1lbnQoZWw6IGFueSk6IHZvaWQge1xuICAgIGNvbnN0IHJlY3QgPSBlbC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKTtcbiAgICBjb25zdCBsZWZ0ID0gcmVjdC5sZWZ0ICsgdGhpcy53aW5kb3cucGFnZVhPZmZzZXQ7XG4gICAgY29uc3QgdG9wID0gcmVjdC50b3AgKyB0aGlzLndpbmRvdy5wYWdlWU9mZnNldDtcbiAgICBjb25zdCBvZmZzZXQgPSB0aGlzLm9mZnNldCgpO1xuICAgIHRoaXMud2luZG93LnNjcm9sbFRvKGxlZnQgLSBvZmZzZXRbMF0sIHRvcCAtIG9mZnNldFsxXSk7XG4gIH1cblxuICAvKipcbiAgICogV2Ugb25seSBzdXBwb3J0IHNjcm9sbCByZXN0b3JhdGlvbiB3aGVuIHdlIGNhbiBnZXQgYSBob2xkIG9mIHdpbmRvdy5cbiAgICogVGhpcyBtZWFucyB0aGF0IHdlIGRvIG5vdCBzdXBwb3J0IHRoaXMgYmVoYXZpb3Igd2hlbiBydW5uaW5nIGluIGEgd2ViIHdvcmtlci5cbiAgICpcbiAgICogTGlmdGluZyB0aGlzIHJlc3RyaWN0aW9uIHJpZ2h0IG5vdyB3b3VsZCByZXF1aXJlIG1vcmUgY2hhbmdlcyBpbiB0aGUgZG9tIGFkYXB0ZXIuXG4gICAqIFNpbmNlIHdlYndvcmtlcnMgYXJlbid0IHdpZGVseSB1c2VkLCB3ZSB3aWxsIGxpZnQgaXQgb25jZSBSb3V0ZXJTY3JvbGxlciBpc1xuICAgKiBiYXR0bGUtdGVzdGVkLlxuICAgKi9cbiAgcHJpdmF0ZSBzdXBwb3J0U2Nyb2xsUmVzdG9yYXRpb24oKTogYm9vbGVhbiB7XG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiAhIXRoaXMud2luZG93ICYmICEhdGhpcy53aW5kb3cuc2Nyb2xsVG87XG4gICAgfSBjYXRjaCB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG59XG5cblxuLyoqXG4gKiBQcm92aWRlcyBhbiBlbXB0eSBpbXBsZW1lbnRhdGlvbiBvZiB0aGUgdmlld3BvcnQgc2Nyb2xsZXIuIFRoaXMgd2lsbFxuICogbGl2ZSBpbiBAYW5ndWxhci9jb21tb24gYXMgaXQgd2lsbCBiZSB1c2VkIGJ5IGJvdGggcGxhdGZvcm0tc2VydmVyIGFuZCBwbGF0Zm9ybS13ZWJ3b3JrZXIuXG4gKi9cbmV4cG9ydCBjbGFzcyBOdWxsVmlld3BvcnRTY3JvbGxlciBpbXBsZW1lbnRzIFZpZXdwb3J0U2Nyb2xsZXIge1xuICAvKipcbiAgICogRW1wdHkgaW1wbGVtZW50YXRpb25cbiAgICovXG4gIHNldE9mZnNldChvZmZzZXQ6IFtudW1iZXIsIG51bWJlcl18KCgpID0+IFtudW1iZXIsIG51bWJlcl0pKTogdm9pZCB7fVxuXG4gIC8qKlxuICAgKiBFbXB0eSBpbXBsZW1lbnRhdGlvblxuICAgKi9cbiAgZ2V0U2Nyb2xsUG9zaXRpb24oKTogW251bWJlciwgbnVtYmVyXSB7IHJldHVybiBbMCwgMF07IH1cblxuICAvKipcbiAgICogRW1wdHkgaW1wbGVtZW50YXRpb25cbiAgICovXG4gIHNjcm9sbFRvUG9zaXRpb24ocG9zaXRpb246IFtudW1iZXIsIG51bWJlcl0pOiB2b2lkIHt9XG5cbiAgLyoqXG4gICAqIEVtcHR5IGltcGxlbWVudGF0aW9uXG4gICAqL1xuICBzY3JvbGxUb0FuY2hvcihhbmNob3I6IHN0cmluZyk6IHZvaWQge31cblxuICAvKipcbiAgICogRW1wdHkgaW1wbGVtZW50YXRpb25cbiAgICovXG4gIHNldEhpc3RvcnlTY3JvbGxSZXN0b3JhdGlvbihzY3JvbGxSZXN0b3JhdGlvbjogJ2F1dG8nfCdtYW51YWwnKTogdm9pZCB7fVxufVxuIl19