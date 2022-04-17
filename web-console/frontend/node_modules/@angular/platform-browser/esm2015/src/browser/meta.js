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
import { DOCUMENT } from '@angular/common';
import { Inject, Injectable, ɵɵinject } from '@angular/core';
import { getDOM } from '../dom/dom_adapter';
import * as i0 from "@angular/core";
/**
 * Factory to create Meta service.
 * @return {?}
 */
export function createMeta() {
    return new Meta(ɵɵinject(DOCUMENT));
}
/**
 * A service that can be used to get and add meta tags.
 *
 * \@publicApi
 */
export class Meta {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        this._doc = _doc;
        this._dom = getDOM();
    }
    /**
     * @param {?} tag
     * @param {?=} forceCreation
     * @return {?}
     */
    addTag(tag, forceCreation = false) {
        if (!tag)
            return null;
        return this._getOrCreateElement(tag, forceCreation);
    }
    /**
     * @param {?} tags
     * @param {?=} forceCreation
     * @return {?}
     */
    addTags(tags, forceCreation = false) {
        if (!tags)
            return [];
        return tags.reduce((/**
         * @param {?} result
         * @param {?} tag
         * @return {?}
         */
        (result, tag) => {
            if (tag) {
                result.push(this._getOrCreateElement(tag, forceCreation));
            }
            return result;
        }), []);
    }
    /**
     * @param {?} attrSelector
     * @return {?}
     */
    getTag(attrSelector) {
        if (!attrSelector)
            return null;
        return this._dom.querySelector(this._doc, `meta[${attrSelector}]`) || null;
    }
    /**
     * @param {?} attrSelector
     * @return {?}
     */
    getTags(attrSelector) {
        if (!attrSelector)
            return [];
        /** @type {?} */
        const list /*NodeList*/ = this._dom.querySelectorAll(this._doc, `meta[${attrSelector}]`);
        return list ? [].slice.call(list) : [];
    }
    /**
     * @param {?} tag
     * @param {?=} selector
     * @return {?}
     */
    updateTag(tag, selector) {
        if (!tag)
            return null;
        selector = selector || this._parseSelector(tag);
        /** @type {?} */
        const meta = (/** @type {?} */ (this.getTag(selector)));
        if (meta) {
            return this._setMetaElementAttributes(tag, meta);
        }
        return this._getOrCreateElement(tag, true);
    }
    /**
     * @param {?} attrSelector
     * @return {?}
     */
    removeTag(attrSelector) { this.removeTagElement((/** @type {?} */ (this.getTag(attrSelector)))); }
    /**
     * @param {?} meta
     * @return {?}
     */
    removeTagElement(meta) {
        if (meta) {
            this._dom.remove(meta);
        }
    }
    /**
     * @private
     * @param {?} meta
     * @param {?=} forceCreation
     * @return {?}
     */
    _getOrCreateElement(meta, forceCreation = false) {
        if (!forceCreation) {
            /** @type {?} */
            const selector = this._parseSelector(meta);
            /** @type {?} */
            const elem = (/** @type {?} */ (this.getTag(selector)));
            // It's allowed to have multiple elements with the same name so it's not enough to
            // just check that element with the same name already present on the page. We also need to
            // check if element has tag attributes
            if (elem && this._containsAttributes(meta, elem))
                return elem;
        }
        /** @type {?} */
        const element = (/** @type {?} */ (this._dom.createElement('meta')));
        this._setMetaElementAttributes(meta, element);
        /** @type {?} */
        const head = this._dom.getElementsByTagName(this._doc, 'head')[0];
        this._dom.appendChild(head, element);
        return element;
    }
    /**
     * @private
     * @param {?} tag
     * @param {?} el
     * @return {?}
     */
    _setMetaElementAttributes(tag, el) {
        Object.keys(tag).forEach((/**
         * @param {?} prop
         * @return {?}
         */
        (prop) => this._dom.setAttribute(el, prop, tag[prop])));
        return el;
    }
    /**
     * @private
     * @param {?} tag
     * @return {?}
     */
    _parseSelector(tag) {
        /** @type {?} */
        const attr = tag.name ? 'name' : 'property';
        return `${attr}="${tag[attr]}"`;
    }
    /**
     * @private
     * @param {?} tag
     * @param {?} elem
     * @return {?}
     */
    _containsAttributes(tag, elem) {
        return Object.keys(tag).every((/**
         * @param {?} key
         * @return {?}
         */
        (key) => this._dom.getAttribute(elem, key) === tag[key]));
    }
}
Meta.decorators = [
    { type: Injectable, args: [{ providedIn: 'root', useFactory: createMeta, deps: [] },] }
];
/** @nocollapse */
Meta.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
/** @nocollapse */ Meta.ngInjectableDef = i0.ɵɵdefineInjectable({ factory: createMeta, token: Meta, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    Meta.prototype._dom;
    /**
     * @type {?}
     * @private
     */
    Meta.prototype._doc;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWV0YS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXIvc3JjL2Jyb3dzZXIvbWV0YS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUN6QyxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFM0QsT0FBTyxFQUFhLE1BQU0sRUFBQyxNQUFNLG9CQUFvQixDQUFDOzs7Ozs7QUF3QnRELE1BQU0sVUFBVSxVQUFVO0lBQ3hCLE9BQU8sSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUM7QUFDdEMsQ0FBQzs7Ozs7O0FBUUQsTUFBTSxPQUFPLElBQUk7Ozs7SUFFZixZQUFzQyxJQUFTO1FBQVQsU0FBSSxHQUFKLElBQUksQ0FBSztRQUFJLElBQUksQ0FBQyxJQUFJLEdBQUcsTUFBTSxFQUFFLENBQUM7SUFBQyxDQUFDOzs7Ozs7SUFFMUUsTUFBTSxDQUFDLEdBQW1CLEVBQUUsZ0JBQXlCLEtBQUs7UUFDeEQsSUFBSSxDQUFDLEdBQUc7WUFBRSxPQUFPLElBQUksQ0FBQztRQUN0QixPQUFPLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxHQUFHLEVBQUUsYUFBYSxDQUFDLENBQUM7SUFDdEQsQ0FBQzs7Ozs7O0lBRUQsT0FBTyxDQUFDLElBQXNCLEVBQUUsZ0JBQXlCLEtBQUs7UUFDNUQsSUFBSSxDQUFDLElBQUk7WUFBRSxPQUFPLEVBQUUsQ0FBQztRQUNyQixPQUFPLElBQUksQ0FBQyxNQUFNOzs7OztRQUFDLENBQUMsTUFBeUIsRUFBRSxHQUFtQixFQUFFLEVBQUU7WUFDcEUsSUFBSSxHQUFHLEVBQUU7Z0JBQ1AsTUFBTSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsbUJBQW1CLENBQUMsR0FBRyxF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RyxDQUFDLENBQUMsT0FBTzs7OztRQUFDLENBQUMsSUFBWSxFQUFFLEVBQUUsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxFQUFFLEVBQUUsSUFBSSxFQUFFLEdBQUcsQ0FBQyxJQUFJLENBQUMsQ0FBQyxFQUFDLENBQUM7UUFDeEYsT0FBTyxFQUFFLENBQUM7SUFDWixDQUFDOzs7Ozs7SUFFTyxjQUFjLENBQUMsR0FBbUI7O2NBQ2xDLElBQUksR0FBVyxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLFVBQVU7UUFDbkQsT0FBTyxHQUFHLElBQUksS0FBSyxHQUFHLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQztJQUNsQyxDQUFDOzs7Ozs7O0lBRU8sbUJBQW1CLENBQUMsR0FBbUIsRUFBRSxJQUFxQjtRQUNwRSxPQUFPLE1BQU0sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsS0FBSzs7OztRQUFDLENBQUMsR0FBVyxFQUFFLEVBQUUsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLEVBQUUsR0FBRyxDQUFDLEtBQUssR0FBRyxDQUFDLEdBQUcsQ0FBQyxFQUFDLENBQUM7SUFDakcsQ0FBQzs7O1lBOUVGLFVBQVUsU0FBQyxFQUFDLFVBQVUsRUFBRSxNQUFNLEVBQUUsVUFBVSxFQUFFLFVBQVUsRUFBRSxJQUFJLEVBQUUsRUFBRSxFQUFDOzs7OzRDQUduRCxNQUFNLFNBQUMsUUFBUTs7Ozs7Ozs7SUFENUIsb0JBQXlCOzs7OztJQUNiLG9CQUFtQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtET0NVTUVOVH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7SW5qZWN0LCBJbmplY3RhYmxlLCDJtcm1aW5qZWN0fSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtEb21BZGFwdGVyLCBnZXRET019IGZyb20gJy4uL2RvbS9kb21fYWRhcHRlcic7XG5cblxuXG4vKipcbiAqIFJlcHJlc2VudHMgYSBtZXRhIGVsZW1lbnQuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgdHlwZSBNZXRhRGVmaW5pdGlvbiA9IHtcbiAgY2hhcnNldD86IHN0cmluZzsgY29udGVudD86IHN0cmluZzsgaHR0cEVxdWl2Pzogc3RyaW5nOyBpZD86IHN0cmluZzsgaXRlbXByb3A/OiBzdHJpbmc7XG4gIG5hbWU/OiBzdHJpbmc7XG4gIHByb3BlcnR5Pzogc3RyaW5nO1xuICBzY2hlbWU/OiBzdHJpbmc7XG4gIHVybD86IHN0cmluZztcbn0gJlxue1xuICAvLyBUT0RPKElnb3JNaW5hcik6IHRoaXMgdHlwZSBsb29rcyB3cm9uZ1xuICBbcHJvcDogc3RyaW5nXTogc3RyaW5nO1xufTtcblxuLyoqXG4gKiBGYWN0b3J5IHRvIGNyZWF0ZSBNZXRhIHNlcnZpY2UuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVNZXRhKCkge1xuICByZXR1cm4gbmV3IE1ldGEoybXJtWluamVjdChET0NVTUVOVCkpO1xufVxuXG4vKipcbiAqIEEgc2VydmljZSB0aGF0IGNhbiBiZSB1c2VkIHRvIGdldCBhbmQgYWRkIG1ldGEgdGFncy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKHtwcm92aWRlZEluOiAncm9vdCcsIHVzZUZhY3Rvcnk6IGNyZWF0ZU1ldGEsIGRlcHM6IFtdfSlcbmV4cG9ydCBjbGFzcyBNZXRhIHtcbiAgcHJpdmF0ZSBfZG9tOiBEb21BZGFwdGVyO1xuICBjb25zdHJ1Y3RvcihASW5qZWN0KERPQ1VNRU5UKSBwcml2YXRlIF9kb2M6IGFueSkgeyB0aGlzLl9kb20gPSBnZXRET00oKTsgfVxuXG4gIGFkZFRhZyh0YWc6IE1ldGFEZWZpbml0aW9uLCBmb3JjZUNyZWF0aW9uOiBib29sZWFuID0gZmFsc2UpOiBIVE1MTWV0YUVsZW1lbnR8bnVsbCB7XG4gICAgaWYgKCF0YWcpIHJldHVybiBudWxsO1xuICAgIHJldHVybiB0aGlzLl9nZXRPckNyZWF0ZUVsZW1lbnQodGFnLCBmb3JjZUNyZWF0aW9uKTtcbiAgfVxuXG4gIGFkZFRhZ3ModGFnczogTWV0YURlZmluaXRpb25bXSwgZm9yY2VDcmVhdGlvbjogYm9vbGVhbiA9IGZhbHNlKTogSFRNTE1ldGFFbGVtZW50W10ge1xuICAgIGlmICghdGFncykgcmV0dXJuIFtdO1xuICAgIHJldHVybiB0YWdzLnJlZHVjZSgocmVzdWx0OiBIVE1MTWV0YUVsZW1lbnRbXSwgdGFnOiBNZXRhRGVmaW5pdGlvbikgPT4ge1xuICAgICAgaWYgKHRhZykge1xuICAgICAgICByZXN1bHQucHVzaCh0aGlzLl9nZXRPckNyZWF0ZUVsZW1lbnQodGFnLCBmb3JjZUNyZWF0aW9uKSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gcmVzdWx0O1xuICAgIH0sIFtdKTtcbiAgfVxuXG4gIGdldFRhZyhhdHRyU2VsZWN0b3I6IHN0cmluZyk6IEhUTUxNZXRhRWxlbWVudHxudWxsIHtcbiAgICBpZiAoIWF0dHJTZWxlY3RvcikgcmV0dXJuIG51bGw7XG4gICAgcmV0dXJuIHRoaXMuX2RvbS5xdWVyeVNlbGVjdG9yKHRoaXMuX2RvYywgYG1ldGFbJHthdHRyU2VsZWN0b3J9XWApIHx8IG51bGw7XG4gIH1cblxuICBnZXRUYWdzKGF0dHJTZWxlY3Rvcjogc3RyaW5nKTogSFRNTE1ldGFFbGVtZW50W10ge1xuICAgIGlmICghYXR0clNlbGVjdG9yKSByZXR1cm4gW107XG4gICAgY29uc3QgbGlzdCAvKk5vZGVMaXN0Ki8gPSB0aGlzLl9kb20ucXVlcnlTZWxlY3RvckFsbCh0aGlzLl9kb2MsIGBtZXRhWyR7YXR0clNlbGVjdG9yfV1gKTtcbiAgICByZXR1cm4gbGlzdCA/IFtdLnNsaWNlLmNhbGwobGlzdCkgOiBbXTtcbiAgfVxuXG4gIHVwZGF0ZVRhZyh0YWc6IE1ldGFEZWZpbml0aW9uLCBzZWxlY3Rvcj86IHN0cmluZyk6IEhUTUxNZXRhRWxlbWVudHxudWxsIHtcbiAgICBpZiAoIXRhZykgcmV0dXJuIG51bGw7XG4gICAgc2VsZWN0b3IgPSBzZWxlY3RvciB8fCB0aGlzLl9wYXJzZVNlbGVjdG9yKHRhZyk7XG4gICAgY29uc3QgbWV0YTogSFRNTE1ldGFFbGVtZW50ID0gdGhpcy5nZXRUYWcoc2VsZWN0b3IpICE7XG4gICAgaWYgKG1ldGEpIHtcbiAgICAgIHJldHVybiB0aGlzLl9zZXRNZXRhRWxlbWVudEF0dHJpYnV0ZXModGFnLCBtZXRhKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX2dldE9yQ3JlYXRlRWxlbWVudCh0YWcsIHRydWUpO1xuICB9XG5cbiAgcmVtb3ZlVGFnKGF0dHJTZWxlY3Rvcjogc3RyaW5nKTogdm9pZCB7IHRoaXMucmVtb3ZlVGFnRWxlbWVudCh0aGlzLmdldFRhZyhhdHRyU2VsZWN0b3IpICEpOyB9XG5cbiAgcmVtb3ZlVGFnRWxlbWVudChtZXRhOiBIVE1MTWV0YUVsZW1lbnQpOiB2b2lkIHtcbiAgICBpZiAobWV0YSkge1xuICAgICAgdGhpcy5fZG9tLnJlbW92ZShtZXRhKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9nZXRPckNyZWF0ZUVsZW1lbnQobWV0YTogTWV0YURlZmluaXRpb24sIGZvcmNlQ3JlYXRpb246IGJvb2xlYW4gPSBmYWxzZSk6XG4gICAgICBIVE1MTWV0YUVsZW1lbnQge1xuICAgIGlmICghZm9yY2VDcmVhdGlvbikge1xuICAgICAgY29uc3Qgc2VsZWN0b3I6IHN0cmluZyA9IHRoaXMuX3BhcnNlU2VsZWN0b3IobWV0YSk7XG4gICAgICBjb25zdCBlbGVtOiBIVE1MTWV0YUVsZW1lbnQgPSB0aGlzLmdldFRhZyhzZWxlY3RvcikgITtcbiAgICAgIC8vIEl0J3MgYWxsb3dlZCB0byBoYXZlIG11bHRpcGxlIGVsZW1lbnRzIHdpdGggdGhlIHNhbWUgbmFtZSBzbyBpdCdzIG5vdCBlbm91Z2ggdG9cbiAgICAgIC8vIGp1c3QgY2hlY2sgdGhhdCBlbGVtZW50IHdpdGggdGhlIHNhbWUgbmFtZSBhbHJlYWR5IHByZXNlbnQgb24gdGhlIHBhZ2UuIFdlIGFsc28gbmVlZCB0b1xuICAgICAgLy8gY2hlY2sgaWYgZWxlbWVudCBoYXMgdGFnIGF0dHJpYnV0ZXNcbiAgICAgIGlmIChlbGVtICYmIHRoaXMuX2NvbnRhaW5zQXR0cmlidXRlcyhtZXRhLCBlbGVtKSkgcmV0dXJuIGVsZW07XG4gICAgfVxuICAgIGNvbnN0IGVsZW1lbnQ6IEhUTUxNZXRhRWxlbWVudCA9IHRoaXMuX2RvbS5jcmVhdGVFbGVtZW50KCdtZXRhJykgYXMgSFRNTE1ldGFFbGVtZW50O1xuICAgIHRoaXMuX3NldE1ldGFFbGVtZW50QXR0cmlidXRlcyhtZXRhLCBlbGVtZW50KTtcbiAgICBjb25zdCBoZWFkID0gdGhpcy5fZG9tLmdldEVsZW1lbnRzQnlUYWdOYW1lKHRoaXMuX2RvYywgJ2hlYWQnKVswXTtcbiAgICB0aGlzLl9kb20uYXBwZW5kQ2hpbGQoaGVhZCwgZWxlbWVudCk7XG4gICAgcmV0dXJuIGVsZW1lbnQ7XG4gIH1cblxuICBwcml2YXRlIF9zZXRNZXRhRWxlbWVudEF0dHJpYnV0ZXModGFnOiBNZXRhRGVmaW5pdGlvbiwgZWw6IEhUTUxNZXRhRWxlbWVudCk6IEhUTUxNZXRhRWxlbWVudCB7XG4gICAgT2JqZWN0LmtleXModGFnKS5mb3JFYWNoKChwcm9wOiBzdHJpbmcpID0+IHRoaXMuX2RvbS5zZXRBdHRyaWJ1dGUoZWwsIHByb3AsIHRhZ1twcm9wXSkpO1xuICAgIHJldHVybiBlbDtcbiAgfVxuXG4gIHByaXZhdGUgX3BhcnNlU2VsZWN0b3IodGFnOiBNZXRhRGVmaW5pdGlvbik6IHN0cmluZyB7XG4gICAgY29uc3QgYXR0cjogc3RyaW5nID0gdGFnLm5hbWUgPyAnbmFtZScgOiAncHJvcGVydHknO1xuICAgIHJldHVybiBgJHthdHRyfT1cIiR7dGFnW2F0dHJdfVwiYDtcbiAgfVxuXG4gIHByaXZhdGUgX2NvbnRhaW5zQXR0cmlidXRlcyh0YWc6IE1ldGFEZWZpbml0aW9uLCBlbGVtOiBIVE1MTWV0YUVsZW1lbnQpOiBib29sZWFuIHtcbiAgICByZXR1cm4gT2JqZWN0LmtleXModGFnKS5ldmVyeSgoa2V5OiBzdHJpbmcpID0+IHRoaXMuX2RvbS5nZXRBdHRyaWJ1dGUoZWxlbSwga2V5KSA9PT0gdGFnW2tleV0pO1xuICB9XG59XG4iXX0=