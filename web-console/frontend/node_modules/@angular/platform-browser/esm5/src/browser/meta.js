import * as tslib_1 from "tslib";
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
 */
export function createMeta() {
    return new Meta(ɵɵinject(DOCUMENT));
}
/**
 * A service that can be used to get and add meta tags.
 *
 * @publicApi
 */
var Meta = /** @class */ (function () {
    function Meta(_doc) {
        this._doc = _doc;
        this._dom = getDOM();
    }
    Meta.prototype.addTag = function (tag, forceCreation) {
        if (forceCreation === void 0) { forceCreation = false; }
        if (!tag)
            return null;
        return this._getOrCreateElement(tag, forceCreation);
    };
    Meta.prototype.addTags = function (tags, forceCreation) {
        var _this = this;
        if (forceCreation === void 0) { forceCreation = false; }
        if (!tags)
            return [];
        return tags.reduce(function (result, tag) {
            if (tag) {
                result.push(_this._getOrCreateElement(tag, forceCreation));
            }
            return result;
        }, []);
    };
    Meta.prototype.getTag = function (attrSelector) {
        if (!attrSelector)
            return null;
        return this._dom.querySelector(this._doc, "meta[" + attrSelector + "]") || null;
    };
    Meta.prototype.getTags = function (attrSelector) {
        if (!attrSelector)
            return [];
        var list /*NodeList*/ = this._dom.querySelectorAll(this._doc, "meta[" + attrSelector + "]");
        return list ? [].slice.call(list) : [];
    };
    Meta.prototype.updateTag = function (tag, selector) {
        if (!tag)
            return null;
        selector = selector || this._parseSelector(tag);
        var meta = this.getTag(selector);
        if (meta) {
            return this._setMetaElementAttributes(tag, meta);
        }
        return this._getOrCreateElement(tag, true);
    };
    Meta.prototype.removeTag = function (attrSelector) { this.removeTagElement(this.getTag(attrSelector)); };
    Meta.prototype.removeTagElement = function (meta) {
        if (meta) {
            this._dom.remove(meta);
        }
    };
    Meta.prototype._getOrCreateElement = function (meta, forceCreation) {
        if (forceCreation === void 0) { forceCreation = false; }
        if (!forceCreation) {
            var selector = this._parseSelector(meta);
            var elem = this.getTag(selector);
            // It's allowed to have multiple elements with the same name so it's not enough to
            // just check that element with the same name already present on the page. We also need to
            // check if element has tag attributes
            if (elem && this._containsAttributes(meta, elem))
                return elem;
        }
        var element = this._dom.createElement('meta');
        this._setMetaElementAttributes(meta, element);
        var head = this._dom.getElementsByTagName(this._doc, 'head')[0];
        this._dom.appendChild(head, element);
        return element;
    };
    Meta.prototype._setMetaElementAttributes = function (tag, el) {
        var _this = this;
        Object.keys(tag).forEach(function (prop) { return _this._dom.setAttribute(el, prop, tag[prop]); });
        return el;
    };
    Meta.prototype._parseSelector = function (tag) {
        var attr = tag.name ? 'name' : 'property';
        return attr + "=\"" + tag[attr] + "\"";
    };
    Meta.prototype._containsAttributes = function (tag, elem) {
        var _this = this;
        return Object.keys(tag).every(function (key) { return _this._dom.getAttribute(elem, key) === tag[key]; });
    };
    Meta.ngInjectableDef = i0.ɵɵdefineInjectable({ factory: createMeta, token: Meta, providedIn: "root" });
    Meta = tslib_1.__decorate([
        Injectable({ providedIn: 'root', useFactory: createMeta, deps: [] }),
        tslib_1.__param(0, Inject(DOCUMENT)),
        tslib_1.__metadata("design:paramtypes", [Object])
    ], Meta);
    return Meta;
}());
export { Meta };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWV0YS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXIvc3JjL2Jyb3dzZXIvbWV0YS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiO0FBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3pDLE9BQU8sRUFBQyxNQUFNLEVBQUUsVUFBVSxFQUFFLFFBQVEsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUUzRCxPQUFPLEVBQWEsTUFBTSxFQUFDLE1BQU0sb0JBQW9CLENBQUM7O0FBcUJ0RDs7R0FFRztBQUNILE1BQU0sVUFBVSxVQUFVO0lBQ3hCLE9BQU8sSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUM7QUFDdEMsQ0FBQztBQUVEOzs7O0dBSUc7QUFFSDtJQUVFLGNBQXNDLElBQVM7UUFBVCxTQUFJLEdBQUosSUFBSSxDQUFLO1FBQUksSUFBSSxDQUFDLElBQUksR0FBRyxNQUFNLEVBQUUsQ0FBQztJQUFDLENBQUM7SUFFMUUscUJBQU0sR0FBTixVQUFPLEdBQW1CLEVBQUUsYUFBOEI7UUFBOUIsOEJBQUEsRUFBQSxxQkFBOEI7UUFDeEQsSUFBSSxDQUFDLEdBQUc7WUFBRSxPQUFPLElBQUksQ0FBQztRQUN0QixPQUFPLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxHQUFHLEVBQUUsYUFBYSxDQUFDLENBQUM7SUFDdEQsQ0FBQztJQUVELHNCQUFPLEdBQVAsVUFBUSxJQUFzQixFQUFFLGFBQThCO1FBQTlELGlCQVFDO1FBUitCLDhCQUFBLEVBQUEscUJBQThCO1FBQzVELElBQUksQ0FBQyxJQUFJO1lBQUUsT0FBTyxFQUFFLENBQUM7UUFDckIsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLFVBQUMsTUFBeUIsRUFBRSxHQUFtQjtZQUNoRSxJQUFJLEdBQUcsRUFBRTtnQkFDUCxN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XG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RE9DVU1FTlR9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge0luamVjdCwgSW5qZWN0YWJsZSwgybXJtWluamVjdH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7RG9tQWRhcHRlciwgZ2V0RE9NfSBmcm9tICcuLi9kb20vZG9tX2FkYXB0ZXInO1xuXG5cblxuLyoqXG4gKiBSZXByZXNlbnRzIGEgbWV0YSBlbGVtZW50LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IHR5cGUgTWV0YURlZmluaXRpb24gPSB7XG4gIGNoYXJzZXQ/OiBzdHJpbmc7IGNvbnRlbnQ/OiBzdHJpbmc7IGh0dHBFcXVpdj86IHN0cmluZzsgaWQ/OiBzdHJpbmc7IGl0ZW1wcm9wPzogc3RyaW5nO1xuICBuYW1lPzogc3RyaW5nO1xuICBwcm9wZXJ0eT86IHN0cmluZztcbiAgc2NoZW1lPzogc3RyaW5nO1xuICB1cmw/OiBzdHJpbmc7XG59ICZcbntcbiAgLy8gVE9ETyhJZ29yTWluYXIpOiB0aGlzIHR5cGUgbG9va3Mgd3JvbmdcbiAgW3Byb3A6IHN0cmluZ106IHN0cmluZztcbn07XG5cbi8qKlxuICogRmFjdG9yeSB0byBjcmVhdGUgTWV0YSBzZXJ2aWNlLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlTWV0YSgpIHtcbiAgcmV0dXJuIG5ldyBNZXRhKMm1ybVpbmplY3QoRE9DVU1FTlQpKTtcbn1cblxuLyoqXG4gKiBBIHNlcnZpY2UgdGhhdCBjYW4gYmUgdXNlZCB0byBnZXQgYW5kIGFkZCBtZXRhIHRhZ3MuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5ASW5qZWN0YWJsZSh7cHJvdmlkZWRJbjogJ3Jvb3QnLCB1c2VGYWN0b3J5OiBjcmVhdGVNZXRhLCBkZXBzOiBbXX0pXG5leHBvcnQgY2xhc3MgTWV0YSB7XG4gIHByaXZhdGUgX2RvbTogRG9tQWRhcHRlcjtcbiAgY29uc3RydWN0b3IoQEluamVjdChET0NVTUVOVCkgcHJpdmF0ZSBfZG9jOiBhbnkpIHsgdGhpcy5fZG9tID0gZ2V0RE9NKCk7IH1cblxuICBhZGRUYWcodGFnOiBNZXRhRGVmaW5pdGlvbiwgZm9yY2VDcmVhdGlvbjogYm9vbGVhbiA9IGZhbHNlKTogSFRNTE1ldGFFbGVtZW50fG51bGwge1xuICAgIGlmICghdGFnKSByZXR1cm4gbnVsbDtcbiAgICByZXR1cm4gdGhpcy5fZ2V0T3JDcmVhdGVFbGVtZW50KHRhZywgZm9yY2VDcmVhdGlvbik7XG4gIH1cblxuICBhZGRUYWdzKHRhZ3M6IE1ldGFEZWZpbml0aW9uW10sIGZvcmNlQ3JlYXRpb246IGJvb2xlYW4gPSBmYWxzZSk6IEhUTUxNZXRhRWxlbWVudFtdIHtcbiAgICBpZiAoIXRhZ3MpIHJldHVybiBbXTtcbiAgICByZXR1cm4gdGFncy5yZWR1Y2UoKHJlc3VsdDogSFRNTE1ldGFFbGVtZW50W10sIHRhZzogTWV0YURlZmluaXRpb24pID0+IHtcbiAgICAgIGlmICh0YWcpIHtcbiAgICAgICAgcmVzdWx0LnB1c2godGhpcy5fZ2V0T3JDcmVhdGVFbGVtZW50KHRhZywgZm9yY2VDcmVhdGlvbikpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIHJlc3VsdDtcbiAgICB9LCBbXSk7XG4gIH1cblxuICBnZXRUYWcoYXR0clNlbGVjdG9yOiBzdHJpbmcpOiBIVE1MTWV0YUVsZW1lbnR8bnVsbCB7XG4gICAgaWYgKCFhdHRyU2VsZWN0b3IpIHJldHVybiBudWxsO1xuICAgIHJldHVybiB0aGlzLl9kb20ucXVlcnlTZWxlY3Rvcih0aGlzLl9kb2MsIGBtZXRhWyR7YXR0clNlbGVjdG9yfV1gKSB8fCBudWxsO1xuICB9XG5cbiAgZ2V0VGFncyhhdHRyU2VsZWN0b3I6IHN0cmluZyk6IEhUTUxNZXRhRWxlbWVudFtdIHtcbiAgICBpZiAoIWF0dHJTZWxlY3RvcikgcmV0dXJuIFtdO1xuICAgIGNvbnN0IGxpc3QgLypOb2RlTGlzdCovID0gdGhpcy5fZG9tLnF1ZXJ5U2VsZWN0b3JBbGwodGhpcy5fZG9jLCBgbWV0YVske2F0dHJTZWxlY3Rvcn1dYCk7XG4gICAgcmV0dXJuIGxpc3QgPyBbXS5zbGljZS5jYWxsKGxpc3QpIDogW107XG4gIH1cblxuICB1cGRhdGVUYWcodGFnOiBNZXRhRGVmaW5pdGlvbiwgc2VsZWN0b3I/OiBzdHJpbmcpOiBIVE1MTWV0YUVsZW1lbnR8bnVsbCB7XG4gICAgaWYgKCF0YWcpIHJldHVybiBudWxsO1xuICAgIHNlbGVjdG9yID0gc2VsZWN0b3IgfHwgdGhpcy5fcGFyc2VTZWxlY3Rvcih0YWcpO1xuICAgIGNvbnN0IG1ldGE6IEhUTUxNZXRhRWxlbWVudCA9IHRoaXMuZ2V0VGFnKHNlbGVjdG9yKSAhO1xuICAgIGlmIChtZXRhKSB7XG4gICAgICByZXR1cm4gdGhpcy5fc2V0TWV0YUVsZW1lbnRBdHRyaWJ1dGVzKHRhZywgbWV0YSk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLl9nZXRPckNyZWF0ZUVsZW1lbnQodGFnLCB0cnVlKTtcbiAgfVxuXG4gIHJlbW92ZVRhZyhhdHRyU2VsZWN0b3I6IHN0cmluZyk6IHZvaWQgeyB0aGlzLnJlbW92ZVRhZ0VsZW1lbnQodGhpcy5nZXRUYWcoYXR0clNlbGVjdG9yKSAhKTsgfVxuXG4gIHJlbW92ZVRhZ0VsZW1lbnQobWV0YTogSFRNTE1ldGFFbGVtZW50KTogdm9pZCB7XG4gICAgaWYgKG1ldGEpIHtcbiAgICAgIHRoaXMuX2RvbS5yZW1vdmUobWV0YSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0T3JDcmVhdGVFbGVtZW50KG1ldGE6IE1ldGFEZWZpbml0aW9uLCBmb3JjZUNyZWF0aW9uOiBib29sZWFuID0gZmFsc2UpOlxuICAgICAgSFRNTE1ldGFFbGVtZW50IHtcbiAgICBpZiAoIWZvcmNlQ3JlYXRpb24pIHtcbiAgICAgIGNvbnN0IHNlbGVjdG9yOiBzdHJpbmcgPSB0aGlzLl9wYXJzZVNlbGVjdG9yKG1ldGEpO1xuICAgICAgY29uc3QgZWxlbTogSFRNTE1ldGFFbGVtZW50ID0gdGhpcy5nZXRUYWcoc2VsZWN0b3IpICE7XG4gICAgICAvLyBJdCdzIGFsbG93ZWQgdG8gaGF2ZSBtdWx0aXBsZSBlbGVtZW50cyB3aXRoIHRoZSBzYW1lIG5hbWUgc28gaXQncyBub3QgZW5vdWdoIHRvXG4gICAgICAvLyBqdXN0IGNoZWNrIHRoYXQgZWxlbWVudCB3aXRoIHRoZSBzYW1lIG5hbWUgYWxyZWFkeSBwcmVzZW50IG9uIHRoZSBwYWdlLiBXZSBhbHNvIG5lZWQgdG9cbiAgICAgIC8vIGNoZWNrIGlmIGVsZW1lbnQgaGFzIHRhZyBhdHRyaWJ1dGVzXG4gICAgICBpZiAoZWxlbSAmJiB0aGlzLl9jb250YWluc0F0dHJpYnV0ZXMobWV0YSwgZWxlbSkpIHJldHVybiBlbGVtO1xuICAgIH1cbiAgICBjb25zdCBlbGVtZW50OiBIVE1MTWV0YUVsZW1lbnQgPSB0aGlzLl9kb20uY3JlYXRlRWxlbWVudCgnbWV0YScpIGFzIEhUTUxNZXRhRWxlbWVudDtcbiAgICB0aGlzLl9zZXRNZXRhRWxlbWVudEF0dHJpYnV0ZXMobWV0YSwgZWxlbWVudCk7XG4gICAgY29uc3QgaGVhZCA9IHRoaXMuX2RvbS5nZXRFbGVtZW50c0J5VGFnTmFtZSh0aGlzLl9kb2MsICdoZWFkJylbMF07XG4gICAgdGhpcy5fZG9tLmFwcGVuZENoaWxkKGhlYWQsIGVsZW1lbnQpO1xuICAgIHJldHVybiBlbGVtZW50O1xuICB9XG5cbiAgcHJpdmF0ZSBfc2V0TWV0YUVsZW1lbnRBdHRyaWJ1dGVzKHRhZzogTWV0YURlZmluaXRpb24sIGVsOiBIVE1MTWV0YUVsZW1lbnQpOiBIVE1MTWV0YUVsZW1lbnQge1xuICAgIE9iamVjdC5rZXlzKHRhZykuZm9yRWFjaCgocHJvcDogc3RyaW5nKSA9PiB0aGlzLl9kb20uc2V0QXR0cmlidXRlKGVsLCBwcm9wLCB0YWdbcHJvcF0pKTtcbiAgICByZXR1cm4gZWw7XG4gIH1cblxuICBwcml2YXRlIF9wYXJzZVNlbGVjdG9yKHRhZzogTWV0YURlZmluaXRpb24pOiBzdHJpbmcge1xuICAgIGNvbnN0IGF0dHI6IHN0cmluZyA9IHRhZy5uYW1lID8gJ25hbWUnIDogJ3Byb3BlcnR5JztcbiAgICByZXR1cm4gYCR7YXR0cn09XCIke3RhZ1thdHRyXX1cImA7XG4gIH1cblxuICBwcml2YXRlIF9jb250YWluc0F0dHJpYnV0ZXModGFnOiBNZXRhRGVmaW5pdGlvbiwgZWxlbTogSFRNTE1ldGFFbGVtZW50KTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIE9iamVjdC5rZXlzKHRhZykuZXZlcnkoKGtleTogc3RyaW5nKSA9PiB0aGlzLl9kb20uZ2V0QXR0cmlidXRlKGVsZW0sIGtleSkgPT09IHRhZ1trZXldKTtcbiAgfVxufVxuIl19