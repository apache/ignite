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
import { Inject, Injectable } from '@angular/core';
import { TestComponentRenderer } from '@angular/core/testing';
import { ɵgetDOM as getDOM } from '@angular/platform-browser';
/**
 * A DOM based implementation of the TestComponentRenderer.
 */
export class DOMTestComponentRenderer extends TestComponentRenderer {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        super();
        this._doc = _doc;
    }
    /**
     * @param {?} rootElId
     * @return {?}
     */
    insertRootElement(rootElId) {
        /** @type {?} */
        const rootEl = (/** @type {?} */ (getDOM().firstChild(getDOM().content(getDOM().createTemplate(`<div id="${rootElId}"></div>`)))));
        // TODO(juliemr): can/should this be optional?
        /** @type {?} */
        const oldRoots = getDOM().querySelectorAll(this._doc, '[id^=root]');
        for (let i = 0; i < oldRoots.length; i++) {
            getDOM().remove(oldRoots[i]);
        }
        getDOM().appendChild(this._doc.body, rootEl);
    }
}
DOMTestComponentRenderer.decorators = [
    { type: Injectable }
];
/** @nocollapse */
DOMTestComponentRenderer.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DOMTestComponentRenderer.prototype._doc;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tX3Rlc3RfY29tcG9uZW50X3JlbmRlcmVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci1keW5hbWljL3Rlc3Rpbmcvc3JjL2RvbV90ZXN0X2NvbXBvbmVudF9yZW5kZXJlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUN6QyxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUNqRCxPQUFPLEVBQUMscUJBQXFCLEVBQUMsTUFBTSx1QkFBdUIsQ0FBQztBQUM1RCxPQUFPLEVBQUMsT0FBTyxJQUFJLE1BQU0sRUFBQyxNQUFNLDJCQUEyQixDQUFDOzs7O0FBTTVELE1BQU0sT0FBTyx3QkFBeUIsU0FBUSxxQkFBcUI7Ozs7SUFDakUsWUFBc0MsSUFBUztRQUFJLEtBQUssRUFBRSxDQUFDO1FBQXJCLFNBQUksR0FBSixJQUFJLENBQUs7SUFBYSxDQUFDOzs7OztJQUU3RCxpQkFBaUIsQ0FBQyxRQUFnQjs7Y0FDMUIsTUFBTSxHQUFHLG1CQUFhLE1BQU0sRUFBRSxDQUFDLFVBQVUsQ0FDM0MsTUFBTSxFQUFFLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxDQUFDLGNBQWMsQ0FBQyxZQUFZLFFBQVEsVUFBVSxDQUFDLENBQUMsQ0FBQyxFQUFBOzs7Y0FHeEUsUUFBUSxHQUFHLE1BQU0sRUFBRSxDQUFDLGdCQUFnQixDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsWUFBWSxDQUFDO1FBQ25FLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxRQUFRLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO1lBQ3hDLE1BQU0sRUFBRSxDQUFDLE1BQU0sQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztTQUM5QjtRQUNELE1BQU0sRUFBRSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksRUFBRSxNQUFNLENBQUMsQ0FBQztJQUMvQyxDQUFDOzs7WUFkRixVQUFVOzs7OzRDQUVJLE1BQU0sU0FBQyxRQUFROzs7Ozs7O0lBQWhCLHdDQUFtQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtET0NVTUVOVH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7SW5qZWN0LCBJbmplY3RhYmxlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7VGVzdENvbXBvbmVudFJlbmRlcmVyfSBmcm9tICdAYW5ndWxhci9jb3JlL3Rlc3RpbmcnO1xuaW1wb3J0IHvJtWdldERPTSBhcyBnZXRET019IGZyb20gJ0Bhbmd1bGFyL3BsYXRmb3JtLWJyb3dzZXInO1xuXG4vKipcbiAqIEEgRE9NIGJhc2VkIGltcGxlbWVudGF0aW9uIG9mIHRoZSBUZXN0Q29tcG9uZW50UmVuZGVyZXIuXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBET01UZXN0Q29tcG9uZW50UmVuZGVyZXIgZXh0ZW5kcyBUZXN0Q29tcG9uZW50UmVuZGVyZXIge1xuICBjb25zdHJ1Y3RvcihASW5qZWN0KERPQ1VNRU5UKSBwcml2YXRlIF9kb2M6IGFueSkgeyBzdXBlcigpOyB9XG5cbiAgaW5zZXJ0Um9vdEVsZW1lbnQocm9vdEVsSWQ6IHN0cmluZykge1xuICAgIGNvbnN0IHJvb3RFbCA9IDxIVE1MRWxlbWVudD5nZXRET00oKS5maXJzdENoaWxkKFxuICAgICAgICBnZXRET00oKS5jb250ZW50KGdldERPTSgpLmNyZWF0ZVRlbXBsYXRlKGA8ZGl2IGlkPVwiJHtyb290RWxJZH1cIj48L2Rpdj5gKSkpO1xuXG4gICAgLy8gVE9ETyhqdWxpZW1yKTogY2FuL3Nob3VsZCB0aGlzIGJlIG9wdGlvbmFsP1xuICAgIGNvbnN0IG9sZFJvb3RzID0gZ2V0RE9NKCkucXVlcnlTZWxlY3RvckFsbCh0aGlzLl9kb2MsICdbaWRePXJvb3RdJyk7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBvbGRSb290cy5sZW5ndGg7IGkrKykge1xuICAgICAgZ2V0RE9NKCkucmVtb3ZlKG9sZFJvb3RzW2ldKTtcbiAgICB9XG4gICAgZ2V0RE9NKCkuYXBwZW5kQ2hpbGQodGhpcy5fZG9jLmJvZHksIHJvb3RFbCk7XG4gIH1cbn1cbiJdfQ==