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
/**
 * This helper class is used to get hold of an inert tree of DOM elements containing dirty HTML
 * that needs sanitizing.
 * Depending upon browser support we must use one of three strategies for doing this.
 * Support: Safari 10.x -> XHR strategy
 * Support: Firefox -> DomParser strategy
 * Default: InertDocument strategy
 */
export class InertBodyHelper {
    /**
     * @param {?} defaultDoc
     */
    constructor(defaultDoc) {
        this.defaultDoc = defaultDoc;
        this.inertDocument = this.defaultDoc.implementation.createHTMLDocument('sanitization-inert');
        this.inertBodyElement = this.inertDocument.body;
        if (this.inertBodyElement == null) {
            // usually there should be only one body element in the document, but IE doesn't have any, so
            // we need to create one.
            /** @type {?} */
            const inertHtml = this.inertDocument.createElement('html');
            this.inertDocument.appendChild(inertHtml);
            this.inertBodyElement = this.inertDocument.createElement('body');
            inertHtml.appendChild(this.inertBodyElement);
        }
        this.inertBodyElement.innerHTML = '<svg><g onload="this.parentNode.remove()"></g></svg>';
        if (this.inertBodyElement.querySelector && !this.inertBodyElement.querySelector('svg')) {
            // We just hit the Safari 10.1 bug - which allows JS to run inside the SVG G element
            // so use the XHR strategy.
            this.getInertBodyElement = this.getInertBodyElement_XHR;
            return;
        }
        this.inertBodyElement.innerHTML =
            '<svg><p><style><img src="</style><img src=x onerror=alert(1)//">';
        if (this.inertBodyElement.querySelector && this.inertBodyElement.querySelector('svg img')) {
            // We just hit the Firefox bug - which prevents the inner img JS from being sanitized
            // so use the DOMParser strategy, if it is available.
            // If the DOMParser is not available then we are not in Firefox (Server/WebWorker?) so we
            // fall through to the default strategy below.
            if (isDOMParserAvailable()) {
                this.getInertBodyElement = this.getInertBodyElement_DOMParser;
                return;
            }
        }
        // None of the bugs were hit so it is safe for us to use the default InertDocument strategy
        this.getInertBodyElement = this.getInertBodyElement_InertDocument;
    }
    /**
     * Use XHR to create and fill an inert body element (on Safari 10.1)
     * See
     * https://github.com/cure53/DOMPurify/blob/a992d3a75031cb8bb032e5ea8399ba972bdf9a65/src/purify.js#L439-L449
     * @private
     * @param {?} html
     * @return {?}
     */
    getInertBodyElement_XHR(html) {
        // We add these extra elements to ensure that the rest of the content is parsed as expected
        // e.g. leading whitespace is maintained and tags like `<meta>` do not get hoisted to the
        // `<head>` tag.
        html = '<body><remove></remove>' + html + '</body>';
        try {
            html = encodeURI(html);
        }
        catch (_a) {
            return null;
        }
        /** @type {?} */
        const xhr = new XMLHttpRequest();
        xhr.responseType = 'document';
        xhr.open('GET', 'data:text/html;charset=utf-8,' + html, false);
        xhr.send(undefined);
        /** @type {?} */
        const body = xhr.response.body;
        body.removeChild((/** @type {?} */ (body.firstChild)));
        return body;
    }
    /**
     * Use DOMParser to create and fill an inert body element (on Firefox)
     * See https://github.com/cure53/DOMPurify/releases/tag/0.6.7
     *
     * @private
     * @param {?} html
     * @return {?}
     */
    getInertBodyElement_DOMParser(html) {
        // We add these extra elements to ensure that the rest of the content is parsed as expected
        // e.g. leading whitespace is maintained and tags like `<meta>` do not get hoisted to the
        // `<head>` tag.
        html = '<body><remove></remove>' + html + '</body>';
        try {
            /** @type {?} */
            const body = (/** @type {?} */ (new ((/** @type {?} */ (window)))
                .DOMParser()
                .parseFromString(html, 'text/html')
                .body));
            body.removeChild((/** @type {?} */ (body.firstChild)));
            return body;
        }
        catch (_a) {
            return null;
        }
    }
    /**
     * Use an HTML5 `template` element, if supported, or an inert body element created via
     * `createHtmlDocument` to create and fill an inert DOM element.
     * This is the default sane strategy to use if the browser does not require one of the specialised
     * strategies above.
     * @private
     * @param {?} html
     * @return {?}
     */
    getInertBodyElement_InertDocument(html) {
        // Prefer using <template> element if supported.
        /** @type {?} */
        const templateEl = this.inertDocument.createElement('template');
        if ('content' in templateEl) {
            templateEl.innerHTML = html;
            return templateEl;
        }
        this.inertBodyElement.innerHTML = html;
        // Support: IE 9-11 only
        // strip custom-namespaced attributes on IE<=11
        if (((/** @type {?} */ (this.defaultDoc))).documentMode) {
            this.stripCustomNsAttrs(this.inertBodyElement);
        }
        return this.inertBodyElement;
    }
    /**
     * When IE9-11 comes across an unknown namespaced attribute e.g. 'xlink:foo' it adds 'xmlns:ns1'
     * attribute to declare ns1 namespace and prefixes the attribute with 'ns1' (e.g.
     * 'ns1:xlink:foo').
     *
     * This is undesirable since we don't want to allow any of these custom attributes. This method
     * strips them all.
     * @private
     * @param {?} el
     * @return {?}
     */
    stripCustomNsAttrs(el) {
        /** @type {?} */
        const elAttrs = el.attributes;
        // loop backwards so that we can support removals.
        for (let i = elAttrs.length - 1; 0 < i; i--) {
            /** @type {?} */
            const attrib = elAttrs.item(i);
            /** @type {?} */
            const attrName = (/** @type {?} */ (attrib)).name;
            if (attrName === 'xmlns:ns1' || attrName.indexOf('ns1:') === 0) {
                el.removeAttribute(attrName);
            }
        }
        /** @type {?} */
        let childNode = (/** @type {?} */ (el.firstChild));
        while (childNode) {
            if (childNode.nodeType === Node.ELEMENT_NODE)
                this.stripCustomNsAttrs((/** @type {?} */ (childNode)));
            childNode = childNode.nextSibling;
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    InertBodyHelper.prototype.inertBodyElement;
    /**
     * @type {?}
     * @private
     */
    InertBodyHelper.prototype.inertDocument;
    /**
     * Get an inert DOM element containing DOM created from the dirty HTML string provided.
     * The implementation of this is determined in the constructor, when the class is instantiated.
     * @type {?}
     */
    InertBodyHelper.prototype.getInertBodyElement;
    /**
     * @type {?}
     * @private
     */
    InertBodyHelper.prototype.defaultDoc;
}
/**
 * We need to determine whether the DOMParser exists in the global context.
 * The try-catch is because, on some browsers, trying to access this property
 * on window can actually throw an error.
 *
 * @suppress {uselessCode}
 * @return {?}
 */
function isDOMParserAvailable() {
    try {
        return !!((/** @type {?} */ (window))).DOMParser;
    }
    catch (_a) {
        return false;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5lcnRfYm9keS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3Nhbml0aXphdGlvbi9pbmVydF9ib2R5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFnQkEsTUFBTSxPQUFPLGVBQWU7Ozs7SUFJMUIsWUFBb0IsVUFBb0I7UUFBcEIsZUFBVSxHQUFWLFVBQVUsQ0FBVTtRQUN0QyxJQUFJLENBQUMsYUFBYSxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsY0FBYyxDQUFDLGtCQUFrQixDQUFDLG9CQUFvQixDQUFDLENBQUM7UUFDN0YsSUFBSSxDQUFDLGdCQUFnQixHQUFHLElBQUksQ0FBQyxhQUFhLENBQUMsSUFBSSxDQUFDO1FBRWhELElBQUksSUFBSSxDQUFDLGdCQUFnQixJQUFJLElBQUksRUFBRTs7OztrQkFHM0IsU0FBUyxHQUFHLElBQUksQ0FBQyxhQUFhLENBQUMsYUFBYSxDQUFDLE1BQU0sQ0FBQztZQUMxRCxJQUFJLENBQUMsYUFBYSxDQUFDLFdBQVcsQ0FBQyxTQUFTLENBQUMsQ0FBQztZQUMxQyxJQUFJLENBQUMsZ0JBQWdCLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxhQUFhLENBQUMsTUFBTSxDQUFDLENBQUM7WUFDakUsU0FBUyxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsQ0FBQztTQUM5QztRQUVELElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxTQUFTLEdBQUcsc0RBQXNELENBQUM7UUFDekYsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsYUFBYSxJQUFJLENBQUMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLGFBQWEsQ0FBQyxLQUFLLENBQUMsRUFBRTtZQUN0RixvRkFBb0Y7WUFDcEYsMkJBQTJCO1lBQzNCLElBQUksQ0FBQyxtQkFBbUIsR0FBRyxJQUFJLENBQUMsdUJBQXVCLENBQUM7WUFDeEQsT0FBTztTQUNSO1FBRUQsSUFBSSxDQUFDLGdCQUFnQixDQUFDLFNBQVM7WUFDM0Isa0VBQWtFLENBQUM7UUFDdkUsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsYUFBYSxJQUFJLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxhQUFhLENBQUMsU0FBUyxDQUFDLEVBQUU7WUFDekYscUZBQXFGO1lBQ3JGLHFEQUFxRDtZQUNyRCx5RkFBeUY7WUFDekYsOENBQThDO1lBQzlDLElBQUksb0JBQW9CLEVBQUUsRUFBRTtnQkFDMUIsSUFBSSxDQUFDLG1CQUFtQixHQUFHLElBQUksQ0FBQyw2QkFBNkIsQ0FBQztnQkFDOUQsT0FBTzthQUNSO1NBQ0Y7UUFFRCwyRkFBMkY7UUFDM0YsSUFBSSxDQUFDLG1CQUFtQixHQUFHLElBQUksQ0FBQyxpQ0FBaUMsQ0FBQztJQUNwRSxDQUFDOzs7Ozs7Ozs7SUFhTyx1QkFBdUIsQ0FBQyxJQUFZO1FBQzFDLDJGQUEyRjtRQUMzRix5RkFBeUY7UUFDekYsZ0JBQWdCO1FBQ2hCLElBQUksR0FBRyx5QkFBeUIsR0FBRyxJQUFJLEdBQUcsU0FBUyxDQUFDO1FBQ3BELElBQUk7WUFDRixJQUFJLEdBQUcsU0FBUyxDQUFDLElBQUksQ0FBQyxDQUFDO1NBQ3hCO1FBQUMsV0FBTTtZQUNOLE9BQU8sSUFBSSxDQUFDO1NBQ2I7O2NBQ0ssR0FBRyxHQUFHLElBQUksY0FBYyxFQUFFO1FBQ2hDLEdBQUcsQ0FBQyxZQUFZLEdBQUcsVUFBVSxDQUFDO1FBQzlCLEdBQUcsQ0FBQyxJQUFJLENBQUMsS0FBSyxFQUFFLCtCQUErQixHQUFHLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQztRQUMvRCxHQUFHLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDOztjQUNkLElBQUksR0FBb0IsR0FBRyxDQUFDLFFBQVEsQ0FBQyxJQUFJO1FBQy9DLElBQUksQ0FBQyxXQUFXLENBQUMsbUJBQUEsSUFBSSxDQUFDLFVBQVUsRUFBRSxDQUFDLENBQUM7UUFDcEMsT0FBTyxJQUFJLENBQUM7SUFDZCxDQUFDOzs7Ozs7Ozs7SUFPTyw2QkFBNkIsQ0FBQyxJQUFZO1FBQ2hELDJGQUEyRjtRQUMzRix5RkFBeUY7UUFDekYsZ0JBQWdCO1FBQ2hCLElBQUksR0FBRyx5QkFBeUIsR0FBRyxJQUFJLEdBQUcsU0FBUyxDQUFDO1FBQ3BELElBQUk7O2tCQUNJLElBQUksR0FBRyxtQkFBQSxJQUFJLENBQUMsbUJBQUEsTUFBTSxFQUFPLENBQUM7aUJBQ2QsU0FBUyxFQUFFO2lCQUNYLGVBQWUsQ0FBQyxJQUFJLEVBQUUsV0FBVyxDQUFDO2lCQUNsQyxJQUFJLEVBQW1CO1lBQ3pDLElBQUksQ0FBQyxXQUFXLENBQUMsbUJBQUEsSUFBSSxDQUFDLFVBQVUsRUFBRSxDQUFDLENBQUM7WUFDcEMsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUFDLFdBQU07WUFDTixPQUFPLElBQUksQ0FBQztTQUNiO0lBQ0gsQ0FBQzs7Ozs7Ozs7OztJQVFPLGlDQUFpQyxDQUFDLElBQVk7OztjQUU5QyxVQUFVLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxhQUFhLENBQUMsVUFBVSxDQUFDO1FBQy9ELElBQUksU0FBUyxJQUFJLFVBQVUsRUFBRTtZQUMzQixVQUFVLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQztZQUM1QixPQUFPLFVBQVUsQ0FBQztTQUNuQjtRQUVELElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDO1FBRXZDLHdCQUF3QjtRQUN4QiwrQ0FBK0M7UUFDL0MsSUFBSSxDQUFDLG1CQUFBLElBQUksQ0FBQyxVQUFVLEVBQU8sQ0FBQyxDQUFDLFlBQVksRUFBRTtZQUN6QyxJQUFJLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLENBQUM7U0FDaEQ7UUFFRCxPQUFPLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQztJQUMvQixDQUFDOzs7Ozs7Ozs7Ozs7SUFVTyxrQkFBa0IsQ0FBQyxFQUFXOztjQUM5QixPQUFPLEdBQUcsRUFBRSxDQUFDLFVBQVU7UUFDN0Isa0RBQWtEO1FBQ2xELEtBQUssSUFBSSxDQUFDLEdBQUcsT0FBTyxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRTs7a0JBQ3JDLE1BQU0sR0FBRyxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQzs7a0JBQ3hCLFFBQVEsR0FBRyxtQkFBQSxNQUFNLEVBQUUsQ0FBQyxJQUFJO1lBQzlCLElBQUksUUFBUSxLQUFLLFdBQVcsSUFBSSxRQUFRLENBQUMsT0FBTyxDQUFDLE1BQU0sQ0FBQyxLQUFLLENBQUMsRUFBRTtnQkFDOUQsRUFBRSxDQUFDLGVBQWUsQ0FBQyxRQUFRLENBQUMsQ0FBQzthQUM5QjtTQUNGOztZQUNHLFNBQVMsR0FBRyxtQkFBQSxFQUFFLENBQUMsVUFBVSxFQUFlO1FBQzVDLE9BQU8sU0FBUyxFQUFFO1lBQ2hCLElBQUksU0FBUyxDQUFDLFFBQVEsS0FBSyxJQUFJLENBQUMsWUFBWTtnQkFBRSxJQUFJLENBQUMsa0JBQWtCLENBQUMsbUJBQUEsU0FBUyxFQUFXLENBQUMsQ0FBQztZQUM1RixTQUFTLEdBQUcsU0FBUyxDQUFDLFdBQVcsQ0FBQztTQUNuQztJQUNILENBQUM7Q0FDRjs7Ozs7O0lBOUlDLDJDQUFzQzs7Ozs7SUFDdEMsd0NBQWdDOzs7Ozs7SUE0Q2hDLDhDQUEwRDs7Ozs7SUExQzlDLHFDQUE0Qjs7Ozs7Ozs7OztBQW9KMUMsU0FBUyxvQkFBb0I7SUFDM0IsSUFBSTtRQUNGLE9BQU8sQ0FBQyxDQUFDLENBQUMsbUJBQUEsTUFBTSxFQUFPLENBQUMsQ0FBQyxTQUFTLENBQUM7S0FDcEM7SUFBQyxXQUFNO1FBQ04sT0FBTyxLQUFLLENBQUM7S0FDZDtBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogVGhpcyBoZWxwZXIgY2xhc3MgaXMgdXNlZCB0byBnZXQgaG9sZCBvZiBhbiBpbmVydCB0cmVlIG9mIERPTSBlbGVtZW50cyBjb250YWluaW5nIGRpcnR5IEhUTUxcbiAqIHRoYXQgbmVlZHMgc2FuaXRpemluZy5cbiAqIERlcGVuZGluZyB1cG9uIGJyb3dzZXIgc3VwcG9ydCB3ZSBtdXN0IHVzZSBvbmUgb2YgdGhyZWUgc3RyYXRlZ2llcyBmb3IgZG9pbmcgdGhpcy5cbiAqIFN1cHBvcnQ6IFNhZmFyaSAxMC54IC0+IFhIUiBzdHJhdGVneVxuICogU3VwcG9ydDogRmlyZWZveCAtPiBEb21QYXJzZXIgc3RyYXRlZ3lcbiAqIERlZmF1bHQ6IEluZXJ0RG9jdW1lbnQgc3RyYXRlZ3lcbiAqL1xuZXhwb3J0IGNsYXNzIEluZXJ0Qm9keUhlbHBlciB7XG4gIHByaXZhdGUgaW5lcnRCb2R5RWxlbWVudDogSFRNTEVsZW1lbnQ7XG4gIHByaXZhdGUgaW5lcnREb2N1bWVudDogRG9jdW1lbnQ7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBkZWZhdWx0RG9jOiBEb2N1bWVudCkge1xuICAgIHRoaXMuaW5lcnREb2N1bWVudCA9IHRoaXMuZGVmYXVsdERvYy5pbXBsZW1lbnRhdGlvbi5jcmVhdGVIVE1MRG9jdW1lbnQoJ3Nhbml0aXphdGlvbi1pbmVydCcpO1xuICAgIHRoaXMuaW5lcnRCb2R5RWxlbWVudCA9IHRoaXMuaW5lcnREb2N1bWVudC5ib2R5O1xuXG4gICAgaWYgKHRoaXMuaW5lcnRCb2R5RWxlbWVudCA9PSBudWxsKSB7XG4gICAgICAvLyB1c3VhbGx5IHRoZXJlIHNob3VsZCBiZSBvbmx5IG9uZSBib2R5IGVsZW1lbnQgaW4gdGhlIGRvY3VtZW50LCBidXQgSUUgZG9lc24ndCBoYXZlIGFueSwgc29cbiAgICAgIC8vIHdlIG5lZWQgdG8gY3JlYXRlIG9uZS5cbiAgICAgIGNvbnN0IGluZXJ0SHRtbCA9IHRoaXMuaW5lcnREb2N1bWVudC5jcmVhdGVFbGVtZW50KCdodG1sJyk7XG4gICAgICB0aGlzLmluZXJ0RG9jdW1lbnQuYXBwZW5kQ2hpbGQoaW5lcnRIdG1sKTtcbiAgICAgIHRoaXMuaW5lcnRCb2R5RWxlbWVudCA9IHRoaXMuaW5lcnREb2N1bWVudC5jcmVhdGVFbGVtZW50KCdib2R5Jyk7XG4gICAgICBpbmVydEh0bWwuYXBwZW5kQ2hpbGQodGhpcy5pbmVydEJvZHlFbGVtZW50KTtcbiAgICB9XG5cbiAgICB0aGlzLmluZXJ0Qm9keUVsZW1lbnQuaW5uZXJIVE1MID0gJzxzdmc+PGcgb25sb2FkPVwidGhpcy5wYXJlbnROb2RlLnJlbW92ZSgpXCI+PC9nPjwvc3ZnPic7XG4gICAgaWYgKHRoaXMuaW5lcnRCb2R5RWxlbWVudC5xdWVyeVNlbGVjdG9yICYmICF0aGlzLmluZXJ0Qm9keUVsZW1lbnQucXVlcnlTZWxlY3Rvcignc3ZnJykpIHtcbiAgICAgIC8vIFdlIGp1c3QgaGl0IHRoZSBTYWZhcmkgMTAuMSBidWcgLSB3aGljaCBhbGxvd3MgSlMgdG8gcnVuIGluc2lkZSB0aGUgU1ZHIEcgZWxlbWVudFxuICAgICAgLy8gc28gdXNlIHRoZSBYSFIgc3RyYXRlZ3kuXG4gICAgICB0aGlzLmdldEluZXJ0Qm9keUVsZW1lbnQgPSB0aGlzLmdldEluZXJ0Qm9keUVsZW1lbnRfWEhSO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHRoaXMuaW5lcnRCb2R5RWxlbWVudC5pbm5lckhUTUwgPVxuICAgICAgICAnPHN2Zz48cD48c3R5bGU+PGltZyBzcmM9XCI8L3N0eWxlPjxpbWcgc3JjPXggb25lcnJvcj1hbGVydCgxKS8vXCI+JztcbiAgICBpZiAodGhpcy5pbmVydEJvZHlFbGVtZW50LnF1ZXJ5U2VsZWN0b3IgJiYgdGhpcy5pbmVydEJvZHlFbGVtZW50LnF1ZXJ5U2VsZWN0b3IoJ3N2ZyBpbWcnKSkge1xuICAgICAgLy8gV2UganVzdCBoaXQgdGhlIEZpcmVmb3ggYnVnIC0gd2hpY2ggcHJldmVudHMgdGhlIGlubmVyIGltZyBKUyBmcm9tIGJlaW5nIHNhbml0aXplZFxuICAgICAgLy8gc28gdXNlIHRoZSBET01QYXJzZXIgc3RyYXRlZ3ksIGlmIGl0IGlzIGF2YWlsYWJsZS5cbiAgICAgIC8vIElmIHRoZSBET01QYXJzZXIgaXMgbm90IGF2YWlsYWJsZSB0aGVuIHdlIGFyZSBub3QgaW4gRmlyZWZveCAoU2VydmVyL1dlYldvcmtlcj8pIHNvIHdlXG4gICAgICAvLyBmYWxsIHRocm91Z2ggdG8gdGhlIGRlZmF1bHQgc3RyYXRlZ3kgYmVsb3cuXG4gICAgICBpZiAoaXNET01QYXJzZXJBdmFpbGFibGUoKSkge1xuICAgICAgICB0aGlzLmdldEluZXJ0Qm9keUVsZW1lbnQgPSB0aGlzLmdldEluZXJ0Qm9keUVsZW1lbnRfRE9NUGFyc2VyO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gTm9uZSBvZiB0aGUgYnVncyB3ZXJlIGhpdCBzbyBpdCBpcyBzYWZlIGZvciB1cyB0byB1c2UgdGhlIGRlZmF1bHQgSW5lcnREb2N1bWVudCBzdHJhdGVneVxuICAgIHRoaXMuZ2V0SW5lcnRCb2R5RWxlbWVudCA9IHRoaXMuZ2V0SW5lcnRCb2R5RWxlbWVudF9JbmVydERvY3VtZW50O1xuICB9XG5cbiAgLyoqXG4gICAqIEdldCBhbiBpbmVydCBET00gZWxlbWVudCBjb250YWluaW5nIERPTSBjcmVhdGVkIGZyb20gdGhlIGRpcnR5IEhUTUwgc3RyaW5nIHByb3ZpZGVkLlxuICAgKiBUaGUgaW1wbGVtZW50YXRpb24gb2YgdGhpcyBpcyBkZXRlcm1pbmVkIGluIHRoZSBjb25zdHJ1Y3Rvciwgd2hlbiB0aGUgY2xhc3MgaXMgaW5zdGFudGlhdGVkLlxuICAgKi9cbiAgZ2V0SW5lcnRCb2R5RWxlbWVudDogKGh0bWw6IHN0cmluZykgPT4gSFRNTEVsZW1lbnQgfCBudWxsO1xuXG4gIC8qKlxuICAgKiBVc2UgWEhSIHRvIGNyZWF0ZSBhbmQgZmlsbCBhbiBpbmVydCBib2R5IGVsZW1lbnQgKG9uIFNhZmFyaSAxMC4xKVxuICAgKiBTZWVcbiAgICogaHR0cHM6Ly9naXRodWIuY29tL2N1cmU1My9ET01QdXJpZnkvYmxvYi9hOTkyZDNhNzUwMzFjYjhiYjAzMmU1ZWE4Mzk5YmE5NzJiZGY5YTY1L3NyYy9wdXJpZnkuanMjTDQzOS1MNDQ5XG4gICAqL1xuICBwcml2YXRlIGdldEluZXJ0Qm9keUVsZW1lbnRfWEhSKGh0bWw6IHN0cmluZykge1xuICAgIC8vIFdlIGFkZCB0aGVzZSBleHRyYSBlbGVtZW50cyB0byBlbnN1cmUgdGhhdCB0aGUgcmVzdCBvZiB0aGUgY29udGVudCBpcyBwYXJzZWQgYXMgZXhwZWN0ZWRcbiAgICAvLyBlLmcuIGxlYWRpbmcgd2hpdGVzcGFjZSBpcyBtYWludGFpbmVkIGFuZCB0YWdzIGxpa2UgYDxtZXRhPmAgZG8gbm90IGdldCBob2lzdGVkIHRvIHRoZVxuICAgIC8vIGA8aGVhZD5gIHRhZy5cbiAgICBodG1sID0gJzxib2R5PjxyZW1vdmU+PC9yZW1vdmU+JyArIGh0bWwgKyAnPC9ib2R5Pic7XG4gICAgdHJ5IHtcbiAgICAgIGh0bWwgPSBlbmNvZGVVUkkoaHRtbCk7XG4gICAgfSBjYXRjaCB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgY29uc3QgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KCk7XG4gICAgeGhyLnJlc3BvbnNlVHlwZSA9ICdkb2N1bWVudCc7XG4gICAgeGhyLm9wZW4oJ0dFVCcsICdkYXRhOnRleHQvaHRtbDtjaGFyc2V0PXV0Zi04LCcgKyBodG1sLCBmYWxzZSk7XG4gICAgeGhyLnNlbmQodW5kZWZpbmVkKTtcbiAgICBjb25zdCBib2R5OiBIVE1MQm9keUVsZW1lbnQgPSB4aHIucmVzcG9uc2UuYm9keTtcbiAgICBib2R5LnJlbW92ZUNoaWxkKGJvZHkuZmlyc3RDaGlsZCAhKTtcbiAgICByZXR1cm4gYm9keTtcbiAgfVxuXG4gIC8qKlxuICAgKiBVc2UgRE9NUGFyc2VyIHRvIGNyZWF0ZSBhbmQgZmlsbCBhbiBpbmVydCBib2R5IGVsZW1lbnQgKG9uIEZpcmVmb3gpXG4gICAqIFNlZSBodHRwczovL2dpdGh1Yi5jb20vY3VyZTUzL0RPTVB1cmlmeS9yZWxlYXNlcy90YWcvMC42LjdcbiAgICpcbiAgICovXG4gIHByaXZhdGUgZ2V0SW5lcnRCb2R5RWxlbWVudF9ET01QYXJzZXIoaHRtbDogc3RyaW5nKSB7XG4gICAgLy8gV2UgYWRkIHRoZXNlIGV4dHJhIGVsZW1lbnRzIHRvIGVuc3VyZSB0aGF0IHRoZSByZXN0IG9mIHRoZSBjb250ZW50IGlzIHBhcnNlZCBhcyBleHBlY3RlZFxuICAgIC8vIGUuZy4gbGVhZGluZyB3aGl0ZXNwYWNlIGlzIG1haW50YWluZWQgYW5kIHRhZ3MgbGlrZSBgPG1ldGE+YCBkbyBub3QgZ2V0IGhvaXN0ZWQgdG8gdGhlXG4gICAgLy8gYDxoZWFkPmAgdGFnLlxuICAgIGh0bWwgPSAnPGJvZHk+PHJlbW92ZT48L3JlbW92ZT4nICsgaHRtbCArICc8L2JvZHk+JztcbiAgICB0cnkge1xuICAgICAgY29uc3QgYm9keSA9IG5ldyAod2luZG93IGFzIGFueSlcbiAgICAgICAgICAgICAgICAgICAgICAgLkRPTVBhcnNlcigpXG4gICAgICAgICAgICAgICAgICAgICAgIC5wYXJzZUZyb21TdHJpbmcoaHRtbCwgJ3RleHQvaHRtbCcpXG4gICAgICAgICAgICAgICAgICAgICAgIC5ib2R5IGFzIEhUTUxCb2R5RWxlbWVudDtcbiAgICAgIGJvZHkucmVtb3ZlQ2hpbGQoYm9keS5maXJzdENoaWxkICEpO1xuICAgICAgcmV0dXJuIGJvZHk7XG4gICAgfSBjYXRjaCB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogVXNlIGFuIEhUTUw1IGB0ZW1wbGF0ZWAgZWxlbWVudCwgaWYgc3VwcG9ydGVkLCBvciBhbiBpbmVydCBib2R5IGVsZW1lbnQgY3JlYXRlZCB2aWFcbiAgICogYGNyZWF0ZUh0bWxEb2N1bWVudGAgdG8gY3JlYXRlIGFuZCBmaWxsIGFuIGluZXJ0IERPTSBlbGVtZW50LlxuICAgKiBUaGlzIGlzIHRoZSBkZWZhdWx0IHNhbmUgc3RyYXRlZ3kgdG8gdXNlIGlmIHRoZSBicm93c2VyIGRvZXMgbm90IHJlcXVpcmUgb25lIG9mIHRoZSBzcGVjaWFsaXNlZFxuICAgKiBzdHJhdGVnaWVzIGFib3ZlLlxuICAgKi9cbiAgcHJpdmF0ZSBnZXRJbmVydEJvZHlFbGVtZW50X0luZXJ0RG9jdW1lbnQoaHRtbDogc3RyaW5nKSB7XG4gICAgLy8gUHJlZmVyIHVzaW5nIDx0ZW1wbGF0ZT4gZWxlbWVudCBpZiBzdXBwb3J0ZWQuXG4gICAgY29uc3QgdGVtcGxhdGVFbCA9IHRoaXMuaW5lcnREb2N1bWVudC5jcmVhdGVFbGVtZW50KCd0ZW1wbGF0ZScpO1xuICAgIGlmICgnY29udGVudCcgaW4gdGVtcGxhdGVFbCkge1xuICAgICAgdGVtcGxhdGVFbC5pbm5lckhUTUwgPSBodG1sO1xuICAgICAgcmV0dXJuIHRlbXBsYXRlRWw7XG4gICAgfVxuXG4gICAgdGhpcy5pbmVydEJvZHlFbGVtZW50LmlubmVySFRNTCA9IGh0bWw7XG5cbiAgICAvLyBTdXBwb3J0OiBJRSA5LTExIG9ubHlcbiAgICAvLyBzdHJpcCBjdXN0b20tbmFtZXNwYWNlZCBhdHRyaWJ1dGVzIG9uIElFPD0xMVxuICAgIGlmICgodGhpcy5kZWZhdWx0RG9jIGFzIGFueSkuZG9jdW1lbnRNb2RlKSB7XG4gICAgICB0aGlzLnN0cmlwQ3VzdG9tTnNBdHRycyh0aGlzLmluZXJ0Qm9keUVsZW1lbnQpO1xuICAgIH1cblxuICAgIHJldHVybiB0aGlzLmluZXJ0Qm9keUVsZW1lbnQ7XG4gIH1cblxuICAvKipcbiAgICogV2hlbiBJRTktMTEgY29tZXMgYWNyb3NzIGFuIHVua25vd24gbmFtZXNwYWNlZCBhdHRyaWJ1dGUgZS5nLiAneGxpbms6Zm9vJyBpdCBhZGRzICd4bWxuczpuczEnXG4gICAqIGF0dHJpYnV0ZSB0byBkZWNsYXJlIG5zMSBuYW1lc3BhY2UgYW5kIHByZWZpeGVzIHRoZSBhdHRyaWJ1dGUgd2l0aCAnbnMxJyAoZS5nLlxuICAgKiAnbnMxOnhsaW5rOmZvbycpLlxuICAgKlxuICAgKiBUaGlzIGlzIHVuZGVzaXJhYmxlIHNpbmNlIHdlIGRvbid0IHdhbnQgdG8gYWxsb3cgYW55IG9mIHRoZXNlIGN1c3RvbSBhdHRyaWJ1dGVzLiBUaGlzIG1ldGhvZFxuICAgKiBzdHJpcHMgdGhlbSBhbGwuXG4gICAqL1xuICBwcml2YXRlIHN0cmlwQ3VzdG9tTnNBdHRycyhlbDogRWxlbWVudCkge1xuICAgIGNvbnN0IGVsQXR0cnMgPSBlbC5hdHRyaWJ1dGVzO1xuICAgIC8vIGxvb3AgYmFja3dhcmRzIHNvIHRoYXQgd2UgY2FuIHN1cHBvcnQgcmVtb3ZhbHMuXG4gICAgZm9yIChsZXQgaSA9IGVsQXR0cnMubGVuZ3RoIC0gMTsgMCA8IGk7IGktLSkge1xuICAgICAgY29uc3QgYXR0cmliID0gZWxBdHRycy5pdGVtKGkpO1xuICAgICAgY29uc3QgYXR0ck5hbWUgPSBhdHRyaWIgIS5uYW1lO1xuICAgICAgaWYgKGF0dHJOYW1lID09PSAneG1sbnM6bnMxJyB8fCBhdHRyTmFtZS5pbmRleE9mKCduczE6JykgPT09IDApIHtcbiAgICAgICAgZWwucmVtb3ZlQXR0cmlidXRlKGF0dHJOYW1lKTtcbiAgICAgIH1cbiAgICB9XG4gICAgbGV0IGNoaWxkTm9kZSA9IGVsLmZpcnN0Q2hpbGQgYXMgTm9kZSB8IG51bGw7XG4gICAgd2hpbGUgKGNoaWxkTm9kZSkge1xuICAgICAgaWYgKGNoaWxkTm9kZS5ub2RlVHlwZSA9PT0gTm9kZS5FTEVNRU5UX05PREUpIHRoaXMuc3RyaXBDdXN0b21Oc0F0dHJzKGNoaWxkTm9kZSBhcyBFbGVtZW50KTtcbiAgICAgIGNoaWxkTm9kZSA9IGNoaWxkTm9kZS5uZXh0U2libGluZztcbiAgICB9XG4gIH1cbn1cblxuLyoqXG4gKiBXZSBuZWVkIHRvIGRldGVybWluZSB3aGV0aGVyIHRoZSBET01QYXJzZXIgZXhpc3RzIGluIHRoZSBnbG9iYWwgY29udGV4dC5cbiAqIFRoZSB0cnktY2F0Y2ggaXMgYmVjYXVzZSwgb24gc29tZSBicm93c2VycywgdHJ5aW5nIHRvIGFjY2VzcyB0aGlzIHByb3BlcnR5XG4gKiBvbiB3aW5kb3cgY2FuIGFjdHVhbGx5IHRocm93IGFuIGVycm9yLlxuICpcbiAqIEBzdXBwcmVzcyB7dXNlbGVzc0NvZGV9XG4gKi9cbmZ1bmN0aW9uIGlzRE9NUGFyc2VyQXZhaWxhYmxlKCkge1xuICB0cnkge1xuICAgIHJldHVybiAhISh3aW5kb3cgYXMgYW55KS5ET01QYXJzZXI7XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuIl19