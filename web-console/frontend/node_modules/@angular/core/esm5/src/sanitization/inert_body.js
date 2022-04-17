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
var InertBodyHelper = /** @class */ (function () {
    function InertBodyHelper(defaultDoc) {
        this.defaultDoc = defaultDoc;
        this.inertDocument = this.defaultDoc.implementation.createHTMLDocument('sanitization-inert');
        this.inertBodyElement = this.inertDocument.body;
        if (this.inertBodyElement == null) {
            // usually there should be only one body element in the document, but IE doesn't have any, so
            // we need to create one.
            var inertHtml = this.inertDocument.createElement('html');
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
     */
    InertBodyHelper.prototype.getInertBodyElement_XHR = function (html) {
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
        var xhr = new XMLHttpRequest();
        xhr.responseType = 'document';
        xhr.open('GET', 'data:text/html;charset=utf-8,' + html, false);
        xhr.send(undefined);
        var body = xhr.response.body;
        body.removeChild(body.firstChild);
        return body;
    };
    /**
     * Use DOMParser to create and fill an inert body element (on Firefox)
     * See https://github.com/cure53/DOMPurify/releases/tag/0.6.7
     *
     */
    InertBodyHelper.prototype.getInertBodyElement_DOMParser = function (html) {
        // We add these extra elements to ensure that the rest of the content is parsed as expected
        // e.g. leading whitespace is maintained and tags like `<meta>` do not get hoisted to the
        // `<head>` tag.
        html = '<body><remove></remove>' + html + '</body>';
        try {
            var body = new window
                .DOMParser()
                .parseFromString(html, 'text/html')
                .body;
            body.removeChild(body.firstChild);
            return body;
        }
        catch (_a) {
            return null;
        }
    };
    /**
     * Use an HTML5 `template` element, if supported, or an inert body element created via
     * `createHtmlDocument` to create and fill an inert DOM element.
     * This is the default sane strategy to use if the browser does not require one of the specialised
     * strategies above.
     */
    InertBodyHelper.prototype.getInertBodyElement_InertDocument = function (html) {
        // Prefer using <template> element if supported.
        var templateEl = this.inertDocument.createElement('template');
        if ('content' in templateEl) {
            templateEl.innerHTML = html;
            return templateEl;
        }
        this.inertBodyElement.innerHTML = html;
        // Support: IE 9-11 only
        // strip custom-namespaced attributes on IE<=11
        if (this.defaultDoc.documentMode) {
            this.stripCustomNsAttrs(this.inertBodyElement);
        }
        return this.inertBodyElement;
    };
    /**
     * When IE9-11 comes across an unknown namespaced attribute e.g. 'xlink:foo' it adds 'xmlns:ns1'
     * attribute to declare ns1 namespace and prefixes the attribute with 'ns1' (e.g.
     * 'ns1:xlink:foo').
     *
     * This is undesirable since we don't want to allow any of these custom attributes. This method
     * strips them all.
     */
    InertBodyHelper.prototype.stripCustomNsAttrs = function (el) {
        var elAttrs = el.attributes;
        // loop backwards so that we can support removals.
        for (var i = elAttrs.length - 1; 0 < i; i--) {
            var attrib = elAttrs.item(i);
            var attrName = attrib.name;
            if (attrName === 'xmlns:ns1' || attrName.indexOf('ns1:') === 0) {
                el.removeAttribute(attrName);
            }
        }
        var childNode = el.firstChild;
        while (childNode) {
            if (childNode.nodeType === Node.ELEMENT_NODE)
                this.stripCustomNsAttrs(childNode);
            childNode = childNode.nextSibling;
        }
    };
    return InertBodyHelper;
}());
export { InertBodyHelper };
/**
 * We need to determine whether the DOMParser exists in the global context.
 * The try-catch is because, on some browsers, trying to access this property
 * on window can actually throw an error.
 *
 * @suppress {uselessCode}
 */
function isDOMParserAvailable() {
    try {
        return !!window.DOMParser;
    }
    catch (_a) {
        return false;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5lcnRfYm9keS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3Nhbml0aXphdGlvbi9pbmVydF9ib2R5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVIOzs7Ozs7O0dBT0c7QUFDSDtJQUlFLHlCQUFvQixVQUFvQjtRQUFwQixlQUFVLEdBQVYsVUFBVSxDQUFVO1FBQ3RDLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxjQUFjLENBQUMsa0JBQWtCLENBQUMsb0JBQW9CLENBQUMsQ0FBQztRQUM3RixJQUFJLENBQUMsZ0JBQWdCLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUM7UUFFaEQsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLElBQUksSUFBSSxFQUFFO1lBQ2pDLDZGQUE2RjtZQUM3Rix5QkFBeUI7WUFDekIsSUFBTSxTQUFTLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxhQUFhLENBQUMsTUFBTSxDQUFDLENBQUM7WUFDM0QsSUFBSSxDQUFDLGFBQWEsQ0FBQyxXQUFXLENBQUMsU0FBUyxDQUFDLENBQUM7WUFDMUMsSUFBSSxDQUFDLGdCQUFnQixHQUFHLElBQUksQ0FBQyxhQUFhLENBQUMsYUFBYSxDQUFDLE1BQU0sQ0FBQyxDQUFDO1lBQ2pFLFNBQVMsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLENBQUM7U0FDOUM7UUFFRCxJQUFJLENBQUMsZ0JBQWdCLENBQUMsU0FBUyxHQUFHLHNEQUFzRCxDQUFDO1FBQ3pGLElBQUksSUFBSSxDQUFDLGdCQUFnQixDQUFDLGFBQWEsSUFBSSxDQUFDLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxhQUFhLENBQUMsS0FBSyxDQUFDLEVBQUU7WUFDdEYsb0ZBQW9GO1lBQ3BGLDJCQUEyQjtZQUMzQixJQUFJLENBQUMsbUJBQW1CLEdB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SUFBTSxPQUFPLEdBQUcsRUFBRSxDQUFDLFVBQVUsQ0FBQztRQUM5QixrREFBa0Q7UUFDbEQsS0FBSyxJQUFJLENBQUMsR0FBRyxPQUFPLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFO1lBQzNDLElBQU0sTUFBTSxHQUFHLE9BQU8sQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7WUFDL0IsSUFBTSxRQUFRLEdBQUcsTUFBUSxDQUFDLElBQUksQ0FBQztZQUMvQixJQUFJLFFBQVEsS0FBSyxXQUFXLElBQUksUUFBUSxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLEVBQUU7Z0JBQzlELEVBQUUsQ0FBQyxlQUFlLENBQUMsUUFBUSxDQUFDLENBQUM7YUFDOUI7U0FDRjtRQUNELElBQUksU0FBUyxHQUFHLEVBQUUsQ0FBQyxVQUF5QixDQUFDO1FBQzdDLE9BQU8sU0FBUyxFQUFFO1lBQ2hCLElBQUksU0FBUyxDQUFDLFFBQVEsS0FBSyxJQUFJLENBQUMsWUFBWTtnQkFBRSxJQUFJLENBQUMsa0JBQWtCLENBQUMsU0FBb0IsQ0FBQyxDQUFDO1lBQzVGLFNBQVMsR0FBRyxTQUFTLENBQUMsV0FBVyxDQUFDO1NBQ25DO0lBQ0gsQ0FBQztJQUNILHNCQUFDO0FBQUQsQ0FBQyxBQS9JRCxJQStJQzs7QUFFRDs7Ozs7O0dBTUc7QUFDSCxTQUFTLG9CQUFvQjtJQUMzQixJQUFJO1FBQ0YsT0FBTyxDQUFDLENBQUUsTUFBYyxDQUFDLFNBQVMsQ0FBQztLQUNwQztJQUFDLFdBQU07UUFDTixPQUFPLEtBQUssQ0FBQztLQUNkO0FBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBUaGlzIGhlbHBlciBjbGFzcyBpcyB1c2VkIHRvIGdldCBob2xkIG9mIGFuIGluZXJ0IHRyZWUgb2YgRE9NIGVsZW1lbnRzIGNvbnRhaW5pbmcgZGlydHkgSFRNTFxuICogdGhhdCBuZWVkcyBzYW5pdGl6aW5nLlxuICogRGVwZW5kaW5nIHVwb24gYnJvd3NlciBzdXBwb3J0IHdlIG11c3QgdXNlIG9uZSBvZiB0aHJlZSBzdHJhdGVnaWVzIGZvciBkb2luZyB0aGlzLlxuICogU3VwcG9ydDogU2FmYXJpIDEwLnggLT4gWEhSIHN0cmF0ZWd5XG4gKiBTdXBwb3J0OiBGaXJlZm94IC0+IERvbVBhcnNlciBzdHJhdGVneVxuICogRGVmYXVsdDogSW5lcnREb2N1bWVudCBzdHJhdGVneVxuICovXG5leHBvcnQgY2xhc3MgSW5lcnRCb2R5SGVscGVyIHtcbiAgcHJpdmF0ZSBpbmVydEJvZHlFbGVtZW50OiBIVE1MRWxlbWVudDtcbiAgcHJpdmF0ZSBpbmVydERvY3VtZW50OiBEb2N1bWVudDtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGRlZmF1bHREb2M6IERvY3VtZW50KSB7XG4gICAgdGhpcy5pbmVydERvY3VtZW50ID0gdGhpcy5kZWZhdWx0RG9jLmltcGxlbWVudGF0aW9uLmNyZWF0ZUhUTUxEb2N1bWVudCgnc2FuaXRpemF0aW9uLWluZXJ0Jyk7XG4gICAgdGhpcy5pbmVydEJvZHlFbGVtZW50ID0gdGhpcy5pbmVydERvY3VtZW50LmJvZHk7XG5cbiAgICBpZiAodGhpcy5pbmVydEJvZHlFbGVtZW50ID09IG51bGwpIHtcbiAgICAgIC8vIHVzdWFsbHkgdGhlcmUgc2hvdWxkIGJlIG9ubHkgb25lIGJvZHkgZWxlbWVudCBpbiB0aGUgZG9jdW1lbnQsIGJ1dCBJRSBkb2Vzbid0IGhhdmUgYW55LCBzb1xuICAgICAgLy8gd2UgbmVlZCB0byBjcmVhdGUgb25lLlxuICAgICAgY29uc3QgaW5lcnRIdG1sID0gdGhpcy5pbmVydERvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2h0bWwnKTtcbiAgICAgIHRoaXMuaW5lcnREb2N1bWVudC5hcHBlbmRDaGlsZChpbmVydEh0bWwpO1xuICAgICAgdGhpcy5pbmVydEJvZHlFbGVtZW50ID0gdGhpcy5pbmVydERvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2JvZHknKTtcbiAgICAgIGluZXJ0SHRtbC5hcHBlbmRDaGlsZCh0aGlzLmluZXJ0Qm9keUVsZW1lbnQpO1xuICAgIH1cblxuICAgIHRoaXMuaW5lcnRCb2R5RWxlbWVudC5pbm5lckhUTUwgPSAnPHN2Zz48ZyBvbmxvYWQ9XCJ0aGlzLnBhcmVudE5vZGUucmVtb3ZlKClcIj48L2c+PC9zdmc+JztcbiAgICBpZiAodGhpcy5pbmVydEJvZHlFbGVtZW50LnF1ZXJ5U2VsZWN0b3IgJiYgIXRoaXMuaW5lcnRCb2R5RWxlbWVudC5xdWVyeVNlbGVjdG9yKCdzdmcnKSkge1xuICAgICAgLy8gV2UganVzdCBoaXQgdGhlIFNhZmFyaSAxMC4xIGJ1ZyAtIHdoaWNoIGFsbG93cyBKUyB0byBydW4gaW5zaWRlIHRoZSBTVkcgRyBlbGVtZW50XG4gICAgICAvLyBzbyB1c2UgdGhlIFhIUiBzdHJhdGVneS5cbiAgICAgIHRoaXMuZ2V0SW5lcnRCb2R5RWxlbWVudCA9IHRoaXMuZ2V0SW5lcnRCb2R5RWxlbWVudF9YSFI7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgdGhpcy5pbmVydEJvZHlFbGVtZW50LmlubmVySFRNTCA9XG4gICAgICAgICc8c3ZnPjxwPjxzdHlsZT48aW1nIHNyYz1cIjwvc3R5bGU+PGltZyBzcmM9eCBvbmVycm9yPWFsZXJ0KDEpLy9cIj4nO1xuICAgIGlmICh0aGlzLmluZXJ0Qm9keUVsZW1lbnQucXVlcnlTZWxlY3RvciAmJiB0aGlzLmluZXJ0Qm9keUVsZW1lbnQucXVlcnlTZWxlY3Rvcignc3ZnIGltZycpKSB7XG4gICAgICAvLyBXZSBqdXN0IGhpdCB0aGUgRmlyZWZveCBidWcgLSB3aGljaCBwcmV2ZW50cyB0aGUgaW5uZXIgaW1nIEpTIGZyb20gYmVpbmcgc2FuaXRpemVkXG4gICAgICAvLyBzbyB1c2UgdGhlIERPTVBhcnNlciBzdHJhdGVneSwgaWYgaXQgaXMgYXZhaWxhYmxlLlxuICAgICAgLy8gSWYgdGhlIERPTVBhcnNlciBpcyBub3QgYXZhaWxhYmxlIHRoZW4gd2UgYXJlIG5vdCBpbiBGaXJlZm94IChTZXJ2ZXIvV2ViV29ya2VyPykgc28gd2VcbiAgICAgIC8vIGZhbGwgdGhyb3VnaCB0byB0aGUgZGVmYXVsdCBzdHJhdGVneSBiZWxvdy5cbiAgICAgIGlmIChpc0RPTVBhcnNlckF2YWlsYWJsZSgpKSB7XG4gICAgICAgIHRoaXMuZ2V0SW5lcnRCb2R5RWxlbWVudCA9IHRoaXMuZ2V0SW5lcnRCb2R5RWxlbWVudF9ET01QYXJzZXI7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBOb25lIG9mIHRoZSBidWdzIHdlcmUgaGl0IHNvIGl0IGlzIHNhZmUgZm9yIHVzIHRvIHVzZSB0aGUgZGVmYXVsdCBJbmVydERvY3VtZW50IHN0cmF0ZWd5XG4gICAgdGhpcy5nZXRJbmVydEJvZHlFbGVtZW50ID0gdGhpcy5nZXRJbmVydEJvZHlFbGVtZW50X0luZXJ0RG9jdW1lbnQ7XG4gIH1cblxuICAvKipcbiAgICogR2V0IGFuIGluZXJ0IERPTSBlbGVtZW50IGNvbnRhaW5pbmcgRE9NIGNyZWF0ZWQgZnJvbSB0aGUgZGlydHkgSFRNTCBzdHJpbmcgcHJvdmlkZWQuXG4gICAqIFRoZSBpbXBsZW1lbnRhdGlvbiBvZiB0aGlzIGlzIGRldGVybWluZWQgaW4gdGhlIGNvbnN0cnVjdG9yLCB3aGVuIHRoZSBjbGFzcyBpcyBpbnN0YW50aWF0ZWQuXG4gICAqL1xuICBnZXRJbmVydEJvZHlFbGVtZW50OiAoaHRtbDogc3RyaW5nKSA9PiBIVE1MRWxlbWVudCB8IG51bGw7XG5cbiAgLyoqXG4gICAqIFVzZSBYSFIgdG8gY3JlYXRlIGFuZCBmaWxsIGFuIGluZXJ0IGJvZHkgZWxlbWVudCAob24gU2FmYXJpIDEwLjEpXG4gICAqIFNlZVxuICAgKiBodHRwczovL2dpdGh1Yi5jb20vY3VyZTUzL0RPTVB1cmlmeS9ibG9iL2E5OTJkM2E3NTAzMWNiOGJiMDMyZTVlYTgzOTliYTk3MmJkZjlhNjUvc3JjL3B1cmlmeS5qcyNMNDM5LUw0NDlcbiAgICovXG4gIHByaXZhdGUgZ2V0SW5lcnRCb2R5RWxlbWVudF9YSFIoaHRtbDogc3RyaW5nKSB7XG4gICAgLy8gV2UgYWRkIHRoZXNlIGV4dHJhIGVsZW1lbnRzIHRvIGVuc3VyZSB0aGF0IHRoZSByZXN0IG9mIHRoZSBjb250ZW50IGlzIHBhcnNlZCBhcyBleHBlY3RlZFxuICAgIC8vIGUuZy4gbGVhZGluZyB3aGl0ZXNwYWNlIGlzIG1haW50YWluZWQgYW5kIHRhZ3MgbGlrZSBgPG1ldGE+YCBkbyBub3QgZ2V0IGhvaXN0ZWQgdG8gdGhlXG4gICAgLy8gYDxoZWFkPmAgdGFnLlxuICAgIGh0bWwgPSAnPGJvZHk+PHJlbW92ZT48L3JlbW92ZT4nICsgaHRtbCArICc8L2JvZHk+JztcbiAgICB0cnkge1xuICAgICAgaHRtbCA9IGVuY29kZVVSSShodG1sKTtcbiAgICB9IGNhdGNoIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICBjb25zdCB4aHIgPSBuZXcgWE1MSHR0cFJlcXVlc3QoKTtcbiAgICB4aHIucmVzcG9uc2VUeXBlID0gJ2RvY3VtZW50JztcbiAgICB4aHIub3BlbignR0VUJywgJ2RhdGE6dGV4dC9odG1sO2NoYXJzZXQ9dXRmLTgsJyArIGh0bWwsIGZhbHNlKTtcbiAgICB4aHIuc2VuZCh1bmRlZmluZWQpO1xuICAgIGNvbnN0IGJvZHk6IEhUTUxCb2R5RWxlbWVudCA9IHhoci5yZXNwb25zZS5ib2R5O1xuICAgIGJvZHkucmVtb3ZlQ2hpbGQoYm9keS5maXJzdENoaWxkICEpO1xuICAgIHJldHVybiBib2R5O1xuICB9XG5cbiAgLyoqXG4gICAqIFVzZSBET01QYXJzZXIgdG8gY3JlYXRlIGFuZCBmaWxsIGFuIGluZXJ0IGJvZHkgZWxlbWVudCAob24gRmlyZWZveClcbiAgICogU2VlIGh0dHBzOi8vZ2l0aHViLmNvbS9jdXJlNTMvRE9NUHVyaWZ5L3JlbGVhc2VzL3RhZy8wLjYuN1xuICAgKlxuICAgKi9cbiAgcHJpdmF0ZSBnZXRJbmVydEJvZHlFbGVtZW50X0RPTVBhcnNlcihodG1sOiBzdHJpbmcpIHtcbiAgICAvLyBXZSBhZGQgdGhlc2UgZXh0cmEgZWxlbWVudHMgdG8gZW5zdXJlIHRoYXQgdGhlIHJlc3Qgb2YgdGhlIGNvbnRlbnQgaXMgcGFyc2VkIGFzIGV4cGVjdGVkXG4gICAgLy8gZS5nLiBsZWFkaW5nIHdoaXRlc3BhY2UgaXMgbWFpbnRhaW5lZCBhbmQgdGFncyBsaWtlIGA8bWV0YT5gIGRvIG5vdCBnZXQgaG9pc3RlZCB0byB0aGVcbiAgICAvLyBgPGhlYWQ+YCB0YWcuXG4gICAgaHRtbCA9ICc8Ym9keT48cmVtb3ZlPjwvcmVtb3ZlPicgKyBodG1sICsgJzwvYm9keT4nO1xuICAgIHRyeSB7XG4gICAgICBjb25zdCBib2R5ID0gbmV3ICh3aW5kb3cgYXMgYW55KVxuICAgICAgICAgICAgICAgICAgICAgICAuRE9NUGFyc2VyKClcbiAgICAgICAgICAgICAgICAgICAgICAgLnBhcnNlRnJvbVN0cmluZyhodG1sLCAndGV4dC9odG1sJylcbiAgICAgICAgICAgICAgICAgICAgICAgLmJvZHkgYXMgSFRNTEJvZHlFbGVtZW50O1xuICAgICAgYm9keS5yZW1vdmVDaGlsZChib2R5LmZpcnN0Q2hpbGQgISk7XG4gICAgICByZXR1cm4gYm9keTtcbiAgICB9IGNhdGNoIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBVc2UgYW4gSFRNTDUgYHRlbXBsYXRlYCBlbGVtZW50LCBpZiBzdXBwb3J0ZWQsIG9yIGFuIGluZXJ0IGJvZHkgZWxlbWVudCBjcmVhdGVkIHZpYVxuICAgKiBgY3JlYXRlSHRtbERvY3VtZW50YCB0byBjcmVhdGUgYW5kIGZpbGwgYW4gaW5lcnQgRE9NIGVsZW1lbnQuXG4gICAqIFRoaXMgaXMgdGhlIGRlZmF1bHQgc2FuZSBzdHJhdGVneSB0byB1c2UgaWYgdGhlIGJyb3dzZXIgZG9lcyBub3QgcmVxdWlyZSBvbmUgb2YgdGhlIHNwZWNpYWxpc2VkXG4gICAqIHN0cmF0ZWdpZXMgYWJvdmUuXG4gICAqL1xuICBwcml2YXRlIGdldEluZXJ0Qm9keUVsZW1lbnRfSW5lcnREb2N1bWVudChodG1sOiBzdHJpbmcpIHtcbiAgICAvLyBQcmVmZXIgdXNpbmcgPHRlbXBsYXRlPiBlbGVtZW50IGlmIHN1cHBvcnRlZC5cbiAgICBjb25zdCB0ZW1wbGF0ZUVsID0gdGhpcy5pbmVydERvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3RlbXBsYXRlJyk7XG4gICAgaWYgKCdjb250ZW50JyBpbiB0ZW1wbGF0ZUVsKSB7XG4gICAgICB0ZW1wbGF0ZUVsLmlubmVySFRNTCA9IGh0bWw7XG4gICAgICByZXR1cm4gdGVtcGxhdGVFbDtcbiAgICB9XG5cbiAgICB0aGlzLmluZXJ0Qm9keUVsZW1lbnQuaW5uZXJIVE1MID0gaHRtbDtcblxuICAgIC8vIFN1cHBvcnQ6IElFIDktMTEgb25seVxuICAgIC8vIHN0cmlwIGN1c3RvbS1uYW1lc3BhY2VkIGF0dHJpYnV0ZXMgb24gSUU8PTExXG4gICAgaWYgKCh0aGlzLmRlZmF1bHREb2MgYXMgYW55KS5kb2N1bWVudE1vZGUpIHtcbiAgICAgIHRoaXMuc3RyaXBDdXN0b21Oc0F0dHJzKHRoaXMuaW5lcnRCb2R5RWxlbWVudCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMuaW5lcnRCb2R5RWxlbWVudDtcbiAgfVxuXG4gIC8qKlxuICAgKiBXaGVuIElFOS0xMSBjb21lcyBhY3Jvc3MgYW4gdW5rbm93biBuYW1lc3BhY2VkIGF0dHJpYnV0ZSBlLmcuICd4bGluazpmb28nIGl0IGFkZHMgJ3htbG5zOm5zMSdcbiAgICogYXR0cmlidXRlIHRvIGRlY2xhcmUgbnMxIG5hbWVzcGFjZSBhbmQgcHJlZml4ZXMgdGhlIGF0dHJpYnV0ZSB3aXRoICduczEnIChlLmcuXG4gICAqICduczE6eGxpbms6Zm9vJykuXG4gICAqXG4gICAqIFRoaXMgaXMgdW5kZXNpcmFibGUgc2luY2Ugd2UgZG9uJ3Qgd2FudCB0byBhbGxvdyBhbnkgb2YgdGhlc2UgY3VzdG9tIGF0dHJpYnV0ZXMuIFRoaXMgbWV0aG9kXG4gICAqIHN0cmlwcyB0aGVtIGFsbC5cbiAgICovXG4gIHByaXZhdGUgc3RyaXBDdXN0b21Oc0F0dHJzKGVsOiBFbGVtZW50KSB7XG4gICAgY29uc3QgZWxBdHRycyA9IGVsLmF0dHJpYnV0ZXM7XG4gICAgLy8gbG9vcCBiYWNrd2FyZHMgc28gdGhhdCB3ZSBjYW4gc3VwcG9ydCByZW1vdmFscy5cbiAgICBmb3IgKGxldCBpID0gZWxBdHRycy5sZW5ndGggLSAxOyAwIDwgaTsgaS0tKSB7XG4gICAgICBjb25zdCBhdHRyaWIgPSBlbEF0dHJzLml0ZW0oaSk7XG4gICAgICBjb25zdCBhdHRyTmFtZSA9IGF0dHJpYiAhLm5hbWU7XG4gICAgICBpZiAoYXR0ck5hbWUgPT09ICd4bWxuczpuczEnIHx8IGF0dHJOYW1lLmluZGV4T2YoJ25zMTonKSA9PT0gMCkge1xuICAgICAgICBlbC5yZW1vdmVBdHRyaWJ1dGUoYXR0ck5hbWUpO1xuICAgICAgfVxuICAgIH1cbiAgICBsZXQgY2hpbGROb2RlID0gZWwuZmlyc3RDaGlsZCBhcyBOb2RlIHwgbnVsbDtcbiAgICB3aGlsZSAoY2hpbGROb2RlKSB7XG4gICAgICBpZiAoY2hpbGROb2RlLm5vZGVUeXBlID09PSBOb2RlLkVMRU1FTlRfTk9ERSkgdGhpcy5zdHJpcEN1c3RvbU5zQXR0cnMoY2hpbGROb2RlIGFzIEVsZW1lbnQpO1xuICAgICAgY2hpbGROb2RlID0gY2hpbGROb2RlLm5leHRTaWJsaW5nO1xuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIFdlIG5lZWQgdG8gZGV0ZXJtaW5lIHdoZXRoZXIgdGhlIERPTVBhcnNlciBleGlzdHMgaW4gdGhlIGdsb2JhbCBjb250ZXh0LlxuICogVGhlIHRyeS1jYXRjaCBpcyBiZWNhdXNlLCBvbiBzb21lIGJyb3dzZXJzLCB0cnlpbmcgdG8gYWNjZXNzIHRoaXMgcHJvcGVydHlcbiAqIG9uIHdpbmRvdyBjYW4gYWN0dWFsbHkgdGhyb3cgYW4gZXJyb3IuXG4gKlxuICogQHN1cHByZXNzIHt1c2VsZXNzQ29kZX1cbiAqL1xuZnVuY3Rpb24gaXNET01QYXJzZXJBdmFpbGFibGUoKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuICEhKHdpbmRvdyBhcyBhbnkpLkRPTVBhcnNlcjtcbiAgfSBjYXRjaCB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG4iXX0=