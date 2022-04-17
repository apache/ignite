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
 * A codec for encoding and decoding URL parts.
 *
 * \@publicApi
 *
 * @abstract
 */
export class UrlCodec {
}
if (false) {
    /**
     * Encodes the path from the provided string
     *
     * @abstract
     * @param {?} path The path string
     * @return {?}
     */
    UrlCodec.prototype.encodePath = function (path) { };
    /**
     * Decodes the path from the provided string
     *
     * @abstract
     * @param {?} path The path string
     * @return {?}
     */
    UrlCodec.prototype.decodePath = function (path) { };
    /**
     * Encodes the search string from the provided string or object
     *
     * @abstract
     * @param {?} search
     * @return {?}
     */
    UrlCodec.prototype.encodeSearch = function (search) { };
    /**
     * Decodes the search objects from the provided string
     *
     * @abstract
     * @param {?} search
     * @return {?}
     */
    UrlCodec.prototype.decodeSearch = function (search) { };
    /**
     * Encodes the hash from the provided string
     *
     * @abstract
     * @param {?} hash
     * @return {?}
     */
    UrlCodec.prototype.encodeHash = function (hash) { };
    /**
     * Decodes the hash from the provided string
     *
     * @abstract
     * @param {?} hash
     * @return {?}
     */
    UrlCodec.prototype.decodeHash = function (hash) { };
    /**
     * Normalizes the URL from the provided string
     *
     * @abstract
     * @param {?} href
     * @return {?}
     */
    UrlCodec.prototype.normalize = function (href) { };
    /**
     * Normalizes the URL from the provided string, search, hash, and base URL parameters
     *
     * @abstract
     * @param {?} path The URL path
     * @param {?} search The search object
     * @param {?} hash The has string
     * @param {?=} baseUrl The base URL for the URL
     * @return {?}
     */
    UrlCodec.prototype.normalize = function (path, search, hash, baseUrl) { };
    /**
     * Checks whether the two strings are equal
     * @abstract
     * @param {?} valA First string for comparison
     * @param {?} valB Second string for comparison
     * @return {?}
     */
    UrlCodec.prototype.areEqual = function (valA, valB) { };
    /**
     * Parses the URL string based on the base URL
     *
     * @abstract
     * @param {?} url The full URL string
     * @param {?=} base The base for the URL
     * @return {?}
     */
    UrlCodec.prototype.parse = function (url, base) { };
}
/**
 * A `UrlCodec` that uses logic from AngularJS to serialize and parse URLs
 * and URL parameters.
 *
 * \@publicApi
 */
export class AngularJSUrlCodec {
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L15
    /**
     * @param {?} path
     * @return {?}
     */
    encodePath(path) {
        /** @type {?} */
        const segments = path.split('/');
        /** @type {?} */
        let i = segments.length;
        while (i--) {
            // decode forward slashes to prevent them from being double encoded
            segments[i] = encodeUriSegment(segments[i].replace(/%2F/g, '/'));
        }
        path = segments.join('/');
        return _stripIndexHtml((path && path[0] !== '/' && '/' || '') + path);
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L42
    /**
     * @param {?} search
     * @return {?}
     */
    encodeSearch(search) {
        if (typeof search === 'string') {
            search = parseKeyValue(search);
        }
        search = toKeyValue(search);
        return search ? '?' + search : '';
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L44
    /**
     * @param {?} hash
     * @return {?}
     */
    encodeHash(hash) {
        hash = encodeUriSegment(hash);
        return hash ? '#' + hash : '';
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L27
    /**
     * @param {?} path
     * @param {?=} html5Mode
     * @return {?}
     */
    decodePath(path, html5Mode = true) {
        /** @type {?} */
        const segments = path.split('/');
        /** @type {?} */
        let i = segments.length;
        while (i--) {
            segments[i] = decodeURIComponent(segments[i]);
            if (html5Mode) {
                // encode forward slashes to prevent them from being mistaken for path separators
                segments[i] = segments[i].replace(/\//g, '%2F');
            }
        }
        return segments.join('/');
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L72
    /**
     * @param {?} search
     * @return {?}
     */
    decodeSearch(search) { return parseKeyValue(search); }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L73
    /**
     * @param {?} hash
     * @return {?}
     */
    decodeHash(hash) {
        hash = decodeURIComponent(hash);
        return hash[0] === '#' ? hash.substring(1) : hash;
    }
    /**
     * @param {?} pathOrHref
     * @param {?=} search
     * @param {?=} hash
     * @param {?=} baseUrl
     * @return {?}
     */
    normalize(pathOrHref, search, hash, baseUrl) {
        if (arguments.length === 1) {
            /** @type {?} */
            const parsed = this.parse(pathOrHref, baseUrl);
            if (typeof parsed === 'string') {
                return parsed;
            }
            /** @type {?} */
            const serverUrl = `${parsed.protocol}://${parsed.hostname}${parsed.port ? ':' + parsed.port : ''}`;
            return this.normalize(this.decodePath(parsed.pathname), this.decodeSearch(parsed.search), this.decodeHash(parsed.hash), serverUrl);
        }
        else {
            /** @type {?} */
            const encPath = this.encodePath(pathOrHref);
            /** @type {?} */
            const encSearch = search && this.encodeSearch(search) || '';
            /** @type {?} */
            const encHash = hash && this.encodeHash(hash) || '';
            /** @type {?} */
            let joinedPath = (baseUrl || '') + encPath;
            if (!joinedPath.length || joinedPath[0] !== '/') {
                joinedPath = '/' + joinedPath;
            }
            return joinedPath + encSearch + encHash;
        }
    }
    /**
     * @param {?} valA
     * @param {?} valB
     * @return {?}
     */
    areEqual(valA, valB) { return this.normalize(valA) === this.normalize(valB); }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/urlUtils.js#L60
    /**
     * @param {?} url
     * @param {?=} base
     * @return {?}
     */
    parse(url, base) {
        try {
            // Safari 12 throws an error when the URL constructor is called with an undefined base.
            /** @type {?} */
            const parsed = !base ? new URL(url) : new URL(url, base);
            return {
                href: parsed.href,
                protocol: parsed.protocol ? parsed.protocol.replace(/:$/, '') : '',
                host: parsed.host,
                search: parsed.search ? parsed.search.replace(/^\?/, '') : '',
                hash: parsed.hash ? parsed.hash.replace(/^#/, '') : '',
                hostname: parsed.hostname,
                port: parsed.port,
                pathname: (parsed.pathname.charAt(0) === '/') ? parsed.pathname : '/' + parsed.pathname
            };
        }
        catch (e) {
            throw new Error(`Invalid URL (${url}) with base (${base})`);
        }
    }
}
/**
 * @param {?} url
 * @return {?}
 */
function _stripIndexHtml(url) {
    return url.replace(/\/index.html$/, '');
}
/**
 * Tries to decode the URI component without throwing an exception.
 *
 * @param {?} value
 * @return {?}
 */
function tryDecodeURIComponent(value) {
    try {
        return decodeURIComponent(value);
    }
    catch (e) {
        // Ignore any invalid uri component.
        return undefined;
    }
}
/**
 * Parses an escaped url query string into key-value pairs. Logic taken from
 * https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1382
 * @param {?} keyValue
 * @return {?}
 */
function parseKeyValue(keyValue) {
    /** @type {?} */
    const obj = {};
    (keyValue || '').split('&').forEach((/**
     * @param {?} keyValue
     * @return {?}
     */
    (keyValue) => {
        /** @type {?} */
        let splitPoint;
        /** @type {?} */
        let key;
        /** @type {?} */
        let val;
        if (keyValue) {
            key = keyValue = keyValue.replace(/\+/g, '%20');
            splitPoint = keyValue.indexOf('=');
            if (splitPoint !== -1) {
                key = keyValue.substring(0, splitPoint);
                val = keyValue.substring(splitPoint + 1);
            }
            key = tryDecodeURIComponent(key);
            if (typeof key !== 'undefined') {
                val = typeof val !== 'undefined' ? tryDecodeURIComponent(val) : true;
                if (!obj.hasOwnProperty(key)) {
                    obj[key] = val;
                }
                else if (Array.isArray(obj[key])) {
                    ((/** @type {?} */ (obj[key]))).push(val);
                }
                else {
                    obj[key] = [obj[key], val];
                }
            }
        }
    }));
    return obj;
}
/**
 * Serializes into key-value pairs. Logic taken from
 * https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1409
 * @param {?} obj
 * @return {?}
 */
function toKeyValue(obj) {
    /** @type {?} */
    const parts = [];
    for (const key in obj) {
        /** @type {?} */
        let value = obj[key];
        if (Array.isArray(value)) {
            value.forEach((/**
             * @param {?} arrayValue
             * @return {?}
             */
            (arrayValue) => {
                parts.push(encodeUriQuery(key, true) +
                    (arrayValue === true ? '' : '=' + encodeUriQuery(arrayValue, true)));
            }));
        }
        else {
            parts.push(encodeUriQuery(key, true) +
                (value === true ? '' : '=' + encodeUriQuery((/** @type {?} */ (value)), true)));
        }
    }
    return parts.length ? parts.join('&') : '';
}
/**
 * We need our custom method because encodeURIComponent is too aggressive and doesn't follow
 * http://www.ietf.org/rfc/rfc3986.txt with regards to the character set (pchar) allowed in path
 * segments:
 *    segment       = *pchar
 *    pchar         = unreserved / pct-encoded / sub-delims / ":" / "\@"
 *    pct-encoded   = "%" HEXDIG HEXDIG
 *    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
 *    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
 *                     / "*" / "+" / "," / ";" / "="
 *
 * Logic from https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1437
 * @param {?} val
 * @return {?}
 */
function encodeUriSegment(val) {
    return encodeUriQuery(val, true)
        .replace(/%26/gi, '&')
        .replace(/%3D/gi, '=')
        .replace(/%2B/gi, '+');
}
/**
 * This method is intended for encoding *key* or *value* parts of query component. We need a custom
 * method because encodeURIComponent is too aggressive and encodes stuff that doesn't have to be
 * encoded per http://tools.ietf.org/html/rfc3986:
 *    query         = *( pchar / "/" / "?" )
 *    pchar         = unreserved / pct-encoded / sub-delims / ":" / "\@"
 *    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
 *    pct-encoded   = "%" HEXDIG HEXDIG
 *    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
 *                     / "*" / "+" / "," / ";" / "="
 *
 * Logic from https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1456
 * @param {?} val
 * @param {?=} pctEncodeSpaces
 * @return {?}
 */
function encodeUriQuery(val, pctEncodeSpaces = false) {
    return encodeURIComponent(val)
        .replace(/%40/gi, '@')
        .replace(/%3A/gi, ':')
        .replace(/%24/g, '$')
        .replace(/%2C/gi, ',')
        .replace(/%3B/gi, ';')
        .replace(/%20/g, (pctEncodeSpaces ? '%20' : '+'));
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGFyYW1zLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3VwZ3JhZGUvc3JjL3BhcmFtcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFhQSxNQUFNLE9BQWdCLFFBQVE7Q0FxRjdCOzs7Ozs7Ozs7SUEvRUMsb0RBQTBDOzs7Ozs7OztJQU8xQyxvREFBMEM7Ozs7Ozs7O0lBTzFDLHdEQUFxRTs7Ozs7Ozs7SUFPckUsd0RBQThEOzs7Ozs7OztJQU85RCxvREFBMEM7Ozs7Ozs7O0lBTzFDLG9EQUEwQzs7Ozs7Ozs7SUFPMUMsbURBQXlDOzs7Ozs7Ozs7OztJQVd6QywwRUFDVzs7Ozs7Ozs7SUFPWCx3REFBdUQ7Ozs7Ozs7OztJQVF2RCxvREFTRTs7Ozs7Ozs7QUFTSixNQUFNLE9BQU8saUJBQWlCOzs7Ozs7SUFFNUIsVUFBVSxDQUFDLElBQVk7O2NBQ2YsUUFBUSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDOztZQUM1QixDQUFDLEdBQUcsUUFBUSxDQUFDLE1BQU07UUFFdkIsT0FBTyxDQUFDLEVBQUUsRUFBRTtZQUNWLG1FQUFtRTtZQUNuRSxRQUFRLENBQUMsQ0FBQyxDQUFDLEdBQUcsZ0JBQWdCLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxNQUFNLEVBQUUsR0FBRyxDQUFDLENBQUMsQ0FBQztTQUNsRTtRQUVELElBQUksR0FBRyxRQUFRLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDO1FBQzFCLE9BQU8sZUFBZSxDQUFDLENBQUMsSUFBSSxJQUFJLElBQUksQ0FBQyxDQUFDLENBQUMsS0FBSyxHQUFHLElBQUksR0FBRyxJQUFJLEVBQUUsQ0FBQyxHQUFHLElBQUksQ0FBQyxDQUFDO0lBQ3hFLENBQUM7Ozs7OztJQUdELFlBQVksQ0FBQyxNQUFxQztRQUNoRCxJQUFJLE9BQU8sTUFBTSxLQUFL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Q3JCLE9BQU8sQ0FBQyxPQUFPLEVBQUUsR0FBRyxDQUFDO1NBQ3JCLE9BQU8sQ0FBQyxNQUFNLEVBQUUsR0FBRyxDQUFDO1NBQ3BCLE9BQU8sQ0FBQyxPQUFPLEVBQUUsR0FBRyxDQUFDO1NBQ3JCLE9BQU8sQ0FBQyxPQUFPLEVBQUUsR0FBRyxDQUFDO1NBQ3JCLE9BQU8sQ0FBQyxNQUFNLEVBQUUsQ0FBQyxlQUFlLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQztBQUN4RCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIEEgY29kZWMgZm9yIGVuY29kaW5nIGFuZCBkZWNvZGluZyBVUkwgcGFydHMuXG4gKlxuICogQHB1YmxpY0FwaVxuICoqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFVybENvZGVjIHtcbiAgLyoqXG4gICAqIEVuY29kZXMgdGhlIHBhdGggZnJvbSB0aGUgcHJvdmlkZWQgc3RyaW5nXG4gICAqXG4gICAqIEBwYXJhbSBwYXRoIFRoZSBwYXRoIHN0cmluZ1xuICAgKi9cbiAgYWJzdHJhY3QgZW5jb2RlUGF0aChwYXRoOiBzdHJpbmcpOiBzdHJpbmc7XG5cbiAgLyoqXG4gICAqIERlY29kZXMgdGhlIHBhdGggZnJvbSB0aGUgcHJvdmlkZWQgc3RyaW5nXG4gICAqXG4gICAqIEBwYXJhbSBwYXRoIFRoZSBwYXRoIHN0cmluZ1xuICAgKi9cbiAgYWJzdHJhY3QgZGVjb2RlUGF0aChwYXRoOiBzdHJpbmcpOiBzdHJpbmc7XG5cbiAgLyoqXG4gICAqIEVuY29kZXMgdGhlIHNlYXJjaCBzdHJpbmcgZnJvbSB0aGUgcHJvdmlkZWQgc3RyaW5nIG9yIG9iamVjdFxuICAgKlxuICAgKiBAcGFyYW0gcGF0aCBUaGUgcGF0aCBzdHJpbmcgb3Igb2JqZWN0XG4gICAqL1xuICBhYnN0cmFjdCBlbmNvZGVTZWFyY2goc2VhcmNoOiBzdHJpbmd8e1trOiBzdHJpbmddOiB1bmtub3dufSk6IHN0cmluZztcblxuICAvKipcbiAgICogRGVjb2RlcyB0aGUgc2VhcmNoIG9iamVjdHMgZnJvbSB0aGUgcHJvdmlkZWQgc3RyaW5nXG4gICAqXG4gICAqIEBwYXJhbSBwYXRoIFRoZSBwYXRoIHN0cmluZ1xuICAgKi9cbiAgYWJzdHJhY3QgZGVjb2RlU2VhcmNoKHNlYXJjaDogc3RyaW5nKToge1trOiBzdHJpbmddOiB1bmtub3dufTtcblxuICAvKipcbiAgICogRW5jb2RlcyB0aGUgaGFzaCBmcm9tIHRoZSBwcm92aWRlZCBzdHJpbmdcbiAgICpcbiAgICogQHBhcmFtIHBhdGggVGhlIGhhc2ggc3RyaW5nXG4gICAqL1xuICBhYnN0cmFjdCBlbmNvZGVIYXNoKGhhc2g6IHN0cmluZyk6IHN0cmluZztcblxuICAvKipcbiAgICogRGVjb2RlcyB0aGUgaGFzaCBmcm9tIHRoZSBwcm92aWRlZCBzdHJpbmdcbiAgICpcbiAgICogQHBhcmFtIHBhdGggVGhlIGhhc2ggc3RyaW5nXG4gICAqL1xuICBhYnN0cmFjdCBkZWNvZGVIYXNoKGhhc2g6IHN0cmluZyk6IHN0cmluZztcblxuICAvKipcbiAgICogTm9ybWFsaXplcyB0aGUgVVJMIGZyb20gdGhlIHByb3ZpZGVkIHN0cmluZ1xuICAgKlxuICAgKiBAcGFyYW0gcGF0aCBUaGUgVVJMIHN0cmluZ1xuICAgKi9cbiAgYWJzdHJhY3Qgbm9ybWFsaXplKGhyZWY6IHN0cmluZyk6IHN0cmluZztcblxuXG4gIC8qKlxuICAgKiBOb3JtYWxpemVzIHRoZSBVUkwgZnJvbSB0aGUgcHJvdmlkZWQgc3RyaW5nLCBzZWFyY2gsIGhhc2gsIGFuZCBiYXNlIFVSTCBwYXJhbWV0ZXJzXG4gICAqXG4gICAqIEBwYXJhbSBwYXRoIFRoZSBVUkwgcGF0aFxuICAgKiBAcGFyYW0gc2VhcmNoIFRoZSBzZWFyY2ggb2JqZWN0XG4gICAqIEBwYXJhbSBoYXNoIFRoZSBoYXMgc3RyaW5nXG4gICAqIEBwYXJhbSBiYXNlVXJsIFRoZSBiYXNlIFVSTCBmb3IgdGhlIFVSTFxuICAgKi9cbiAgYWJzdHJhY3Qgbm9ybWFsaXplKHBhdGg6IHN0cmluZywgc2VhcmNoOiB7W2s6IHN0cmluZ106IHVua25vd259LCBoYXNoOiBzdHJpbmcsIGJhc2VVcmw/OiBzdHJpbmcpOlxuICAgICAgc3RyaW5nO1xuXG4gIC8qKlxuICAgKiBDaGVja3Mgd2hldGhlciB0aGUgdHdvIHN0cmluZ3MgYXJlIGVxdWFsXG4gICAqIEBwYXJhbSB2YWxBIEZpcnN0IHN0cmluZyBmb3IgY29tcGFyaXNvblxuICAgKiBAcGFyYW0gdmFsQiBTZWNvbmQgc3RyaW5nIGZvciBjb21wYXJpc29uXG4gICAqL1xuICBhYnN0cmFjdCBhcmVFcXVhbCh2YWxBOiBzdHJpbmcsIHZhbEI6IHN0cmluZyk6IGJvb2xlYW47XG5cbiAgLyoqXG4gICAqIFBhcnNlcyB0aGUgVVJMIHN0cmluZyBiYXNlZCBvbiB0aGUgYmFzZSBVUkxcbiAgICpcbiAgICogQHBhcmFtIHVybCBUaGUgZnVsbCBVUkwgc3RyaW5nXG4gICAqIEBwYXJhbSBiYXNlIFRoZSBiYXNlIGZvciB0aGUgVVJMXG4gICAqL1xuICBhYnN0cmFjdCBwYXJzZSh1cmw6IHN0cmluZywgYmFzZT86IHN0cmluZyk6IHtcbiAgICBocmVmOiBzdHJpbmcsXG4gICAgcHJvdG9jb2w6IHN0cmluZyxcbiAgICBob3N0OiBzdHJpbmcsXG4gICAgc2VhcmNoOiBzdHJpbmcsXG4gICAgaGFzaDogc3RyaW5nLFxuICAgIGhvc3RuYW1lOiBzdHJpbmcsXG4gICAgcG9ydDogc3RyaW5nLFxuICAgIHBhdGhuYW1lOiBzdHJpbmdcbiAgfTtcbn1cblxuLyoqXG4gKiBBIGBVcmxDb2RlY2AgdGhhdCB1c2VzIGxvZ2ljIGZyb20gQW5ndWxhckpTIHRvIHNlcmlhbGl6ZSBhbmQgcGFyc2UgVVJMc1xuICogYW5kIFVSTCBwYXJhbWV0ZXJzLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIEFuZ3VsYXJKU1VybENvZGVjIGltcGxlbWVudHMgVXJsQ29kZWMge1xuICAvLyBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvODY0YzdmMC9zcmMvbmcvbG9jYXRpb24uanMjTDE1XG4gIGVuY29kZVBhdGgocGF0aDogc3RyaW5nKTogc3RyaW5nIHtcbiAgICBjb25zdCBzZWdtZW50cyA9IHBhdGguc3BsaXQoJy8nKTtcbiAgICBsZXQgaSA9IHNlZ21lbnRzLmxlbmd0aDtcblxuICAgIHdoaWxlIChpLS0pIHtcbiAgICAgIC8vIGRlY29kZSBmb3J3YXJkIHNsYXNoZXMgdG8gcHJldmVudCB0aGVtIGZyb20gYmVpbmcgZG91YmxlIGVuY29kZWRcbiAgICAgIHNlZ21lbnRzW2ldID0gZW5jb2RlVXJpU2VnbWVudChzZWdtZW50c1tpXS5yZXBsYWNlKC8lMkYvZywgJy8nKSk7XG4gICAgfVxuXG4gICAgcGF0aCA9IHNlZ21lbnRzLmpvaW4oJy8nKTtcbiAgICByZXR1cm4gX3N0cmlwSW5kZXhIdG1sKChwYXRoICYmIHBhdGhbMF0gIT09ICcvJyAmJiAnLycgfHwgJycpICsgcGF0aCk7XG4gIH1cblxuICAvLyBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvODY0YzdmMC9zcmMvbmcvbG9jYXRpb24uanMjTDQyXG4gIGVuY29kZVNlYXJjaChzZWFyY2g6IHN0cmluZ3x7W2s6IHN0cmluZ106IHVua25vd259KTogc3RyaW5nIHtcbiAgICBpZiAodHlwZW9mIHNlYXJjaCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHNlYXJjaCA9IHBhcnNlS2V5VmFsdWUoc2VhcmNoKTtcbiAgICB9XG5cbiAgICBzZWFyY2ggPSB0b0tleVZhbHVlKHNlYXJjaCk7XG4gICAgcmV0dXJuIHNlYXJjaCA/ICc/JyArIHNlYXJjaCA6ICcnO1xuICB9XG5cbiAgLy8gaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci5qcy9ibG9iLzg2NGM3ZjAvc3JjL25nL2xvY2F0aW9uLmpzI0w0NFxuICBlbmNvZGVIYXNoKGhhc2g6IHN0cmluZykge1xuICAgIGhhc2ggPSBlbmNvZGVVcmlTZWdtZW50KGhhc2gpO1xuICAgIHJldHVybiBoYXNoID8gJyMnICsgaGFzaCA6ICcnO1xuICB9XG5cbiAgLy8gaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci5qcy9ibG9iLzg2NGM3ZjAvc3JjL25nL2xvY2F0aW9uLmpzI0wyN1xuICBkZWNvZGVQYXRoKHBhdGg6IHN0cmluZywgaHRtbDVNb2RlID0gdHJ1ZSk6IHN0cmluZyB7XG4gICAgY29uc3Qgc2VnbWVudHMgPSBwYXRoLnNwbGl0KCcvJyk7XG4gICAgbGV0IGkgPSBzZWdtZW50cy5sZW5ndGg7XG5cbiAgICB3aGlsZSAoaS0tKSB7XG4gICAgICBzZWdtZW50c1tpXSA9IGRlY29kZVVSSUNvbXBvbmVudChzZWdtZW50c1tpXSk7XG4gICAgICBpZiAoaHRtbDVNb2RlKSB7XG4gICAgICAgIC8vIGVuY29kZSBmb3J3YXJkIHNsYXNoZXMgdG8gcHJldmVudCB0aGVtIGZyb20gYmVpbmcgbWlzdGFrZW4gZm9yIHBhdGggc2VwYXJhdG9yc1xuICAgICAgICBzZWdtZW50c1tpXSA9IHNlZ21lbnRzW2ldLnJlcGxhY2UoL1xcLy9nLCAnJTJGJyk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgcmV0dXJuIHNlZ21lbnRzLmpvaW4oJy8nKTtcbiAgfVxuXG4gIC8vIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIuanMvYmxvYi84NjRjN2YwL3NyYy9uZy9sb2NhdGlvbi5qcyNMNzJcbiAgZGVjb2RlU2VhcmNoKHNlYXJjaDogc3RyaW5nKSB7IHJldHVybiBwYXJzZUtleVZhbHVlKHNlYXJjaCk7IH1cblxuICAvLyBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvODY0YzdmMC9zcmMvbmcvbG9jYXRpb24uanMjTDczXG4gIGRlY29kZUhhc2goaGFzaDogc3RyaW5nKSB7XG4gICAgaGFzaCA9IGRlY29kZVVSSUNvbXBvbmVudChoYXNoKTtcbiAgICByZXR1cm4gaGFzaFswXSA9PT0gJyMnID8gaGFzaC5zdWJzdHJpbmcoMSkgOiBoYXNoO1xuICB9XG5cbiAgLy8gaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci5qcy9ibG9iLzg2NGM3ZjAvc3JjL25nL2xvY2F0aW9uLmpzI0wxNDlcbiAgLy8gaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci5qcy9ibG9iLzg2NGM3ZjAvc3JjL25nL2xvY2F0aW9uLmpzI0w0MlxuICBub3JtYWxpemUoaHJlZjogc3RyaW5nKTogc3RyaW5nO1xuICBub3JtYWxpemUocGF0aDogc3RyaW5nLCBzZWFyY2g6IHtbazogc3RyaW5nXTogdW5rbm93bn0sIGhhc2g6IHN0cmluZywgYmFzZVVybD86IHN0cmluZyk6IHN0cmluZztcbiAgbm9ybWFsaXplKHBhdGhPckhyZWY6IHN0cmluZywgc2VhcmNoPzoge1trOiBzdHJpbmddOiB1bmtub3dufSwgaGFzaD86IHN0cmluZywgYmFzZVVybD86IHN0cmluZyk6XG4gICAgICBzdHJpbmcge1xuICAgIGlmIChhcmd1bWVudHMubGVuZ3RoID09PSAxKSB7XG4gICAgICBjb25zdCBwYXJzZWQgPSB0aGlzLnBhcnNlKHBhdGhPckhyZWYsIGJhc2VVcmwpO1xuXG4gICAgICBpZiAodHlwZW9mIHBhcnNlZCA9PT0gJ3N0cmluZycpIHtcbiAgICAgICAgcmV0dXJuIHBhcnNlZDtcbiAgICAgIH1cblxuICAgICAgY29uc3Qgc2VydmVyVXJsID1cbiAgICAgICAgICBgJHtwYXJzZWQucHJvdG9jb2x9Oi8vJHtwYXJzZWQuaG9zdG5hbWV9JHtwYXJzZWQucG9ydCA/ICc6JyArIHBhcnNlZC5wb3J0IDogJyd9YDtcblxuICAgICAgcmV0dXJuIHRoaXMubm9ybWFsaXplKFxuICAgICAgICAgIHRoaXMuZGVjb2RlUGF0aChwYXJzZWQucGF0aG5hbWUpLCB0aGlzLmRlY29kZVNlYXJjaChwYXJzZWQuc2VhcmNoKSxcbiAgICAgICAgICB0aGlzLmRlY29kZUhhc2gocGFyc2VkLmhhc2gpLCBzZXJ2ZXJVcmwpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBlbmNQYXRoID0gdGhpcy5lbmNvZGVQYXRoKHBhdGhPckhyZWYpO1xuICAgICAgY29uc3QgZW5jU2VhcmNoID0gc2VhcmNoICYmIHRoaXMuZW5jb2RlU2VhcmNoKHNlYXJjaCkgfHwgJyc7XG4gICAgICBjb25zdCBlbmNIYXNoID0gaGFzaCAmJiB0aGlzLmVuY29kZUhhc2goaGFzaCkgfHwgJyc7XG5cbiAgICAgIGxldCBqb2luZWRQYXRoID0gKGJhc2VVcmwgfHwgJycpICsgZW5jUGF0aDtcblxuICAgICAgaWYgKCFqb2luZWRQYXRoLmxlbmd0aCB8fCBqb2luZWRQYXRoWzBdICE9PSAnLycpIHtcbiAgICAgICAgam9pbmVkUGF0aCA9ICcvJyArIGpvaW5lZFBhdGg7XG4gICAgICB9XG4gICAgICByZXR1cm4gam9pbmVkUGF0aCArIGVuY1NlYXJjaCArIGVuY0hhc2g7XG4gICAgfVxuICB9XG5cbiAgYXJlRXF1YWwodmFsQTogc3RyaW5nLCB2YWxCOiBzdHJpbmcpIHsgcmV0dXJuIHRoaXMubm9ybWFsaXplKHZhbEEpID09PSB0aGlzLm5vcm1hbGl6ZSh2YWxCKTsgfVxuXG4gIC8vIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIuanMvYmxvYi84NjRjN2YwL3NyYy9uZy91cmxVdGlscy5qcyNMNjBcbiAgcGFyc2UodXJsOiBzdHJpbmcsIGJhc2U/OiBzdHJpbmcpIHtcbiAgICB0cnkge1xuICAgICAgLy8gU2FmYXJpIDEyIHRocm93cyBhbiBlcnJvciB3aGVuIHRoZSBVUkwgY29uc3RydWN0b3IgaXMgY2FsbGVkIHdpdGggYW4gdW5kZWZpbmVkIGJhc2UuXG4gICAgICBjb25zdCBwYXJzZWQgPSAhYmFzZSA/IG5ldyBVUkwodXJsKSA6IG5ldyBVUkwodXJsLCBiYXNlKTtcbiAgICAgIHJldHVybiB7XG4gICAgICAgIGhyZWY6IHBhcnNlZC5ocmVmLFxuICAgICAgICBwcm90b2NvbDogcGFyc2VkLnByb3RvY29sID8gcGFyc2VkLnByb3RvY29sLnJlcGxhY2UoLzokLywgJycpIDogJycsXG4gICAgICAgIGhvc3Q6IHBhcnNlZC5ob3N0LFxuICAgICAgICBzZWFyY2g6IHBhcnNlZC5zZWFyY2ggPyBwYXJzZWQuc2VhcmNoLnJlcGxhY2UoL15cXD8vLCAnJykgOiAnJyxcbiAgICAgICAgaGFzaDogcGFyc2VkLmhhc2ggPyBwYXJzZWQuaGFzaC5yZXBsYWNlKC9eIy8sICcnKSA6ICcnLFxuICAgICAgICBob3N0bmFtZTogcGFyc2VkLmhvc3RuYW1lLFxuICAgICAgICBwb3J0OiBwYXJzZWQucG9ydCxcbiAgICAgICAgcGF0aG5hbWU6IChwYXJzZWQucGF0aG5hbWUuY2hhckF0KDApID09PSAnLycpID8gcGFyc2VkLnBhdGhuYW1lIDogJy8nICsgcGFyc2VkLnBhdGhuYW1lXG4gICAgICB9O1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgSW52YWxpZCBVUkwgKCR7dXJsfSkgd2l0aCBiYXNlICgke2Jhc2V9KWApO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBfc3RyaXBJbmRleEh0bWwodXJsOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gdXJsLnJlcGxhY2UoL1xcL2luZGV4Lmh0bWwkLywgJycpO1xufVxuXG4vKipcbiAqIFRyaWVzIHRvIGRlY29kZSB0aGUgVVJJIGNvbXBvbmVudCB3aXRob3V0IHRocm93aW5nIGFuIGV4Y2VwdGlvbi5cbiAqXG4gKiBAcHJpdmF0ZVxuICogQHBhcmFtIHN0ciB2YWx1ZSBwb3RlbnRpYWwgVVJJIGNvbXBvbmVudCB0byBjaGVjay5cbiAqIEByZXR1cm5zIHtib29sZWFufSBUcnVlIGlmIGB2YWx1ZWAgY2FuIGJlIGRlY29kZWRcbiAqIHdpdGggdGhlIGRlY29kZVVSSUNvbXBvbmVudCBmdW5jdGlvbi5cbiAqL1xuZnVuY3Rpb24gdHJ5RGVjb2RlVVJJQ29tcG9uZW50KHZhbHVlOiBzdHJpbmcpIHtcbiAgdHJ5IHtcbiAgICByZXR1cm4gZGVjb2RlVVJJQ29tcG9uZW50KHZhbHVlKTtcbiAgfSBjYXRjaCAoZSkge1xuICAgIC8vIElnbm9yZSBhbnkgaW52YWxpZCB1cmkgY29tcG9uZW50LlxuICAgIHJldHVybiB1bmRlZmluZWQ7XG4gIH1cbn1cblxuXG4vKipcbiAqIFBhcnNlcyBhbiBlc2NhcGVkIHVybCBxdWVyeSBzdHJpbmcgaW50byBrZXktdmFsdWUgcGFpcnMuIExvZ2ljIHRha2VuIGZyb21cbiAqIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIuanMvYmxvYi84NjRjN2YwL3NyYy9Bbmd1bGFyLmpzI0wxMzgyXG4gKiBAcmV0dXJucyB7T2JqZWN0LjxzdHJpbmcsYm9vbGVhbnxBcnJheT59XG4gKi9cbmZ1bmN0aW9uIHBhcnNlS2V5VmFsdWUoa2V5VmFsdWU6IHN0cmluZyk6IHtbazogc3RyaW5nXTogdW5rbm93bn0ge1xuICBjb25zdCBvYmo6IHtbazogc3RyaW5nXTogdW5rbm93bn0gPSB7fTtcbiAgKGtleVZhbHVlIHx8ICcnKS5zcGxpdCgnJicpLmZvckVhY2goKGtleVZhbHVlKSA9PiB7XG4gICAgbGV0IHNwbGl0UG9pbnQsIGtleSwgdmFsO1xuICAgIGlmIChrZXlWYWx1ZSkge1xuICAgICAga2V5ID0ga2V5VmFsdWUgPSBrZXlWYWx1ZS5yZXBsYWNlKC9cXCsvZywgJyUyMCcpO1xuICAgICAgc3BsaXRQb2ludCA9IGtleVZhbHVlLmluZGV4T2YoJz0nKTtcbiAgICAgIGlmIChzcGxpdFBvaW50ICE9PSAtMSkge1xuICAgICAgICBrZXkgPSBrZXlWYWx1ZS5zdWJzdHJpbmcoMCwgc3BsaXRQb2ludCk7XG4gICAgICAgIHZhbCA9IGtleVZhbHVlLnN1YnN0cmluZyhzcGxpdFBvaW50ICsgMSk7XG4gICAgICB9XG4gICAgICBrZXkgPSB0cnlEZWNvZGVVUklDb21wb25lbnQoa2V5KTtcbiAgICAgIGlmICh0eXBlb2Yga2V5ICE9PSAndW5kZWZpbmVkJykge1xuICAgICAgICB2YWwgPSB0eXBlb2YgdmFsICE9PSAndW5kZWZpbmVkJyA/IHRyeURlY29kZVVSSUNvbXBvbmVudCh2YWwpIDogdHJ1ZTtcbiAgICAgICAgaWYgKCFvYmouaGFzT3duUHJvcGVydHkoa2V5KSkge1xuICAgICAgICAgIG9ialtrZXldID0gdmFsO1xuICAgICAgICB9IGVsc2UgaWYgKEFycmF5LmlzQXJyYXkob2JqW2tleV0pKSB7XG4gICAgICAgICAgKG9ialtrZXldIGFzIHVua25vd25bXSkucHVzaCh2YWwpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG9ialtrZXldID0gW29ialtrZXldLCB2YWxdO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIG9iajtcbn1cblxuLyoqXG4gKiBTZXJpYWxpemVzIGludG8ga2V5LXZhbHVlIHBhaXJzLiBMb2dpYyB0YWtlbiBmcm9tXG4gKiBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvODY0YzdmMC9zcmMvQW5ndWxhci5qcyNMMTQwOVxuICovXG5mdW5jdGlvbiB0b0tleVZhbHVlKG9iajoge1trOiBzdHJpbmddOiB1bmtub3dufSkge1xuICBjb25zdCBwYXJ0czogdW5rbm93bltdID0gW107XG4gIGZvciAoY29uc3Qga2V5IGluIG9iaikge1xuICAgIGxldCB2YWx1ZSA9IG9ialtrZXldO1xuICAgIGlmIChBcnJheS5pc0FycmF5KHZhbHVlKSkge1xuICAgICAgdmFsdWUuZm9yRWFjaCgoYXJyYXlWYWx1ZSkgPT4ge1xuICAgICAgICBwYXJ0cy5wdXNoKFxuICAgICAgICAgICAgZW5jb2RlVXJpUXVlcnkoa2V5LCB0cnVlKSArXG4gICAgICAgICAgICAoYXJyYXlWYWx1ZSA9PT0gdHJ1ZSA/ICcnIDogJz0nICsgZW5jb2RlVXJpUXVlcnkoYXJyYXlWYWx1ZSwgdHJ1ZSkpKTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICBwYXJ0cy5wdXNoKFxuICAgICAgICAgIGVuY29kZVVyaVF1ZXJ5KGtleSwgdHJ1ZSkgK1xuICAgICAgICAgICh2YWx1ZSA9PT0gdHJ1ZSA/ICcnIDogJz0nICsgZW5jb2RlVXJpUXVlcnkodmFsdWUgYXMgYW55LCB0cnVlKSkpO1xuICAgIH1cbiAgfVxuICByZXR1cm4gcGFydHMubGVuZ3RoID8gcGFydHMuam9pbignJicpIDogJyc7XG59XG5cblxuLyoqXG4gKiBXZSBuZWVkIG91ciBjdXN0b20gbWV0aG9kIGJlY2F1c2UgZW5jb2RlVVJJQ29tcG9uZW50IGlzIHRvbyBhZ2dyZXNzaXZlIGFuZCBkb2Vzbid0IGZvbGxvd1xuICogaHR0cDovL3d3dy5pZXRmLm9yZy9yZmMvcmZjMzk4Ni50eHQgd2l0aCByZWdhcmRzIHRvIHRoZSBjaGFyYWN0ZXIgc2V0IChwY2hhcikgYWxsb3dlZCBpbiBwYXRoXG4gKiBzZWdtZW50czpcbiAqICAgIHNlZ21lbnQgICAgICAgPSAqcGNoYXJcbiAqICAgIHBjaGFyICAgICAgICAgPSB1bnJlc2VydmVkIC8gcGN0LWVuY29kZWQgLyBzdWItZGVsaW1zIC8gXCI6XCIgLyBcIkBcIlxuICogICAgcGN0LWVuY29kZWQgICA9IFwiJVwiIEhFWERJRyBIRVhESUdcbiAqICAgIHVucmVzZXJ2ZWQgICAgPSBBTFBIQSAvIERJR0lUIC8gXCItXCIgLyBcIi5cIiAvIFwiX1wiIC8gXCJ+XCJcbiAqICAgIHN1Yi1kZWxpbXMgICAgPSBcIiFcIiAvIFwiJFwiIC8gXCImXCIgLyBcIidcIiAvIFwiKFwiIC8gXCIpXCJcbiAqICAgICAgICAgICAgICAgICAgICAgLyBcIipcIiAvIFwiK1wiIC8gXCIsXCIgLyBcIjtcIiAvIFwiPVwiXG4gKlxuICogTG9naWMgZnJvbSBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvODY0YzdmMC9zcmMvQW5ndWxhci5qcyNMMTQzN1xuICovXG5mdW5jdGlvbiBlbmNvZGVVcmlTZWdtZW50KHZhbDogc3RyaW5nKSB7XG4gIHJldHVybiBlbmNvZGVVcmlRdWVyeSh2YWwsIHRydWUpXG4gICAgICAucmVwbGFjZSgvJTI2L2dpLCAnJicpXG4gICAgICAucmVwbGFjZSgvJTNEL2dpLCAnPScpXG4gICAgICAucmVwbGFjZSgvJTJCL2dpLCAnKycpO1xufVxuXG5cbi8qKlxuICogVGhpcyBtZXRob2QgaXMgaW50ZW5kZWQgZm9yIGVuY29kaW5nICprZXkqIG9yICp2YWx1ZSogcGFydHMgb2YgcXVlcnkgY29tcG9uZW50LiBXZSBuZWVkIGEgY3VzdG9tXG4gKiBtZXRob2QgYmVjYXVzZSBlbmNvZGVVUklDb21wb25lbnQgaXMgdG9vIGFnZ3Jlc3NpdmUgYW5kIGVuY29kZXMgc3R1ZmYgdGhhdCBkb2Vzbid0IGhhdmUgdG8gYmVcbiAqIGVuY29kZWQgcGVyIGh0dHA6Ly90b29scy5pZXRmLm9yZy9odG1sL3JmYzM5ODY6XG4gKiAgICBxdWVyeSAgICAgICAgID0gKiggcGNoYXIgLyBcIi9cIiAvIFwiP1wiIClcbiAqICAgIHBjaGFyICAgICAgICAgPSB1bnJlc2VydmVkIC8gcGN0LWVuY29kZWQgLyBzdWItZGVsaW1zIC8gXCI6XCIgLyBcIkBcIlxuICogICAgdW5yZXNlcnZlZCAgICA9IEFMUEhBIC8gRElHSVQgLyBcIi1cIiAvIFwiLlwiIC8gXCJfXCIgLyBcIn5cIlxuICogICAgcGN0LWVuY29kZWQgICA9IFwiJVwiIEhFWERJRyBIRVhESUdcbiAqICAgIHN1Yi1kZWxpbXMgICAgPSBcIiFcIiAvIFwiJFwiIC8gXCImXCIgLyBcIidcIiAvIFwiKFwiIC8gXCIpXCJcbiAqICAgICAgICAgICAgICAgICAgICAgLyBcIipcIiAvIFwiK1wiIC8gXCIsXCIgLyBcIjtcIiAvIFwiPVwiXG4gKlxuICogTG9naWMgZnJvbSBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvODY0YzdmMC9zcmMvQW5ndWxhci5qcyNMMTQ1NlxuICovXG5mdW5jdGlvbiBlbmNvZGVVcmlRdWVyeSh2YWw6IHN0cmluZywgcGN0RW5jb2RlU3BhY2VzOiBib29sZWFuID0gZmFsc2UpIHtcbiAgcmV0dXJuIGVuY29kZVVSSUNvbXBvbmVudCh2YWwpXG4gICAgICAucmVwbGFjZSgvJTQwL2dpLCAnQCcpXG4gICAgICAucmVwbGFjZSgvJTNBL2dpLCAnOicpXG4gICAgICAucmVwbGFjZSgvJTI0L2csICckJylcbiAgICAgIC5yZXBsYWNlKC8lMkMvZ2ksICcsJylcbiAgICAgIC5yZXBsYWNlKC8lM0IvZ2ksICc7JylcbiAgICAgIC5yZXBsYWNlKC8lMjAvZywgKHBjdEVuY29kZVNwYWNlcyA/ICclMjAnIDogJysnKSk7XG59XG4iXX0=