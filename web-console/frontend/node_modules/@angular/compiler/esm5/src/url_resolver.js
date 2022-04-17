/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * Create a {@link UrlResolver} with no package prefix.
 */
export function createUrlResolverWithoutPackagePrefix() {
    return new UrlResolver();
}
export function createOfflineCompileUrlResolver() {
    return new UrlResolver('.');
}
export var UrlResolver = /** @class */ (function () {
    function UrlResolverImpl(_packagePrefix) {
        if (_packagePrefix === void 0) { _packagePrefix = null; }
        this._packagePrefix = _packagePrefix;
    }
    /**
     * Resolves the `url` given the `baseUrl`:
     * - when the `url` is null, the `baseUrl` is returned,
     * - if `url` is relative ('path/to/here', './path/to/here'), the resolved url is a combination of
     * `baseUrl` and `url`,
     * - if `url` is absolute (it has a scheme: 'http://', 'https://' or start with '/'), the `url` is
     * returned as is (ignoring the `baseUrl`)
     */
    UrlResolverImpl.prototype.resolve = function (baseUrl, url) {
        var resolvedUrl = url;
        if (baseUrl != null && baseUrl.length > 0) {
            resolvedUrl = _resolveUrl(baseUrl, resolvedUrl);
        }
        var resolvedParts = _split(resolvedUrl);
        var prefix = this._packagePrefix;
        if (prefix != null && resolvedParts != null &&
            resolvedParts[_ComponentIndex.Scheme] == 'package') {
            var path = resolvedParts[_ComponentIndex.Path];
            prefix = prefix.replace(/\/+$/, '');
            path = path.replace(/^\/+/, '');
            return prefix + "/" + path;
        }
        return resolvedUrl;
    };
    return UrlResolverImpl;
}());
/**
 * Extract the scheme of a URL.
 */
export function getUrlScheme(url) {
    var match = _split(url);
    return (match && match[_ComponentIndex.Scheme]) || '';
}
// The code below is adapted from Traceur:
// https://github.com/google/traceur-compiler/blob/9511c1dafa972bf0de1202a8a863bad02f0f95a8/src/runtime/url.js
/**
 * Builds a URI string from already-encoded parts.
 *
 * No encoding is performed.  Any component may be omitted as either null or
 * undefined.
 *
 * @param opt_scheme The scheme such as 'http'.
 * @param opt_userInfo The user name before the '@'.
 * @param opt_domain The domain such as 'www.google.com', already
 *     URI-encoded.
 * @param opt_port The port number.
 * @param opt_path The path, already URI-encoded.  If it is not
 *     empty, it must begin with a slash.
 * @param opt_queryData The URI-encoded query data.
 * @param opt_fragment The URI-encoded fragment identifier.
 * @return The fully combined URI.
 */
function _buildFromEncodedParts(opt_scheme, opt_userInfo, opt_domain, opt_port, opt_path, opt_queryData, opt_fragment) {
    var out = [];
    if (opt_scheme != null) {
        out.push(opt_scheme + ':');
    }
    if (opt_domain != null) {
        out.push('//');
        if (opt_userInfo != null) {
            out.push(opt_userInfo + '@');
        }
        out.push(opt_domain);
        if (opt_port != null) {
            out.push(':' + opt_port);
        }
    }
    if (opt_path != null) {
        out.push(opt_path);
    }
    if (opt_queryData != null) {
        out.push('?' + opt_queryData);
    }
    if (opt_fragment != null) {
        out.push('#' + opt_fragment);
    }
    return out.join('');
}
/**
 * A regular expression for breaking a URI into its component parts.
 *
 * {@link http://www.gbiv.com/protocols/uri/rfc/rfc3986.html#RFC2234} says
 * As the "first-match-wins" algorithm is identical to the "greedy"
 * disambiguation method used by POSIX regular expressions, it is natural and
 * commonplace to use a regular expression for parsing the potential five
 * components of a URI reference.
 *
 * The following line is the regular expression for breaking-down a
 * well-formed URI reference into its components.
 *
 * <pre>
 * ^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?
 *  12            3  4          5       6  7        8 9
 * </pre>
 *
 * The numbers in the second line above are only to assist readability; they
 * indicate the reference points for each subexpression (i.e., each paired
 * parenthesis). We refer to the value matched for subexpression <n> as $<n>.
 * For example, matching the above expression to
 * <pre>
 *     http://www.ics.uci.edu/pub/ietf/uri/#Related
 * </pre>
 * results in the following subexpression matches:
 * <pre>
 *    $1 = http:
 *    $2 = http
 *    $3 = //www.ics.uci.edu
 *    $4 = www.ics.uci.edu
 *    $5 = /pub/ietf/uri/
 *    $6 = <undefined>
 *    $7 = <undefined>
 *    $8 = #Related
 *    $9 = Related
 * </pre>
 * where <undefined> indicates that the component is not present, as is the
 * case for the query component in the above example. Therefore, we can
 * determine the value of the five components as
 * <pre>
 *    scheme    = $2
 *    authority = $4
 *    path      = $5
 *    query     = $7
 *    fragment  = $9
 * </pre>
 *
 * The regular expression has been modified slightly to expose the
 * userInfo, domain, and port separately from the authority.
 * The modified version yields
 * <pre>
 *    $1 = http              scheme
 *    $2 = <undefined>       userInfo -\
 *    $3 = www.ics.uci.edu   domain     | authority
 *    $4 = <undefined>       port     -/
 *    $5 = /pub/ietf/uri/    path
 *    $6 = <undefined>       query without ?
 *    $7 = Related           fragment without #
 * </pre>
 * @internal
 */
var _splitRe = new RegExp('^' +
    '(?:' +
    '([^:/?#.]+)' + // scheme - ignore special characters
    // used by other URL parts such as :,
    // ?, /, #, and .
    ':)?' +
    '(?://' +
    '(?:([^/?#]*)@)?' + // userInfo
    '([\\w\\d\\-\\u0100-\\uffff.%]*)' + // domain - restrict to letters,
    // digits, dashes, dots, percent
    // escapes, and unicode characters.
    '(?::([0-9]+))?' + // port
    ')?' +
    '([^?#]+)?' + // path
    '(?:\\?([^#]*))?' + // query
    '(?:#(.*))?' + // fragment
    '$');
/**
 * The index of each URI component in the return value of goog.uri.utils.split.
 * @enum {number}
 */
var _ComponentIndex;
(function (_ComponentIndex) {
    _ComponentIndex[_ComponentIndex["Scheme"] = 1] = "Scheme";
    _ComponentIndex[_ComponentIndex["UserInfo"] = 2] = "UserInfo";
    _ComponentIndex[_ComponentIndex["Domain"] = 3] = "Domain";
    _ComponentIndex[_ComponentIndex["Port"] = 4] = "Port";
    _ComponentIndex[_ComponentIndex["Path"] = 5] = "Path";
    _ComponentIndex[_ComponentIndex["QueryData"] = 6] = "QueryData";
    _ComponentIndex[_ComponentIndex["Fragment"] = 7] = "Fragment";
})(_ComponentIndex || (_ComponentIndex = {}));
/**
 * Splits a URI into its component parts.
 *
 * Each component can be accessed via the component indices; for example:
 * <pre>
 * goog.uri.utils.split(someStr)[goog.uri.utils.CompontentIndex.QUERY_DATA];
 * </pre>
 *
 * @param uri The URI string to examine.
 * @return Each component still URI-encoded.
 *     Each component that is present will contain the encoded value, whereas
 *     components that are not present will be undefined or empty, depending
 *     on the browser's regular expression implementation.  Never null, since
 *     arbitrary strings may still look like path names.
 */
function _split(uri) {
    return uri.match(_splitRe);
}
/**
  * Removes dot segments in given path component, as described in
  * RFC 3986, section 5.2.4.
  *
  * @param path A non-empty path component.
  * @return Path component with removed dot segments.
  */
function _removeDotSegments(path) {
    if (path == '/')
        return '/';
    var leadingSlash = path[0] == '/' ? '/' : '';
    var trailingSlash = path[path.length - 1] === '/' ? '/' : '';
    var segments = path.split('/');
    var out = [];
    var up = 0;
    for (var pos = 0; pos < segments.length; pos++) {
        var segment = segments[pos];
        switch (segment) {
            case '':
            case '.':
                break;
            case '..':
                if (out.length > 0) {
                    out.pop();
                }
                else {
                    up++;
                }
                break;
            default:
                out.push(segment);
        }
    }
    if (leadingSlash == '') {
        while (up-- > 0) {
            out.unshift('..');
        }
        if (out.length === 0)
            out.push('.');
    }
    return leadingSlash + out.join('/') + trailingSlash;
}
/**
 * Takes an array of the parts from split and canonicalizes the path part
 * and then joins all the parts.
 */
function _joinAndCanonicalizePath(parts) {
    var path = parts[_ComponentIndex.Path];
    path = path == null ? '' : _removeDotSegments(path);
    parts[_ComponentIndex.Path] = path;
    return _buildFromEncodedParts(parts[_ComponentIndex.Scheme], parts[_ComponentIndex.UserInfo], parts[_ComponentIndex.Domain], parts[_ComponentIndex.Port], path, parts[_ComponentIndex.QueryData], parts[_ComponentIndex.Fragment]);
}
/**
 * Resolves a URL.
 * @param base The URL acting as the base URL.
 * @param to The URL to resolve.
 */
function _resolveUrl(base, url) {
    var parts = _split(encodeURI(url));
    var baseParts = _split(base);
    if (parts[_ComponentIndex.Scheme] != null) {
        return _joinAndCanonicalizePath(parts);
    }
    else {
        parts[_ComponentIndex.Scheme] = baseParts[_ComponentIndex.Scheme];
    }
    for (var i = _ComponentIndex.Scheme; i <= _ComponentIndex.Port; i++) {
        if (parts[i] == null) {
            parts[i] = baseParts[i];
        }
    }
    if (parts[_ComponentIndex.Path][0] == '/') {
        return _joinAndCanonicalizePath(parts);
    }
    var path = baseParts[_ComponentIndex.Path];
    if (path == null)
        path = '/';
    var index = path.lastIndexOf('/');
    path = path.substring(0, index + 1) + parts[_ComponentIndex.Path];
    parts[_ComponentIndex.Path] = path;
    return _joinAndCanonicalizePath(parts);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXJsX3Jlc29sdmVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3VybF9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSDs7R0FFRztBQUNILE1BQU0sVUFBVSxxQ0FBcUM7SUFDbkQsT0FBTyxJQUFJLFdBQVcsRUFBRSxDQUFDO0FBQzNCLENBQUM7QUFFRCxNQUFNLFVBQVUsK0JBQStCO0lBQzdDLE9BQU8sSUFBSSxXQUFXLENBQUMsR0FBRyxDQUFDLENBQUM7QUFDOUIsQ0FBQztBQXNCRCxNQUFNLENBQUMsSUFBTSxXQUFXO0lBQ3RCLHlCQUFvQixjQUFrQztRQUFsQywrQkFBQSxFQUFBLHFCQUFrQztRQUFsQyxtQkFBYyxHQUFkLGNBQWMsQ0FBb0I7SUFBRyxDQUFDO0lBRTFEOzs7Ozs7O09BT0c7SUFDSCxpQ0FBTyxHQUFQLFVBQVEsT0FBZSxFQUFFLEdBQVc7UUFDbEMsSUFBSSxXQUFXLEdBQUcsR0FBRyxDQUFDO1FBQ3RCLElBQUksT0FBTyxJQUFJLElBQUksSUFBSSxPQUFPLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtZQUN6QyxXQUFXLEdBQUcsV0FBVyxDQUFDLE9BQU8sRUFBRSxXQUFXLENBQUMsQ0FBQztTQUNqRDtRQUNELElBQU0sYUFBYSxHQUFHLE1BQU0sQ0FBQyxXQUFXLENBQUMsQ0FBQztRQUMxQyxJQUFJLE1BQU0sR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDO1FBQ2pDLElBQUksTUFBTSxJQUFJLElBQUksSUFBSSxhQUFhLElBQUksSUFBSTtZQUN2QyxhQUFhLENBQUMsZUFBZSxDQUFDLE1BQU0sQ0FBQyxJQUFJLFNBQVMsRUFBRTtZQUN0RCxJQUFJLElBQUksR0FBRyxhQUFhLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxDQUFDO1lBQy9DLE1BQU0sR0FBRyxNQUFNLENBQUMsT0FBTyxD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QUMsQ0FBQztBQUN6QyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIENyZWF0ZSBhIHtAbGluayBVcmxSZXNvbHZlcn0gd2l0aCBubyBwYWNrYWdlIHByZWZpeC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVVybFJlc29sdmVyV2l0aG91dFBhY2thZ2VQcmVmaXgoKTogVXJsUmVzb2x2ZXIge1xuICByZXR1cm4gbmV3IFVybFJlc29sdmVyKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVPZmZsaW5lQ29tcGlsZVVybFJlc29sdmVyKCk6IFVybFJlc29sdmVyIHtcbiAgcmV0dXJuIG5ldyBVcmxSZXNvbHZlcignLicpO1xufVxuXG4vKipcbiAqIFVzZWQgYnkgdGhlIHtAbGluayBDb21waWxlcn0gd2hlbiByZXNvbHZpbmcgSFRNTCBhbmQgQ1NTIHRlbXBsYXRlIFVSTHMuXG4gKlxuICogVGhpcyBjbGFzcyBjYW4gYmUgb3ZlcnJpZGRlbiBieSB0aGUgYXBwbGljYXRpb24gZGV2ZWxvcGVyIHRvIGNyZWF0ZSBjdXN0b20gYmVoYXZpb3IuXG4gKlxuICogU2VlIHtAbGluayBDb21waWxlcn1cbiAqXG4gKiAjIyBFeGFtcGxlXG4gKlxuICoge0BleGFtcGxlIGNvbXBpbGVyL3RzL3VybF9yZXNvbHZlci91cmxfcmVzb2x2ZXIudHMgcmVnaW9uPSd1cmxfcmVzb2x2ZXInfVxuICpcbiAqIEBzZWN1cml0eSAgV2hlbiBjb21waWxpbmcgdGVtcGxhdGVzIGF0IHJ1bnRpbWUsIHlvdSBtdXN0XG4gKiBlbnN1cmUgdGhhdCB0aGUgZW50aXJlIHRlbXBsYXRlIGNvbWVzIGZyb20gYSB0cnVzdGVkIHNvdXJjZS5cbiAqIEF0dGFja2VyLWNvbnRyb2xsZWQgZGF0YSBpbnRyb2R1Y2VkIGJ5IGEgdGVtcGxhdGUgY291bGQgZXhwb3NlIHlvdXJcbiAqIGFwcGxpY2F0aW9uIHRvIFhTUyByaXNrcy4gRm9yIG1vcmUgZGV0YWlsLCBzZWUgdGhlIFtTZWN1cml0eSBHdWlkZV0oaHR0cDovL2cuY28vbmcvc2VjdXJpdHkpLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFVybFJlc29sdmVyIHsgcmVzb2x2ZShiYXNlVXJsOiBzdHJpbmcsIHVybDogc3RyaW5nKTogc3RyaW5nOyB9XG5cbmV4cG9ydCBpbnRlcmZhY2UgVXJsUmVzb2x2ZXJDdG9yIHsgbmV3IChwYWNrYWdlUHJlZml4Pzogc3RyaW5nfG51bGwpOiBVcmxSZXNvbHZlcjsgfVxuXG5leHBvcnQgY29uc3QgVXJsUmVzb2x2ZXI6IFVybFJlc29sdmVyQ3RvciA9IGNsYXNzIFVybFJlc29sdmVySW1wbCB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3BhY2thZ2VQcmVmaXg6IHN0cmluZ3xudWxsID0gbnVsbCkge31cblxuICAvKipcbiAgICogUmVzb2x2ZXMgdGhlIGB1cmxgIGdpdmVuIHRoZSBgYmFzZVVybGA6XG4gICAqIC0gd2hlbiB0aGUgYHVybGAgaXMgbnVsbCwgdGhlIGBiYXNlVXJsYCBpcyByZXR1cm5lZCxcbiAgICogLSBpZiBgdXJsYCBpcyByZWxhdGl2ZSAoJ3BhdGgvdG8vaGVyZScsICcuL3BhdGgvdG8vaGVyZScpLCB0aGUgcmVzb2x2ZWQgdXJsIGlzIGEgY29tYmluYXRpb24gb2ZcbiAgICogYGJhc2VVcmxgIGFuZCBgdXJsYCxcbiAgICogLSBpZiBgdXJsYCBpcyBhYnNvbHV0ZSAoaXQgaGFzIGEgc2NoZW1lOiAnaHR0cDovLycsICdodHRwczovLycgb3Igc3RhcnQgd2l0aCAnLycpLCB0aGUgYHVybGAgaXNcbiAgICogcmV0dXJuZWQgYXMgaXMgKGlnbm9yaW5nIHRoZSBgYmFzZVVybGApXG4gICAqL1xuICByZXNvbHZlKGJhc2VVcmw6IHN0cmluZywgdXJsOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGxldCByZXNvbHZlZFVybCA9IHVybDtcbiAgICBpZiAoYmFzZVVybCAhPSBudWxsICYmIGJhc2VVcmwubGVuZ3RoID4gMCkge1xuICAgICAgcmVzb2x2ZWRVcmwgPSBfcmVzb2x2ZVVybChiYXNlVXJsLCByZXNvbHZlZFVybCk7XG4gICAgfVxuICAgIGNvbnN0IHJlc29sdmVkUGFydHMgPSBfc3BsaXQocmVzb2x2ZWRVcmwpO1xuICAgIGxldCBwcmVmaXggPSB0aGlzLl9wYWNrYWdlUHJlZml4O1xuICAgIGlmIChwcmVmaXggIT0gbnVsbCAmJiByZXNvbHZlZFBhcnRzICE9IG51bGwgJiZcbiAgICAgICAgcmVzb2x2ZWRQYXJ0c1tfQ29tcG9uZW50SW5kZXguU2NoZW1lXSA9PSAncGFja2FnZScpIHtcbiAgICAgIGxldCBwYXRoID0gcmVzb2x2ZWRQYXJ0c1tfQ29tcG9uZW50SW5kZXguUGF0aF07XG4gICAgICBwcmVmaXggPSBwcmVmaXgucmVwbGFjZSgvXFwvKyQvLCAnJyk7XG4gICAgICBwYXRoID0gcGF0aC5yZXBsYWNlKC9eXFwvKy8sICcnKTtcbiAgICAgIHJldHVybiBgJHtwcmVmaXh9LyR7cGF0aH1gO1xuICAgIH1cbiAgICByZXR1cm4gcmVzb2x2ZWRVcmw7XG4gIH1cbn07XG5cbi8qKlxuICogRXh0cmFjdCB0aGUgc2NoZW1lIG9mIGEgVVJMLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0VXJsU2NoZW1lKHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgY29uc3QgbWF0Y2ggPSBfc3BsaXQodXJsKTtcbiAgcmV0dXJuIChtYXRjaCAmJiBtYXRjaFtfQ29tcG9uZW50SW5kZXguU2NoZW1lXSkgfHwgJyc7XG59XG5cbi8vIFRoZSBjb2RlIGJlbG93IGlzIGFkYXB0ZWQgZnJvbSBUcmFjZXVyOlxuLy8gaHR0cHM6Ly9naXRodWIuY29tL2dvb2dsZS90cmFjZXVyLWNvbXBpbGVyL2Jsb2IvOTUxMWMxZGFmYTk3MmJmMGRlMTIwMmE4YTg2M2JhZDAyZjBmOTVhOC9zcmMvcnVudGltZS91cmwuanNcblxuLyoqXG4gKiBCdWlsZHMgYSBVUkkgc3RyaW5nIGZyb20gYWxyZWFkeS1lbmNvZGVkIHBhcnRzLlxuICpcbiAqIE5vIGVuY29kaW5nIGlzIHBlcmZvcm1lZC4gIEFueSBjb21wb25lbnQgbWF5IGJlIG9taXR0ZWQgYXMgZWl0aGVyIG51bGwgb3JcbiAqIHVuZGVmaW5lZC5cbiAqXG4gKiBAcGFyYW0gb3B0X3NjaGVtZSBUaGUgc2NoZW1lIHN1Y2ggYXMgJ2h0dHAnLlxuICogQHBhcmFtIG9wdF91c2VySW5mbyBUaGUgdXNlciBuYW1lIGJlZm9yZSB0aGUgJ0AnLlxuICogQHBhcmFtIG9wdF9kb21haW4gVGhlIGRvbWFpbiBzdWNoIGFzICd3d3cuZ29vZ2xlLmNvbScsIGFscmVhZHlcbiAqICAgICBVUkktZW5jb2RlZC5cbiAqIEBwYXJhbSBvcHRfcG9ydCBUaGUgcG9ydCBudW1iZXIuXG4gKiBAcGFyYW0gb3B0X3BhdGggVGhlIHBhdGgsIGFscmVhZHkgVVJJLWVuY29kZWQuICBJZiBpdCBpcyBub3RcbiAqICAgICBlbXB0eSwgaXQgbXVzdCBiZWdpbiB3aXRoIGEgc2xhc2guXG4gKiBAcGFyYW0gb3B0X3F1ZXJ5RGF0YSBUaGUgVVJJLWVuY29kZWQgcXVlcnkgZGF0YS5cbiAqIEBwYXJhbSBvcHRfZnJhZ21lbnQgVGhlIFVSSS1lbmNvZGVkIGZyYWdtZW50IGlkZW50aWZpZXIuXG4gKiBAcmV0dXJuIFRoZSBmdWxseSBjb21iaW5lZCBVUkkuXG4gKi9cbmZ1bmN0aW9uIF9idWlsZEZyb21FbmNvZGVkUGFydHMoXG4gICAgb3B0X3NjaGVtZT86IHN0cmluZywgb3B0X3VzZXJJbmZvPzogc3RyaW5nLCBvcHRfZG9tYWluPzogc3RyaW5nLCBvcHRfcG9ydD86IHN0cmluZyxcbiAgICBvcHRfcGF0aD86IHN0cmluZywgb3B0X3F1ZXJ5RGF0YT86IHN0cmluZywgb3B0X2ZyYWdtZW50Pzogc3RyaW5nKTogc3RyaW5nIHtcbiAgY29uc3Qgb3V0OiBzdHJpbmdbXSA9IFtdO1xuXG4gIGlmIChvcHRfc2NoZW1lICE9IG51bGwpIHtcbiAgICBvdXQucHVzaChvcHRfc2NoZW1lICsgJzonKTtcbiAgfVxuXG4gIGlmIChvcHRfZG9tYWluICE9IG51bGwpIHtcbiAgICBvdXQucHVzaCgnLy8nKTtcblxuICAgIGlmIChvcHRfdXNlckluZm8gIT0gbnVsbCkge1xuICAgICAgb3V0LnB1c2gob3B0X3VzZXJJbmZvICsgJ0AnKTtcbiAgICB9XG5cbiAgICBvdXQucHVzaChvcHRfZG9tYWluKTtcblxuICAgIGlmIChvcHRfcG9ydCAhPSBudWxsKSB7XG4gICAgICBvdXQucHVzaCgnOicgKyBvcHRfcG9ydCk7XG4gICAgfVxuICB9XG5cbiAgaWYgKG9wdF9wYXRoICE9IG51bGwpIHtcbiAgICBvdXQucHVzaChvcHRfcGF0aCk7XG4gIH1cblxuICBpZiAob3B0X3F1ZXJ5RGF0YSAhPSBudWxsKSB7XG4gICAgb3V0LnB1c2goJz8nICsgb3B0X3F1ZXJ5RGF0YSk7XG4gIH1cblxuICBpZiAob3B0X2ZyYWdtZW50ICE9IG51bGwpIHtcbiAgICBvdXQucHVzaCgnIycgKyBvcHRfZnJhZ21lbnQpO1xuICB9XG5cbiAgcmV0dXJuIG91dC5qb2luKCcnKTtcbn1cblxuLyoqXG4gKiBBIHJlZ3VsYXIgZXhwcmVzc2lvbiBmb3IgYnJlYWtpbmcgYSBVUkkgaW50byBpdHMgY29tcG9uZW50IHBhcnRzLlxuICpcbiAqIHtAbGluayBodHRwOi8vd3d3LmdiaXYuY29tL3Byb3RvY29scy91cmkvcmZjL3JmYzM5ODYuaHRtbCNSRkMyMjM0fSBzYXlzXG4gKiBBcyB0aGUgXCJmaXJzdC1tYXRjaC13aW5zXCIgYWxnb3JpdGhtIGlzIGlkZW50aWNhbCB0byB0aGUgXCJncmVlZHlcIlxuICogZGlzYW1iaWd1YXRpb24gbWV0aG9kIHVzZWQgYnkgUE9TSVggcmVndWxhciBleHByZXNzaW9ucywgaXQgaXMgbmF0dXJhbCBhbmRcbiAqIGNvbW1vbnBsYWNlIHRvIHVzZSBhIHJlZ3VsYXIgZXhwcmVzc2lvbiBmb3IgcGFyc2luZyB0aGUgcG90ZW50aWFsIGZpdmVcbiAqIGNvbXBvbmVudHMgb2YgYSBVUkkgcmVmZXJlbmNlLlxuICpcbiAqIFRoZSBmb2xsb3dpbmcgbGluZSBpcyB0aGUgcmVndWxhciBleHByZXNzaW9uIGZvciBicmVha2luZy1kb3duIGFcbiAqIHdlbGwtZm9ybWVkIFVSSSByZWZlcmVuY2UgaW50byBpdHMgY29tcG9uZW50cy5cbiAqXG4gKiA8cHJlPlxuICogXigoW146Lz8jXSspOik/KC8vKFteLz8jXSopKT8oW14/I10qKShcXD8oW14jXSopKT8oIyguKikpP1xuICogIDEyICAgICAgICAgICAgMyAgNCAgICAgICAgICA1ICAgICAgIDYgIDcgICAgICAgIDggOVxuICogPC9wcmU+XG4gKlxuICogVGhlIG51bWJlcnMgaW4gdGhlIHNlY29uZCBsaW5lIGFib3ZlIGFyZSBvbmx5IHRvIGFzc2lzdCByZWFkYWJpbGl0eTsgdGhleVxuICogaW5kaWNhdGUgdGhlIHJlZmVyZW5jZSBwb2ludHMgZm9yIGVhY2ggc3ViZXhwcmVzc2lvbiAoaS5lLiwgZWFjaCBwYWlyZWRcbiAqIHBhcmVudGhlc2lzKS4gV2UgcmVmZXIgdG8gdGhlIHZhbHVlIG1hdGNoZWQgZm9yIHN1YmV4cHJlc3Npb24gPG4+IGFzICQ8bj4uXG4gKiBGb3IgZXhhbXBsZSwgbWF0Y2hpbmcgdGhlIGFib3ZlIGV4cHJlc3Npb24gdG9cbiAqIDxwcmU+XG4gKiAgICAgaHR0cDovL3d3dy5pY3MudWNpLmVkdS9wdWIvaWV0Zi91cmkvI1JlbGF0ZWRcbiAqIDwvcHJlPlxuICogcmVzdWx0cyBpbiB0aGUgZm9sbG93aW5nIHN1YmV4cHJlc3Npb24gbWF0Y2hlczpcbiAqIDxwcmU+XG4gKiAgICAkMSA9IGh0dHA6XG4gKiAgICAkMiA9IGh0dHBcbiAqICAgICQzID0gLy93d3cuaWNzLnVjaS5lZHVcbiAqICAgICQ0ID0gd3d3Lmljcy51Y2kuZWR1XG4gKiAgICAkNSA9IC9wdWIvaWV0Zi91cmkvXG4gKiAgICAkNiA9IDx1bmRlZmluZWQ+XG4gKiAgICAkNyA9IDx1bmRlZmluZWQ+XG4gKiAgICAkOCA9ICNSZWxhdGVkXG4gKiAgICAkOSA9IFJlbGF0ZWRcbiAqIDwvcHJlPlxuICogd2hlcmUgPHVuZGVmaW5lZD4gaW5kaWNhdGVzIHRoYXQgdGhlIGNvbXBvbmVudCBpcyBub3QgcHJlc2VudCwgYXMgaXMgdGhlXG4gKiBjYXNlIGZvciB0aGUgcXVlcnkgY29tcG9uZW50IGluIHRoZSBhYm92ZSBleGFtcGxlLiBUaGVyZWZvcmUsIHdlIGNhblxuICogZGV0ZXJtaW5lIHRoZSB2YWx1ZSBvZiB0aGUgZml2ZSBjb21wb25lbnRzIGFzXG4gKiA8cHJlPlxuICogICAgc2NoZW1lICAgID0gJDJcbiAqICAgIGF1dGhvcml0eSA9ICQ0XG4gKiAgICBwYXRoICAgICAgPSAkNVxuICogICAgcXVlcnkgICAgID0gJDdcbiAqICAgIGZyYWdtZW50ICA9ICQ5XG4gKiA8L3ByZT5cbiAqXG4gKiBUaGUgcmVndWxhciBleHByZXNzaW9uIGhhcyBiZWVuIG1vZGlmaWVkIHNsaWdodGx5IHRvIGV4cG9zZSB0aGVcbiAqIHVzZXJJbmZvLCBkb21haW4sIGFuZCBwb3J0IHNlcGFyYXRlbHkgZnJvbSB0aGUgYXV0aG9yaXR5LlxuICogVGhlIG1vZGlmaWVkIHZlcnNpb24geWllbGRzXG4gKiA8cHJlPlxuICogICAgJDEgPSBodHRwICAgICAgICAgICAgICBzY2hlbWVcbiAqICAgICQyID0gPHVuZGVmaW5lZD4gICAgICAgdXNlckluZm8gLVxcXG4gKiAgICAkMyA9IHd3dy5pY3MudWNpLmVkdSAgIGRvbWFpbiAgICAgfCBhdXRob3JpdHlcbiAqICAgICQ0ID0gPHVuZGVmaW5lZD4gICAgICAgcG9ydCAgICAgLS9cbiAqICAgICQ1ID0gL3B1Yi9pZXRmL3VyaS8gICAgcGF0aFxuICogICAgJDYgPSA8dW5kZWZpbmVkPiAgICAgICBxdWVyeSB3aXRob3V0ID9cbiAqICAgICQ3ID0gUmVsYXRlZCAgICAgICAgICAgZnJhZ21lbnQgd2l0aG91dCAjXG4gKiA8L3ByZT5cbiAqIEBpbnRlcm5hbFxuICovXG5jb25zdCBfc3BsaXRSZSA9IG5ldyBSZWdFeHAoXG4gICAgJ14nICtcbiAgICAnKD86JyArXG4gICAgJyhbXjovPyMuXSspJyArICAvLyBzY2hlbWUgLSBpZ25vcmUgc3BlY2lhbCBjaGFyYWN0ZXJzXG4gICAgICAgICAgICAgICAgICAgICAvLyB1c2VkIGJ5IG90aGVyIFVSTCBwYXJ0cyBzdWNoIGFzIDosXG4gICAgICAgICAgICAgICAgICAgICAvLyA/LCAvLCAjLCBhbmQgLlxuICAgICc6KT8nICtcbiAgICAnKD86Ly8nICtcbiAgICAnKD86KFteLz8jXSopQCk/JyArICAgICAgICAgICAgICAgICAgLy8gdXNlckluZm9cbiAgICAnKFtcXFxcd1xcXFxkXFxcXC1cXFxcdTAxMDAtXFxcXHVmZmZmLiVdKiknICsgIC8vIGRvbWFpbiAtIHJlc3RyaWN0IHRvIGxldHRlcnMsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIGRpZ2l0cywgZGFzaGVzLCBkb3RzLCBwZXJjZW50XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIGVzY2FwZXMsIGFuZCB1bmljb2RlIGNoYXJhY3RlcnMuXG4gICAgJyg/OjooWzAtOV0rKSk/JyArICAgICAgICAgICAgICAgICAgIC8vIHBvcnRcbiAgICAnKT8nICtcbiAgICAnKFtePyNdKyk/JyArICAgICAgICAvLyBwYXRoXG4gICAgJyg/OlxcXFw/KFteI10qKSk/JyArICAvLyBxdWVyeVxuICAgICcoPzojKC4qKSk/JyArICAgICAgIC8vIGZyYWdtZW50XG4gICAgJyQnKTtcblxuLyoqXG4gKiBUaGUgaW5kZXggb2YgZWFjaCBVUkkgY29tcG9uZW50IGluIHRoZSByZXR1cm4gdmFsdWUgb2YgZ29vZy51cmkudXRpbHMuc3BsaXQuXG4gKiBAZW51bSB7bnVtYmVyfVxuICovXG5lbnVtIF9Db21wb25lbnRJbmRleCB7XG4gIFNjaGVtZSA9IDEsXG4gIFVzZXJJbmZvLFxuICBEb21haW4sXG4gIFBvcnQsXG4gIFBhdGgsXG4gIFF1ZXJ5RGF0YSxcbiAgRnJhZ21lbnRcbn1cblxuLyoqXG4gKiBTcGxpdHMgYSBVUkkgaW50byBpdHMgY29tcG9uZW50IHBhcnRzLlxuICpcbiAqIEVhY2ggY29tcG9uZW50IGNhbiBiZSBhY2Nlc3NlZCB2aWEgdGhlIGNvbXBvbmVudCBpbmRpY2VzOyBmb3IgZXhhbXBsZTpcbiAqIDxwcmU+XG4gKiBnb29nLnVyaS51dGlscy5zcGxpdChzb21lU3RyKVtnb29nLnVyaS51dGlscy5Db21wb250ZW50SW5kZXguUVVFUllfREFUQV07XG4gKiA8L3ByZT5cbiAqXG4gKiBAcGFyYW0gdXJpIFRoZSBVUkkgc3RyaW5nIHRvIGV4YW1pbmUuXG4gKiBAcmV0dXJuIEVhY2ggY29tcG9uZW50IHN0aWxsIFVSSS1lbmNvZGVkLlxuICogICAgIEVhY2ggY29tcG9uZW50IHRoYXQgaXMgcHJlc2VudCB3aWxsIGNvbnRhaW4gdGhlIGVuY29kZWQgdmFsdWUsIHdoZXJlYXNcbiAqICAgICBjb21wb25lbnRzIHRoYXQgYXJlIG5vdCBwcmVzZW50IHdpbGwgYmUgdW5kZWZpbmVkIG9yIGVtcHR5LCBkZXBlbmRpbmdcbiAqICAgICBvbiB0aGUgYnJvd3NlcidzIHJlZ3VsYXIgZXhwcmVzc2lvbiBpbXBsZW1lbnRhdGlvbi4gIE5ldmVyIG51bGwsIHNpbmNlXG4gKiAgICAgYXJiaXRyYXJ5IHN0cmluZ3MgbWF5IHN0aWxsIGxvb2sgbGlrZSBwYXRoIG5hbWVzLlxuICovXG5mdW5jdGlvbiBfc3BsaXQodXJpOiBzdHJpbmcpOiBBcnJheTxzdHJpbmd8YW55PiB7XG4gIHJldHVybiB1cmkubWF0Y2goX3NwbGl0UmUpICE7XG59XG5cbi8qKlxuICAqIFJlbW92ZXMgZG90IHNlZ21lbnRzIGluIGdpdmVuIHBhdGggY29tcG9uZW50LCBhcyBkZXNjcmliZWQgaW5cbiAgKiBSRkMgMzk4Niwgc2VjdGlvbiA1LjIuNC5cbiAgKlxuICAqIEBwYXJhbSBwYXRoIEEgbm9uLWVtcHR5IHBhdGggY29tcG9uZW50LlxuICAqIEByZXR1cm4gUGF0aCBjb21wb25lbnQgd2l0aCByZW1vdmVkIGRvdCBzZWdtZW50cy5cbiAgKi9cbmZ1bmN0aW9uIF9yZW1vdmVEb3RTZWdtZW50cyhwYXRoOiBzdHJpbmcpOiBzdHJpbmcge1xuICBpZiAocGF0aCA9PSAnLycpIHJldHVybiAnLyc7XG5cbiAgY29uc3QgbGVhZGluZ1NsYXNoID0gcGF0aFswXSA9PSAnLycgPyAnLycgOiAnJztcbiAgY29uc3QgdHJhaWxpbmdTbGFzaCA9IHBhdGhbcGF0aC5sZW5ndGggLSAxXSA9PT0gJy8nID8gJy8nIDogJyc7XG4gIGNvbnN0IHNlZ21lbnRzID0gcGF0aC5zcGxpdCgnLycpO1xuXG4gIGNvbnN0IG91dDogc3RyaW5nW10gPSBbXTtcbiAgbGV0IHVwID0gMDtcbiAgZm9yIChsZXQgcG9zID0gMDsgcG9zIDwgc2VnbWVudHMubGVuZ3RoOyBwb3MrKykge1xuICAgIGNvbnN0IHNlZ21lbnQgPSBzZWdtZW50c1twb3NdO1xuICAgIHN3aXRjaCAoc2VnbWVudCkge1xuICAgICAgY2FzZSAnJzpcbiAgICAgIGNhc2UgJy4nOlxuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgJy4uJzpcbiAgICAgICAgaWYgKG91dC5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgb3V0LnBvcCgpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHVwKys7XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICBvdXQucHVzaChzZWdtZW50KTtcbiAgICB9XG4gIH1cblxuICBpZiAobGVhZGluZ1NsYXNoID09ICcnKSB7XG4gICAgd2hpbGUgKHVwLS0gPiAwKSB7XG4gICAgICBvdXQudW5zaGlmdCgnLi4nKTtcbiAgICB9XG5cbiAgICBpZiAob3V0Lmxlbmd0aCA9PT0gMCkgb3V0LnB1c2goJy4nKTtcbiAgfVxuXG4gIHJldHVybiBsZWFkaW5nU2xhc2ggKyBvdXQuam9pbignLycpICsgdHJhaWxpbmdTbGFzaDtcbn1cblxuLyoqXG4gKiBUYWtlcyBhbiBhcnJheSBvZiB0aGUgcGFydHMgZnJvbSBzcGxpdCBhbmQgY2Fub25pY2FsaXplcyB0aGUgcGF0aCBwYXJ0XG4gKiBhbmQgdGhlbiBqb2lucyBhbGwgdGhlIHBhcnRzLlxuICovXG5mdW5jdGlvbiBfam9pbkFuZENhbm9uaWNhbGl6ZVBhdGgocGFydHM6IGFueVtdKTogc3RyaW5nIHtcbiAgbGV0IHBhdGggPSBwYXJ0c1tfQ29tcG9uZW50SW5kZXguUGF0aF07XG4gIHBhdGggPSBwYXRoID09IG51bGwgPyAnJyA6IF9yZW1vdmVEb3RTZWdtZW50cyhwYXRoKTtcbiAgcGFydHNbX0NvbXBvbmVudEluZGV4LlBhdGhdID0gcGF0aDtcblxuICByZXR1cm4gX2J1aWxkRnJvbUVuY29kZWRQYXJ0cyhcbiAgICAgIHBhcnRzW19Db21wb25lbnRJbmRleC5TY2hlbWVdLCBwYXJ0c1tfQ29tcG9uZW50SW5kZXguVXNlckluZm9dLCBwYXJ0c1tfQ29tcG9uZW50SW5kZXguRG9tYWluXSxcbiAgICAgIHBhcnRzW19Db21wb25lbnRJbmRleC5Qb3J0XSwgcGF0aCwgcGFydHNbX0NvbXBvbmVudEluZGV4LlF1ZXJ5RGF0YV0sXG4gICAgICBwYXJ0c1tfQ29tcG9uZW50SW5kZXguRnJhZ21lbnRdKTtcbn1cblxuLyoqXG4gKiBSZXNvbHZlcyBhIFVSTC5cbiAqIEBwYXJhbSBiYXNlIFRoZSBVUkwgYWN0aW5nIGFzIHRoZSBiYXNlIFVSTC5cbiAqIEBwYXJhbSB0byBUaGUgVVJMIHRvIHJlc29sdmUuXG4gKi9cbmZ1bmN0aW9uIF9yZXNvbHZlVXJsKGJhc2U6IHN0cmluZywgdXJsOiBzdHJpbmcpOiBzdHJpbmcge1xuICBjb25zdCBwYXJ0cyA9IF9zcGxpdChlbmNvZGVVUkkodXJsKSk7XG4gIGNvbnN0IGJhc2VQYXJ0cyA9IF9zcGxpdChiYXNlKTtcblxuICBpZiAocGFydHNbX0NvbXBvbmVudEluZGV4LlNjaGVtZV0gIT0gbnVsbCkge1xuICAgIHJldHVybiBfam9pbkFuZENhbm9uaWNhbGl6ZVBhdGgocGFydHMpO1xuICB9IGVsc2Uge1xuICAgIHBhcnRzW19Db21wb25lbnRJbmRleC5TY2hlbWVdID0gYmFzZVBhcnRzW19Db21wb25lbnRJbmRleC5TY2hlbWVdO1xuICB9XG5cbiAgZm9yIChsZXQgaSA9IF9Db21wb25lbnRJbmRleC5TY2hlbWU7IGkgPD0gX0NvbXBvbmVudEluZGV4LlBvcnQ7IGkrKykge1xuICAgIGlmIChwYXJ0c1tpXSA9PSBudWxsKSB7XG4gICAgICBwYXJ0c1tpXSA9IGJhc2VQYXJ0c1tpXTtcbiAgICB9XG4gIH1cblxuICBpZiAocGFydHNbX0NvbXBvbmVudEluZGV4LlBhdGhdWzBdID09ICcvJykge1xuICAgIHJldHVybiBfam9pbkFuZENhbm9uaWNhbGl6ZVBhdGgocGFydHMpO1xuICB9XG5cbiAgbGV0IHBhdGggPSBiYXNlUGFydHNbX0NvbXBvbmVudEluZGV4LlBhdGhdO1xuICBpZiAocGF0aCA9PSBudWxsKSBwYXRoID0gJy8nO1xuICBjb25zdCBpbmRleCA9IHBhdGgubGFzdEluZGV4T2YoJy8nKTtcbiAgcGF0aCA9IHBhdGguc3Vic3RyaW5nKDAsIGluZGV4ICsgMSkgKyBwYXJ0c1tfQ29tcG9uZW50SW5kZXguUGF0aF07XG4gIHBhcnRzW19Db21wb25lbnRJbmRleC5QYXRoXSA9IHBhdGg7XG4gIHJldHVybiBfam9pbkFuZENhbm9uaWNhbGl6ZVBhdGgocGFydHMpO1xufVxuIl19