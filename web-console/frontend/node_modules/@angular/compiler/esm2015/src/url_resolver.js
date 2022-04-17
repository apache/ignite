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
export const UrlResolver = class UrlResolverImpl {
    constructor(_packagePrefix = null) {
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
    resolve(baseUrl, url) {
        let resolvedUrl = url;
        if (baseUrl != null && baseUrl.length > 0) {
            resolvedUrl = _resolveUrl(baseUrl, resolvedUrl);
        }
        const resolvedParts = _split(resolvedUrl);
        let prefix = this._packagePrefix;
        if (prefix != null && resolvedParts != null &&
            resolvedParts[_ComponentIndex.Scheme] == 'package') {
            let path = resolvedParts[_ComponentIndex.Path];
            prefix = prefix.replace(/\/+$/, '');
            path = path.replace(/^\/+/, '');
            return `${prefix}/${path}`;
        }
        return resolvedUrl;
    }
};
/**
 * Extract the scheme of a URL.
 */
export function getUrlScheme(url) {
    const match = _split(url);
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
    const out = [];
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
const _splitRe = new RegExp('^' +
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
    const leadingSlash = path[0] == '/' ? '/' : '';
    const trailingSlash = path[path.length - 1] === '/' ? '/' : '';
    const segments = path.split('/');
    const out = [];
    let up = 0;
    for (let pos = 0; pos < segments.length; pos++) {
        const segment = segments[pos];
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
    let path = parts[_ComponentIndex.Path];
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
    const parts = _split(encodeURI(url));
    const baseParts = _split(base);
    if (parts[_ComponentIndex.Scheme] != null) {
        return _joinAndCanonicalizePath(parts);
    }
    else {
        parts[_ComponentIndex.Scheme] = baseParts[_ComponentIndex.Scheme];
    }
    for (let i = _ComponentIndex.Scheme; i <= _ComponentIndex.Port; i++) {
        if (parts[i] == null) {
            parts[i] = baseParts[i];
        }
    }
    if (parts[_ComponentIndex.Path][0] == '/') {
        return _joinAndCanonicalizePath(parts);
    }
    let path = baseParts[_ComponentIndex.Path];
    if (path == null)
        path = '/';
    const index = path.lastIndexOf('/');
    path = path.substring(0, index + 1) + parts[_ComponentIndex.Path];
    parts[_ComponentIndex.Path] = path;
    return _joinAndCanonicalizePath(parts);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXJsX3Jlc29sdmVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3VybF9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSDs7R0FFRztBQUNILE1BQU0sVUFBVSxxQ0FBcUM7SUFDbkQsT0FBTyxJQUFJLFdBQVcsRUFBRSxDQUFDO0FBQzNCLENBQUM7QUFFRCxNQUFNLFVBQVUsK0JBQStCO0lBQzdDLE9BQU8sSUFBSSxXQUFXLENBQUMsR0FBRyxDQUFDLENBQUM7QUFDOUIsQ0FBQztBQXNCRCxNQUFNLENBQUMsTUFBTSxXQUFXLEdBQW9CLE1BQU0sZUFBZTtJQUMvRCxZQUFvQixpQkFBOEIsSUFBSTtRQUFsQyxtQkFBYyxHQUFkLGNBQWMsQ0FBb0I7SUFBRyxDQUFDO0lBRTFEOzs7Ozs7O09BT0c7SUFDSCxPQUFPLENBQUMsT0FBZSxFQUFFLEdBQVc7UUFDbEMsSUFBSSxXQUFXLEdBQUcsR0FBRyxDQUFDO1FBQ3RCLElBQUksT0FBTyxJQUFJLElBQUksSUFBSSxPQUFPLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtZQUN6QyxXQUFXLEdBQUcsV0FBVyxDQUFDLE9BQU8sRUFBRSxXQUFXLENBQUMsQ0FBQztTQUNqRDtRQUNELE1BQU0sYUFBYSxHQUFHLE1BQU0sQ0FBQyxXQUFXLENBQUMsQ0FBQztRQUMxQyxJQUFJLE1BQU0sR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDO1FBQ2pDLElBQUksTUFBTSxJQUFJLElBQUksSUFBSSxhQUFhLElBQUksSUFBSTtZQUN2QyxhQUFhLENBQUMsZUFBZSxDQUFDLE1BQU0sQ0FBQyxJQUFJLFNBQVMsRUFBRTtZQUN0RCxJQUFJLElBQUksR0FBRyxhQUFhLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxDQUFDO1lBQy9DLE1BQU0sR0FBRyxNQUFNLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxFQUFF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Ii8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBDcmVhdGUgYSB7QGxpbmsgVXJsUmVzb2x2ZXJ9IHdpdGggbm8gcGFja2FnZSBwcmVmaXguXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVVcmxSZXNvbHZlcldpdGhvdXRQYWNrYWdlUHJlZml4KCk6IFVybFJlc29sdmVyIHtcbiAgcmV0dXJuIG5ldyBVcmxSZXNvbHZlcigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlT2ZmbGluZUNvbXBpbGVVcmxSZXNvbHZlcigpOiBVcmxSZXNvbHZlciB7XG4gIHJldHVybiBuZXcgVXJsUmVzb2x2ZXIoJy4nKTtcbn1cblxuLyoqXG4gKiBVc2VkIGJ5IHRoZSB7QGxpbmsgQ29tcGlsZXJ9IHdoZW4gcmVzb2x2aW5nIEhUTUwgYW5kIENTUyB0ZW1wbGF0ZSBVUkxzLlxuICpcbiAqIFRoaXMgY2xhc3MgY2FuIGJlIG92ZXJyaWRkZW4gYnkgdGhlIGFwcGxpY2F0aW9uIGRldmVsb3BlciB0byBjcmVhdGUgY3VzdG9tIGJlaGF2aW9yLlxuICpcbiAqIFNlZSB7QGxpbmsgQ29tcGlsZXJ9XG4gKlxuICogIyMgRXhhbXBsZVxuICpcbiAqIHtAZXhhbXBsZSBjb21waWxlci90cy91cmxfcmVzb2x2ZXIvdXJsX3Jlc29sdmVyLnRzIHJlZ2lvbj0ndXJsX3Jlc29sdmVyJ31cbiAqXG4gKiBAc2VjdXJpdHkgIFdoZW4gY29tcGlsaW5nIHRlbXBsYXRlcyBhdCBydW50aW1lLCB5b3UgbXVzdFxuICogZW5zdXJlIHRoYXQgdGhlIGVudGlyZSB0ZW1wbGF0ZSBjb21lcyBmcm9tIGEgdHJ1c3RlZCBzb3VyY2UuXG4gKiBBdHRhY2tlci1jb250cm9sbGVkIGRhdGEgaW50cm9kdWNlZCBieSBhIHRlbXBsYXRlIGNvdWxkIGV4cG9zZSB5b3VyXG4gKiBhcHBsaWNhdGlvbiB0byBYU1Mgcmlza3MuIEZvciBtb3JlIGRldGFpbCwgc2VlIHRoZSBbU2VjdXJpdHkgR3VpZGVdKGh0dHA6Ly9nLmNvL25nL3NlY3VyaXR5KS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBVcmxSZXNvbHZlciB7IHJlc29sdmUoYmFzZVVybDogc3RyaW5nLCB1cmw6IHN0cmluZyk6IHN0cmluZzsgfVxuXG5leHBvcnQgaW50ZXJmYWNlIFVybFJlc29sdmVyQ3RvciB7IG5ldyAocGFja2FnZVByZWZpeD86IHN0cmluZ3xudWxsKTogVXJsUmVzb2x2ZXI7IH1cblxuZXhwb3J0IGNvbnN0IFVybFJlc29sdmVyOiBVcmxSZXNvbHZlckN0b3IgPSBjbGFzcyBVcmxSZXNvbHZlckltcGwge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9wYWNrYWdlUHJlZml4OiBzdHJpbmd8bnVsbCA9IG51bGwpIHt9XG5cbiAgLyoqXG4gICAqIFJlc29sdmVzIHRoZSBgdXJsYCBnaXZlbiB0aGUgYGJhc2VVcmxgOlxuICAgKiAtIHdoZW4gdGhlIGB1cmxgIGlzIG51bGwsIHRoZSBgYmFzZVVybGAgaXMgcmV0dXJuZWQsXG4gICAqIC0gaWYgYHVybGAgaXMgcmVsYXRpdmUgKCdwYXRoL3RvL2hlcmUnLCAnLi9wYXRoL3RvL2hlcmUnKSwgdGhlIHJlc29sdmVkIHVybCBpcyBhIGNvbWJpbmF0aW9uIG9mXG4gICAqIGBiYXNlVXJsYCBhbmQgYHVybGAsXG4gICAqIC0gaWYgYHVybGAgaXMgYWJzb2x1dGUgKGl0IGhhcyBhIHNjaGVtZTogJ2h0dHA6Ly8nLCAnaHR0cHM6Ly8nIG9yIHN0YXJ0IHdpdGggJy8nKSwgdGhlIGB1cmxgIGlzXG4gICAqIHJldHVybmVkIGFzIGlzIChpZ25vcmluZyB0aGUgYGJhc2VVcmxgKVxuICAgKi9cbiAgcmVzb2x2ZShiYXNlVXJsOiBzdHJpbmcsIHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgICBsZXQgcmVzb2x2ZWRVcmwgPSB1cmw7XG4gICAgaWYgKGJhc2VVcmwgIT0gbnVsbCAmJiBiYXNlVXJsLmxlbmd0aCA+IDApIHtcbiAgICAgIHJlc29sdmVkVXJsID0gX3Jlc29sdmVVcmwoYmFzZVVybCwgcmVzb2x2ZWRVcmwpO1xuICAgIH1cbiAgICBjb25zdCByZXNvbHZlZFBhcnRzID0gX3NwbGl0KHJlc29sdmVkVXJsKTtcbiAgICBsZXQgcHJlZml4ID0gdGhpcy5fcGFja2FnZVByZWZpeDtcbiAgICBpZiAocHJlZml4ICE9IG51bGwgJiYgcmVzb2x2ZWRQYXJ0cyAhPSBudWxsICYmXG4gICAgICAgIHJlc29sdmVkUGFydHNbX0NvbXBvbmVudEluZGV4LlNjaGVtZV0gPT0gJ3BhY2thZ2UnKSB7XG4gICAgICBsZXQgcGF0aCA9IHJlc29sdmVkUGFydHNbX0NvbXBvbmVudEluZGV4LlBhdGhdO1xuICAgICAgcHJlZml4ID0gcHJlZml4LnJlcGxhY2UoL1xcLyskLywgJycpO1xuICAgICAgcGF0aCA9IHBhdGgucmVwbGFjZSgvXlxcLysvLCAnJyk7XG4gICAgICByZXR1cm4gYCR7cHJlZml4fS8ke3BhdGh9YDtcbiAgICB9XG4gICAgcmV0dXJuIHJlc29sdmVkVXJsO1xuICB9XG59O1xuXG4vKipcbiAqIEV4dHJhY3QgdGhlIHNjaGVtZSBvZiBhIFVSTC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldFVybFNjaGVtZSh1cmw6IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IG1hdGNoID0gX3NwbGl0KHVybCk7XG4gIHJldHVybiAobWF0Y2ggJiYgbWF0Y2hbX0NvbXBvbmVudEluZGV4LlNjaGVtZV0pIHx8ICcnO1xufVxuXG4vLyBUaGUgY29kZSBiZWxvdyBpcyBhZGFwdGVkIGZyb20gVHJhY2V1cjpcbi8vIGh0dHBzOi8vZ2l0aHViLmNvbS9nb29nbGUvdHJhY2V1ci1jb21waWxlci9ibG9iLzk1MTFjMWRhZmE5NzJiZjBkZTEyMDJhOGE4NjNiYWQwMmYwZjk1YTgvc3JjL3J1bnRpbWUvdXJsLmpzXG5cbi8qKlxuICogQnVpbGRzIGEgVVJJIHN0cmluZyBmcm9tIGFscmVhZHktZW5jb2RlZCBwYXJ0cy5cbiAqXG4gKiBObyBlbmNvZGluZyBpcyBwZXJmb3JtZWQuICBBbnkgY29tcG9uZW50IG1heSBiZSBvbWl0dGVkIGFzIGVpdGhlciBudWxsIG9yXG4gKiB1bmRlZmluZWQuXG4gKlxuICogQHBhcmFtIG9wdF9zY2hlbWUgVGhlIHNjaGVtZSBzdWNoIGFzICdodHRwJy5cbiAqIEBwYXJhbSBvcHRfdXNlckluZm8gVGhlIHVzZXIgbmFtZSBiZWZvcmUgdGhlICdAJy5cbiAqIEBwYXJhbSBvcHRfZG9tYWluIFRoZSBkb21haW4gc3VjaCBhcyAnd3d3Lmdvb2dsZS5jb20nLCBhbHJlYWR5XG4gKiAgICAgVVJJLWVuY29kZWQuXG4gKiBAcGFyYW0gb3B0X3BvcnQgVGhlIHBvcnQgbnVtYmVyLlxuICogQHBhcmFtIG9wdF9wYXRoIFRoZSBwYXRoLCBhbHJlYWR5IFVSSS1lbmNvZGVkLiAgSWYgaXQgaXMgbm90XG4gKiAgICAgZW1wdHksIGl0IG11c3QgYmVnaW4gd2l0aCBhIHNsYXNoLlxuICogQHBhcmFtIG9wdF9xdWVyeURhdGEgVGhlIFVSSS1lbmNvZGVkIHF1ZXJ5IGRhdGEuXG4gKiBAcGFyYW0gb3B0X2ZyYWdtZW50IFRoZSBVUkktZW5jb2RlZCBmcmFnbWVudCBpZGVudGlmaWVyLlxuICogQHJldHVybiBUaGUgZnVsbHkgY29tYmluZWQgVVJJLlxuICovXG5mdW5jdGlvbiBfYnVpbGRGcm9tRW5jb2RlZFBhcnRzKFxuICAgIG9wdF9zY2hlbWU/OiBzdHJpbmcsIG9wdF91c2VySW5mbz86IHN0cmluZywgb3B0X2RvbWFpbj86IHN0cmluZywgb3B0X3BvcnQ/OiBzdHJpbmcsXG4gICAgb3B0X3BhdGg/OiBzdHJpbmcsIG9wdF9xdWVyeURhdGE/OiBzdHJpbmcsIG9wdF9mcmFnbWVudD86IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IG91dDogc3RyaW5nW10gPSBbXTtcblxuICBpZiAob3B0X3NjaGVtZSAhPSBudWxsKSB7XG4gICAgb3V0LnB1c2gob3B0X3NjaGVtZSArICc6Jyk7XG4gIH1cblxuICBpZiAob3B0X2RvbWFpbiAhPSBudWxsKSB7XG4gICAgb3V0LnB1c2goJy8vJyk7XG5cbiAgICBpZiAob3B0X3VzZXJJbmZvICE9IG51bGwpIHtcbiAgICAgIG91dC5wdXNoKG9wdF91c2VySW5mbyArICdAJyk7XG4gICAgfVxuXG4gICAgb3V0LnB1c2gob3B0X2RvbWFpbik7XG5cbiAgICBpZiAob3B0X3BvcnQgIT0gbnVsbCkge1xuICAgICAgb3V0LnB1c2goJzonICsgb3B0X3BvcnQpO1xuICAgIH1cbiAgfVxuXG4gIGlmIChvcHRfcGF0aCAhPSBudWxsKSB7XG4gICAgb3V0LnB1c2gob3B0X3BhdGgpO1xuICB9XG5cbiAgaWYgKG9wdF9xdWVyeURhdGEgIT0gbnVsbCkge1xuICAgIG91dC5wdXNoKCc/JyArIG9wdF9xdWVyeURhdGEpO1xuICB9XG5cbiAgaWYgKG9wdF9mcmFnbWVudCAhPSBudWxsKSB7XG4gICAgb3V0LnB1c2goJyMnICsgb3B0X2ZyYWdtZW50KTtcbiAgfVxuXG4gIHJldHVybiBvdXQuam9pbignJyk7XG59XG5cbi8qKlxuICogQSByZWd1bGFyIGV4cHJlc3Npb24gZm9yIGJyZWFraW5nIGEgVVJJIGludG8gaXRzIGNvbXBvbmVudCBwYXJ0cy5cbiAqXG4gKiB7QGxpbmsgaHR0cDovL3d3dy5nYml2LmNvbS9wcm90b2NvbHMvdXJpL3JmYy9yZmMzOTg2Lmh0bWwjUkZDMjIzNH0gc2F5c1xuICogQXMgdGhlIFwiZmlyc3QtbWF0Y2gtd2luc1wiIGFsZ29yaXRobSBpcyBpZGVudGljYWwgdG8gdGhlIFwiZ3JlZWR5XCJcbiAqIGRpc2FtYmlndWF0aW9uIG1ldGhvZCB1c2VkIGJ5IFBPU0lYIHJlZ3VsYXIgZXhwcmVzc2lvbnMsIGl0IGlzIG5hdHVyYWwgYW5kXG4gKiBjb21tb25wbGFjZSB0byB1c2UgYSByZWd1bGFyIGV4cHJlc3Npb24gZm9yIHBhcnNpbmcgdGhlIHBvdGVudGlhbCBmaXZlXG4gKiBjb21wb25lbnRzIG9mIGEgVVJJIHJlZmVyZW5jZS5cbiAqXG4gKiBUaGUgZm9sbG93aW5nIGxpbmUgaXMgdGhlIHJlZ3VsYXIgZXhwcmVzc2lvbiBmb3IgYnJlYWtpbmctZG93biBhXG4gKiB3ZWxsLWZvcm1lZCBVUkkgcmVmZXJlbmNlIGludG8gaXRzIGNvbXBvbmVudHMuXG4gKlxuICogPHByZT5cbiAqIF4oKFteOi8/I10rKTopPygvLyhbXi8/I10qKSk/KFtePyNdKikoXFw/KFteI10qKSk/KCMoLiopKT9cbiAqICAxMiAgICAgICAgICAgIDMgIDQgICAgICAgICAgNSAgICAgICA2ICA3ICAgICAgICA4IDlcbiAqIDwvcHJlPlxuICpcbiAqIFRoZSBudW1iZXJzIGluIHRoZSBzZWNvbmQgbGluZSBhYm92ZSBhcmUgb25seSB0byBhc3Npc3QgcmVhZGFiaWxpdHk7IHRoZXlcbiAqIGluZGljYXRlIHRoZSByZWZlcmVuY2UgcG9pbnRzIGZvciBlYWNoIHN1YmV4cHJlc3Npb24gKGkuZS4sIGVhY2ggcGFpcmVkXG4gKiBwYXJlbnRoZXNpcykuIFdlIHJlZmVyIHRvIHRoZSB2YWx1ZSBtYXRjaGVkIGZvciBzdWJleHByZXNzaW9uIDxuPiBhcyAkPG4+LlxuICogRm9yIGV4YW1wbGUsIG1hdGNoaW5nIHRoZSBhYm92ZSBleHByZXNzaW9uIHRvXG4gKiA8cHJlPlxuICogICAgIGh0dHA6Ly93d3cuaWNzLnVjaS5lZHUvcHViL2lldGYvdXJpLyNSZWxhdGVkXG4gKiA8L3ByZT5cbiAqIHJlc3VsdHMgaW4gdGhlIGZvbGxvd2luZyBzdWJleHByZXNzaW9uIG1hdGNoZXM6XG4gKiA8cHJlPlxuICogICAgJDEgPSBodHRwOlxuICogICAgJDIgPSBodHRwXG4gKiAgICAkMyA9IC8vd3d3Lmljcy51Y2kuZWR1XG4gKiAgICAkNCA9IHd3dy5pY3MudWNpLmVkdVxuICogICAgJDUgPSAvcHViL2lldGYvdXJpL1xuICogICAgJDYgPSA8dW5kZWZpbmVkPlxuICogICAgJDcgPSA8dW5kZWZpbmVkPlxuICogICAgJDggPSAjUmVsYXRlZFxuICogICAgJDkgPSBSZWxhdGVkXG4gKiA8L3ByZT5cbiAqIHdoZXJlIDx1bmRlZmluZWQ+IGluZGljYXRlcyB0aGF0IHRoZSBjb21wb25lbnQgaXMgbm90IHByZXNlbnQsIGFzIGlzIHRoZVxuICogY2FzZSBmb3IgdGhlIHF1ZXJ5IGNvbXBvbmVudCBpbiB0aGUgYWJvdmUgZXhhbXBsZS4gVGhlcmVmb3JlLCB3ZSBjYW5cbiAqIGRldGVybWluZSB0aGUgdmFsdWUgb2YgdGhlIGZpdmUgY29tcG9uZW50cyBhc1xuICogPHByZT5cbiAqICAgIHNjaGVtZSAgICA9ICQyXG4gKiAgICBhdXRob3JpdHkgPSAkNFxuICogICAgcGF0aCAgICAgID0gJDVcbiAqICAgIHF1ZXJ5ICAgICA9ICQ3XG4gKiAgICBmcmFnbWVudCAgPSAkOVxuICogPC9wcmU+XG4gKlxuICogVGhlIHJlZ3VsYXIgZXhwcmVzc2lvbiBoYXMgYmVlbiBtb2RpZmllZCBzbGlnaHRseSB0byBleHBvc2UgdGhlXG4gKiB1c2VySW5mbywgZG9tYWluLCBhbmQgcG9ydCBzZXBhcmF0ZWx5IGZyb20gdGhlIGF1dGhvcml0eS5cbiAqIFRoZSBtb2RpZmllZCB2ZXJzaW9uIHlpZWxkc1xuICogPHByZT5cbiAqICAgICQxID0gaHR0cCAgICAgICAgICAgICAgc2NoZW1lXG4gKiAgICAkMiA9IDx1bmRlZmluZWQ+ICAgICAgIHVzZXJJbmZvIC1cXFxuICogICAgJDMgPSB3d3cuaWNzLnVjaS5lZHUgICBkb21haW4gICAgIHwgYXV0aG9yaXR5XG4gKiAgICAkNCA9IDx1bmRlZmluZWQ+ICAgICAgIHBvcnQgICAgIC0vXG4gKiAgICAkNSA9IC9wdWIvaWV0Zi91cmkvICAgIHBhdGhcbiAqICAgICQ2ID0gPHVuZGVmaW5lZD4gICAgICAgcXVlcnkgd2l0aG91dCA/XG4gKiAgICAkNyA9IFJlbGF0ZWQgICAgICAgICAgIGZyYWdtZW50IHdpdGhvdXQgI1xuICogPC9wcmU+XG4gKiBAaW50ZXJuYWxcbiAqL1xuY29uc3QgX3NwbGl0UmUgPSBuZXcgUmVnRXhwKFxuICAgICdeJyArXG4gICAgJyg/OicgK1xuICAgICcoW146Lz8jLl0rKScgKyAgLy8gc2NoZW1lIC0gaWdub3JlIHNwZWNpYWwgY2hhcmFjdGVyc1xuICAgICAgICAgICAgICAgICAgICAgLy8gdXNlZCBieSBvdGhlciBVUkwgcGFydHMgc3VjaCBhcyA6LFxuICAgICAgICAgICAgICAgICAgICAgLy8gPywgLywgIywgYW5kIC5cbiAgICAnOik/JyArXG4gICAgJyg/Oi8vJyArXG4gICAgJyg/OihbXi8/I10qKUApPycgKyAgICAgICAgICAgICAgICAgIC8vIHVzZXJJbmZvXG4gICAgJyhbXFxcXHdcXFxcZFxcXFwtXFxcXHUwMTAwLVxcXFx1ZmZmZi4lXSopJyArICAvLyBkb21haW4gLSByZXN0cmljdCB0byBsZXR0ZXJzLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBkaWdpdHMsIGRhc2hlcywgZG90cywgcGVyY2VudFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBlc2NhcGVzLCBhbmQgdW5pY29kZSBjaGFyYWN0ZXJzLlxuICAgICcoPzo6KFswLTldKykpPycgKyAgICAgICAgICAgICAgICAgICAvLyBwb3J0XG4gICAgJyk/JyArXG4gICAgJyhbXj8jXSspPycgKyAgICAgICAgLy8gcGF0aFxuICAgICcoPzpcXFxcPyhbXiNdKikpPycgKyAgLy8gcXVlcnlcbiAgICAnKD86IyguKikpPycgKyAgICAgICAvLyBmcmFnbWVudFxuICAgICckJyk7XG5cbi8qKlxuICogVGhlIGluZGV4IG9mIGVhY2ggVVJJIGNvbXBvbmVudCBpbiB0aGUgcmV0dXJuIHZhbHVlIG9mIGdvb2cudXJpLnV0aWxzLnNwbGl0LlxuICogQGVudW0ge251bWJlcn1cbiAqL1xuZW51bSBfQ29tcG9uZW50SW5kZXgge1xuICBTY2hlbWUgPSAxLFxuICBVc2VySW5mbyxcbiAgRG9tYWluLFxuICBQb3J0LFxuICBQYXRoLFxuICBRdWVyeURhdGEsXG4gIEZyYWdtZW50XG59XG5cbi8qKlxuICogU3BsaXRzIGEgVVJJIGludG8gaXRzIGNvbXBvbmVudCBwYXJ0cy5cbiAqXG4gKiBFYWNoIGNvbXBvbmVudCBjYW4gYmUgYWNjZXNzZWQgdmlhIHRoZSBjb21wb25lbnQgaW5kaWNlczsgZm9yIGV4YW1wbGU6XG4gKiA8cHJlPlxuICogZ29vZy51cmkudXRpbHMuc3BsaXQoc29tZVN0cilbZ29vZy51cmkudXRpbHMuQ29tcG9udGVudEluZGV4LlFVRVJZX0RBVEFdO1xuICogPC9wcmU+XG4gKlxuICogQHBhcmFtIHVyaSBUaGUgVVJJIHN0cmluZyB0byBleGFtaW5lLlxuICogQHJldHVybiBFYWNoIGNvbXBvbmVudCBzdGlsbCBVUkktZW5jb2RlZC5cbiAqICAgICBFYWNoIGNvbXBvbmVudCB0aGF0IGlzIHByZXNlbnQgd2lsbCBjb250YWluIHRoZSBlbmNvZGVkIHZhbHVlLCB3aGVyZWFzXG4gKiAgICAgY29tcG9uZW50cyB0aGF0IGFyZSBub3QgcHJlc2VudCB3aWxsIGJlIHVuZGVmaW5lZCBvciBlbXB0eSwgZGVwZW5kaW5nXG4gKiAgICAgb24gdGhlIGJyb3dzZXIncyByZWd1bGFyIGV4cHJlc3Npb24gaW1wbGVtZW50YXRpb24uICBOZXZlciBudWxsLCBzaW5jZVxuICogICAgIGFyYml0cmFyeSBzdHJpbmdzIG1heSBzdGlsbCBsb29rIGxpa2UgcGF0aCBuYW1lcy5cbiAqL1xuZnVuY3Rpb24gX3NwbGl0KHVyaTogc3RyaW5nKTogQXJyYXk8c3RyaW5nfGFueT4ge1xuICByZXR1cm4gdXJpLm1hdGNoKF9zcGxpdFJlKSAhO1xufVxuXG4vKipcbiAgKiBSZW1vdmVzIGRvdCBzZWdtZW50cyBpbiBnaXZlbiBwYXRoIGNvbXBvbmVudCwgYXMgZGVzY3JpYmVkIGluXG4gICogUkZDIDM5ODYsIHNlY3Rpb24gNS4yLjQuXG4gICpcbiAgKiBAcGFyYW0gcGF0aCBBIG5vbi1lbXB0eSBwYXRoIGNvbXBvbmVudC5cbiAgKiBAcmV0dXJuIFBhdGggY29tcG9uZW50IHdpdGggcmVtb3ZlZCBkb3Qgc2VnbWVudHMuXG4gICovXG5mdW5jdGlvbiBfcmVtb3ZlRG90U2VnbWVudHMocGF0aDogc3RyaW5nKTogc3RyaW5nIHtcbiAgaWYgKHBhdGggPT0gJy8nKSByZXR1cm4gJy8nO1xuXG4gIGNvbnN0IGxlYWRpbmdTbGFzaCA9IHBhdGhbMF0gPT0gJy8nID8gJy8nIDogJyc7XG4gIGNvbnN0IHRyYWlsaW5nU2xhc2ggPSBwYXRoW3BhdGgubGVuZ3RoIC0gMV0gPT09ICcvJyA/ICcvJyA6ICcnO1xuICBjb25zdCBzZWdtZW50cyA9IHBhdGguc3BsaXQoJy8nKTtcblxuICBjb25zdCBvdXQ6IHN0cmluZ1tdID0gW107XG4gIGxldCB1cCA9IDA7XG4gIGZvciAobGV0IHBvcyA9IDA7IHBvcyA8IHNlZ21lbnRzLmxlbmd0aDsgcG9zKyspIHtcbiAgICBjb25zdCBzZWdtZW50ID0gc2VnbWVudHNbcG9zXTtcbiAgICBzd2l0Y2ggKHNlZ21lbnQpIHtcbiAgICAgIGNhc2UgJyc6XG4gICAgICBjYXNlICcuJzpcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICcuLic6XG4gICAgICAgIGlmIChvdXQubGVuZ3RoID4gMCkge1xuICAgICAgICAgIG91dC5wb3AoKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB1cCsrO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgb3V0LnB1c2goc2VnbWVudCk7XG4gICAgfVxuICB9XG5cbiAgaWYgKGxlYWRpbmdTbGFzaCA9PSAnJykge1xuICAgIHdoaWxlICh1cC0tID4gMCkge1xuICAgICAgb3V0LnVuc2hpZnQoJy4uJyk7XG4gICAgfVxuXG4gICAgaWYgKG91dC5sZW5ndGggPT09IDApIG91dC5wdXNoKCcuJyk7XG4gIH1cblxuICByZXR1cm4gbGVhZGluZ1NsYXNoICsgb3V0LmpvaW4oJy8nKSArIHRyYWlsaW5nU2xhc2g7XG59XG5cbi8qKlxuICogVGFrZXMgYW4gYXJyYXkgb2YgdGhlIHBhcnRzIGZyb20gc3BsaXQgYW5kIGNhbm9uaWNhbGl6ZXMgdGhlIHBhdGggcGFydFxuICogYW5kIHRoZW4gam9pbnMgYWxsIHRoZSBwYXJ0cy5cbiAqL1xuZnVuY3Rpb24gX2pvaW5BbmRDYW5vbmljYWxpemVQYXRoKHBhcnRzOiBhbnlbXSk6IHN0cmluZyB7XG4gIGxldCBwYXRoID0gcGFydHNbX0NvbXBvbmVudEluZGV4LlBhdGhdO1xuICBwYXRoID0gcGF0aCA9PSBudWxsID8gJycgOiBfcmVtb3ZlRG90U2VnbWVudHMocGF0aCk7XG4gIHBhcnRzW19Db21wb25lbnRJbmRleC5QYXRoXSA9IHBhdGg7XG5cbiAgcmV0dXJuIF9idWlsZEZyb21FbmNvZGVkUGFydHMoXG4gICAgICBwYXJ0c1tfQ29tcG9uZW50SW5kZXguU2NoZW1lXSwgcGFydHNbX0NvbXBvbmVudEluZGV4LlVzZXJJbmZvXSwgcGFydHNbX0NvbXBvbmVudEluZGV4LkRvbWFpbl0sXG4gICAgICBwYXJ0c1tfQ29tcG9uZW50SW5kZXguUG9ydF0sIHBhdGgsIHBhcnRzW19Db21wb25lbnRJbmRleC5RdWVyeURhdGFdLFxuICAgICAgcGFydHNbX0NvbXBvbmVudEluZGV4LkZyYWdtZW50XSk7XG59XG5cbi8qKlxuICogUmVzb2x2ZXMgYSBVUkwuXG4gKiBAcGFyYW0gYmFzZSBUaGUgVVJMIGFjdGluZyBhcyB0aGUgYmFzZSBVUkwuXG4gKiBAcGFyYW0gdG8gVGhlIFVSTCB0byByZXNvbHZlLlxuICovXG5mdW5jdGlvbiBfcmVzb2x2ZVVybChiYXNlOiBzdHJpbmcsIHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgY29uc3QgcGFydHMgPSBfc3BsaXQoZW5jb2RlVVJJKHVybCkpO1xuICBjb25zdCBiYXNlUGFydHMgPSBfc3BsaXQoYmFzZSk7XG5cbiAgaWYgKHBhcnRzW19Db21wb25lbnRJbmRleC5TY2hlbWVdICE9IG51bGwpIHtcbiAgICByZXR1cm4gX2pvaW5BbmRDYW5vbmljYWxpemVQYXRoKHBhcnRzKTtcbiAgfSBlbHNlIHtcbiAgICBwYXJ0c1tfQ29tcG9uZW50SW5kZXguU2NoZW1lXSA9IGJhc2VQYXJ0c1tfQ29tcG9uZW50SW5kZXguU2NoZW1lXTtcbiAgfVxuXG4gIGZvciAobGV0IGkgPSBfQ29tcG9uZW50SW5kZXguU2NoZW1lOyBpIDw9IF9Db21wb25lbnRJbmRleC5Qb3J0OyBpKyspIHtcbiAgICBpZiAocGFydHNbaV0gPT0gbnVsbCkge1xuICAgICAgcGFydHNbaV0gPSBiYXNlUGFydHNbaV07XG4gICAgfVxuICB9XG5cbiAgaWYgKHBhcnRzW19Db21wb25lbnRJbmRleC5QYXRoXVswXSA9PSAnLycpIHtcbiAgICByZXR1cm4gX2pvaW5BbmRDYW5vbmljYWxpemVQYXRoKHBhcnRzKTtcbiAgfVxuXG4gIGxldCBwYXRoID0gYmFzZVBhcnRzW19Db21wb25lbnRJbmRleC5QYXRoXTtcbiAgaWYgKHBhdGggPT0gbnVsbCkgcGF0aCA9ICcvJztcbiAgY29uc3QgaW5kZXggPSBwYXRoLmxhc3RJbmRleE9mKCcvJyk7XG4gIHBhdGggPSBwYXRoLnN1YnN0cmluZygwLCBpbmRleCArIDEpICsgcGFydHNbX0NvbXBvbmVudEluZGV4LlBhdGhdO1xuICBwYXJ0c1tfQ29tcG9uZW50SW5kZXguUGF0aF0gPSBwYXRoO1xuICByZXR1cm4gX2pvaW5BbmRDYW5vbmljYWxpemVQYXRoKHBhcnRzKTtcbn1cbiJdfQ==