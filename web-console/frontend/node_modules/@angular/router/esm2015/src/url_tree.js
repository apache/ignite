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
import { PRIMARY_OUTLET, convertToParamMap } from './shared';
import { forEach, shallowEqual } from './utils/collection';
/**
 * @return {?}
 */
export function createEmptyUrlTree() {
    return new UrlTree(new UrlSegmentGroup([], {}), {}, null);
}
/**
 * @param {?} container
 * @param {?} containee
 * @param {?} exact
 * @return {?}
 */
export function containsTree(container, containee, exact) {
    if (exact) {
        return equalQueryParams(container.queryParams, containee.queryParams) &&
            equalSegmentGroups(container.root, containee.root);
    }
    return containsQueryParams(container.queryParams, containee.queryParams) &&
        containsSegmentGroup(container.root, containee.root);
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function equalQueryParams(container, containee) {
    // TODO: This does not handle array params correctly.
    return shallowEqual(container, containee);
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function equalSegmentGroups(container, containee) {
    if (!equalPath(container.segments, containee.segments))
        return false;
    if (container.numberOfChildren !== containee.numberOfChildren)
        return false;
    for (const c in containee.children) {
        if (!container.children[c])
            return false;
        if (!equalSegmentGroups(container.children[c], containee.children[c]))
            return false;
    }
    return true;
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function containsQueryParams(container, containee) {
    // TODO: This does not handle array params correctly.
    return Object.keys(containee).length <= Object.keys(container).length &&
        Object.keys(containee).every((/**
         * @param {?} key
         * @return {?}
         */
        key => containee[key] === container[key]));
}
/**
 * @param {?} container
 * @param {?} containee
 * @return {?}
 */
function containsSegmentGroup(container, containee) {
    return containsSegmentGroupHelper(container, containee, containee.segments);
}
/**
 * @param {?} container
 * @param {?} containee
 * @param {?} containeePaths
 * @return {?}
 */
function containsSegmentGroupHelper(container, containee, containeePaths) {
    if (container.segments.length > containeePaths.length) {
        /** @type {?} */
        const current = container.segments.slice(0, containeePaths.length);
        if (!equalPath(current, containeePaths))
            return false;
        if (containee.hasChildren())
            return false;
        return true;
    }
    else if (container.segments.length === containeePaths.length) {
        if (!equalPath(container.segments, containeePaths))
            return false;
        for (const c in containee.children) {
            if (!container.children[c])
                return false;
            if (!containsSegmentGroup(container.children[c], containee.children[c]))
                return false;
        }
        return true;
    }
    else {
        /** @type {?} */
        const current = containeePaths.slice(0, container.segments.length);
        /** @type {?} */
        const next = containeePaths.slice(container.segments.length);
        if (!equalPath(container.segments, current))
            return false;
        if (!container.children[PRIMARY_OUTLET])
            return false;
        return containsSegmentGroupHelper(container.children[PRIMARY_OUTLET], containee, next);
    }
}
/**
 * \@description
 *
 * Represents the parsed URL.
 *
 * Since a router state is a tree, and the URL is nothing but a serialized state, the URL is a
 * serialized tree.
 * UrlTree is a data structure that provides a lot of affordances in dealing with URLs
 *
 * \@usageNotes
 * ### Example
 *
 * ```
 * \@Component({templateUrl:'template.html'})
 * class MyComponent {
 *   constructor(router: Router) {
 *     const tree: UrlTree =
 *       router.parseUrl('/team/33/(user/victor//support:help)?debug=true#fragment');
 *     const f = tree.fragment; // return 'fragment'
 *     const q = tree.queryParams; // returns {debug: 'true'}
 *     const g: UrlSegmentGroup = tree.root.children[PRIMARY_OUTLET];
 *     const s: UrlSegment[] = g.segments; // returns 2 segments 'team' and '33'
 *     g.children[PRIMARY_OUTLET].segments; // returns 2 segments 'user' and 'victor'
 *     g.children['support'].segments; // return 1 segment 'help'
 *   }
 * }
 * ```
 *
 * \@publicApi
 */
export class UrlTree {
    /**
     * \@internal
     * @param {?} root
     * @param {?} queryParams
     * @param {?} fragment
     */
    constructor(root, queryParams, fragment) {
        this.root = root;
        this.queryParams = queryParams;
        this.fragment = fragment;
    }
    /**
     * @return {?}
     */
    get queryParamMap() {
        if (!this._queryParamMap) {
            this._queryParamMap = convertToParamMap(this.queryParams);
        }
        return this._queryParamMap;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return DEFAULT_SERIALIZER.serialize(this); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    UrlTree.prototype._queryParamMap;
    /**
     * The root segment group of the URL tree
     * @type {?}
     */
    UrlTree.prototype.root;
    /**
     * The query params of the URL
     * @type {?}
     */
    UrlTree.prototype.queryParams;
    /**
     * The fragment of the URL
     * @type {?}
     */
    UrlTree.prototype.fragment;
}
/**
 * \@description
 *
 * Represents the parsed URL segment group.
 *
 * See `UrlTree` for more information.
 *
 * \@publicApi
 */
export class UrlSegmentGroup {
    /**
     * @param {?} segments
     * @param {?} children
     */
    constructor(segments, children) {
        this.segments = segments;
        this.children = children;
        /**
         * The parent node in the url tree
         */
        this.parent = null;
        forEach(children, (/**
         * @template THIS
         * @this {THIS}
         * @param {?} v
         * @param {?} k
         * @return {THIS}
         */
        (v, k) => v.parent = this));
    }
    /**
     * Whether the segment has child segments
     * @return {?}
     */
    hasChildren() { return this.numberOfChildren > 0; }
    /**
     * Number of child segments
     * @return {?}
     */
    get numberOfChildren() { return Object.keys(this.children).length; }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return serializePaths(this); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    UrlSegmentGroup.prototype._sourceSegment;
    /**
     * \@internal
     * @type {?}
     */
    UrlSegmentGroup.prototype._segmentIndexShift;
    /**
     * The parent node in the url tree
     * @type {?}
     */
    UrlSegmentGroup.prototype.parent;
    /**
     * The URL segments of this group. See `UrlSegment` for more information
     * @type {?}
     */
    UrlSegmentGroup.prototype.segments;
    /**
     * The list of children of this group
     * @type {?}
     */
    UrlSegmentGroup.prototype.children;
}
/**
 * \@description
 *
 * Represents a single URL segment.
 *
 * A UrlSegment is a part of a URL between the two slashes. It contains a path and the matrix
 * parameters associated with the segment.
 *
 * \@usageNotes
 *  ### Example
 *
 * ```
 * \@Component({templateUrl:'template.html'})
 * class MyComponent {
 *   constructor(router: Router) {
 *     const tree: UrlTree = router.parseUrl('/team;id=33');
 *     const g: UrlSegmentGroup = tree.root.children[PRIMARY_OUTLET];
 *     const s: UrlSegment[] = g.segments;
 *     s[0].path; // returns 'team'
 *     s[0].parameters; // returns {id: 33}
 *   }
 * }
 * ```
 *
 * \@publicApi
 */
export class UrlSegment {
    /**
     * @param {?} path
     * @param {?} parameters
     */
    constructor(path, parameters) {
        this.path = path;
        this.parameters = parameters;
    }
    /**
     * @return {?}
     */
    get parameterMap() {
        if (!this._parameterMap) {
            this._parameterMap = convertToParamMap(this.parameters);
        }
        return this._parameterMap;
    }
    /**
     * \@docsNotRequired
     * @return {?}
     */
    toString() { return serializePath(this); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    UrlSegment.prototype._parameterMap;
    /**
     * The path part of a URL segment
     * @type {?}
     */
    UrlSegment.prototype.path;
    /**
     * The matrix parameters associated with a segment
     * @type {?}
     */
    UrlSegment.prototype.parameters;
}
/**
 * @param {?} as
 * @param {?} bs
 * @return {?}
 */
export function equalSegments(as, bs) {
    return equalPath(as, bs) && as.every((/**
     * @param {?} a
     * @param {?} i
     * @return {?}
     */
    (a, i) => shallowEqual(a.parameters, bs[i].parameters)));
}
/**
 * @param {?} as
 * @param {?} bs
 * @return {?}
 */
export function equalPath(as, bs) {
    if (as.length !== bs.length)
        return false;
    return as.every((/**
     * @param {?} a
     * @param {?} i
     * @return {?}
     */
    (a, i) => a.path === bs[i].path));
}
/**
 * @template T
 * @param {?} segment
 * @param {?} fn
 * @return {?}
 */
export function mapChildrenIntoArray(segment, fn) {
    /** @type {?} */
    let res = [];
    forEach(segment.children, (/**
     * @param {?} child
     * @param {?} childOutlet
     * @return {?}
     */
    (child, childOutlet) => {
        if (childOutlet === PRIMARY_OUTLET) {
            res = res.concat(fn(child, childOutlet));
        }
    }));
    forEach(segment.children, (/**
     * @param {?} child
     * @param {?} childOutlet
     * @return {?}
     */
    (child, childOutlet) => {
        if (childOutlet !== PRIMARY_OUTLET) {
            res = res.concat(fn(child, childOutlet));
        }
    }));
    return res;
}
/**
 * \@description
 *
 * Serializes and deserializes a URL string into a URL tree.
 *
 * The url serialization strategy is customizable. You can
 * make all URLs case insensitive by providing a custom UrlSerializer.
 *
 * See `DefaultUrlSerializer` for an example of a URL serializer.
 *
 * \@publicApi
 * @abstract
 */
export class UrlSerializer {
}
if (false) {
    /**
     * Parse a url into a `UrlTree`
     * @abstract
     * @param {?} url
     * @return {?}
     */
    UrlSerializer.prototype.parse = function (url) { };
    /**
     * Converts a `UrlTree` into a url
     * @abstract
     * @param {?} tree
     * @return {?}
     */
    UrlSerializer.prototype.serialize = function (tree) { };
}
/**
 * \@description
 *
 * A default implementation of the `UrlSerializer`.
 *
 * Example URLs:
 *
 * ```
 * /inbox/33(popup:compose)
 * /inbox/33;open=true/messages/44
 * ```
 *
 * DefaultUrlSerializer uses parentheses to serialize secondary segments (e.g., popup:compose), the
 * colon syntax to specify the outlet, and the ';parameter=value' syntax (e.g., open=true) to
 * specify route specific parameters.
 *
 * \@publicApi
 */
export class DefaultUrlSerializer {
    /**
     * Parses a url into a `UrlTree`
     * @param {?} url
     * @return {?}
     */
    parse(url) {
        /** @type {?} */
        const p = new UrlParser(url);
        return new UrlTree(p.parseRootSegment(), p.parseQueryParams(), p.parseFragment());
    }
    /**
     * Converts a `UrlTree` into a url
     * @param {?} tree
     * @return {?}
     */
    serialize(tree) {
        /** @type {?} */
        const segment = `/${serializeSegment(tree.root, true)}`;
        /** @type {?} */
        const query = serializeQueryParams(tree.queryParams);
        /** @type {?} */
        const fragment = typeof tree.fragment === `string` ? `#${encodeUriFragment((/** @type {?} */ (tree.fragment)))}` : '';
        return `${segment}${query}${fragment}`;
    }
}
/** @type {?} */
const DEFAULT_SERIALIZER = new DefaultUrlSerializer();
/**
 * @param {?} segment
 * @return {?}
 */
export function serializePaths(segment) {
    return segment.segments.map((/**
     * @param {?} p
     * @return {?}
     */
    p => serializePath(p))).join('/');
}
/**
 * @param {?} segment
 * @param {?} root
 * @return {?}
 */
function serializeSegment(segment, root) {
    if (!segment.hasChildren()) {
        return serializePaths(segment);
    }
    if (root) {
        /** @type {?} */
        const primary = segment.children[PRIMARY_OUTLET] ?
            serializeSegment(segment.children[PRIMARY_OUTLET], false) :
            '';
        /** @type {?} */
        const children = [];
        forEach(segment.children, (/**
         * @param {?} v
         * @param {?} k
         * @return {?}
         */
        (v, k) => {
            if (k !== PRIMARY_OUTLET) {
                children.push(`${k}:${serializeSegment(v, false)}`);
            }
        }));
        return children.length > 0 ? `${primary}(${children.join('//')})` : primary;
    }
    else {
        /** @type {?} */
        const children = mapChildrenIntoArray(segment, (/**
         * @param {?} v
         * @param {?} k
         * @return {?}
         */
        (v, k) => {
            if (k === PRIMARY_OUTLET) {
                return [serializeSegment(segment.children[PRIMARY_OUTLET], false)];
            }
            return [`${k}:${serializeSegment(v, false)}`];
        }));
        return `${serializePaths(segment)}/(${children.join('//')})`;
    }
}
/**
 * Encodes a URI string with the default encoding. This function will only ever be called from
 * `encodeUriQuery` or `encodeUriSegment` as it's the base set of encodings to be used. We need
 * a custom encoding because encodeURIComponent is too aggressive and encodes stuff that doesn't
 * have to be encoded per https://url.spec.whatwg.org.
 * @param {?} s
 * @return {?}
 */
function encodeUriString(s) {
    return encodeURIComponent(s)
        .replace(/%40/g, '@')
        .replace(/%3A/gi, ':')
        .replace(/%24/g, '$')
        .replace(/%2C/gi, ',');
}
/**
 * This function should be used to encode both keys and values in a query string key/value. In
 * the following URL, you need to call encodeUriQuery on "k" and "v":
 *
 * http://www.site.org/html;mk=mv?k=v#f
 * @param {?} s
 * @return {?}
 */
export function encodeUriQuery(s) {
    return encodeUriString(s).replace(/%3B/gi, ';');
}
/**
 * This function should be used to encode a URL fragment. In the following URL, you need to call
 * encodeUriFragment on "f":
 *
 * http://www.site.org/html;mk=mv?k=v#f
 * @param {?} s
 * @return {?}
 */
export function encodeUriFragment(s) {
    return encodeURI(s);
}
/**
 * This function should be run on any URI segment as well as the key and value in a key/value
 * pair for matrix params. In the following URL, you need to call encodeUriSegment on "html",
 * "mk", and "mv":
 *
 * http://www.site.org/html;mk=mv?k=v#f
 * @param {?} s
 * @return {?}
 */
export function encodeUriSegment(s) {
    return encodeUriString(s).replace(/\(/g, '%28').replace(/\)/g, '%29').replace(/%26/gi, '&');
}
/**
 * @param {?} s
 * @return {?}
 */
export function decode(s) {
    return decodeURIComponent(s);
}
// Query keys/values should have the "+" replaced first, as "+" in a query string is " ".
// decodeURIComponent function will not decode "+" as a space.
/**
 * @param {?} s
 * @return {?}
 */
export function decodeQuery(s) {
    return decode(s.replace(/\+/g, '%20'));
}
/**
 * @param {?} path
 * @return {?}
 */
export function serializePath(path) {
    return `${encodeUriSegment(path.path)}${serializeMatrixParams(path.parameters)}`;
}
/**
 * @param {?} params
 * @return {?}
 */
function serializeMatrixParams(params) {
    return Object.keys(params)
        .map((/**
     * @param {?} key
     * @return {?}
     */
    key => `;${encodeUriSegment(key)}=${encodeUriSegment(params[key])}`))
        .join('');
}
/**
 * @param {?} params
 * @return {?}
 */
function serializeQueryParams(params) {
    /** @type {?} */
    const strParams = Object.keys(params).map((/**
     * @param {?} name
     * @return {?}
     */
    (name) => {
        /** @type {?} */
        const value = params[name];
        return Array.isArray(value) ?
            value.map((/**
             * @param {?} v
             * @return {?}
             */
            v => `${encodeUriQuery(name)}=${encodeUriQuery(v)}`)).join('&') :
            `${encodeUriQuery(name)}=${encodeUriQuery(value)}`;
    }));
    return strParams.length ? `?${strParams.join("&")}` : '';
}
/** @type {?} */
const SEGMENT_RE = /^[^\/()?;=#]+/;
/**
 * @param {?} str
 * @return {?}
 */
function matchSegments(str) {
    /** @type {?} */
    const match = str.match(SEGMENT_RE);
    return match ? match[0] : '';
}
/** @type {?} */
const QUERY_PARAM_RE = /^[^=?&#]+/;
// Return the name of the query param at the start of the string or an empty string
/**
 * @param {?} str
 * @return {?}
 */
function matchQueryParams(str) {
    /** @type {?} */
    const match = str.match(QUERY_PARAM_RE);
    return match ? match[0] : '';
}
/** @type {?} */
const QUERY_PARAM_VALUE_RE = /^[^?&#]+/;
// Return the value of the query param at the start of the string or an empty string
/**
 * @param {?} str
 * @return {?}
 */
function matchUrlQueryParamValue(str) {
    /** @type {?} */
    const match = str.match(QUERY_PARAM_VALUE_RE);
    return match ? match[0] : '';
}
class UrlParser {
    /**
     * @param {?} url
     */
    constructor(url) {
        this.url = url;
        this.remaining = url;
    }
    /**
     * @return {?}
     */
    parseRootSegment() {
        this.consumeOptional('/');
        if (this.remaining === '' || this.peekStartsWith('?') || this.peekStartsWith('#')) {
            return new UrlSegmentGroup([], {});
        }
        // The root segment group never has segments
        return new UrlSegmentGroup([], this.parseChildren());
    }
    /**
     * @return {?}
     */
    parseQueryParams() {
        /** @type {?} */
        const params = {};
        if (this.consumeOptional('?')) {
            do {
                this.parseQueryParam(params);
            } while (this.consumeOptional('&'));
        }
        return params;
    }
    /**
     * @return {?}
     */
    parseFragment() {
        return this.consumeOptional('#') ? decodeURIComponent(this.remaining) : null;
    }
    /**
     * @private
     * @return {?}
     */
    parseChildren() {
        if (this.remaining === '') {
            return {};
        }
        this.consumeOptional('/');
        /** @type {?} */
        const segments = [];
        if (!this.peekStartsWith('(')) {
            segments.push(this.parseSegment());
        }
        while (this.peekStartsWith('/') && !this.peekStartsWith('//') && !this.peekStartsWith('/(')) {
            this.capture('/');
            segments.push(this.parseSegment());
        }
        /** @type {?} */
        let children = {};
        if (this.peekStartsWith('/(')) {
            this.capture('/');
            children = this.parseParens(true);
        }
        /** @type {?} */
        let res = {};
        if (this.peekStartsWith('(')) {
            res = this.parseParens(false);
        }
        if (segments.length > 0 || Object.keys(children).length > 0) {
            res[PRIMARY_OUTLET] = new UrlSegmentGroup(segments, children);
        }
        return res;
    }
    // parse a segment with its matrix parameters
    // ie `name;k1=v1;k2`
    /**
     * @private
     * @return {?}
     */
    parseSegment() {
        /** @type {?} */
        const path = matchSegments(this.remaining);
        if (path === '' && this.peekStartsWith(';')) {
            throw new Error(`Empty path url segment cannot have parameters: '${this.remaining}'.`);
        }
        this.capture(path);
        return new UrlSegment(decode(path), this.parseMatrixParams());
    }
    /**
     * @private
     * @return {?}
     */
    parseMatrixParams() {
        /** @type {?} */
        const params = {};
        while (this.consumeOptional(';')) {
            this.parseParam(params);
        }
        return params;
    }
    /**
     * @private
     * @param {?} params
     * @return {?}
     */
    parseParam(params) {
        /** @type {?} */
        const key = matchSegments(this.remaining);
        if (!key) {
            return;
        }
        this.capture(key);
        /** @type {?} */
        let value = '';
        if (this.consumeOptional('=')) {
            /** @type {?} */
            const valueMatch = matchSegments(this.remaining);
            if (valueMatch) {
                value = valueMatch;
                this.capture(value);
            }
        }
        params[decode(key)] = decode(value);
    }
    // Parse a single query parameter `name[=value]`
    /**
     * @private
     * @param {?} params
     * @return {?}
     */
    parseQueryParam(params) {
        /** @type {?} */
        const key = matchQueryParams(this.remaining);
        if (!key) {
            return;
        }
        this.capture(key);
        /** @type {?} */
        let value = '';
        if (this.consumeOptional('=')) {
            /** @type {?} */
            const valueMatch = matchUrlQueryParamValue(this.remaining);
            if (valueMatch) {
                value = valueMatch;
                this.capture(value);
            }
        }
        /** @type {?} */
        const decodedKey = decodeQuery(key);
        /** @type {?} */
        const decodedVal = decodeQuery(value);
        if (params.hasOwnProperty(decodedKey)) {
            // Append to existing values
            /** @type {?} */
            let currentVal = params[decodedKey];
            if (!Array.isArray(currentVal)) {
                currentVal = [currentVal];
                params[decodedKey] = currentVal;
            }
            currentVal.push(decodedVal);
        }
        else {
            // Create a new value
            params[decodedKey] = decodedVal;
        }
    }
    // parse `(a/b//outlet_name:c/d)`
    /**
     * @private
     * @param {?} allowPrimary
     * @return {?}
     */
    parseParens(allowPrimary) {
        /** @type {?} */
        const segments = {};
        this.capture('(');
        while (!this.consumeOptional(')') && this.remaining.length > 0) {
            /** @type {?} */
            const path = matchSegments(this.remaining);
            /** @type {?} */
            const next = this.remaining[path.length];
            // if is is not one of these characters, then the segment was unescaped
            // or the group was not closed
            if (next !== '/' && next !== ')' && next !== ';') {
                throw new Error(`Cannot parse url '${this.url}'`);
            }
            /** @type {?} */
            let outletName = (/** @type {?} */ (undefined));
            if (path.indexOf(':') > -1) {
                outletName = path.substr(0, path.indexOf(':'));
                this.capture(outletName);
                this.capture(':');
            }
            else if (allowPrimary) {
                outletName = PRIMARY_OUTLET;
            }
            /** @type {?} */
            const children = this.parseChildren();
            segments[outletName] = Object.keys(children).length === 1 ? children[PRIMARY_OUTLET] :
                new UrlSegmentGroup([], children);
            this.consumeOptional('//');
        }
        return segments;
    }
    /**
     * @private
     * @param {?} str
     * @return {?}
     */
    peekStartsWith(str) { return this.remaining.startsWith(str); }
    // Consumes the prefix when it is present and returns whether it has been consumed
    /**
     * @private
     * @param {?} str
     * @return {?}
     */
    consumeOptional(str) {
        if (this.peekStartsWith(str)) {
            this.remaining = this.remaining.substring(str.length);
            return true;
        }
        return false;
    }
    /**
     * @private
     * @param {?} str
     * @return {?}
     */
    capture(str) {
        if (!this.consumeOptional(str)) {
            throw new Error(`Expected "${str}".`);
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    UrlParser.prototype.remaining;
    /**
     * @type {?}
     * @private
     */
    UrlParser.prototype.url;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXJsX3RyZWUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9yb3V0ZXIvc3JjL3VybF90cmVlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGNBQWMsRUFBb0IsaUJBQWlCLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFDN0UsT0FBTyxFQUFDLE9BQU8sRUFBRSxZQUFZLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQzs7OztBQUV6RCxNQUFNLFVBQVUsa0JBQWtCO0lBQ2hDLE9BQU8sSUFBSSxPQUFPLENBQUMsSUFBSSxlQUFlLENBQUMsRUFBRSxFQUFFLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxJQUFJLENBQUMsQ0FBQztBQUM1RCxDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLFlBQVksQ0FBQyxTQUFrQixFQUFFLFNBQWtCLEVBQUUsS0FBYztJQUNqRixJQUFJLEtBQUssRUFBRTtRQUNULE9BQU8sZ0JBQWdCLENBQUMsU0FBUyxDQUFDLFdBQVcsRUFBRSxTQUFTLENBQUMsV0FBVyxDQUFDO1lBQ2pFLGtCQUFrQixDQUFDLFNBQVMsQ0FBQyxJQUFJLEVBQUUsU0FBUyxDQUFDLElBQUksQ0FBQyxDQUFDO0tBQ3hEO0lBRUQsT0FBTyxtQkFBbUIsQ0FBQyxTQUFTLENBQUMsV0FBVyxFQUFFLFNBQVMsQ0FBQyxXQUFXLENBQUM7UUFDcEUsb0JBQW9CLENBQUMsU0FBUyxDQUFDLElBQUksRUFBRSxTQUFTLENBQUMsSUFBSSxDQUFDLENBQUM7QUFDM0QsQ0FBQzs7Ozs7O0FBRUQsU0FBUyxnQkFBZ0IsQ0FBQyxTQUFpQixFQUFFLFNBQWlCO0lBQzVELHFEQUFxRDtJQUNyRCxPQUFPLFlBQVksQ0FBQyxTQUFTLEVBQUUsU0FBUyxDQUFDLENBQUM7QUFDNUMsQ0FBQzs7Ozs7O0FBRUQsU0FBUyxrQkFBa0IsQ0FBQyxTQUEwQixFQUFFLFNBQTBCO0lBQ2hGLElBQUksQ0FB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SUFDZixDQUFDOzs7Ozs7SUFFTyxPQUFPLENBQUMsR0FBVztRQUN6QixJQUFJLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxHQUFHLENBQUMsRUFBRTtZQUM5QixNQUFNLElBQUksS0FBSyxDQUFDLGFBQWEsR0FBRyxJQUFJLENBQUMsQ0FBQztTQUN2QztJQUNILENBQUM7Q0FDRjs7Ozs7O0lBekxDLDhCQUEwQjs7Ozs7SUFFZCx3QkFBbUIiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7UFJJTUFSWV9PVVRMRVQsIFBhcmFtTWFwLCBQYXJhbXMsIGNvbnZlcnRUb1BhcmFtTWFwfSBmcm9tICcuL3NoYXJlZCc7XG5pbXBvcnQge2ZvckVhY2gsIHNoYWxsb3dFcXVhbH0gZnJvbSAnLi91dGlscy9jb2xsZWN0aW9uJztcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZUVtcHR5VXJsVHJlZSgpIHtcbiAgcmV0dXJuIG5ldyBVcmxUcmVlKG5ldyBVcmxTZWdtZW50R3JvdXAoW10sIHt9KSwge30sIG51bGwpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY29udGFpbnNUcmVlKGNvbnRhaW5lcjogVXJsVHJlZSwgY29udGFpbmVlOiBVcmxUcmVlLCBleGFjdDogYm9vbGVhbik6IGJvb2xlYW4ge1xuICBpZiAoZXhhY3QpIHtcbiAgICByZXR1cm4gZXF1YWxRdWVyeVBhcmFtcyhjb250YWluZXIucXVlcnlQYXJhbXMsIGNvbnRhaW5lZS5xdWVyeVBhcmFtcykgJiZcbiAgICAgICAgZXF1YWxTZWdtZW50R3JvdXBzKGNvbnRhaW5lci5yb290LCBjb250YWluZWUucm9vdCk7XG4gIH1cblxuICByZXR1cm4gY29udGFpbnNRdWVyeVBhcmFtcyhjb250YWluZXIucXVlcnlQYXJhbXMsIGNvbnRhaW5lZS5xdWVyeVBhcmFtcykgJiZcbiAgICAgIGNvbnRhaW5zU2VnbWVudEdyb3VwKGNvbnRhaW5lci5yb290LCBjb250YWluZWUucm9vdCk7XG59XG5cbmZ1bmN0aW9uIGVxdWFsUXVlcnlQYXJhbXMoY29udGFpbmVyOiBQYXJhbXMsIGNvbnRhaW5lZTogUGFyYW1zKTogYm9vbGVhbiB7XG4gIC8vIFRPRE86IFRoaXMgZG9lcyBub3QgaGFuZGxlIGFycmF5IHBhcmFtcyBjb3JyZWN0bHkuXG4gIHJldHVybiBzaGFsbG93RXF1YWwoY29udGFpbmVyLCBjb250YWluZWUpO1xufVxuXG5mdW5jdGlvbiBlcXVhbFNlZ21lbnRHcm91cHMoY29udGFpbmVyOiBVcmxTZWdtZW50R3JvdXAsIGNvbnRhaW5lZTogVXJsU2VnbWVudEdyb3VwKTogYm9vbGVhbiB7XG4gIGlmICghZXF1YWxQYXRoKGNvbnRhaW5lci5zZWdtZW50cywgY29udGFpbmVlLnNlZ21lbnRzKSkgcmV0dXJuIGZhbHNlO1xuICBpZiAoY29udGFpbmVyLm51bWJlck9mQ2hpbGRyZW4gIT09IGNvbnRhaW5lZS5udW1iZXJPZkNoaWxkcmVuKSByZXR1cm4gZmFsc2U7XG4gIGZvciAoY29uc3QgYyBpbiBjb250YWluZWUuY2hpbGRyZW4pIHtcbiAgICBpZiAoIWNvbnRhaW5lci5jaGlsZHJlbltjXSkgcmV0dXJuIGZhbHNlO1xuICAgIGlmICghZXF1YWxTZWdtZW50R3JvdXBzKGNvbnRhaW5lci5jaGlsZHJlbltjXSwgY29udGFpbmVlLmNoaWxkcmVuW2NdKSkgcmV0dXJuIGZhbHNlO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5mdW5jdGlvbiBjb250YWluc1F1ZXJ5UGFyYW1zKGNvbnRhaW5lcjogUGFyYW1zLCBjb250YWluZWU6IFBhcmFtcyk6IGJvb2xlYW4ge1xuICAvLyBUT0RPOiBUaGlzIGRvZXMgbm90IGhhbmRsZSBhcnJheSBwYXJhbXMgY29ycmVjdGx5LlxuICByZXR1cm4gT2JqZWN0LmtleXMoY29udGFpbmVlKS5sZW5ndGggPD0gT2JqZWN0LmtleXMoY29udGFpbmVyKS5sZW5ndGggJiZcbiAgICAgIE9iamVjdC5rZXlzKGNvbnRhaW5lZSkuZXZlcnkoa2V5ID0+IGNvbnRhaW5lZVtrZXldID09PSBjb250YWluZXJba2V5XSk7XG59XG5cbmZ1bmN0aW9uIGNvbnRhaW5zU2VnbWVudEdyb3VwKGNvbnRhaW5lcjogVXJsU2VnbWVudEdyb3VwLCBjb250YWluZWU6IFVybFNlZ21lbnRHcm91cCk6IGJvb2xlYW4ge1xuICByZXR1cm4gY29udGFpbnNTZWdtZW50R3JvdXBIZWxwZXIoY29udGFpbmVyLCBjb250YWluZWUsIGNvbnRhaW5lZS5zZWdtZW50cyk7XG59XG5cbmZ1bmN0aW9uIGNvbnRhaW5zU2VnbWVudEdyb3VwSGVscGVyKFxuICAgIGNvbnRhaW5lcjogVXJsU2VnbWVudEdyb3VwLCBjb250YWluZWU6IFVybFNlZ21lbnRHcm91cCwgY29udGFpbmVlUGF0aHM6IFVybFNlZ21lbnRbXSk6IGJvb2xlYW4ge1xuICBpZiAoY29udGFpbmVyLnNlZ21lbnRzLmxlbmd0aCA+IGNvbnRhaW5lZVBhdGhzLmxlbmd0aCkge1xuICAgIGNvbnN0IGN1cnJlbnQgPSBjb250YWluZXIuc2VnbWVudHMuc2xpY2UoMCwgY29udGFpbmVlUGF0aHMubGVuZ3RoKTtcbiAgICBpZiAoIWVxdWFsUGF0aChjdXJyZW50LCBjb250YWluZWVQYXRocykpIHJldHVybiBmYWxzZTtcbiAgICBpZiAoY29udGFpbmVlLmhhc0NoaWxkcmVuKCkpIHJldHVybiBmYWxzZTtcbiAgICByZXR1cm4gdHJ1ZTtcblxuICB9IGVsc2UgaWYgKGNvbnRhaW5lci5zZWdtZW50cy5sZW5ndGggPT09IGNvbnRhaW5lZVBhdGhzLmxlbmd0aCkge1xuICAgIGlmICghZXF1YWxQYXRoKGNvbnRhaW5lci5zZWdtZW50cywgY29udGFpbmVlUGF0aHMpKSByZXR1cm4gZmFsc2U7XG4gICAgZm9yIChjb25zdCBjIGluIGNvbnRhaW5lZS5jaGlsZHJlbikge1xuICAgICAgaWYgKCFjb250YWluZXIuY2hpbGRyZW5bY10pIHJldHVybiBmYWxzZTtcbiAgICAgIGlmICghY29udGFpbnNTZWdtZW50R3JvdXAoY29udGFpbmVyLmNoaWxkcmVuW2NdLCBjb250YWluZWUuY2hpbGRyZW5bY10pKSByZXR1cm4gZmFsc2U7XG4gICAgfVxuICAgIHJldHVybiB0cnVlO1xuXG4gIH0gZWxzZSB7XG4gICAgY29uc3QgY3VycmVudCA9IGNvbnRhaW5lZVBhdGhzLnNsaWNlKDAsIGNvbnRhaW5lci5zZWdtZW50cy5sZW5ndGgpO1xuICAgIGNvbnN0IG5leHQgPSBjb250YWluZWVQYXRocy5zbGljZShjb250YWluZXIuc2VnbWVudHMubGVuZ3RoKTtcbiAgICBpZiAoIWVxdWFsUGF0aChjb250YWluZXIuc2VnbWVudHMsIGN1cnJlbnQpKSByZXR1cm4gZmFsc2U7XG4gICAgaWYgKCFjb250YWluZXIuY2hpbGRyZW5bUFJJTUFSWV9PVVRMRVRdKSByZXR1cm4gZmFsc2U7XG4gICAgcmV0dXJuIGNvbnRhaW5zU2VnbWVudEdyb3VwSGVscGVyKGNvbnRhaW5lci5jaGlsZHJlbltQUklNQVJZX09VVExFVF0sIGNvbnRhaW5lZSwgbmV4dCk7XG4gIH1cbn1cblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBSZXByZXNlbnRzIHRoZSBwYXJzZWQgVVJMLlxuICpcbiAqIFNpbmNlIGEgcm91dGVyIHN0YXRlIGlzIGEgdHJlZSwgYW5kIHRoZSBVUkwgaXMgbm90aGluZyBidXQgYSBzZXJpYWxpemVkIHN0YXRlLCB0aGUgVVJMIGlzIGFcbiAqIHNlcmlhbGl6ZWQgdHJlZS5cbiAqIFVybFRyZWUgaXMgYSBkYXRhIHN0cnVjdHVyZSB0aGF0IHByb3ZpZGVzIGEgbG90IG9mIGFmZm9yZGFuY2VzIGluIGRlYWxpbmcgd2l0aCBVUkxzXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogYGBgXG4gKiBAQ29tcG9uZW50KHt0ZW1wbGF0ZVVybDondGVtcGxhdGUuaHRtbCd9KVxuICogY2xhc3MgTXlDb21wb25lbnQge1xuICogICBjb25zdHJ1Y3Rvcihyb3V0ZXI6IFJvdXRlcikge1xuICogICAgIGNvbnN0IHRyZWU6IFVybFRyZWUgPVxuICogICAgICAgcm91dGVyLnBhcnNlVXJsKCcvdGVhbS8zMy8odXNlci92aWN0b3IvL3N1cHBvcnQ6aGVscCk/ZGVidWc9dHJ1ZSNmcmFnbWVudCcpO1xuICogICAgIGNvbnN0IGYgPSB0cmVlLmZyYWdtZW50OyAvLyByZXR1cm4gJ2ZyYWdtZW50J1xuICogICAgIGNvbnN0IHEgPSB0cmVlLnF1ZXJ5UGFyYW1zOyAvLyByZXR1cm5zIHtkZWJ1ZzogJ3RydWUnfVxuICogICAgIGNvbnN0IGc6IFVybFNlZ21lbnRHcm91cCA9IHRyZWUucm9vdC5jaGlsZHJlbltQUklNQVJZX09VVExFVF07XG4gKiAgICAgY29uc3QgczogVXJsU2VnbWVudFtdID0gZy5zZWdtZW50czsgLy8gcmV0dXJucyAyIHNlZ21lbnRzICd0ZWFtJyBhbmQgJzMzJ1xuICogICAgIGcuY2hpbGRyZW5bUFJJTUFSWV9PVVRMRVRdLnNlZ21lbnRzOyAvLyByZXR1cm5zIDIgc2VnbWVudHMgJ3VzZXInIGFuZCAndmljdG9yJ1xuICogICAgIGcuY2hpbGRyZW5bJ3N1cHBvcnQnXS5zZWdtZW50czsgLy8gcmV0dXJuIDEgc2VnbWVudCAnaGVscCdcbiAqICAgfVxuICogfVxuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgVXJsVHJlZSB7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9xdWVyeVBhcmFtTWFwICE6IFBhcmFtTWFwO1xuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgY29uc3RydWN0b3IoXG4gICAgICAvKiogVGhlIHJvb3Qgc2VnbWVudCBncm91cCBvZiB0aGUgVVJMIHRyZWUgKi9cbiAgICAgIHB1YmxpYyByb290OiBVcmxTZWdtZW50R3JvdXAsXG4gICAgICAvKiogVGhlIHF1ZXJ5IHBhcmFtcyBvZiB0aGUgVVJMICovXG4gICAgICBwdWJsaWMgcXVlcnlQYXJhbXM6IFBhcmFtcyxcbiAgICAgIC8qKiBUaGUgZnJhZ21lbnQgb2YgdGhlIFVSTCAqL1xuICAgICAgcHVibGljIGZyYWdtZW50OiBzdHJpbmd8bnVsbCkge31cblxuICBnZXQgcXVlcnlQYXJhbU1hcCgpOiBQYXJhbU1hcCB7XG4gICAgaWYgKCF0aGlzLl9xdWVyeVBhcmFtTWFwKSB7XG4gICAgICB0aGlzLl9xdWVyeVBhcmFtTWFwID0gY29udmVydFRvUGFyYW1NYXAodGhpcy5xdWVyeVBhcmFtcyk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLl9xdWVyeVBhcmFtTWFwO1xuICB9XG5cbiAgLyoqIEBkb2NzTm90UmVxdWlyZWQgKi9cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHsgcmV0dXJuIERFRkFVTFRfU0VSSUFMSVpFUi5zZXJpYWxpemUodGhpcyk7IH1cbn1cblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBSZXByZXNlbnRzIHRoZSBwYXJzZWQgVVJMIHNlZ21lbnQgZ3JvdXAuXG4gKlxuICogU2VlIGBVcmxUcmVlYCBmb3IgbW9yZSBpbmZvcm1hdGlvbi5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBVcmxTZWdtZW50R3JvdXAge1xuICAvKiogQGludGVybmFsICovXG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBfc291cmNlU2VnbWVudCAhOiBVcmxTZWdtZW50R3JvdXA7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9zZWdtZW50SW5kZXhTaGlmdCAhOiBudW1iZXI7XG4gIC8qKiBUaGUgcGFyZW50IG5vZGUgaW4gdGhlIHVybCB0cmVlICovXG4gIHBhcmVudDogVXJsU2VnbWVudEdyb3VwfG51bGwgPSBudWxsO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqIFRoZSBVUkwgc2VnbWVudHMgb2YgdGhpcyBncm91cC4gU2VlIGBVcmxTZWdtZW50YCBmb3IgbW9yZSBpbmZvcm1hdGlvbiAqL1xuICAgICAgcHVibGljIHNlZ21lbnRzOiBVcmxTZWdtZW50W10sXG4gICAgICAvKiogVGhlIGxpc3Qgb2YgY2hpbGRyZW4gb2YgdGhpcyBncm91cCAqL1xuICAgICAgcHVibGljIGNoaWxkcmVuOiB7W2tleTogc3RyaW5nXTogVXJsU2VnbWVudEdyb3VwfSkge1xuICAgIGZvckVhY2goY2hpbGRyZW4sICh2OiBhbnksIGs6IGFueSkgPT4gdi5wYXJlbnQgPSB0aGlzKTtcbiAgfVxuXG4gIC8qKiBXaGV0aGVyIHRoZSBzZWdtZW50IGhhcyBjaGlsZCBzZWdtZW50cyAqL1xuICBoYXNDaGlsZHJlbigpOiBib29sZWFuIHsgcmV0dXJuIHRoaXMubnVtYmVyT2ZDaGlsZHJlbiA+IDA7IH1cblxuICAvKiogTnVtYmVyIG9mIGNoaWxkIHNlZ21lbnRzICovXG4gIGdldCBudW1iZXJPZkNoaWxkcmVuKCk6IG51bWJlciB7IHJldHVybiBPYmplY3Qua2V5cyh0aGlzLmNoaWxkcmVuKS5sZW5ndGg7IH1cblxuICAvKiogQGRvY3NOb3RSZXF1aXJlZCAqL1xuICB0b1N0cmluZygpOiBzdHJpbmcgeyByZXR1cm4gc2VyaWFsaXplUGF0aHModGhpcyk7IH1cbn1cblxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIFJlcHJlc2VudHMgYSBzaW5nbGUgVVJMIHNlZ21lbnQuXG4gKlxuICogQSBVcmxTZWdtZW50IGlzIGEgcGFydCBvZiBhIFVSTCBiZXR3ZWVuIHRoZSB0d28gc2xhc2hlcy4gSXQgY29udGFpbnMgYSBwYXRoIGFuZCB0aGUgbWF0cml4XG4gKiBwYXJhbWV0ZXJzIGFzc29jaWF0ZWQgd2l0aCB0aGUgc2VnbWVudC5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICrCoCMjIyBFeGFtcGxlXG4gKlxuICogYGBgXG4gKiBAQ29tcG9uZW50KHt0ZW1wbGF0ZVVybDondGVtcGxhdGUuaHRtbCd9KVxuICogY2xhc3MgTXlDb21wb25lbnQge1xuICogICBjb25zdHJ1Y3Rvcihyb3V0ZXI6IFJvdXRlcikge1xuICogICAgIGNvbnN0IHRyZWU6IFVybFRyZWUgPSByb3V0ZXIucGFyc2VVcmwoJy90ZWFtO2lkPTMzJyk7XG4gKiAgICAgY29uc3QgZzogVXJsU2VnbWVudEdyb3VwID0gdHJlZS5yb290LmNoaWxkcmVuW1BSSU1BUllfT1VUTEVUXTtcbiAqICAgICBjb25zdCBzOiBVcmxTZWdtZW50W10gPSBnLnNlZ21lbnRzO1xuICogICAgIHNbMF0ucGF0aDsgLy8gcmV0dXJucyAndGVhbSdcbiAqICAgICBzWzBdLnBhcmFtZXRlcnM7IC8vIHJldHVybnMge2lkOiAzM31cbiAqICAgfVxuICogfVxuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgVXJsU2VnbWVudCB7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9wYXJhbWV0ZXJNYXAgITogUGFyYW1NYXA7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICAvKiogVGhlIHBhdGggcGFydCBvZiBhIFVSTCBzZWdtZW50ICovXG4gICAgICBwdWJsaWMgcGF0aDogc3RyaW5nLFxuXG4gICAgICAvKiogVGhlIG1hdHJpeCBwYXJhbWV0ZXJzIGFzc29jaWF0ZWQgd2l0aCBhIHNlZ21lbnQgKi9cbiAgICAgIHB1YmxpYyBwYXJhbWV0ZXJzOiB7W25hbWU6IHN0cmluZ106IHN0cmluZ30pIHt9XG5cbiAgZ2V0IHBhcmFtZXRlck1hcCgpIHtcbiAgICBpZiAoIXRoaXMuX3BhcmFtZXRlck1hcCkge1xuICAgICAgdGhpcy5fcGFyYW1ldGVyTWFwID0gY29udmVydFRvUGFyYW1NYXAodGhpcy5wYXJhbWV0ZXJzKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3BhcmFtZXRlck1hcDtcbiAgfVxuXG4gIC8qKiBAZG9jc05vdFJlcXVpcmVkICovXG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7IHJldHVybiBzZXJpYWxpemVQYXRoKHRoaXMpOyB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBlcXVhbFNlZ21lbnRzKGFzOiBVcmxTZWdtZW50W10sIGJzOiBVcmxTZWdtZW50W10pOiBib29sZWFuIHtcbiAgcmV0dXJuIGVxdWFsUGF0aChhcywgYnMpICYmIGFzLmV2ZXJ5KChhLCBpKSA9PiBzaGFsbG93RXF1YWwoYS5wYXJhbWV0ZXJzLCBic1tpXS5wYXJhbWV0ZXJzKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBlcXVhbFBhdGgoYXM6IFVybFNlZ21lbnRbXSwgYnM6IFVybFNlZ21lbnRbXSk6IGJvb2xlYW4ge1xuICBpZiAoYXMubGVuZ3RoICE9PSBicy5sZW5ndGgpIHJldHVybiBmYWxzZTtcbiAgcmV0dXJuIGFzLmV2ZXJ5KChhLCBpKSA9PiBhLnBhdGggPT09IGJzW2ldLnBhdGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbWFwQ2hpbGRyZW5JbnRvQXJyYXk8VD4oXG4gICAgc2VnbWVudDogVXJsU2VnbWVudEdyb3VwLCBmbjogKHY6IFVybFNlZ21lbnRHcm91cCwgazogc3RyaW5nKSA9PiBUW10pOiBUW10ge1xuICBsZXQgcmVzOiBUW10gPSBbXTtcbiAgZm9yRWFjaChzZWdtZW50LmNoaWxkcmVuLCAoY2hpbGQ6IFVybFNlZ21lbnRHcm91cCwgY2hpbGRPdXRsZXQ6IHN0cmluZykgPT4ge1xuICAgIGlmIChjaGlsZE91dGxldCA9PT0gUFJJTUFSWV9PVVRMRVQpIHtcbiAgICAgIHJlcyA9IHJlcy5jb25jYXQoZm4oY2hpbGQsIGNoaWxkT3V0bGV0KSk7XG4gICAgfVxuICB9KTtcbiAgZm9yRWFjaChzZWdtZW50LmNoaWxkcmVuLCAoY2hpbGQ6IFVybFNlZ21lbnRHcm91cCwgY2hpbGRPdXRsZXQ6IHN0cmluZykgPT4ge1xuICAgIGlmIChjaGlsZE91dGxldCAhPT0gUFJJTUFSWV9PVVRMRVQpIHtcbiAgICAgIHJlcyA9IHJlcy5jb25jYXQoZm4oY2hpbGQsIGNoaWxkT3V0bGV0KSk7XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIHJlcztcbn1cblxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIFNlcmlhbGl6ZXMgYW5kIGRlc2VyaWFsaXplcyBhIFVSTCBzdHJpbmcgaW50byBhIFVSTCB0cmVlLlxuICpcbiAqIFRoZSB1cmwgc2VyaWFsaXphdGlvbiBzdHJhdGVneSBpcyBjdXN0b21pemFibGUuIFlvdSBjYW5cbiAqIG1ha2UgYWxsIFVSTHMgY2FzZSBpbnNlbnNpdGl2ZSBieSBwcm92aWRpbmcgYSBjdXN0b20gVXJsU2VyaWFsaXplci5cbiAqXG4gKiBTZWUgYERlZmF1bHRVcmxTZXJpYWxpemVyYCBmb3IgYW4gZXhhbXBsZSBvZiBhIFVSTCBzZXJpYWxpemVyLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFVybFNlcmlhbGl6ZXIge1xuICAvKiogUGFyc2UgYSB1cmwgaW50byBhIGBVcmxUcmVlYCAqL1xuICBhYnN0cmFjdCBwYXJzZSh1cmw6IHN0cmluZyk6IFVybFRyZWU7XG5cbiAgLyoqIENvbnZlcnRzIGEgYFVybFRyZWVgIGludG8gYSB1cmwgKi9cbiAgYWJzdHJhY3Qgc2VyaWFsaXplKHRyZWU6IFVybFRyZWUpOiBzdHJpbmc7XG59XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogQSBkZWZhdWx0IGltcGxlbWVudGF0aW9uIG9mIHRoZSBgVXJsU2VyaWFsaXplcmAuXG4gKlxuICogRXhhbXBsZSBVUkxzOlxuICpcbiAqIGBgYFxuICogL2luYm94LzMzKHBvcHVwOmNvbXBvc2UpXG4gKiAvaW5ib3gvMzM7b3Blbj10cnVlL21lc3NhZ2VzLzQ0XG4gKiBgYGBcbiAqXG4gKiBEZWZhdWx0VXJsU2VyaWFsaXplciB1c2VzIHBhcmVudGhlc2VzIHRvIHNlcmlhbGl6ZSBzZWNvbmRhcnkgc2VnbWVudHMgKGUuZy4sIHBvcHVwOmNvbXBvc2UpLCB0aGVcbiAqIGNvbG9uIHN5bnRheCB0byBzcGVjaWZ5IHRoZSBvdXRsZXQsIGFuZCB0aGUgJztwYXJhbWV0ZXI9dmFsdWUnIHN5bnRheCAoZS5nLiwgb3Blbj10cnVlKSB0b1xuICogc3BlY2lmeSByb3V0ZSBzcGVjaWZpYyBwYXJhbWV0ZXJzLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIERlZmF1bHRVcmxTZXJpYWxpemVyIGltcGxlbWVudHMgVXJsU2VyaWFsaXplciB7XG4gIC8qKiBQYXJzZXMgYSB1cmwgaW50byBhIGBVcmxUcmVlYCAqL1xuICBwYXJzZSh1cmw6IHN0cmluZyk6IFVybFRyZWUge1xuICAgIGNvbnN0IHAgPSBuZXcgVXJsUGFyc2VyKHVybCk7XG4gICAgcmV0dXJuIG5ldyBVcmxUcmVlKHAucGFyc2VSb290U2VnbWVudCgpLCBwLnBhcnNlUXVlcnlQYXJhbXMoKSwgcC5wYXJzZUZyYWdtZW50KCkpO1xuICB9XG5cbiAgLyoqIENvbnZlcnRzIGEgYFVybFRyZWVgIGludG8gYSB1cmwgKi9cbiAgc2VyaWFsaXplKHRyZWU6IFVybFRyZWUpOiBzdHJpbmcge1xuICAgIGNvbnN0IHNlZ21lbnQgPSBgLyR7c2VyaWFsaXplU2VnbWVudCh0cmVlLnJvb3QsIHRydWUpfWA7XG4gICAgY29uc3QgcXVlcnkgPSBzZXJpYWxpemVRdWVyeVBhcmFtcyh0cmVlLnF1ZXJ5UGFyYW1zKTtcbiAgICBjb25zdCBmcmFnbWVudCA9XG4gICAgICAgIHR5cGVvZiB0cmVlLmZyYWdtZW50ID09PSBgc3RyaW5nYCA/IGAjJHtlbmNvZGVVcmlGcmFnbWVudCh0cmVlLmZyYWdtZW50ICEpfWAgOiAnJztcblxuICAgIHJldHVybiBgJHtzZWdtZW50fSR7cXVlcnl9JHtmcmFnbWVudH1gO1xuICB9XG59XG5cbmNvbnN0IERFRkFVTFRfU0VSSUFMSVpFUiA9IG5ldyBEZWZhdWx0VXJsU2VyaWFsaXplcigpO1xuXG5leHBvcnQgZnVuY3Rpb24gc2VyaWFsaXplUGF0aHMoc2VnbWVudDogVXJsU2VnbWVudEdyb3VwKTogc3RyaW5nIHtcbiAgcmV0dXJuIHNlZ21lbnQuc2VnbWVudHMubWFwKHAgPT4gc2VyaWFsaXplUGF0aChwKSkuam9pbignLycpO1xufVxuXG5mdW5jdGlvbiBzZXJpYWxpemVTZWdtZW50KHNlZ21lbnQ6IFVybFNlZ21lbnRHcm91cCwgcm9vdDogYm9vbGVhbik6IHN0cmluZyB7XG4gIGlmICghc2VnbWVudC5oYXNDaGlsZHJlbigpKSB7XG4gICAgcmV0dXJuIHNlcmlhbGl6ZVBhdGhzKHNlZ21lbnQpO1xuICB9XG5cbiAgaWYgKHJvb3QpIHtcbiAgICBjb25zdCBwcmltYXJ5ID0gc2VnbWVudC5jaGlsZHJlbltQUklNQVJZX09VVExFVF0gP1xuICAgICAgICBzZXJpYWxpemVTZWdtZW50KHNlZ21lbnQuY2hpbGRyZW5bUFJJTUFSWV9PVVRMRVRdLCBmYWxzZSkgOlxuICAgICAgICAnJztcbiAgICBjb25zdCBjaGlsZHJlbjogc3RyaW5nW10gPSBbXTtcblxuICAgIGZvckVhY2goc2VnbWVudC5jaGlsZHJlbiwgKHY6IFVybFNlZ21lbnRHcm91cCwgazogc3RyaW5nKSA9PiB7XG4gICAgICBpZiAoayAhPT0gUFJJTUFSWV9PVVRMRVQpIHtcbiAgICAgICAgY2hpbGRyZW4ucHVzaChgJHtrfToke3NlcmlhbGl6ZVNlZ21lbnQodiwgZmFsc2UpfWApO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgcmV0dXJuIGNoaWxkcmVuLmxlbmd0aCA+IDAgPyBgJHtwcmltYXJ5fSgke2NoaWxkcmVuLmpvaW4oJy8vJyl9KWAgOiBwcmltYXJ5O1xuXG4gIH0gZWxzZSB7XG4gICAgY29uc3QgY2hpbGRyZW4gPSBtYXBDaGlsZHJlbkludG9BcnJheShzZWdtZW50LCAodjogVXJsU2VnbWVudEdyb3VwLCBrOiBzdHJpbmcpID0+IHtcbiAgICAgIGlmIChrID09PSBQUklNQVJZX09VVExFVCkge1xuICAgICAgICByZXR1cm4gW3NlcmlhbGl6ZVNlZ21lbnQoc2VnbWVudC5jaGlsZHJlbltQUklNQVJZX09VVExFVF0sIGZhbHNlKV07XG4gICAgICB9XG5cbiAgICAgIHJldHVybiBbYCR7a306JHtzZXJpYWxpemVTZWdtZW50KHYsIGZhbHNlKX1gXTtcblxuICAgIH0pO1xuXG4gICAgcmV0dXJuIGAke3NlcmlhbGl6ZVBhdGhzKHNlZ21lbnQpfS8oJHtjaGlsZHJlbi5qb2luKCcvLycpfSlgO1xuICB9XG59XG5cbi8qKlxuICogRW5jb2RlcyBhIFVSSSBzdHJpbmcgd2l0aCB0aGUgZGVmYXVsdCBlbmNvZGluZy4gVGhpcyBmdW5jdGlvbiB3aWxsIG9ubHkgZXZlciBiZSBjYWxsZWQgZnJvbVxuICogYGVuY29kZVVyaVF1ZXJ5YCBvciBgZW5jb2RlVXJpU2VnbWVudGAgYXMgaXQncyB0aGUgYmFzZSBzZXQgb2YgZW5jb2RpbmdzIHRvIGJlIHVzZWQuIFdlIG5lZWRcbiAqIGEgY3VzdG9tIGVuY29kaW5nIGJlY2F1c2UgZW5jb2RlVVJJQ29tcG9uZW50IGlzIHRvbyBhZ2dyZXNzaXZlIGFuZCBlbmNvZGVzIHN0dWZmIHRoYXQgZG9lc24ndFxuICogaGF2ZSB0byBiZSBlbmNvZGVkIHBlciBodHRwczovL3VybC5zcGVjLndoYXR3Zy5vcmcuXG4gKi9cbmZ1bmN0aW9uIGVuY29kZVVyaVN0cmluZyhzOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gZW5jb2RlVVJJQ29tcG9uZW50KHMpXG4gICAgICAucmVwbGFjZSgvJTQwL2csICdAJylcbiAgICAgIC5yZXBsYWNlKC8lM0EvZ2ksICc6JylcbiAgICAgIC5yZXBsYWNlKC8lMjQvZywgJyQnKVxuICAgICAgLnJlcGxhY2UoLyUyQy9naSwgJywnKTtcbn1cblxuLyoqXG4gKiBUaGlzIGZ1bmN0aW9uIHNob3VsZCBiZSB1c2VkIHRvIGVuY29kZSBib3RoIGtleXMgYW5kIHZhbHVlcyBpbiBhIHF1ZXJ5IHN0cmluZyBrZXkvdmFsdWUuIEluXG4gKiB0aGUgZm9sbG93aW5nIFVSTCwgeW91IG5lZWQgdG8gY2FsbCBlbmNvZGVVcmlRdWVyeSBvbiBcImtcIiBhbmQgXCJ2XCI6XG4gKlxuICogaHR0cDovL3d3dy5zaXRlLm9yZy9odG1sO21rPW12P2s9diNmXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBlbmNvZGVVcmlRdWVyeShzOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gZW5jb2RlVXJpU3RyaW5nKHMpLnJlcGxhY2UoLyUzQi9naSwgJzsnKTtcbn1cblxuLyoqXG4gKiBUaGlzIGZ1bmN0aW9uIHNob3VsZCBiZSB1c2VkIHRvIGVuY29kZSBhIFVSTCBmcmFnbWVudC4gSW4gdGhlIGZvbGxvd2luZyBVUkwsIHlvdSBuZWVkIHRvIGNhbGxcbiAqIGVuY29kZVVyaUZyYWdtZW50IG9uIFwiZlwiOlxuICpcbiAqIGh0dHA6Ly93d3cuc2l0ZS5vcmcvaHRtbDttaz1tdj9rPXYjZlxuICovXG5leHBvcnQgZnVuY3Rpb24gZW5jb2RlVXJpRnJhZ21lbnQoczogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGVuY29kZVVSSShzKTtcbn1cblxuLyoqXG4gKiBUaGlzIGZ1bmN0aW9uIHNob3VsZCBiZSBydW4gb24gYW55IFVSSSBzZWdtZW50IGFzIHdlbGwgYXMgdGhlIGtleSBhbmQgdmFsdWUgaW4gYSBrZXkvdmFsdWVcbiAqIHBhaXIgZm9yIG1hdHJpeCBwYXJhbXMuIEluIHRoZSBmb2xsb3dpbmcgVVJMLCB5b3UgbmVlZCB0byBjYWxsIGVuY29kZVVyaVNlZ21lbnQgb24gXCJodG1sXCIsXG4gKiBcIm1rXCIsIGFuZCBcIm12XCI6XG4gKlxuICogaHR0cDovL3d3dy5zaXRlLm9yZy9odG1sO21rPW12P2s9diNmXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBlbmNvZGVVcmlTZWdtZW50KHM6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBlbmNvZGVVcmlTdHJpbmcocykucmVwbGFjZSgvXFwoL2csICclMjgnKS5yZXBsYWNlKC9cXCkvZywgJyUyOScpLnJlcGxhY2UoLyUyNi9naSwgJyYnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlY29kZShzOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gZGVjb2RlVVJJQ29tcG9uZW50KHMpO1xufVxuXG4vLyBRdWVyeSBrZXlzL3ZhbHVlcyBzaG91bGQgaGF2ZSB0aGUgXCIrXCIgcmVwbGFjZWQgZmlyc3QsIGFzIFwiK1wiIGluIGEgcXVlcnkgc3RyaW5nIGlzIFwiIFwiLlxuLy8gZGVjb2RlVVJJQ29tcG9uZW50IGZ1bmN0aW9uIHdpbGwgbm90IGRlY29kZSBcIitcIiBhcyBhIHNwYWNlLlxuZXhwb3J0IGZ1bmN0aW9uIGRlY29kZVF1ZXJ5KHM6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBkZWNvZGUocy5yZXBsYWNlKC9cXCsvZywgJyUyMCcpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNlcmlhbGl6ZVBhdGgocGF0aDogVXJsU2VnbWVudCk6IHN0cmluZyB7XG4gIHJldHVybiBgJHtlbmNvZGVVcmlTZWdtZW50KHBhdGgucGF0aCl9JHtzZXJpYWxpemVNYXRyaXhQYXJhbXMocGF0aC5wYXJhbWV0ZXJzKX1gO1xufVxuXG5mdW5jdGlvbiBzZXJpYWxpemVNYXRyaXhQYXJhbXMocGFyYW1zOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSk6IHN0cmluZyB7XG4gIHJldHVybiBPYmplY3Qua2V5cyhwYXJhbXMpXG4gICAgICAubWFwKGtleSA9PiBgOyR7ZW5jb2RlVXJpU2VnbWVudChrZXkpfT0ke2VuY29kZVVyaVNlZ21lbnQocGFyYW1zW2tleV0pfWApXG4gICAgICAuam9pbignJyk7XG59XG5cbmZ1bmN0aW9uIHNlcmlhbGl6ZVF1ZXJ5UGFyYW1zKHBhcmFtczoge1trZXk6IHN0cmluZ106IGFueX0pOiBzdHJpbmcge1xuICBjb25zdCBzdHJQYXJhbXM6IHN0cmluZ1tdID0gT2JqZWN0LmtleXMocGFyYW1zKS5tYXAoKG5hbWUpID0+IHtcbiAgICBjb25zdCB2YWx1ZSA9IHBhcmFtc1tuYW1lXTtcbiAgICByZXR1cm4gQXJyYXkuaXNBcnJheSh2YWx1ZSkgP1xuICAgICAgICB2YWx1ZS5tYXAodiA9PiBgJHtlbmNvZGVVcmlRdWVyeShuYW1lKX09JHtlbmNvZGVVcmlRdWVyeSh2KX1gKS5qb2luKCcmJykgOlxuICAgICAgICBgJHtlbmNvZGVVcmlRdWVyeShuYW1lKX09JHtlbmNvZGVVcmlRdWVyeSh2YWx1ZSl9YDtcbiAgfSk7XG5cbiAgcmV0dXJuIHN0clBhcmFtcy5sZW5ndGggPyBgPyR7c3RyUGFyYW1zLmpvaW4oXCImXCIpfWAgOiAnJztcbn1cblxuY29uc3QgU0VHTUVOVF9SRSA9IC9eW15cXC8oKT87PSNdKy87XG5mdW5jdGlvbiBtYXRjaFNlZ21lbnRzKHN0cjogc3RyaW5nKTogc3RyaW5nIHtcbiAgY29uc3QgbWF0Y2ggPSBzdHIubWF0Y2goU0VHTUVOVF9SRSk7XG4gIHJldHVybiBtYXRjaCA/IG1hdGNoWzBdIDogJyc7XG59XG5cbmNvbnN0IFFVRVJZX1BBUkFNX1JFID0gL15bXj0/JiNdKy87XG4vLyBSZXR1cm4gdGhlIG5hbWUgb2YgdGhlIHF1ZXJ5IHBhcmFtIGF0IHRoZSBzdGFydCBvZiB0aGUgc3RyaW5nIG9yIGFuIGVtcHR5IHN0cmluZ1xuZnVuY3Rpb24gbWF0Y2hRdWVyeVBhcmFtcyhzdHI6IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IG1hdGNoID0gc3RyLm1hdGNoKFFVRVJZX1BBUkFNX1JFKTtcbiAgcmV0dXJuIG1hdGNoID8gbWF0Y2hbMF0gOiAnJztcbn1cblxuY29uc3QgUVVFUllfUEFSQU1fVkFMVUVfUkUgPSAvXltePyYjXSsvO1xuLy8gUmV0dXJuIHRoZSB2YWx1ZSBvZiB0aGUgcXVlcnkgcGFyYW0gYXQgdGhlIHN0YXJ0IG9mIHRoZSBzdHJpbmcgb3IgYW4gZW1wdHkgc3RyaW5nXG5mdW5jdGlvbiBtYXRjaFVybFF1ZXJ5UGFyYW1WYWx1ZShzdHI6IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IG1hdGNoID0gc3RyLm1hdGNoKFFVRVJZX1BBUkFNX1ZBTFVFX1JFKTtcbiAgcmV0dXJuIG1hdGNoID8gbWF0Y2hbMF0gOiAnJztcbn1cblxuY2xhc3MgVXJsUGFyc2VyIHtcbiAgcHJpdmF0ZSByZW1haW5pbmc6IHN0cmluZztcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHVybDogc3RyaW5nKSB7IHRoaXMucmVtYWluaW5nID0gdXJsOyB9XG5cbiAgcGFyc2VSb290U2VnbWVudCgpOiBVcmxTZWdtZW50R3JvdXAge1xuICAgIHRoaXMuY29uc3VtZU9wdGlvbmFsKCcvJyk7XG5cbiAgICBpZiAodGhpcy5yZW1haW5pbmcgPT09ICcnIHx8IHRoaXMucGVla1N0YXJ0c1dpdGgoJz8nKSB8fCB0aGlzLnBlZWtTdGFydHNXaXRoKCcjJykpIHtcbiAgICAgIHJldHVybiBuZXcgVXJsU2VnbWVudEdyb3VwKFtdLCB7fSk7XG4gICAgfVxuXG4gICAgLy8gVGhlIHJvb3Qgc2VnbWVudCBncm91cCBuZXZlciBoYXMgc2VnbWVudHNcbiAgICByZXR1cm4gbmV3IFVybFNlZ21lbnRHcm91cChbXSwgdGhpcy5wYXJzZUNoaWxkcmVuKCkpO1xuICB9XG5cbiAgcGFyc2VRdWVyeVBhcmFtcygpOiBQYXJhbXMge1xuICAgIGNvbnN0IHBhcmFtczogUGFyYW1zID0ge307XG4gICAgaWYgKHRoaXMuY29uc3VtZU9wdGlvbmFsKCc/JykpIHtcbiAgICAgIGRvIHtcbiAgICAgICAgdGhpcy5wYXJzZVF1ZXJ5UGFyYW0ocGFyYW1zKTtcbiAgICAgIH0gd2hpbGUgKHRoaXMuY29uc3VtZU9wdGlvbmFsKCcmJykpO1xuICAgIH1cbiAgICByZXR1cm4gcGFyYW1zO1xuICB9XG5cbiAgcGFyc2VGcmFnbWVudCgpOiBzdHJpbmd8bnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuY29uc3VtZU9wdGlvbmFsKCcjJykgPyBkZWNvZGVVUklDb21wb25lbnQodGhpcy5yZW1haW5pbmcpIDogbnVsbDtcbiAgfVxuXG4gIHByaXZhdGUgcGFyc2VDaGlsZHJlbigpOiB7W291dGxldDogc3RyaW5nXTogVXJsU2VnbWVudEdyb3VwfSB7XG4gICAgaWYgKHRoaXMucmVtYWluaW5nID09PSAnJykge1xuICAgICAgcmV0dXJuIHt9O1xuICAgIH1cblxuICAgIHRoaXMuY29uc3VtZU9wdGlvbmFsKCcvJyk7XG5cbiAgICBjb25zdCBzZWdtZW50czogVXJsU2VnbWVudFtdID0gW107XG4gICAgaWYgKCF0aGlzLnBlZWtTdGFydHNXaXRoKCcoJykpIHtcbiAgICAgIHNlZ21lbnRzLnB1c2godGhpcy5wYXJzZVNlZ21lbnQoKSk7XG4gICAgfVxuXG4gICAgd2hpbGUgKHRoaXMucGVla1N0YXJ0c1dpdGgoJy8nKSAmJiAhdGhpcy5wZWVrU3RhcnRzV2l0aCgnLy8nKSAmJiAhdGhpcy5wZWVrU3RhcnRzV2l0aCgnLygnKSkge1xuICAgICAgdGhpcy5jYXB0dXJlKCcvJyk7XG4gICAgICBzZWdtZW50cy5wdXNoKHRoaXMucGFyc2VTZWdtZW50KCkpO1xuICAgIH1cblxuICAgIGxldCBjaGlsZHJlbjoge1tvdXRsZXQ6IHN0cmluZ106IFVybFNlZ21lbnRHcm91cH0gPSB7fTtcbiAgICBpZiAodGhpcy5wZWVrU3RhcnRzV2l0aCgnLygnKSkge1xuICAgICAgdGhpcy5jYXB0dXJlKCcvJyk7XG4gICAgICBjaGlsZHJlbiA9IHRoaXMucGFyc2VQYXJlbnModHJ1ZSk7XG4gICAgfVxuXG4gICAgbGV0IHJlczoge1tvdXRsZXQ6IHN0cmluZ106IFVybFNlZ21lbnRHcm91cH0gPSB7fTtcbiAgICBpZiAodGhpcy5wZWVrU3RhcnRzV2l0aCgnKCcpKSB7XG4gICAgICByZXMgPSB0aGlzLnBhcnNlUGFyZW5zKGZhbHNlKTtcbiAgICB9XG5cbiAgICBpZiAoc2VnbWVudHMubGVuZ3RoID4gMCB8fCBPYmplY3Qua2V5cyhjaGlsZHJlbikubGVuZ3RoID4gMCkge1xuICAgICAgcmVzW1BSSU1BUllfT1VUTEVUXSA9IG5ldyBVcmxTZWdtZW50R3JvdXAoc2VnbWVudHMsIGNoaWxkcmVuKTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzO1xuICB9XG5cbiAgLy8gcGFyc2UgYSBzZWdtZW50IHdpdGggaXRzIG1hdHJpeCBwYXJhbWV0ZXJzXG4gIC8vIGllIGBuYW1lO2sxPXYxO2syYFxuICBwcml2YXRlIHBhcnNlU2VnbWVudCgpOiBVcmxTZWdtZW50IHtcbiAgICBjb25zdCBwYXRoID0gbWF0Y2hTZWdtZW50cyh0aGlzLnJlbWFpbmluZyk7XG4gICAgaWYgKHBhdGggPT09ICcnICYmIHRoaXMucGVla1N0YXJ0c1dpdGgoJzsnKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBFbXB0eSBwYXRoIHVybCBzZWdtZW50IGNhbm5vdCBoYXZlIHBhcmFtZXRlcnM6ICcke3RoaXMucmVtYWluaW5nfScuYCk7XG4gICAgfVxuXG4gICAgdGhpcy5jYXB0dXJlKHBhdGgpO1xuICAgIHJldHVybiBuZXcgVXJsU2VnbWVudChkZWNvZGUocGF0aCksIHRoaXMucGFyc2VNYXRyaXhQYXJhbXMoKSk7XG4gIH1cblxuICBwcml2YXRlIHBhcnNlTWF0cml4UGFyYW1zKCk6IHtba2V5OiBzdHJpbmddOiBhbnl9IHtcbiAgICBjb25zdCBwYXJhbXM6IHtba2V5OiBzdHJpbmddOiBhbnl9ID0ge307XG4gICAgd2hpbGUgKHRoaXMuY29uc3VtZU9wdGlvbmFsKCc7JykpIHtcbiAgICAgIHRoaXMucGFyc2VQYXJhbShwYXJhbXMpO1xuICAgIH1cbiAgICByZXR1cm4gcGFyYW1zO1xuICB9XG5cbiAgcHJpdmF0ZSBwYXJzZVBhcmFtKHBhcmFtczoge1trZXk6IHN0cmluZ106IGFueX0pOiB2b2lkIHtcbiAgICBjb25zdCBrZXkgPSBtYXRjaFNlZ21lbnRzKHRoaXMucmVtYWluaW5nKTtcbiAgICBpZiAoIWtleSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLmNhcHR1cmUoa2V5KTtcbiAgICBsZXQgdmFsdWU6IGFueSA9ICcnO1xuICAgIGlmICh0aGlzLmNvbnN1bWVPcHRpb25hbCgnPScpKSB7XG4gICAgICBjb25zdCB2YWx1ZU1hdGNoID0gbWF0Y2hTZWdtZW50cyh0aGlzLnJlbWFpbmluZyk7XG4gICAgICBpZiAodmFsdWVNYXRjaCkge1xuICAgICAgICB2YWx1ZSA9IHZhbHVlTWF0Y2g7XG4gICAgICAgIHRoaXMuY2FwdHVyZSh2YWx1ZSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgcGFyYW1zW2RlY29kZShrZXkpXSA9IGRlY29kZSh2YWx1ZSk7XG4gIH1cblxuICAvLyBQYXJzZSBhIHNpbmdsZSBxdWVyeSBwYXJhbWV0ZXIgYG5hbWVbPXZhbHVlXWBcbiAgcHJpdmF0ZSBwYXJzZVF1ZXJ5UGFyYW0ocGFyYW1zOiBQYXJhbXMpOiB2b2lkIHtcbiAgICBjb25zdCBrZXkgPSBtYXRjaFF1ZXJ5UGFyYW1zKHRoaXMucmVtYWluaW5nKTtcbiAgICBpZiAoIWtleSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLmNhcHR1cmUoa2V5KTtcbiAgICBsZXQgdmFsdWU6IGFueSA9ICcnO1xuICAgIGlmICh0aGlzLmNvbnN1bWVPcHRpb25hbCgnPScpKSB7XG4gICAgICBjb25zdCB2YWx1ZU1hdGNoID0gbWF0Y2hVcmxRdWVyeVBhcmFtVmFsdWUodGhpcy5yZW1haW5pbmcpO1xuICAgICAgaWYgKHZhbHVlTWF0Y2gpIHtcbiAgICAgICAgdmFsdWUgPSB2YWx1ZU1hdGNoO1xuICAgICAgICB0aGlzLmNhcHR1cmUodmFsdWUpO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGRlY29kZWRLZXkgPSBkZWNvZGVRdWVyeShrZXkpO1xuICAgIGNvbnN0IGRlY29kZWRWYWwgPSBkZWNvZGVRdWVyeSh2YWx1ZSk7XG5cbiAgICBpZiAocGFyYW1zLmhhc093blByb3BlcnR5KGRlY29kZWRLZXkpKSB7XG4gICAgICAvLyBBcHBlbmQgdG8gZXhpc3RpbmcgdmFsdWVzXG4gICAgICBsZXQgY3VycmVudFZhbCA9IHBhcmFtc1tkZWNvZGVkS2V5XTtcbiAgICAgIGlmICghQXJyYXkuaXNBcnJheShjdXJyZW50VmFsKSkge1xuICAgICAgICBjdXJyZW50VmFsID0gW2N1cnJlbnRWYWxdO1xuICAgICAgICBwYXJhbXNbZGVjb2RlZEtleV0gPSBjdXJyZW50VmFsO1xuICAgICAgfVxuICAgICAgY3VycmVudFZhbC5wdXNoKGRlY29kZWRWYWwpO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBDcmVhdGUgYSBuZXcgdmFsdWVcbiAgICAgIHBhcmFtc1tkZWNvZGVkS2V5XSA9IGRlY29kZWRWYWw7XG4gICAgfVxuICB9XG5cbiAgLy8gcGFyc2UgYChhL2IvL291dGxldF9uYW1lOmMvZClgXG4gIHByaXZhdGUgcGFyc2VQYXJlbnMoYWxsb3dQcmltYXJ5OiBib29sZWFuKToge1tvdXRsZXQ6IHN0cmluZ106IFVybFNlZ21lbnRHcm91cH0ge1xuICAgIGNvbnN0IHNlZ21lbnRzOiB7W2tleTogc3RyaW5nXTogVXJsU2VnbWVudEdyb3VwfSA9IHt9O1xuICAgIHRoaXMuY2FwdHVyZSgnKCcpO1xuXG4gICAgd2hpbGUgKCF0aGlzLmNvbnN1bWVPcHRpb25hbCgnKScpICYmIHRoaXMucmVtYWluaW5nLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IHBhdGggPSBtYXRjaFNlZ21lbnRzKHRoaXMucmVtYWluaW5nKTtcblxuICAgICAgY29uc3QgbmV4dCA9IHRoaXMucmVtYWluaW5nW3BhdGgubGVuZ3RoXTtcblxuICAgICAgLy8gaWYgaXMgaXMgbm90IG9uZSBvZiB0aGVzZSBjaGFyYWN0ZXJzLCB0aGVuIHRoZSBzZWdtZW50IHdhcyB1bmVzY2FwZWRcbiAgICAgIC8vIG9yIHRoZSBncm91cCB3YXMgbm90IGNsb3NlZFxuICAgICAgaWYgKG5leHQgIT09ICcvJyAmJiBuZXh0ICE9PSAnKScgJiYgbmV4dCAhPT0gJzsnKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgQ2Fubm90IHBhcnNlIHVybCAnJHt0aGlzLnVybH0nYCk7XG4gICAgICB9XG5cbiAgICAgIGxldCBvdXRsZXROYW1lOiBzdHJpbmcgPSB1bmRlZmluZWQgITtcbiAgICAgIGlmIChwYXRoLmluZGV4T2YoJzonKSA+IC0xKSB7XG4gICAgICAgIG91dGxldE5hbWUgPSBwYXRoLnN1YnN0cigwLCBwYXRoLmluZGV4T2YoJzonKSk7XG4gICAgICAgIHRoaXMuY2FwdHVyZShvdXRsZXROYW1lKTtcbiAgICAgICAgdGhpcy5jYXB0dXJlKCc6Jyk7XG4gICAgICB9IGVsc2UgaWYgKGFsbG93UHJpbWFyeSkge1xuICAgICAgICBvdXRsZXROYW1lID0gUFJJTUFSWV9PVVRMRVQ7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGNoaWxkcmVuID0gdGhpcy5wYXJzZUNoaWxkcmVuKCk7XG4gICAgICBzZWdtZW50c1tvdXRsZXROYW1lXSA9IE9iamVjdC5rZXlzKGNoaWxkcmVuKS5sZW5ndGggPT09IDEgPyBjaGlsZHJlbltQUklNQVJZX09VVExFVF0gOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgbmV3IFVybFNlZ21lbnRHcm91cChbXSwgY2hpbGRyZW4pO1xuICAgICAgdGhpcy5jb25zdW1lT3B0aW9uYWwoJy8vJyk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHNlZ21lbnRzO1xuICB9XG5cbiAgcHJpdmF0ZSBwZWVrU3RhcnRzV2l0aChzdHI6IHN0cmluZyk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5yZW1haW5pbmcuc3RhcnRzV2l0aChzdHIpOyB9XG5cbiAgLy8gQ29uc3VtZXMgdGhlIHByZWZpeCB3aGVuIGl0IGlzIHByZXNlbnQgYW5kIHJldHVybnMgd2hldGhlciBpdCBoYXMgYmVlbiBjb25zdW1lZFxuICBwcml2YXRlIGNvbnN1bWVPcHRpb25hbChzdHI6IHN0cmluZyk6IGJvb2xlYW4ge1xuICAgIGlmICh0aGlzLnBlZWtTdGFydHNXaXRoKHN0cikpIHtcbiAgICAgIHRoaXMucmVtYWluaW5nID0gdGhpcy5yZW1haW5pbmcuc3Vic3RyaW5nKHN0ci5sZW5ndGgpO1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuXG4gIHByaXZhdGUgY2FwdHVyZShzdHI6IHN0cmluZyk6IHZvaWQge1xuICAgIGlmICghdGhpcy5jb25zdW1lT3B0aW9uYWwoc3RyKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBFeHBlY3RlZCBcIiR7c3RyfVwiLmApO1xuICAgIH1cbiAgfVxufVxuIl19