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
 * @template T
 */
export class Tree {
    /**
     * @param {?} root
     */
    constructor(root) { this._root = root; }
    /**
     * @return {?}
     */
    get root() { return this._root.value; }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    parent(t) {
        /** @type {?} */
        const p = this.pathFromRoot(t);
        return p.length > 1 ? p[p.length - 2] : null;
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    children(t) {
        /** @type {?} */
        const n = findNode(t, this._root);
        return n ? n.children.map((/**
         * @param {?} t
         * @return {?}
         */
        t => t.value)) : [];
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    firstChild(t) {
        /** @type {?} */
        const n = findNode(t, this._root);
        return n && n.children.length > 0 ? n.children[0].value : null;
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    siblings(t) {
        /** @type {?} */
        const p = findPath(t, this._root);
        if (p.length < 2)
            return [];
        /** @type {?} */
        const c = p[p.length - 2].children.map((/**
         * @param {?} c
         * @return {?}
         */
        c => c.value));
        return c.filter((/**
         * @param {?} cc
         * @return {?}
         */
        cc => cc !== t));
    }
    /**
     * \@internal
     * @param {?} t
     * @return {?}
     */
    pathFromRoot(t) { return findPath(t, this._root).map((/**
     * @param {?} s
     * @return {?}
     */
    s => s.value)); }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    Tree.prototype._root;
}
// DFS for the node matching the value
/**
 * @template T
 * @param {?} value
 * @param {?} node
 * @return {?}
 */
function findNode(value, node) {
    if (value === node.value)
        return node;
    for (const child of node.children) {
        /** @type {?} */
        const node = findNode(value, child);
        if (node)
            return node;
    }
    return null;
}
// Return the path to the node with the given value using DFS
/**
 * @template T
 * @param {?} value
 * @param {?} node
 * @return {?}
 */
function findPath(value, node) {
    if (value === node.value)
        return [node];
    for (const child of node.children) {
        /** @type {?} */
        const path = findPath(value, child);
        if (path.length) {
            path.unshift(node);
            return path;
        }
    }
    return [];
}
/**
 * @template T
 */
export class TreeNode {
    /**
     * @param {?} value
     * @param {?} children
     */
    constructor(value, children) {
        this.value = value;
        this.children = children;
    }
    /**
     * @return {?}
     */
    toString() { return `TreeNode(${this.value})`; }
}
if (false) {
    /** @type {?} */
    TreeNode.prototype.value;
    /** @type {?} */
    TreeNode.prototype.children;
}
// Return the list of T indexed by outlet name
/**
 * @template T
 * @param {?} node
 * @return {?}
 */
export function nodeChildrenAsMap(node) {
    /** @type {?} */
    const map = {};
    if (node) {
        node.children.forEach((/**
         * @param {?} child
         * @return {?}
         */
        child => map[child.value.outlet] = child));
    }
    return map;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHJlZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvdXRpbHMvdHJlZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7OztBQVFBLE1BQU0sT0FBTyxJQUFJOzs7O0lBSWYsWUFBWSxJQUFpQixJQUFJLElBQUksQ0FBQyxLQUFLLEdBQUcsSUFBSSxDQUFDLENBQUMsQ0FBQzs7OztJQUVyRCxJQUFJLElBQUksS0FBUSxPQUFPLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQzs7Ozs7O0lBSzFDLE1BQU0sQ0FBQyxDQUFJOztjQUNILENBQUMsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztRQUM5QixPQUFPLENBQUMsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO0lBQy9DLENBQUM7Ozs7OztJQUtELFFBQVEsQ0FBQyxDQUFJOztjQUNMLENBQUMsR0FBRyxRQUFRLENBQUMsQ0FBQyxFQUFFLElBQUksQ0FBQyxLQUFLLENBQUM7UUFDakMsT0FBTyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsR0FBRzs7OztRQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDLEtBQUssRUFBQyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7SUFDL0MsQ0FBQzs7Ozs7O0lBS0QsVUFBVSxDQUFDLENBQUk7O2NBQ1AsQ0FBQyxHQUFHLFFBQVEsQ0FBQyxDQUFDLEVBQUUsSUFBSSxDQUFDLEtBQUssQ0FBQztRQUNqQyxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUMsUUFBUSxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7SUFDakUsQ0FBQzs7Ozs7O0lBS0QsUUFBUSxDQUFDLENBQUk7O2NBQ0wsQ0FBQyxHQUFHLFFBQVEsQ0FBQyxDQUFDLEVBQUUsSUFBSSxDQUFDLEtBQUssQ0FBQztRQUNqQyxJQUFJLENBQUMsQ0FBQyxNQUFNLEdBQUcsQ0FBQztZQUFFLE9BQU8sRUFBRSxDQUFDOztjQUV0QixDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLEdBQUc7Ozs7UUFBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxLQUFLLEVBQUM7UUFDcEQsT0FBTyxDQUFDLENBQUMsTUFBTTs7OztRQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxLQUFLLENBQUMsRUFBQyxDQUFDO0lBQ2xDLENBQUM7Ozs7OztJQUtELFlBQVksQ0FBQyxDQUFJLElBQVMsT0FBTyxRQUFRLENBQUMsQ0FBQyxFQUFFLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxHQUFHOzs7O0lBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsS0FBSyxFQUFDLENBQUMsQ0FBQyxDQUFDO0NBQzlFOzs7Ozs7SUE3Q0MscUJBQW1COzs7Ozs7Ozs7QUFpRHJCLFNBQVMsUUFBUSxDQUFJLEtBQVEsRUFBRSxJQUFpQjtJQUM5QyxJQUFJLEtBQUssS0FBSyxJQUFJLENBQUMsS0FBSztRQUFFLE9BQU8sSUFBSSxDQUFDO0lBRXRDLEtBQUssTUFBTSxLQUFLLElBQUksSUFBSSxDQUFDLFFBQVEsRUFBRTs7Y0FDM0IsSUFBSSxHQUFHLFFBQVEsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDO1FBQ25DLElBQUksSUFBSTtZQUFFLE9BQU8sSUFBSSxDQUFDO0tBQ3ZCO0lBRUQsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7Ozs7OztBQUdELFNBQVMsUUFBUSxDQUFJLEtBQVEsRUFBRSxJQUFpQjtJQUM5QyxJQUFJLEtBQUssS0FBSyxJQUFJLENBQUMsS0FBSztRQUFFLE9BQU8sQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUV4QyxLQUFLLE1BQU0sS0FBSyxJQUFJLElBQUksQ0FBQyxRQUFRLEVBQUU7O2NBQzNCLElBQUksR0FBRyxRQUFRLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQztRQUNuQyxJQUFJLElBQUksQ0FBQyxNQUFNLEVBQUU7WUFDZixJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxDQUFDO1lBQ25CLE9BQU8sSUFBSSxDQUFDO1NBQ2I7S0FDRjtJQUVELE9BQU8sRUFBRSxDQUFDO0FBQ1osQ0FBQzs7OztBQUVELE1BQU0sT0FBTyxRQUFROzs7OztJQUNuQixZQUFtQixLQUFRLEVBQVMsUUFBdUI7UUFBeEMsVUFBSyxHQUFMLEtBQUssQ0FBRztRQUFTLGFBQVEsR0FBUixRQUFRLENBQWU7SUFBRyxDQUFDOzs7O0lBRS9ELFFBQVEsS0FBYSxPQUFPLFlBQVksSUFBSSxDQUFDLEtBQUssR0FBRyxDQUFDLENBQUMsQ0FBQztDQUN6RDs7O0lBSGEseUJBQWU7O0lBQUUsNEJBQThCOzs7Ozs7OztBQU03RCxNQUFNLFVBQVUsaUJBQWlCLENBQTRCLElBQXVCOztVQUM1RSxHQUFHLEdBQW9DLEVBQUU7SUFFL0MsSUFBSSxJQUFJLEVBQUU7UUFDUixJQUFJLENBQUMsUUFBUSxDQUFDLE9BQU87Ozs7UUFBQyxLQUFLLENBQUMsRUFBRSxDQUFDLEdBQUcsQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLE1BQU0sQ0FBQyxHQUFHLEtBQUssRUFBQyxDQUFDO0tBQ2pFO0lBRUQsT0FBTyxHQUFHLENBQUM7QUFDYixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5leHBvcnQgY2xhc3MgVHJlZTxUPiB7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3Jvb3Q6IFRyZWVOb2RlPFQ+O1xuXG4gIGNvbnN0cnVjdG9yKHJvb3Q6IFRyZWVOb2RlPFQ+KSB7IHRoaXMuX3Jvb3QgPSByb290OyB9XG5cbiAgZ2V0IHJvb3QoKTogVCB7IHJldHVybiB0aGlzLl9yb290LnZhbHVlOyB9XG5cbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgcGFyZW50KHQ6IFQpOiBUfG51bGwge1xuICAgIGNvbnN0IHAgPSB0aGlzLnBhdGhGcm9tUm9vdCh0KTtcbiAgICByZXR1cm4gcC5sZW5ndGggPiAxID8gcFtwLmxlbmd0aCAtIDJdIDogbnVsbDtcbiAgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIGNoaWxkcmVuKHQ6IFQpOiBUW10ge1xuICAgIGNvbnN0IG4gPSBmaW5kTm9kZSh0LCB0aGlzLl9yb290KTtcbiAgICByZXR1cm4gbiA/IG4uY2hpbGRyZW4ubWFwKHQgPT4gdC52YWx1ZSkgOiBbXTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIGZpcnN0Q2hpbGQodDogVCk6IFR8bnVsbCB7XG4gICAgY29uc3QgbiA9IGZpbmROb2RlKHQsIHRoaXMuX3Jvb3QpO1xuICAgIHJldHVybiBuICYmIG4uY2hpbGRyZW4ubGVuZ3RoID4gMCA/IG4uY2hpbGRyZW5bMF0udmFsdWUgOiBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgc2libGluZ3ModDogVCk6IFRbXSB7XG4gICAgY29uc3QgcCA9IGZpbmRQYXRoKHQsIHRoaXMuX3Jvb3QpO1xuICAgIGlmIChwLmxlbmd0aCA8IDIpIHJldHVybiBbXTtcblxuICAgIGNvbnN0IGMgPSBwW3AubGVuZ3RoIC0gMl0uY2hpbGRyZW4ubWFwKGMgPT4gYy52YWx1ZSk7XG4gICAgcmV0dXJuIGMuZmlsdGVyKGNjID0+IGNjICE9PSB0KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHBhdGhGcm9tUm9vdCh0OiBUKTogVFtdIHsgcmV0dXJuIGZpbmRQYXRoKHQsIHRoaXMuX3Jvb3QpLm1hcChzID0+IHMudmFsdWUpOyB9XG59XG5cblxuLy8gREZTIGZvciB0aGUgbm9kZSBtYXRjaGluZyB0aGUgdmFsdWVcbmZ1bmN0aW9uIGZpbmROb2RlPFQ+KHZhbHVlOiBULCBub2RlOiBUcmVlTm9kZTxUPik6IFRyZWVOb2RlPFQ+fG51bGwge1xuICBpZiAodmFsdWUgPT09IG5vZGUudmFsdWUpIHJldHVybiBub2RlO1xuXG4gIGZvciAoY29uc3QgY2hpbGQgb2Ygbm9kZS5jaGlsZHJlbikge1xuICAgIGNvbnN0IG5vZGUgPSBmaW5kTm9kZSh2YWx1ZSwgY2hpbGQpO1xuICAgIGlmIChub2RlKSByZXR1cm4gbm9kZTtcbiAgfVxuXG4gIHJldHVybiBudWxsO1xufVxuXG4vLyBSZXR1cm4gdGhlIHBhdGggdG8gdGhlIG5vZGUgd2l0aCB0aGUgZ2l2ZW4gdmFsdWUgdXNpbmcgREZTXG5mdW5jdGlvbiBmaW5kUGF0aDxUPih2YWx1ZTogVCwgbm9kZTogVHJlZU5vZGU8VD4pOiBUcmVlTm9kZTxUPltdIHtcbiAgaWYgKHZhbHVlID09PSBub2RlLnZhbHVlKSByZXR1cm4gW25vZGVdO1xuXG4gIGZvciAoY29uc3QgY2hpbGQgb2Ygbm9kZS5jaGlsZHJlbikge1xuICAgIGNvbnN0IHBhdGggPSBmaW5kUGF0aCh2YWx1ZSwgY2hpbGQpO1xuICAgIGlmIChwYXRoLmxlbmd0aCkge1xuICAgICAgcGF0aC51bnNoaWZ0KG5vZGUpO1xuICAgICAgcmV0dXJuIHBhdGg7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIFtdO1xufVxuXG5leHBvcnQgY2xhc3MgVHJlZU5vZGU8VD4ge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgdmFsdWU6IFQsIHB1YmxpYyBjaGlsZHJlbjogVHJlZU5vZGU8VD5bXSkge31cblxuICB0b1N0cmluZygpOiBzdHJpbmcgeyByZXR1cm4gYFRyZWVOb2RlKCR7dGhpcy52YWx1ZX0pYDsgfVxufVxuXG4vLyBSZXR1cm4gdGhlIGxpc3Qgb2YgVCBpbmRleGVkIGJ5IG91dGxldCBuYW1lXG5leHBvcnQgZnVuY3Rpb24gbm9kZUNoaWxkcmVuQXNNYXA8VCBleHRlbmRze291dGxldDogc3RyaW5nfT4obm9kZTogVHJlZU5vZGU8VD58IG51bGwpIHtcbiAgY29uc3QgbWFwOiB7W291dGxldDogc3RyaW5nXTogVHJlZU5vZGU8VD59ID0ge307XG5cbiAgaWYgKG5vZGUpIHtcbiAgICBub2RlLmNoaWxkcmVuLmZvckVhY2goY2hpbGQgPT4gbWFwW2NoaWxkLnZhbHVlLm91dGxldF0gPSBjaGlsZCk7XG4gIH1cblxuICByZXR1cm4gbWFwO1xufSJdfQ==