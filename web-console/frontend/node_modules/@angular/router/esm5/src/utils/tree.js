/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
var Tree = /** @class */ (function () {
    function Tree(root) {
        this._root = root;
    }
    Object.defineProperty(Tree.prototype, "root", {
        get: function () { return this._root.value; },
        enumerable: true,
        configurable: true
    });
    /**
     * @internal
     */
    Tree.prototype.parent = function (t) {
        var p = this.pathFromRoot(t);
        return p.length > 1 ? p[p.length - 2] : null;
    };
    /**
     * @internal
     */
    Tree.prototype.children = function (t) {
        var n = findNode(t, this._root);
        return n ? n.children.map(function (t) { return t.value; }) : [];
    };
    /**
     * @internal
     */
    Tree.prototype.firstChild = function (t) {
        var n = findNode(t, this._root);
        return n && n.children.length > 0 ? n.children[0].value : null;
    };
    /**
     * @internal
     */
    Tree.prototype.siblings = function (t) {
        var p = findPath(t, this._root);
        if (p.length < 2)
            return [];
        var c = p[p.length - 2].children.map(function (c) { return c.value; });
        return c.filter(function (cc) { return cc !== t; });
    };
    /**
     * @internal
     */
    Tree.prototype.pathFromRoot = function (t) { return findPath(t, this._root).map(function (s) { return s.value; }); };
    return Tree;
}());
export { Tree };
// DFS for the node matching the value
function findNode(value, node) {
    var e_1, _a;
    if (value === node.value)
        return node;
    try {
        for (var _b = tslib_1.__values(node.children), _c = _b.next(); !_c.done; _c = _b.next()) {
            var child = _c.value;
            var node_1 = findNode(value, child);
            if (node_1)
                return node_1;
        }
    }
    catch (e_1_1) { e_1 = { error: e_1_1 }; }
    finally {
        try {
            if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
        }
        finally { if (e_1) throw e_1.error; }
    }
    return null;
}
// Return the path to the node with the given value using DFS
function findPath(value, node) {
    var e_2, _a;
    if (value === node.value)
        return [node];
    try {
        for (var _b = tslib_1.__values(node.children), _c = _b.next(); !_c.done; _c = _b.next()) {
            var child = _c.value;
            var path = findPath(value, child);
            if (path.length) {
                path.unshift(node);
                return path;
            }
        }
    }
    catch (e_2_1) { e_2 = { error: e_2_1 }; }
    finally {
        try {
            if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
        }
        finally { if (e_2) throw e_2.error; }
    }
    return [];
}
var TreeNode = /** @class */ (function () {
    function TreeNode(value, children) {
        this.value = value;
        this.children = children;
    }
    TreeNode.prototype.toString = function () { return "TreeNode(" + this.value + ")"; };
    return TreeNode;
}());
export { TreeNode };
// Return the list of T indexed by outlet name
export function nodeChildrenAsMap(node) {
    var map = {};
    if (node) {
        node.children.forEach(function (child) { return map[child.value.outlet] = child; });
    }
    return map;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHJlZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvdXRpbHMvdHJlZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUg7SUFJRSxjQUFZLElBQWlCO1FBQUksSUFBSSxDQUFDLEtBQUssR0FBRyxJQUFJLENBQUM7SUFBQyxDQUFDO0lBRXJELHNCQUFJLHNCQUFJO2FBQVIsY0FBZ0IsT0FBTyxJQUFJLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7OztPQUFBO0lBRTFDOztPQUVHO0lBQ0gscUJBQU0sR0FBTixVQUFPLENBQUk7UUFDVCxJQUFNLENBQUMsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQy9CLE9BQU8sQ0FBQyxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsTUFBTSxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7SUFDL0MsQ0FBQztJQUVEOztPQUVHO0lBQ0gsdUJBQVEsR0FBUixVQUFTLENBQUk7UUFDWCxJQUFNLENBQUMsR0FBRyxRQUFRLENBQUMsQ0FBQyxFQUFFLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUNsQyxPQUFPLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxDQUFDLENBQUMsS0FBSyxFQUFQLENBQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7SUFDL0MsQ0FBQztJQUVEOztPQUVHO0lBQ0gseUJBQVUsR0FBVixVQUFXLENBQUk7UUFDYixJQUFNLENBQUMsR0FBRyxRQUFRLENBQUMsQ0FBQyxFQUFFLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUNsQyxPQUFPLENBQUMsSUFB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QUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsR0FBRyxLQUFLLEVBQS9CLENBQStCLENBQUMsQ0FBQztLQUNqRTtJQUVELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuZXhwb3J0IGNsYXNzIFRyZWU8VD4ge1xuICAvKiogQGludGVybmFsICovXG4gIF9yb290OiBUcmVlTm9kZTxUPjtcblxuICBjb25zdHJ1Y3Rvcihyb290OiBUcmVlTm9kZTxUPikgeyB0aGlzLl9yb290ID0gcm9vdDsgfVxuXG4gIGdldCByb290KCk6IFQgeyByZXR1cm4gdGhpcy5fcm9vdC52YWx1ZTsgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHBhcmVudCh0OiBUKTogVHxudWxsIHtcbiAgICBjb25zdCBwID0gdGhpcy5wYXRoRnJvbVJvb3QodCk7XG4gICAgcmV0dXJuIHAubGVuZ3RoID4gMSA/IHBbcC5sZW5ndGggLSAyXSA6IG51bGw7XG4gIH1cblxuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICBjaGlsZHJlbih0OiBUKTogVFtdIHtcbiAgICBjb25zdCBuID0gZmluZE5vZGUodCwgdGhpcy5fcm9vdCk7XG4gICAgcmV0dXJuIG4gPyBuLmNoaWxkcmVuLm1hcCh0ID0+IHQudmFsdWUpIDogW107XG4gIH1cblxuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICBmaXJzdENoaWxkKHQ6IFQpOiBUfG51bGwge1xuICAgIGNvbnN0IG4gPSBmaW5kTm9kZSh0LCB0aGlzLl9yb290KTtcbiAgICByZXR1cm4gbiAmJiBuLmNoaWxkcmVuLmxlbmd0aCA+IDAgPyBuLmNoaWxkcmVuWzBdLnZhbHVlIDogbnVsbDtcbiAgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHNpYmxpbmdzKHQ6IFQpOiBUW10ge1xuICAgIGNvbnN0IHAgPSBmaW5kUGF0aCh0LCB0aGlzLl9yb290KTtcbiAgICBpZiAocC5sZW5ndGggPCAyKSByZXR1cm4gW107XG5cbiAgICBjb25zdCBjID0gcFtwLmxlbmd0aCAtIDJdLmNoaWxkcmVuLm1hcChjID0+IGMudmFsdWUpO1xuICAgIHJldHVybiBjLmZpbHRlcihjYyA9PiBjYyAhPT0gdCk7XG4gIH1cblxuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICBwYXRoRnJvbVJvb3QodDogVCk6IFRbXSB7IHJldHVybiBmaW5kUGF0aCh0LCB0aGlzLl9yb290KS5tYXAocyA9PiBzLnZhbHVlKTsgfVxufVxuXG5cbi8vIERGUyBmb3IgdGhlIG5vZGUgbWF0Y2hpbmcgdGhlIHZhbHVlXG5mdW5jdGlvbiBmaW5kTm9kZTxUPih2YWx1ZTogVCwgbm9kZTogVHJlZU5vZGU8VD4pOiBUcmVlTm9kZTxUPnxudWxsIHtcbiAgaWYgKHZhbHVlID09PSBub2RlLnZhbHVlKSByZXR1cm4gbm9kZTtcblxuICBmb3IgKGNvbnN0IGNoaWxkIG9mIG5vZGUuY2hpbGRyZW4pIHtcbiAgICBjb25zdCBub2RlID0gZmluZE5vZGUodmFsdWUsIGNoaWxkKTtcbiAgICBpZiAobm9kZSkgcmV0dXJuIG5vZGU7XG4gIH1cblxuICByZXR1cm4gbnVsbDtcbn1cblxuLy8gUmV0dXJuIHRoZSBwYXRoIHRvIHRoZSBub2RlIHdpdGggdGhlIGdpdmVuIHZhbHVlIHVzaW5nIERGU1xuZnVuY3Rpb24gZmluZFBhdGg8VD4odmFsdWU6IFQsIG5vZGU6IFRyZWVOb2RlPFQ+KTogVHJlZU5vZGU8VD5bXSB7XG4gIGlmICh2YWx1ZSA9PT0gbm9kZS52YWx1ZSkgcmV0dXJuIFtub2RlXTtcblxuICBmb3IgKGNvbnN0IGNoaWxkIG9mIG5vZGUuY2hpbGRyZW4pIHtcbiAgICBjb25zdCBwYXRoID0gZmluZFBhdGgodmFsdWUsIGNoaWxkKTtcbiAgICBpZiAocGF0aC5sZW5ndGgpIHtcbiAgICAgIHBhdGgudW5zaGlmdChub2RlKTtcbiAgICAgIHJldHVybiBwYXRoO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBbXTtcbn1cblxuZXhwb3J0IGNsYXNzIFRyZWVOb2RlPFQ+IHtcbiAgY29uc3RydWN0b3IocHVibGljIHZhbHVlOiBULCBwdWJsaWMgY2hpbGRyZW46IFRyZWVOb2RlPFQ+W10pIHt9XG5cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHsgcmV0dXJuIGBUcmVlTm9kZSgke3RoaXMudmFsdWV9KWA7IH1cbn1cblxuLy8gUmV0dXJuIHRoZSBsaXN0IG9mIFQgaW5kZXhlZCBieSBvdXRsZXQgbmFtZVxuZXhwb3J0IGZ1bmN0aW9uIG5vZGVDaGlsZHJlbkFzTWFwPFQgZXh0ZW5kc3tvdXRsZXQ6IHN0cmluZ30+KG5vZGU6IFRyZWVOb2RlPFQ+fCBudWxsKSB7XG4gIGNvbnN0IG1hcDoge1tvdXRsZXQ6IHN0cmluZ106IFRyZWVOb2RlPFQ+fSA9IHt9O1xuXG4gIGlmIChub2RlKSB7XG4gICAgbm9kZS5jaGlsZHJlbi5mb3JFYWNoKGNoaWxkID0+IG1hcFtjaGlsZC52YWx1ZS5vdXRsZXRdID0gY2hpbGQpO1xuICB9XG5cbiAgcmV0dXJuIG1hcDtcbn0iXX0=