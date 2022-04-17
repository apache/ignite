/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/ast_path", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    /**
     * A path is an ordered set of elements. Typically a path is to  a
     * particular offset in a source file. The head of the list is the top
     * most node. The tail is the node that contains the offset directly.
     *
     * For example, the expression `a + b + c` might have an ast that looks
     * like:
     *     +
     *    / \
     *   a   +
     *      / \
     *     b   c
     *
     * The path to the node at offset 9 would be `['+' at 1-10, '+' at 7-10,
     * 'c' at 9-10]` and the path the node at offset 1 would be
     * `['+' at 1-10, 'a' at 1-2]`.
     */
    var AstPath = /** @class */ (function () {
        function AstPath(path, position) {
            if (position === void 0) { position = -1; }
            this.path = path;
            this.position = position;
        }
        Object.defineProperty(AstPath.prototype, "empty", {
            get: function () { return !this.path || !this.path.length; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AstPath.prototype, "head", {
            get: function () { return this.path[0]; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AstPath.prototype, "tail", {
            get: function () { return this.path[this.path.length - 1]; },
            enumerable: true,
            configurable: true
        });
        AstPath.prototype.parentOf = function (node) {
            return node && this.path[this.path.indexOf(node) - 1];
        };
        AstPath.prototype.childOf = function (node) { return this.path[this.path.indexOf(node) + 1]; };
        AstPath.prototype.first = function (ctor) {
            for (var i = this.path.length - 1; i >= 0; i--) {
                var item = this.path[i];
                if (item instanceof ctor)
                    return item;
            }
        };
        AstPath.prototype.push = function (node) { this.path.push(node); };
        AstPath.prototype.pop = function () { return this.path.pop(); };
        return AstPath;
    }());
    exports.AstPath = AstPath;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXN0X3BhdGguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvYXN0X3BhdGgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFFSDs7Ozs7Ozs7Ozs7Ozs7OztPQWdCRztJQUNIO1FBQ0UsaUJBQW9CLElBQVMsRUFBUyxRQUFxQjtZQUFyQix5QkFBQSxFQUFBLFlBQW9CLENBQUM7WUFBdkMsU0FBSSxHQUFKLElBQUksQ0FBSztZQUFTLGFBQVEsR0FBUixRQUFRLENBQWE7UUFBRyxDQUFDO1FBRS9ELHNCQUFJLDBCQUFLO2lCQUFULGNBQXVCLE9BQU8sQ0FBQyxJQUFJLENBQUMsSUFBSSxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDOzs7V0FBQTtRQUNoRSxzQkFBSSx5QkFBSTtpQkFBUixjQUEwQixPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7V0FBQTtRQUNoRCxzQkFBSSx5QkFBSTtpQkFBUixjQUEwQixPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7V0FBQTtRQUVuRSwwQkFBUSxHQUFSLFVBQVMsSUFBaUI7WUFDeEIsT0FBTyxJQUFJLElBQUksSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQztRQUN4RCxDQUFDO1FBQ0QseUJBQU8sR0FBUCxVQUFRLElBQU8sSUFBaUIsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUVoRix1QkFBSyxHQUFMLFVBQW1CLElBQStCO1lBQ2hELEtBQUssSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUU7Z0JBQzlDLElBQUksSUFBSSxHQUFHLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7Z0JBQ3hCLElBQUksSUFBSSxZQUFZLElBQUk7b0JBQUUsT0FBVSxJQUFJLENBQUM7YUFDMUM7UUFDSCxDQUFDO1FBRUQsc0JBQUksR0FBSixVQUFLLElBQU8sSUFBSSxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFFdkMscUJBQUcsR0FBSCxjQUFXLE9BQU8sSUFBSSxDQUFDLElBQUksQ0FBQyxHQUFHLEVBQUksQ0FBQyxDQUFDLENBQUM7UUFDeEMsY0FBQztJQUFELENBQUMsQUF0QkQsSUFzQkM7SUF0QlksMEJBQU8iLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogQSBwYXRoIGlzIGFuIG9yZGVyZWQgc2V0IG9mIGVsZW1lbnRzLiBUeXBpY2FsbHkgYSBwYXRoIGlzIHRvICBhXG4gKiBwYXJ0aWN1bGFyIG9mZnNldCBpbiBhIHNvdXJjZSBmaWxlLiBUaGUgaGVhZCBvZiB0aGUgbGlzdCBpcyB0aGUgdG9wXG4gKiBtb3N0IG5vZGUuIFRoZSB0YWlsIGlzIHRoZSBub2RlIHRoYXQgY29udGFpbnMgdGhlIG9mZnNldCBkaXJlY3RseS5cbiAqXG4gKiBGb3IgZXhhbXBsZSwgdGhlIGV4cHJlc3Npb24gYGEgKyBiICsgY2AgbWlnaHQgaGF2ZSBhbiBhc3QgdGhhdCBsb29rc1xuICogbGlrZTpcbiAqICAgICArXG4gKiAgICAvIFxcXG4gKiAgIGEgICArXG4gKiAgICAgIC8gXFxcbiAqICAgICBiICAgY1xuICpcbiAqIFRoZSBwYXRoIHRvIHRoZSBub2RlIGF0IG9mZnNldCA5IHdvdWxkIGJlIGBbJysnIGF0IDEtMTAsICcrJyBhdCA3LTEwLFxuICogJ2MnIGF0IDktMTBdYCBhbmQgdGhlIHBhdGggdGhlIG5vZGUgYXQgb2Zmc2V0IDEgd291bGQgYmVcbiAqIGBbJysnIGF0IDEtMTAsICdhJyBhdCAxLTJdYC5cbiAqL1xuZXhwb3J0IGNsYXNzIEFzdFBhdGg8VD4ge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHBhdGg6IFRbXSwgcHVibGljIHBvc2l0aW9uOiBudW1iZXIgPSAtMSkge31cblxuICBnZXQgZW1wdHkoKTogYm9vbGVhbiB7IHJldHVybiAhdGhpcy5wYXRoIHx8ICF0aGlzLnBhdGgubGVuZ3RoOyB9XG4gIGdldCBoZWFkKCk6IFR8dW5kZWZpbmVkIHsgcmV0dXJuIHRoaXMucGF0aFswXTsgfVxuICBnZXQgdGFpbCgpOiBUfHVuZGVmaW5lZCB7IHJldHVybiB0aGlzLnBhdGhbdGhpcy5wYXRoLmxlbmd0aCAtIDFdOyB9XG5cbiAgcGFyZW50T2Yobm9kZTogVHx1bmRlZmluZWQpOiBUfHVuZGVmaW5lZCB7XG4gICAgcmV0dXJuIG5vZGUgJiYgdGhpcy5wYXRoW3RoaXMucGF0aC5pbmRleE9mKG5vZGUpIC0gMV07XG4gIH1cbiAgY2hpbGRPZihub2RlOiBUKTogVHx1bmRlZmluZWQgeyByZXR1cm4gdGhpcy5wYXRoW3RoaXMucGF0aC5pbmRleE9mKG5vZGUpICsgMV07IH1cblxuICBmaXJzdDxOIGV4dGVuZHMgVD4oY3Rvcjoge25ldyAoLi4uYXJnczogYW55W10pOiBOfSk6IE58dW5kZWZpbmVkIHtcbiAgICBmb3IgKGxldCBpID0gdGhpcy5wYXRoLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgICBsZXQgaXRlbSA9IHRoaXMucGF0aFtpXTtcbiAgICAgIGlmIChpdGVtIGluc3RhbmNlb2YgY3RvcikgcmV0dXJuIDxOPml0ZW07XG4gICAgfVxuICB9XG5cbiAgcHVzaChub2RlOiBUKSB7IHRoaXMucGF0aC5wdXNoKG5vZGUpOyB9XG5cbiAgcG9wKCk6IFQgeyByZXR1cm4gdGhpcy5wYXRoLnBvcCgpICE7IH1cbn1cbiJdfQ==