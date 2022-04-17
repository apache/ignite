/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * A token representing the a reference to a static type.
 *
 * This token is unique for a filePath and name and can be used as a hash table key.
 */
var StaticSymbol = /** @class */ (function () {
    function StaticSymbol(filePath, name, members) {
        this.filePath = filePath;
        this.name = name;
        this.members = members;
    }
    StaticSymbol.prototype.assertNoMembers = function () {
        if (this.members.length) {
            throw new Error("Illegal state: symbol without members expected, but got " + JSON.stringify(this) + ".");
        }
    };
    return StaticSymbol;
}());
export { StaticSymbol };
/**
 * A cache of static symbol used by the StaticReflector to return the same symbol for the
 * same symbol values.
 */
var StaticSymbolCache = /** @class */ (function () {
    function StaticSymbolCache() {
        this.cache = new Map();
    }
    StaticSymbolCache.prototype.get = function (declarationFile, name, members) {
        members = members || [];
        var memberSuffix = members.length ? "." + members.join('.') : '';
        var key = "\"" + declarationFile + "\"." + name + memberSuffix;
        var result = this.cache.get(key);
        if (!result) {
            result = new StaticSymbol(declarationFile, name, members);
            this.cache.set(key, result);
        }
        return result;
    };
    return StaticSymbolCache;
}());
export { StaticSymbolCache };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RhdGljX3N5bWJvbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9hb3Qvc3RhdGljX3N5bWJvbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSDs7OztHQUlHO0FBQ0g7SUFDRSxzQkFBbUIsUUFBZ0IsRUFBUyxJQUFZLEVBQVMsT0FBaUI7UUFBL0QsYUFBUSxHQUFSLFFBQVEsQ0FBUTtRQUFTLFNBQUksR0FBSixJQUFJLENBQVE7UUFBUyxZQUFPLEdBQVAsT0FBTyxDQUFVO0lBQUcsQ0FBQztJQUV0RixzQ0FBZSxHQUFmO1FBQ0UsSUFBSSxJQUFJLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRTtZQUN2QixNQUFNLElBQUksS0FBSyxDQUNYLDZEQUEyRCxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxNQUFHLENBQUMsQ0FBQztTQUN6RjtJQUNILENBQUM7SUFDSCxtQkFBQztBQUFELENBQUMsQUFURCxJQVNDOztBQUVEOzs7R0FHRztBQUNIO0lBQUE7UUFDVSxVQUFLLEdBQUcsSUFBSSxHQUFHLEVBQXdCLENBQUM7SUFhbEQsQ0FBQztJQVhDLCtCQUFHLEdBQUgsVUFBSSxlQUF1QixFQUFFLElBQVksRUFBRSxPQUFrQjtRQUMzRCxPQUFPLEdBQUcsT0FBTyxJQUFJLEVBQUUsQ0FBQztRQUN4QixJQUFNLFlBQVksR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxNQUFLLE9BQU8sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFHLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztRQUNwRSxJQUFNLEdBQUcsR0FBRyxPQUFJLGVBQWUsV0FBSyxJQUFJLEdBQUcsWUFBYyxDQUFDO1FBQzFELElBQUksTUFBTSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLEdBQUcsQ0FBQyxDQUFDO1FBQ2pDLElBQUksQ0FBQyxNQUFNLEVBQUU7WUFDWCxNQUFNLEdBQUcsSUFBSSxZQUFZLENBQUMsZUFBZSxFQUFFLElBQUksRUFBRSxPQUFPLENBQUMsQ0FBQztZQUMxRCxJQUFJLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxHQUFHLEVBQUUsTUFBTSxDQUFDLENBQUM7U0FDN0I7UUFDRCxPQUFPLE1BQU0sQ0FBQztJQUNoQixDQUFDO0lBQ0gsd0JBQUM7QUFBRCxDQUFDLEFBZEQsSUFjQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBBIHRva2VuIHJlcHJlc2VudGluZyB0aGUgYSByZWZlcmVuY2UgdG8gYSBzdGF0aWMgdHlwZS5cbiAqXG4gKiBUaGlzIHRva2VuIGlzIHVuaXF1ZSBmb3IgYSBmaWxlUGF0aCBhbmQgbmFtZSBhbmQgY2FuIGJlIHVzZWQgYXMgYSBoYXNoIHRhYmxlIGtleS5cbiAqL1xuZXhwb3J0IGNsYXNzIFN0YXRpY1N5bWJvbCB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBmaWxlUGF0aDogc3RyaW5nLCBwdWJsaWMgbmFtZTogc3RyaW5nLCBwdWJsaWMgbWVtYmVyczogc3RyaW5nW10pIHt9XG5cbiAgYXNzZXJ0Tm9NZW1iZXJzKCkge1xuICAgIGlmICh0aGlzLm1lbWJlcnMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYElsbGVnYWwgc3RhdGU6IHN5bWJvbCB3aXRob3V0IG1lbWJlcnMgZXhwZWN0ZWQsIGJ1dCBnb3QgJHtKU09OLnN0cmluZ2lmeSh0aGlzKX0uYCk7XG4gICAgfVxuICB9XG59XG5cbi8qKlxuICogQSBjYWNoZSBvZiBzdGF0aWMgc3ltYm9sIHVzZWQgYnkgdGhlIFN0YXRpY1JlZmxlY3RvciB0byByZXR1cm4gdGhlIHNhbWUgc3ltYm9sIGZvciB0aGVcbiAqIHNhbWUgc3ltYm9sIHZhbHVlcy5cbiAqL1xuZXhwb3J0IGNsYXNzIFN0YXRpY1N5bWJvbENhY2hlIHtcbiAgcHJpdmF0ZSBjYWNoZSA9IG5ldyBNYXA8c3RyaW5nLCBTdGF0aWNTeW1ib2w+KCk7XG5cbiAgZ2V0KGRlY2xhcmF0aW9uRmlsZTogc3RyaW5nLCBuYW1lOiBzdHJpbmcsIG1lbWJlcnM/OiBzdHJpbmdbXSk6IFN0YXRpY1N5bWJvbCB7XG4gICAgbWVtYmVycyA9IG1lbWJlcnMgfHwgW107XG4gICAgY29uc3QgbWVtYmVyU3VmZml4ID0gbWVtYmVycy5sZW5ndGggPyBgLiR7IG1lbWJlcnMuam9pbignLicpfWAgOiAnJztcbiAgICBjb25zdCBrZXkgPSBgXCIke2RlY2xhcmF0aW9uRmlsZX1cIi4ke25hbWV9JHttZW1iZXJTdWZmaXh9YDtcbiAgICBsZXQgcmVzdWx0ID0gdGhpcy5jYWNoZS5nZXQoa2V5KTtcbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgcmVzdWx0ID0gbmV3IFN0YXRpY1N5bWJvbChkZWNsYXJhdGlvbkZpbGUsIG5hbWUsIG1lbWJlcnMpO1xuICAgICAgdGhpcy5jYWNoZS5zZXQoa2V5LCByZXN1bHQpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG59Il19