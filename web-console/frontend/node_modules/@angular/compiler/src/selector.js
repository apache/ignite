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
        define("@angular/compiler/src/selector", ["require", "exports", "@angular/compiler/src/ml_parser/html_tags"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var html_tags_1 = require("@angular/compiler/src/ml_parser/html_tags");
    var _SELECTOR_REGEXP = new RegExp('(\\:not\\()|' + //":not("
        '([-\\w]+)|' + // "tag"
        '(?:\\.([-\\w]+))|' + // ".class"
        // "-" should appear first in the regexp below as FF31 parses "[.-\w]" as a range
        '(?:\\[([-.\\w*]+)(?:=([\"\']?)([^\\]\"\']*)\\5)?\\])|' + // "[name]", "[name=value]",
        // "[name="value"]",
        // "[name='value']"
        '(\\))|' + // ")"
        '(\\s*,\\s*)', // ","
    'g');
    /**
     * A css selector contains an element name,
     * css classes and attribute/value pairs with the purpose
     * of selecting subsets out of them.
     */
    var CssSelector = /** @class */ (function () {
        function CssSelector() {
            this.element = null;
            this.classNames = [];
            /**
             * The selectors are encoded in pairs where:
             * - even locations are attribute names
             * - odd locations are attribute values.
             *
             * Example:
             * Selector: `[key1=value1][key2]` would parse to:
             * ```
             * ['key1', 'value1', 'key2', '']
             * ```
             */
            this.attrs = [];
            this.notSelectors = [];
        }
        CssSelector.parse = function (selector) {
            var results = [];
            var _addResult = function (res, cssSel) {
                if (cssSel.notSelectors.length > 0 && !cssSel.element && cssSel.classNames.length == 0 &&
                    cssSel.attrs.length == 0) {
                    cssSel.element = '*';
                }
                res.push(cssSel);
            };
            var cssSelector = new CssSelector();
            var match;
            var current = cssSelector;
            var inNot = false;
            _SELECTOR_REGEXP.lastIndex = 0;
            while (match = _SELECTOR_REGEXP.exec(selector)) {
                if (match[1]) {
                    if (inNot) {
                        throw new Error('Nesting :not is not allowed in a selector');
                    }
                    inNot = true;
                    current = new CssSelector();
                    cssSelector.notSelectors.push(current);
                }
                if (match[2]) {
                    current.setElement(match[2]);
                }
                if (match[3]) {
                    current.addClassName(match[3]);
                }
                if (match[4]) {
                    current.addAttribute(match[4], match[6]);
                }
                if (match[7]) {
                    inNot = false;
                    current = cssSelector;
                }
                if (match[8]) {
                    if (inNot) {
                        throw new Error('Multiple selectors in :not are not supported');
                    }
                    _addResult(results, cssSelector);
                    cssSelector = current = new CssSelector();
                }
            }
            _addResult(results, cssSelector);
            return results;
        };
        CssSelector.prototype.isElementSelector = function () {
            return this.hasElementSelector() && this.classNames.length == 0 && this.attrs.length == 0 &&
                this.notSelectors.length === 0;
        };
        CssSelector.prototype.hasElementSelector = function () { return !!this.element; };
        CssSelector.prototype.setElement = function (element) {
            if (element === void 0) { element = null; }
            this.element = element;
        };
        /** Gets a template string for an element that matches the selector. */
        CssSelector.prototype.getMatchingElementTemplate = function () {
            var tagName = this.element || 'div';
            var classAttr = this.classNames.length > 0 ? " class=\"" + this.classNames.join(' ') + "\"" : '';
            var attrs = '';
            for (var i = 0; i < this.attrs.length; i += 2) {
                var attrName = this.attrs[i];
                var attrValue = this.attrs[i + 1] !== '' ? "=\"" + this.attrs[i + 1] + "\"" : '';
                attrs += " " + attrName + attrValue;
            }
            return html_tags_1.getHtmlTagDefinition(tagName).isVoid ? "<" + tagName + classAttr + attrs + "/>" :
                "<" + tagName + classAttr + attrs + "></" + tagName + ">";
        };
        CssSelector.prototype.getAttrs = function () {
            var result = [];
            if (this.classNames.length > 0) {
                result.push('class', this.classNames.join(' '));
            }
            return result.concat(this.attrs);
        };
        CssSelector.prototype.addAttribute = function (name, value) {
            if (value === void 0) { value = ''; }
            this.attrs.push(name, value && value.toLowerCase() || '');
        };
        CssSelector.prototype.addClassName = function (name) { this.classNames.push(name.toLowerCase()); };
        CssSelector.prototype.toString = function () {
            var res = this.element || '';
            if (this.classNames) {
                this.classNames.forEach(function (klass) { return res += "." + klass; });
            }
            if (this.attrs) {
                for (var i = 0; i < this.attrs.length; i += 2) {
                    var name_1 = this.attrs[i];
                    var value = this.attrs[i + 1];
                    res += "[" + name_1 + (value ? '=' + value : '') + "]";
                }
            }
            this.notSelectors.forEach(function (notSelector) { return res += ":not(" + notSelector + ")"; });
            return res;
        };
        return CssSelector;
    }());
    exports.CssSelector = CssSelector;
    /**
     * Reads a list of CssSelectors and allows to calculate which ones
     * are contained in a given CssSelector.
     */
    var SelectorMatcher = /** @class */ (function () {
        function SelectorMatcher() {
            this._elementMap = new Map();
            this._elementPartialMap = new Map();
            this._classMap = new Map();
            this._classPartialMap = new Map();
            this._attrValueMap = new Map();
            this._attrValuePartialMap = new Map();
            this._listContexts = [];
        }
        SelectorMatcher.createNotMatcher = function (notSelectors) {
            var notMatcher = new SelectorMatcher();
            notMatcher.addSelectables(notSelectors, null);
            return notMatcher;
        };
        SelectorMatcher.prototype.addSelectables = function (cssSelectors, callbackCtxt) {
            var listContext = null;
            if (cssSelectors.length > 1) {
                listContext = new SelectorListContext(cssSelectors);
                this._listContexts.push(listContext);
            }
            for (var i = 0; i < cssSelectors.length; i++) {
                this._addSelectable(cssSelectors[i], callbackCtxt, listContext);
            }
        };
        /**
         * Add an object that can be found later on by calling `match`.
         * @param cssSelector A css selector
         * @param callbackCtxt An opaque object that will be given to the callback of the `match` function
         */
        SelectorMatcher.prototype._addSelectable = function (cssSelector, callbackCtxt, listContext) {
            var matcher = this;
            var element = cssSelector.element;
            var classNames = cssSelector.classNames;
            var attrs = cssSelector.attrs;
            var selectable = new SelectorContext(cssSelector, callbackCtxt, listContext);
            if (element) {
                var isTerminal = attrs.length === 0 && classNames.length === 0;
                if (isTerminal) {
                    this._addTerminal(matcher._elementMap, element, selectable);
                }
                else {
                    matcher = this._addPartial(matcher._elementPartialMap, element);
                }
            }
            if (classNames) {
                for (var i = 0; i < classNames.length; i++) {
                    var isTerminal = attrs.length === 0 && i === classNames.length - 1;
                    var className = classNames[i];
                    if (isTerminal) {
                        this._addTerminal(matcher._classMap, className, selectable);
                    }
                    else {
                        matcher = this._addPartial(matcher._classPartialMap, className);
                    }
                }
            }
            if (attrs) {
                for (var i = 0; i < attrs.length; i += 2) {
                    var isTerminal = i === attrs.length - 2;
                    var name_2 = attrs[i];
                    var value = attrs[i + 1];
                    if (isTerminal) {
                        var terminalMap = matcher._attrValueMap;
                        var terminalValuesMap = terminalMap.get(name_2);
                        if (!terminalValuesMap) {
                            terminalValuesMap = new Map();
                            terminalMap.set(name_2, terminalValuesMap);
                        }
                        this._addTerminal(terminalValuesMap, value, selectable);
                    }
                    else {
                        var partialMap = matcher._attrValuePartialMap;
                        var partialValuesMap = partialMap.get(name_2);
                        if (!partialValuesMap) {
                            partialValuesMap = new Map();
                            partialMap.set(name_2, partialValuesMap);
                        }
                        matcher = this._addPartial(partialValuesMap, value);
                    }
                }
            }
        };
        SelectorMatcher.prototype._addTerminal = function (map, name, selectable) {
            var terminalList = map.get(name);
            if (!terminalList) {
                terminalList = [];
                map.set(name, terminalList);
            }
            terminalList.push(selectable);
        };
        SelectorMatcher.prototype._addPartial = function (map, name) {
            var matcher = map.get(name);
            if (!matcher) {
                matcher = new SelectorMatcher();
                map.set(name, matcher);
            }
            return matcher;
        };
        /**
         * Find the objects that have been added via `addSelectable`
         * whose css selector is contained in the given css selector.
         * @param cssSelector A css selector
         * @param matchedCallback This callback will be called with the object handed into `addSelectable`
         * @return boolean true if a match was found
        */
        SelectorMatcher.prototype.match = function (cssSelector, matchedCallback) {
            var result = false;
            var element = cssSelector.element;
            var classNames = cssSelector.classNames;
            var attrs = cssSelector.attrs;
            for (var i = 0; i < this._listContexts.length; i++) {
                this._listContexts[i].alreadyMatched = false;
            }
            result = this._matchTerminal(this._elementMap, element, cssSelector, matchedCallback) || result;
            result = this._matchPartial(this._elementPartialMap, element, cssSelector, matchedCallback) ||
                result;
            if (classNames) {
                for (var i = 0; i < classNames.length; i++) {
                    var className = classNames[i];
                    result =
                        this._matchTerminal(this._classMap, className, cssSelector, matchedCallback) || result;
                    result =
                        this._matchPartial(this._classPartialMap, className, cssSelector, matchedCallback) ||
                            result;
                }
            }
            if (attrs) {
                for (var i = 0; i < attrs.length; i += 2) {
                    var name_3 = attrs[i];
                    var value = attrs[i + 1];
                    var terminalValuesMap = this._attrValueMap.get(name_3);
                    if (value) {
                        result =
                            this._matchTerminal(terminalValuesMap, '', cssSelector, matchedCallback) || result;
                    }
                    result =
                        this._matchTerminal(terminalValuesMap, value, cssSelector, matchedCallback) || result;
                    var partialValuesMap = this._attrValuePartialMap.get(name_3);
                    if (value) {
                        result = this._matchPartial(partialValuesMap, '', cssSelector, matchedCallback) || result;
                    }
                    result =
                        this._matchPartial(partialValuesMap, value, cssSelector, matchedCallback) || result;
                }
            }
            return result;
        };
        /** @internal */
        SelectorMatcher.prototype._matchTerminal = function (map, name, cssSelector, matchedCallback) {
            if (!map || typeof name !== 'string') {
                return false;
            }
            var selectables = map.get(name) || [];
            var starSelectables = map.get('*');
            if (starSelectables) {
                selectables = selectables.concat(starSelectables);
            }
            if (selectables.length === 0) {
                return false;
            }
            var selectable;
            var result = false;
            for (var i = 0; i < selectables.length; i++) {
                selectable = selectables[i];
                result = selectable.finalize(cssSelector, matchedCallback) || result;
            }
            return result;
        };
        /** @internal */
        SelectorMatcher.prototype._matchPartial = function (map, name, cssSelector, matchedCallback) {
            if (!map || typeof name !== 'string') {
                return false;
            }
            var nestedSelector = map.get(name);
            if (!nestedSelector) {
                return false;
            }
            // TODO(perf): get rid of recursion and measure again
            // TODO(perf): don't pass the whole selector into the recursion,
            // but only the not processed parts
            return nestedSelector.match(cssSelector, matchedCallback);
        };
        return SelectorMatcher;
    }());
    exports.SelectorMatcher = SelectorMatcher;
    var SelectorListContext = /** @class */ (function () {
        function SelectorListContext(selectors) {
            this.selectors = selectors;
            this.alreadyMatched = false;
        }
        return SelectorListContext;
    }());
    exports.SelectorListContext = SelectorListContext;
    // Store context to pass back selector and context when a selector is matched
    var SelectorContext = /** @class */ (function () {
        function SelectorContext(selector, cbContext, listContext) {
            this.selector = selector;
            this.cbContext = cbContext;
            this.listContext = listContext;
            this.notSelectors = selector.notSelectors;
        }
        SelectorContext.prototype.finalize = function (cssSelector, callback) {
            var result = true;
            if (this.notSelectors.length > 0 && (!this.listContext || !this.listContext.alreadyMatched)) {
                var notMatcher = SelectorMatcher.createNotMatcher(this.notSelectors);
                result = !notMatcher.match(cssSelector, null);
            }
            if (result && callback && (!this.listContext || !this.listContext.alreadyMatched)) {
                if (this.listContext) {
                    this.listContext.alreadyMatched = true;
                }
                callback(this.selector, this.cbContext);
            }
            return result;
        };
        return SelectorContext;
    }());
    exports.SelectorContext = SelectorContext;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VsZWN0b3IuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvc2VsZWN0b3IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFFSCx1RUFBMkQ7SUFFM0QsSUFBTSxnQkFBZ0IsR0FBRyxJQUFJLE1BQU0sQ0FDL0IsY0FBYyxHQUFhLFNBQVM7UUFDaEMsWUFBWSxHQUFXLFFBQVE7UUFDL0IsbUJBQW1CLEdBQUksV0FBVztRQUNsQyxpRkFBaUY7UUFDakYsdURBQXVELEdBQUksNEJBQTRCO1FBQzVCLG9CQUFvQjtRQUNwQixtQkFBbUI7UUFDOUUsUUFBUSxHQUFtRCxNQUFNO1FBQ2pFLGFBQWEsRUFBOEMsTUFBTTtJQUNyRSxHQUFHLENBQUMsQ0FBQztJQUVUOzs7O09BSUc7SUFDSDtRQUFBO1lBQ0UsWUFBTyxHQUFnQixJQUFJLENBQUM7WUFDNUIsZUFBVSxHQUFhLEVBQUUsQ0FBQztZQUMxQjs7Ozs7Ozs7OztlQVVHO1lBQ0gsVUFBSyxHQUFhLEVBQUUsQ0FBQztZQUNyQixpQkFBWSxHQUFrQixFQUFFLENBQUM7UUF3R25DLENBQUM7UUF0R1EsaUJBQUssR0FBWixVQUFhLFFBQWdCO1lBQzNCLElBQU0sT0FBTyxHQUFrQixFQUFFLENBQUM7WUFDbEMsSUFBTSxVQUFVLEdBQUcsVUFBQyxHQUFrQixFQUFFLE1BQW1CO2dCQUN6RCxJQUFJLE1BQU0sQ0FBQyxZQUFZLENBQUMsTUFBTSxHQUFHLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxPQUFPLElBQUksTUFBTSxDQUFDLFVBQVUsQ0FBQyxNQUFNLElBQUksQ0FBQztvQkFDbEYsTUFBTSxDQUFDLEtBQUssQ0FBQyxNQUFNLElBQUksQ0FBQyxFQUFFO29CQUM1QixNQUFNLENBQUMsT0FBTyxHQUFHLEdBQUcsQ0FBQztpQkFDdEI7Z0JBQ0QsR0FBRyxDQUFDLElBQUksQ0FBQyxNQUFN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IEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2dldEh0bWxUYWdEZWZpbml0aW9ufSBmcm9tICcuL21sX3BhcnNlci9odG1sX3RhZ3MnO1xuXG5jb25zdCBfU0VMRUNUT1JfUkVHRVhQID0gbmV3IFJlZ0V4cChcbiAgICAnKFxcXFw6bm90XFxcXCgpfCcgKyAgICAgICAgICAgLy9cIjpub3QoXCJcbiAgICAgICAgJyhbLVxcXFx3XSspfCcgKyAgICAgICAgIC8vIFwidGFnXCJcbiAgICAgICAgJyg/OlxcXFwuKFstXFxcXHddKykpfCcgKyAgLy8gXCIuY2xhc3NcIlxuICAgICAgICAvLyBcIi1cIiBzaG91bGQgYXBwZWFyIGZpcnN0IGluIHRoZSByZWdleHAgYmVsb3cgYXMgRkYzMSBwYXJzZXMgXCJbLi1cXHddXCIgYXMgYSByYW5nZVxuICAgICAgICAnKD86XFxcXFsoWy0uXFxcXHcqXSspKD86PShbXFxcIlxcJ10/KShbXlxcXFxdXFxcIlxcJ10qKVxcXFw1KT9cXFxcXSl8JyArICAvLyBcIltuYW1lXVwiLCBcIltuYW1lPXZhbHVlXVwiLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIFwiW25hbWU9XCJ2YWx1ZVwiXVwiLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIFwiW25hbWU9J3ZhbHVlJ11cIlxuICAgICAgICAnKFxcXFwpKXwnICsgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gXCIpXCJcbiAgICAgICAgJyhcXFxccyosXFxcXHMqKScsICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gXCIsXCJcbiAgICAnZycpO1xuXG4vKipcbiAqIEEgY3NzIHNlbGVjdG9yIGNvbnRhaW5zIGFuIGVsZW1lbnQgbmFtZSxcbiAqIGNzcyBjbGFzc2VzIGFuZCBhdHRyaWJ1dGUvdmFsdWUgcGFpcnMgd2l0aCB0aGUgcHVycG9zZVxuICogb2Ygc2VsZWN0aW5nIHN1YnNldHMgb3V0IG9mIHRoZW0uXG4gKi9cbmV4cG9ydCBjbGFzcyBDc3NTZWxlY3RvciB7XG4gIGVsZW1lbnQ6IHN0cmluZ3xudWxsID0gbnVsbDtcbiAgY2xhc3NOYW1lczogc3RyaW5nW10gPSBbXTtcbiAgLyoqXG4gICAqIFRoZSBzZWxlY3RvcnMgYXJlIGVuY29kZWQgaW4gcGFpcnMgd2hlcmU6XG4gICAqIC0gZXZlbiBsb2NhdGlvbnMgYXJlIGF0dHJpYnV0ZSBuYW1lc1xuICAgKiAtIG9kZCBsb2NhdGlvbnMgYXJlIGF0dHJpYnV0ZSB2YWx1ZXMuXG4gICAqXG4gICAqIEV4YW1wbGU6XG4gICAqIFNlbGVjdG9yOiBgW2tleTE9dmFsdWUxXVtrZXkyXWAgd291bGQgcGFyc2UgdG86XG4gICAqIGBgYFxuICAgKiBbJ2tleTEnLCAndmFsdWUxJywgJ2tleTInLCAnJ11cbiAgICogYGBgXG4gICAqL1xuICBhdHRyczogc3RyaW5nW10gPSBbXTtcbiAgbm90U2VsZWN0b3JzOiBDc3NTZWxlY3RvcltdID0gW107XG5cbiAgc3RhdGljIHBhcnNlKHNlbGVjdG9yOiBzdHJpbmcpOiBDc3NTZWxlY3RvcltdIHtcbiAgICBjb25zdCByZXN1bHRzOiBDc3NTZWxlY3RvcltdID0gW107XG4gICAgY29uc3QgX2FkZFJlc3VsdCA9IChyZXM6IENzc1NlbGVjdG9yW10sIGNzc1NlbDogQ3NzU2VsZWN0b3IpID0+IHtcbiAgICAgIGlmIChjc3NTZWwubm90U2VsZWN0b3JzLmxlbmd0aCA+IDAgJiYgIWNzc1NlbC5lbGVtZW50ICYmIGNzc1NlbC5jbGFzc05hbWVzLmxlbmd0aCA9PSAwICYmXG4gICAgICAgICAgY3NzU2VsLmF0dHJzLmxlbmd0aCA9PSAwKSB7XG4gICAgICAgIGNzc1NlbC5lbGVtZW50ID0gJyonO1xuICAgICAgfVxuICAgICAgcmVzLnB1c2goY3NzU2VsKTtcbiAgICB9O1xuICAgIGxldCBjc3NTZWxlY3RvciA9IG5ldyBDc3NTZWxlY3RvcigpO1xuICAgIGxldCBtYXRjaDogc3RyaW5nW118bnVsbDtcbiAgICBsZXQgY3VycmVudCA9IGNzc1NlbGVjdG9yO1xuICAgIGxldCBpbk5vdCA9IGZhbHNlO1xuICAgIF9TRUxFQ1RPUl9SRUdFWFAubGFzdEluZGV4ID0gMDtcbiAgICB3aGlsZSAobWF0Y2ggPSBfU0VMRUNUT1JfUkVHRVhQLmV4ZWMoc2VsZWN0b3IpKSB7XG4gICAgICBpZiAobWF0Y2hbMV0pIHtcbiAgICAgICAgaWYgKGluTm90KSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdOZXN0aW5nIDpub3QgaXMgbm90IGFsbG93ZWQgaW4gYSBzZWxlY3RvcicpO1xuICAgICAgICB9XG4gICAgICAgIGluTm90ID0gdHJ1ZTtcbiAgICAgICAgY3VycmVudCA9IG5ldyBDc3NTZWxlY3RvcigpO1xuICAgICAgICBjc3NTZWxlY3Rvci5ub3RTZWxlY3RvcnMucHVzaChjdXJyZW50KTtcbiAgICAgIH1cbiAgICAgIGlmIChtYXRjaFsyXSkge1xuICAgICAgICBjdXJyZW50LnNldEVsZW1lbnQobWF0Y2hbMl0pO1xuICAgICAgfVxuICAgICAgaWYgKG1hdGNoWzNdKSB7XG4gICAgICAgIGN1cnJlbnQuYWRkQ2xhc3NOYW1lKG1hdGNoWzNdKTtcbiAgICAgIH1cbiAgICAgIGlmIChtYXRjaFs0XSkge1xuICAgICAgICBjdXJyZW50LmFkZEF0dHJpYnV0ZShtYXRjaFs0XSwgbWF0Y2hbNl0pO1xuICAgICAgfVxuICAgICAgaWYgKG1hdGNoWzddKSB7XG4gICAgICAgIGluTm90ID0gZmFsc2U7XG4gICAgICAgIGN1cnJlbnQgPSBjc3NTZWxlY3RvcjtcbiAgICAgIH1cbiAgICAgIGlmIChtYXRjaFs4XSkge1xuICAgICAgICBpZiAoaW5Ob3QpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ011bHRpcGxlIHNlbGVjdG9ycyBpbiA6bm90IGFyZSBub3Qgc3VwcG9ydGVkJyk7XG4gICAgICAgIH1cbiAgICAgICAgX2FkZFJlc3VsdChyZXN1bHRzLCBjc3NTZWxlY3Rvcik7XG4gICAgICAgIGNzc1NlbGVjdG9yID0gY3VycmVudCA9IG5ldyBDc3NTZWxlY3RvcigpO1xuICAgICAgfVxuICAgIH1cbiAgICBfYWRkUmVzdWx0KHJlc3VsdHMsIGNzc1NlbGVjdG9yKTtcbiAgICByZXR1cm4gcmVzdWx0cztcbiAgfVxuXG4gIGlzRWxlbWVudFNlbGVjdG9yKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLmhhc0VsZW1lbnRTZWxlY3RvcigpICYmIHRoaXMuY2xhc3NOYW1lcy5sZW5ndGggPT0gMCAmJiB0aGlzLmF0dHJzLmxlbmd0aCA9PSAwICYmXG4gICAgICAgIHRoaXMubm90U2VsZWN0b3JzLmxlbmd0aCA9PT0gMDtcbiAgfVxuXG4gIGhhc0VsZW1lbnRTZWxlY3RvcigpOiBib29sZWFuIHsgcmV0dXJuICEhdGhpcy5lbGVtZW50OyB9XG5cbiAgc2V0RWxlbWVudChlbGVtZW50OiBzdHJpbmd8bnVsbCA9IG51bGwpIHsgdGhpcy5lbGVtZW50ID0gZWxlbWVudDsgfVxuXG4gIC8qKiBHZXRzIGEgdGVtcGxhdGUgc3RyaW5nIGZvciBhbiBlbGVtZW50IHRoYXQgbWF0Y2hlcyB0aGUgc2VsZWN0b3IuICovXG4gIGdldE1hdGNoaW5nRWxlbWVudFRlbXBsYXRlKCk6IHN0cmluZyB7XG4gICAgY29uc3QgdGFnTmFtZSA9IHRoaXMuZWxlbWVudCB8fCAnZGl2JztcbiAgICBjb25zdCBjbGFzc0F0dHIgPSB0aGlzLmNsYXNzTmFtZXMubGVuZ3RoID4gMCA/IGAgY2xhc3M9XCIke3RoaXMuY2xhc3NOYW1lcy5qb2luKCcgJyl9XCJgIDogJyc7XG5cbiAgICBsZXQgYXR0cnMgPSAnJztcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHRoaXMuYXR0cnMubGVuZ3RoOyBpICs9IDIpIHtcbiAgICAgIGNvbnN0IGF0dHJOYW1lID0gdGhpcy5hdHRyc1tpXTtcbiAgICAgIGNvbnN0IGF0dHJWYWx1ZSA9IHRoaXMuYXR0cnNbaSArIDFdICE9PSAnJyA/IGA9XCIke3RoaXMuYXR0cnNbaSArIDFdfVwiYCA6ICcnO1xuICAgICAgYXR0cnMgKz0gYCAke2F0dHJOYW1lfSR7YXR0clZhbHVlfWA7XG4gICAgfVxuXG4gICAgcmV0dXJuIGdldEh0bWxUYWdEZWZpbml0aW9uKHRhZ05hbWUpLmlzVm9pZCA/IGA8JHt0YWdOYW1lfSR7Y2xhc3NBdHRyfSR7YXR0cnN9Lz5gIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYDwke3RhZ05hbWV9JHtjbGFzc0F0dHJ9JHthdHRyc30+PC8ke3RhZ05hbWV9PmA7XG4gIH1cblxuICBnZXRBdHRycygpOiBzdHJpbmdbXSB7XG4gICAgY29uc3QgcmVzdWx0OiBzdHJpbmdbXSA9IFtdO1xuICAgIGlmICh0aGlzLmNsYXNzTmFtZXMubGVuZ3RoID4gMCkge1xuICAgICAgcmVzdWx0LnB1c2goJ2NsYXNzJywgdGhpcy5jbGFzc05hbWVzLmpvaW4oJyAnKSk7XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQuY29uY2F0KHRoaXMuYXR0cnMpO1xuICB9XG5cbiAgYWRkQXR0cmlidXRlKG5hbWU6IHN0cmluZywgdmFsdWU6IHN0cmluZyA9ICcnKSB7XG4gICAgdGhpcy5hdHRycy5wdXNoKG5hbWUsIHZhbHVlICYmIHZhbHVlLnRvTG93ZXJDYXNlKCkgfHwgJycpO1xuICB9XG5cbiAgYWRkQ2xhc3NOYW1lKG5hbWU6IHN0cmluZykgeyB0aGlzLmNsYXNzTmFtZXMucHVzaChuYW1lLnRvTG93ZXJDYXNlKCkpOyB9XG5cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICBsZXQgcmVzOiBzdHJpbmcgPSB0aGlzLmVsZW1lbnQgfHwgJyc7XG4gICAgaWYgKHRoaXMuY2xhc3NOYW1lcykge1xuICAgICAgdGhpcy5jbGFzc05hbWVzLmZvckVhY2goa2xhc3MgPT4gcmVzICs9IGAuJHtrbGFzc31gKTtcbiAgICB9XG4gICAgaWYgKHRoaXMuYXR0cnMpIHtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdGhpcy5hdHRycy5sZW5ndGg7IGkgKz0gMikge1xuICAgICAgICBjb25zdCBuYW1lID0gdGhpcy5hdHRyc1tpXTtcbiAgICAgICAgY29uc3QgdmFsdWUgPSB0aGlzLmF0dHJzW2kgKyAxXTtcbiAgICAgICAgcmVzICs9IGBbJHtuYW1lfSR7dmFsdWUgPyAnPScgKyB2YWx1ZSA6ICcnfV1gO1xuICAgICAgfVxuICAgIH1cbiAgICB0aGlzLm5vdFNlbGVjdG9ycy5mb3JFYWNoKG5vdFNlbGVjdG9yID0+IHJlcyArPSBgOm5vdCgke25vdFNlbGVjdG9yfSlgKTtcbiAgICByZXR1cm4gcmVzO1xuICB9XG59XG5cbi8qKlxuICogUmVhZHMgYSBsaXN0IG9mIENzc1NlbGVjdG9ycyBhbmQgYWxsb3dzIHRvIGNhbGN1bGF0ZSB3aGljaCBvbmVzXG4gKiBhcmUgY29udGFpbmVkIGluIGEgZ2l2ZW4gQ3NzU2VsZWN0b3IuXG4gKi9cbmV4cG9ydCBjbGFzcyBTZWxlY3Rvck1hdGNoZXI8VCA9IGFueT4ge1xuICBzdGF0aWMgY3JlYXRlTm90TWF0Y2hlcihub3RTZWxlY3RvcnM6IENzc1NlbGVjdG9yW10pOiBTZWxlY3Rvck1hdGNoZXI8bnVsbD4ge1xuICAgIGNvbnN0IG5vdE1hdGNoZXIgPSBuZXcgU2VsZWN0b3JNYXRjaGVyPG51bGw+KCk7XG4gICAgbm90TWF0Y2hlci5hZGRTZWxlY3RhYmxlcyhub3RTZWxlY3RvcnMsIG51bGwpO1xuICAgIHJldHVybiBub3RNYXRjaGVyO1xuICB9XG5cbiAgcHJpdmF0ZSBfZWxlbWVudE1hcCA9IG5ldyBNYXA8c3RyaW5nLCBTZWxlY3RvckNvbnRleHQ8VD5bXT4oKTtcbiAgcHJpdmF0ZSBfZWxlbWVudFBhcnRpYWxNYXAgPSBuZXcgTWFwPHN0cmluZywgU2VsZWN0b3JNYXRjaGVyPFQ+PigpO1xuICBwcml2YXRlIF9jbGFzc01hcCA9IG5ldyBNYXA8c3RyaW5nLCBTZWxlY3RvckNvbnRleHQ8VD5bXT4oKTtcbiAgcHJpdmF0ZSBfY2xhc3NQYXJ0aWFsTWFwID0gbmV3IE1hcDxzdHJpbmcsIFNlbGVjdG9yTWF0Y2hlcjxUPj4oKTtcbiAgcHJpdmF0ZSBfYXR0clZhbHVlTWFwID0gbmV3IE1hcDxzdHJpbmcsIE1hcDxzdHJpbmcsIFNlbGVjdG9yQ29udGV4dDxUPltdPj4oKTtcbiAgcHJpdmF0ZSBfYXR0clZhbHVlUGFydGlhbE1hcCA9IG5ldyBNYXA8c3RyaW5nLCBNYXA8c3RyaW5nLCBTZWxlY3Rvck1hdGNoZXI8VD4+PigpO1xuICBwcml2YXRlIF9saXN0Q29udGV4dHM6IFNlbGVjdG9yTGlzdENvbnRleHRbXSA9IFtdO1xuXG4gIGFkZFNlbGVjdGFibGVzKGNzc1NlbGVjdG9yczogQ3NzU2VsZWN0b3JbXSwgY2FsbGJhY2tDdHh0PzogVCkge1xuICAgIGxldCBsaXN0Q29udGV4dDogU2VsZWN0b3JMaXN0Q29udGV4dCA9IG51bGwgITtcbiAgICBpZiAoY3NzU2VsZWN0b3JzLmxlbmd0aCA+IDEpIHtcbiAgICAgIGxpc3RDb250ZXh0ID0gbmV3IFNlbGVjdG9yTGlzdENvbnRleHQoY3NzU2VsZWN0b3JzKTtcbiAgICAgIHRoaXMuX2xpc3RDb250ZXh0cy5wdXNoKGxpc3RDb250ZXh0KTtcbiAgICB9XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjc3NTZWxlY3RvcnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHRoaXMuX2FkZFNlbGVjdGFibGUoY3NzU2VsZWN0b3JzW2ldLCBjYWxsYmFja0N0eHQgYXMgVCwgbGlzdENvbnRleHQpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBBZGQgYW4gb2JqZWN0IHRoYXQgY2FuIGJlIGZvdW5kIGxhdGVyIG9uIGJ5IGNhbGxpbmcgYG1hdGNoYC5cbiAgICogQHBhcmFtIGNzc1NlbGVjdG9yIEEgY3NzIHNlbGVjdG9yXG4gICAqIEBwYXJhbSBjYWxsYmFja0N0eHQgQW4gb3BhcXVlIG9iamVjdCB0aGF0IHdpbGwgYmUgZ2l2ZW4gdG8gdGhlIGNhbGxiYWNrIG9mIHRoZSBgbWF0Y2hgIGZ1bmN0aW9uXG4gICAqL1xuICBwcml2YXRlIF9hZGRTZWxlY3RhYmxlKFxuICAgICAgY3NzU2VsZWN0b3I6IENzc1NlbGVjdG9yLCBjYWxsYmFja0N0eHQ6IFQsIGxpc3RDb250ZXh0OiBTZWxlY3Rvckxpc3RDb250ZXh0KSB7XG4gICAgbGV0IG1hdGNoZXI6IFNlbGVjdG9yTWF0Y2hlcjxUPiA9IHRoaXM7XG4gICAgY29uc3QgZWxlbWVudCA9IGNzc1NlbGVjdG9yLmVsZW1lbnQ7XG4gICAgY29uc3QgY2xhc3NOYW1lcyA9IGNzc1NlbGVjdG9yLmNsYXNzTmFtZXM7XG4gICAgY29uc3QgYXR0cnMgPSBjc3NTZWxlY3Rvci5hdHRycztcbiAgICBjb25zdCBzZWxlY3RhYmxlID0gbmV3IFNlbGVjdG9yQ29udGV4dChjc3NTZWxlY3RvciwgY2FsbGJhY2tDdHh0LCBsaXN0Q29udGV4dCk7XG5cbiAgICBpZiAoZWxlbWVudCkge1xuICAgICAgY29uc3QgaXNUZXJtaW5hbCA9IGF0dHJzLmxlbmd0aCA9PT0gMCAmJiBjbGFzc05hbWVzLmxlbmd0aCA9PT0gMDtcbiAgICAgIGlmIChpc1Rlcm1pbmFsKSB7XG4gICAgICAgIHRoaXMuX2FkZFRlcm1pbmFsKG1hdGNoZXIuX2VsZW1lbnRNYXAsIGVsZW1lbnQsIHNlbGVjdGFibGUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbWF0Y2hlciA9IHRoaXMuX2FkZFBhcnRpYWwobWF0Y2hlci5fZWxlbWVudFBhcnRpYWxNYXAsIGVsZW1lbnQpO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChjbGFzc05hbWVzKSB7XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsYXNzTmFtZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgY29uc3QgaXNUZXJtaW5hbCA9IGF0dHJzLmxlbmd0aCA9PT0gMCAmJiBpID09PSBjbGFzc05hbWVzLmxlbmd0aCAtIDE7XG4gICAgICAgIGNvbnN0IGNsYXNzTmFtZSA9IGNsYXNzTmFtZXNbaV07XG4gICAgICAgIGlmIChpc1Rlcm1pbmFsKSB7XG4gICAgICAgICAgdGhpcy5fYWRkVGVybWluYWwobWF0Y2hlci5fY2xhc3NNYXAsIGNsYXNzTmFtZSwgc2VsZWN0YWJsZSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbWF0Y2hlciA9IHRoaXMuX2FkZFBhcnRpYWwobWF0Y2hlci5fY2xhc3NQYXJ0aWFsTWFwLCBjbGFzc05hbWUpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGF0dHJzKSB7XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGF0dHJzLmxlbmd0aDsgaSArPSAyKSB7XG4gICAgICAgIGNvbnN0IGlzVGVybWluYWwgPSBpID09PSBhdHRycy5sZW5ndGggLSAyO1xuICAgICAgICBjb25zdCBuYW1lID0gYXR0cnNbaV07XG4gICAgICAgIGNvbnN0IHZhbHVlID0gYXR0cnNbaSArIDFdO1xuICAgICAgICBpZiAoaXNUZXJtaW5hbCkge1xuICAgICAgICAgIGNvbnN0IHRlcm1pbmFsTWFwID0gbWF0Y2hlci5fYXR0clZhbHVlTWFwO1xuICAgICAgICAgIGxldCB0ZXJtaW5hbFZhbHVlc01hcCA9IHRlcm1pbmFsTWFwLmdldChuYW1lKTtcbiAgICAgICAgICBpZiAoIXRlcm1pbmFsVmFsdWVzTWFwKSB7XG4gICAgICAgICAgICB0ZXJtaW5hbFZhbHVlc01hcCA9IG5ldyBNYXA8c3RyaW5nLCBTZWxlY3RvckNvbnRleHQ8VD5bXT4oKTtcbiAgICAgICAgICAgIHRlcm1pbmFsTWFwLnNldChuYW1lLCB0ZXJtaW5hbFZhbHVlc01hcCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHRoaXMuX2FkZFRlcm1pbmFsKHRlcm1pbmFsVmFsdWVzTWFwLCB2YWx1ZSwgc2VsZWN0YWJsZSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc3QgcGFydGlhbE1hcCA9IG1hdGNoZXIuX2F0dHJWYWx1ZVBhcnRpYWxNYXA7XG4gICAgICAgICAgbGV0IHBhcnRpYWxWYWx1ZXNNYXAgPSBwYXJ0aWFsTWFwLmdldChuYW1lKTtcbiAgICAgICAgICBpZiAoIXBhcnRpYWxWYWx1ZXNNYXApIHtcbiAgICAgICAgICAgIHBhcnRpYWxWYWx1ZXNNYXAgPSBuZXcgTWFwPHN0cmluZywgU2VsZWN0b3JNYXRjaGVyPFQ+PigpO1xuICAgICAgICAgICAgcGFydGlhbE1hcC5zZXQobmFtZSwgcGFydGlhbFZhbHVlc01hcCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIG1hdGNoZXIgPSB0aGlzLl9hZGRQYXJ0aWFsKHBhcnRpYWxWYWx1ZXNNYXAsIHZhbHVlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgX2FkZFRlcm1pbmFsKFxuICAgICAgbWFwOiBNYXA8c3RyaW5nLCBTZWxlY3RvckNvbnRleHQ8VD5bXT4sIG5hbWU6IHN0cmluZywgc2VsZWN0YWJsZTogU2VsZWN0b3JDb250ZXh0PFQ+KSB7XG4gICAgbGV0IHRlcm1pbmFsTGlzdCA9IG1hcC5nZXQobmFtZSk7XG4gICAgaWYgKCF0ZXJtaW5hbExpc3QpIHtcbiAgICAgIHRlcm1pbmFsTGlzdCA9IFtdO1xuICAgICAgbWFwLnNldChuYW1lLCB0ZXJtaW5hbExpc3QpO1xuICAgIH1cbiAgICB0ZXJtaW5hbExpc3QucHVzaChzZWxlY3RhYmxlKTtcbiAgfVxuXG4gIHByaXZhdGUgX2FkZFBhcnRpYWwobWFwOiBNYXA8c3RyaW5nLCBTZWxlY3Rvck1hdGNoZXI8VD4+LCBuYW1lOiBzdHJpbmcpOiBTZWxlY3Rvck1hdGNoZXI8VD4ge1xuICAgIGxldCBtYXRjaGVyID0gbWFwLmdldChuYW1lKTtcbiAgICBpZiAoIW1hdGNoZXIpIHtcbiAgICAgIG1hdGNoZXIgPSBuZXcgU2VsZWN0b3JNYXRjaGVyPFQ+KCk7XG4gICAgICBtYXAuc2V0KG5hbWUsIG1hdGNoZXIpO1xuICAgIH1cbiAgICByZXR1cm4gbWF0Y2hlcjtcbiAgfVxuXG4gIC8qKlxuICAgKiBGaW5kIHRoZSBvYmplY3RzIHRoYXQgaGF2ZSBiZWVuIGFkZGVkIHZpYSBgYWRkU2VsZWN0YWJsZWBcbiAgICogd2hvc2UgY3NzIHNlbGVjdG9yIGlzIGNvbnRhaW5lZCBpbiB0aGUgZ2l2ZW4gY3NzIHNlbGVjdG9yLlxuICAgKiBAcGFyYW0gY3NzU2VsZWN0b3IgQSBjc3Mgc2VsZWN0b3JcbiAgICogQHBhcmFtIG1hdGNoZWRDYWxsYmFjayBUaGlzIGNhbGxiYWNrIHdpbGwgYmUgY2FsbGVkIHdpdGggdGhlIG9iamVjdCBoYW5kZWQgaW50byBgYWRkU2VsZWN0YWJsZWBcbiAgICogQHJldHVybiBib29sZWFuIHRydWUgaWYgYSBtYXRjaCB3YXMgZm91bmRcbiAgKi9cbiAgbWF0Y2goY3NzU2VsZWN0b3I6IENzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2s6ICgoYzogQ3NzU2VsZWN0b3IsIGE6IFQpID0+IHZvaWQpfG51bGwpOiBib29sZWFuIHtcbiAgICBsZXQgcmVzdWx0ID0gZmFsc2U7XG4gICAgY29uc3QgZWxlbWVudCA9IGNzc1NlbGVjdG9yLmVsZW1lbnQgITtcbiAgICBjb25zdCBjbGFzc05hbWVzID0gY3NzU2VsZWN0b3IuY2xhc3NOYW1lcztcbiAgICBjb25zdCBhdHRycyA9IGNzc1NlbGVjdG9yLmF0dHJzO1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLl9saXN0Q29udGV4dHMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHRoaXMuX2xpc3RDb250ZXh0c1tpXS5hbHJlYWR5TWF0Y2hlZCA9IGZhbHNlO1xuICAgIH1cblxuICAgIHJlc3VsdCA9IHRoaXMuX21hdGNoVGVybWluYWwodGhpcy5fZWxlbWVudE1hcCwgZWxlbWVudCwgY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHwgcmVzdWx0O1xuICAgIHJlc3VsdCA9IHRoaXMuX21hdGNoUGFydGlhbCh0aGlzLl9lbGVtZW50UGFydGlhbE1hcCwgZWxlbWVudCwgY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHxcbiAgICAgICAgcmVzdWx0O1xuXG4gICAgaWYgKGNsYXNzTmFtZXMpIHtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2xhc3NOYW1lcy5sZW5ndGg7IGkrKykge1xuICAgICAgICBjb25zdCBjbGFzc05hbWUgPSBjbGFzc05hbWVzW2ldO1xuICAgICAgICByZXN1bHQgPVxuICAgICAgICAgICAgdGhpcy5fbWF0Y2hUZXJtaW5hbCh0aGlzLl9jbGFzc01hcCwgY2xhc3NOYW1lLCBjc3NTZWxlY3RvciwgbWF0Y2hlZENhbGxiYWNrKSB8fCByZXN1bHQ7XG4gICAgICAgIHJlc3VsdCA9XG4gICAgICAgICAgICB0aGlzLl9tYXRjaFBhcnRpYWwodGhpcy5fY2xhc3NQYXJ0aWFsTWFwLCBjbGFzc05hbWUsIGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spIHx8XG4gICAgICAgICAgICByZXN1bHQ7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGF0dHJzKSB7XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGF0dHJzLmxlbmd0aDsgaSArPSAyKSB7XG4gICAgICAgIGNvbnN0IG5hbWUgPSBhdHRyc1tpXTtcbiAgICAgICAgY29uc3QgdmFsdWUgPSBhdHRyc1tpICsgMV07XG5cbiAgICAgICAgY29uc3QgdGVybWluYWxWYWx1ZXNNYXAgPSB0aGlzLl9hdHRyVmFsdWVNYXAuZ2V0KG5hbWUpICE7XG4gICAgICAgIGlmICh2YWx1ZSkge1xuICAgICAgICAgIHJlc3VsdCA9XG4gICAgICAgICAgICAgIHRoaXMuX21hdGNoVGVybWluYWwodGVybWluYWxWYWx1ZXNNYXAsICcnLCBjc3NTZWxlY3RvciwgbWF0Y2hlZENhbGxiYWNrKSB8fCByZXN1bHQ7XG4gICAgICAgIH1cbiAgICAgICAgcmVzdWx0ID1cbiAgICAgICAgICAgIHRoaXMuX21hdGNoVGVybWluYWwodGVybWluYWxWYWx1ZXNNYXAsIHZhbHVlLCBjc3NTZWxlY3RvciwgbWF0Y2hlZENhbGxiYWNrKSB8fCByZXN1bHQ7XG5cbiAgICAgICAgY29uc3QgcGFydGlhbFZhbHVlc01hcCA9IHRoaXMuX2F0dHJWYWx1ZVBhcnRpYWxNYXAuZ2V0KG5hbWUpICE7XG4gICAgICAgIGlmICh2YWx1ZSkge1xuICAgICAgICAgIHJlc3VsdCA9IHRoaXMuX21hdGNoUGFydGlhbChwYXJ0aWFsVmFsdWVzTWFwLCAnJywgY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHwgcmVzdWx0O1xuICAgICAgICB9XG4gICAgICAgIHJlc3VsdCA9XG4gICAgICAgICAgICB0aGlzLl9tYXRjaFBhcnRpYWwocGFydGlhbFZhbHVlc01hcCwgdmFsdWUsIGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spIHx8IHJlc3VsdDtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX21hdGNoVGVybWluYWwoXG4gICAgICBtYXA6IE1hcDxzdHJpbmcsIFNlbGVjdG9yQ29udGV4dDxUPltdPiwgbmFtZTogc3RyaW5nLCBjc3NTZWxlY3RvcjogQ3NzU2VsZWN0b3IsXG4gICAgICBtYXRjaGVkQ2FsbGJhY2s6ICgoYzogQ3NzU2VsZWN0b3IsIGE6IGFueSkgPT4gdm9pZCl8bnVsbCk6IGJvb2xlYW4ge1xuICAgIGlmICghbWFwIHx8IHR5cGVvZiBuYW1lICE9PSAnc3RyaW5nJykge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cblxuICAgIGxldCBzZWxlY3RhYmxlczogU2VsZWN0b3JDb250ZXh0PFQ+W10gPSBtYXAuZ2V0KG5hbWUpIHx8IFtdO1xuICAgIGNvbnN0IHN0YXJTZWxlY3RhYmxlczogU2VsZWN0b3JDb250ZXh0PFQ+W10gPSBtYXAuZ2V0KCcqJykgITtcbiAgICBpZiAoc3RhclNlbGVjdGFibGVzKSB7XG4gICAgICBzZWxlY3RhYmxlcyA9IHNlbGVjdGFibGVzLmNvbmNhdChzdGFyU2VsZWN0YWJsZXMpO1xuICAgIH1cbiAgICBpZiAoc2VsZWN0YWJsZXMubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICAgIGxldCBzZWxlY3RhYmxlOiBTZWxlY3RvckNvbnRleHQ8VD47XG4gICAgbGV0IHJlc3VsdCA9IGZhbHNlO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgc2VsZWN0YWJsZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHNlbGVjdGFibGUgPSBzZWxlY3RhYmxlc1tpXTtcbiAgICAgIHJlc3VsdCA9IHNlbGVjdGFibGUuZmluYWxpemUoY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHwgcmVzdWx0O1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfbWF0Y2hQYXJ0aWFsKFxuICAgICAgbWFwOiBNYXA8c3RyaW5nLCBTZWxlY3Rvck1hdGNoZXI8VD4+LCBuYW1lOiBzdHJpbmcsIGNzc1NlbGVjdG9yOiBDc3NTZWxlY3RvcixcbiAgICAgIG1hdGNoZWRDYWxsYmFjazogKChjOiBDc3NTZWxlY3RvciwgYTogYW55KSA9PiB2b2lkKXxudWxsKTogYm9vbGVhbiB7XG4gICAgaWYgKCFtYXAgfHwgdHlwZW9mIG5hbWUgIT09ICdzdHJpbmcnKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuXG4gICAgY29uc3QgbmVzdGVkU2VsZWN0b3IgPSBtYXAuZ2V0KG5hbWUpO1xuICAgIGlmICghbmVzdGVkU2VsZWN0b3IpIHtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gICAgLy8gVE9ETyhwZXJmKTogZ2V0IHJpZCBvZiByZWN1cnNpb24gYW5kIG1lYXN1cmUgYWdhaW5cbiAgICAvLyBUT0RPKHBlcmYpOiBkb24ndCBwYXNzIHRoZSB3aG9sZSBzZWxlY3RvciBpbnRvIHRoZSByZWN1cnNpb24sXG4gICAgLy8gYnV0IG9ubHkgdGhlIG5vdCBwcm9jZXNzZWQgcGFydHNcbiAgICByZXR1cm4gbmVzdGVkU2VsZWN0b3IubWF0Y2goY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjayk7XG4gIH1cbn1cblxuXG5leHBvcnQgY2xhc3MgU2VsZWN0b3JMaXN0Q29udGV4dCB7XG4gIGFscmVhZHlNYXRjaGVkOiBib29sZWFuID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IocHVibGljIHNlbGVjdG9yczogQ3NzU2VsZWN0b3JbXSkge31cbn1cblxuLy8gU3RvcmUgY29udGV4dCB0byBwYXNzIGJhY2sgc2VsZWN0b3IgYW5kIGNvbnRleHQgd2hlbiBhIHNlbGVjdG9yIGlzIG1hdGNoZWRcbmV4cG9ydCBjbGFzcyBTZWxlY3RvckNvbnRleHQ8VCA9IGFueT4ge1xuICBub3RTZWxlY3RvcnM6IENzc1NlbGVjdG9yW107XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgc2VsZWN0b3I6IENzc1NlbGVjdG9yLCBwdWJsaWMgY2JDb250ZXh0OiBULCBwdWJsaWMgbGlzdENvbnRleHQ6IFNlbGVjdG9yTGlzdENvbnRleHQpIHtcbiAgICB0aGlzLm5vdFNlbGVjdG9ycyA9IHNlbGVjdG9yLm5vdFNlbGVjdG9ycztcbiAgfVxuXG4gIGZpbmFsaXplKGNzc1NlbGVjdG9yOiBDc3NTZWxlY3RvciwgY2FsbGJhY2s6ICgoYzogQ3NzU2VsZWN0b3IsIGE6IFQpID0+IHZvaWQpfG51bGwpOiBib29sZWFuIHtcbiAgICBsZXQgcmVzdWx0ID0gdHJ1ZTtcbiAgICBpZiAodGhpcy5ub3RTZWxlY3RvcnMubGVuZ3RoID4gMCAmJiAoIXRoaXMubGlzdENvbnRleHQgfHwgIXRoaXMubGlzdENvbnRleHQuYWxyZWFkeU1hdGNoZWQpKSB7XG4gICAgICBjb25zdCBub3RNYXRjaGVyID0gU2VsZWN0b3JNYXRjaGVyLmNyZWF0ZU5vdE1hdGNoZXIodGhpcy5ub3RTZWxlY3RvcnMpO1xuICAgICAgcmVzdWx0ID0gIW5vdE1hdGNoZXIubWF0Y2goY3NzU2VsZWN0b3IsIG51bGwpO1xuICAgIH1cbiAgICBpZiAocmVzdWx0ICYmIGNhbGxiYWNrICYmICghdGhpcy5saXN0Q29udGV4dCB8fCAhdGhpcy5saXN0Q29udGV4dC5hbHJlYWR5TWF0Y2hlZCkpIHtcbiAgICAgIGlmICh0aGlzLmxpc3RDb250ZXh0KSB7XG4gICAgICAgIHRoaXMubGlzdENvbnRleHQuYWxyZWFkeU1hdGNoZWQgPSB0cnVlO1xuICAgICAgfVxuICAgICAgY2FsbGJhY2sodGhpcy5zZWxlY3RvciwgdGhpcy5jYkNvbnRleHQpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG59XG4iXX0=