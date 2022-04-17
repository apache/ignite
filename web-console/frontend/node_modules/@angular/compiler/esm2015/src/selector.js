/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { getHtmlTagDefinition } from './ml_parser/html_tags';
const _SELECTOR_REGEXP = new RegExp('(\\:not\\()|' + //":not("
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
export class CssSelector {
    constructor() {
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
    static parse(selector) {
        const results = [];
        const _addResult = (res, cssSel) => {
            if (cssSel.notSelectors.length > 0 && !cssSel.element && cssSel.classNames.length == 0 &&
                cssSel.attrs.length == 0) {
                cssSel.element = '*';
            }
            res.push(cssSel);
        };
        let cssSelector = new CssSelector();
        let match;
        let current = cssSelector;
        let inNot = false;
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
    }
    isElementSelector() {
        return this.hasElementSelector() && this.classNames.length == 0 && this.attrs.length == 0 &&
            this.notSelectors.length === 0;
    }
    hasElementSelector() { return !!this.element; }
    setElement(element = null) { this.element = element; }
    /** Gets a template string for an element that matches the selector. */
    getMatchingElementTemplate() {
        const tagName = this.element || 'div';
        const classAttr = this.classNames.length > 0 ? ` class="${this.classNames.join(' ')}"` : '';
        let attrs = '';
        for (let i = 0; i < this.attrs.length; i += 2) {
            const attrName = this.attrs[i];
            const attrValue = this.attrs[i + 1] !== '' ? `="${this.attrs[i + 1]}"` : '';
            attrs += ` ${attrName}${attrValue}`;
        }
        return getHtmlTagDefinition(tagName).isVoid ? `<${tagName}${classAttr}${attrs}/>` :
            `<${tagName}${classAttr}${attrs}></${tagName}>`;
    }
    getAttrs() {
        const result = [];
        if (this.classNames.length > 0) {
            result.push('class', this.classNames.join(' '));
        }
        return result.concat(this.attrs);
    }
    addAttribute(name, value = '') {
        this.attrs.push(name, value && value.toLowerCase() || '');
    }
    addClassName(name) { this.classNames.push(name.toLowerCase()); }
    toString() {
        let res = this.element || '';
        if (this.classNames) {
            this.classNames.forEach(klass => res += `.${klass}`);
        }
        if (this.attrs) {
            for (let i = 0; i < this.attrs.length; i += 2) {
                const name = this.attrs[i];
                const value = this.attrs[i + 1];
                res += `[${name}${value ? '=' + value : ''}]`;
            }
        }
        this.notSelectors.forEach(notSelector => res += `:not(${notSelector})`);
        return res;
    }
}
/**
 * Reads a list of CssSelectors and allows to calculate which ones
 * are contained in a given CssSelector.
 */
export class SelectorMatcher {
    constructor() {
        this._elementMap = new Map();
        this._elementPartialMap = new Map();
        this._classMap = new Map();
        this._classPartialMap = new Map();
        this._attrValueMap = new Map();
        this._attrValuePartialMap = new Map();
        this._listContexts = [];
    }
    static createNotMatcher(notSelectors) {
        const notMatcher = new SelectorMatcher();
        notMatcher.addSelectables(notSelectors, null);
        return notMatcher;
    }
    addSelectables(cssSelectors, callbackCtxt) {
        let listContext = null;
        if (cssSelectors.length > 1) {
            listContext = new SelectorListContext(cssSelectors);
            this._listContexts.push(listContext);
        }
        for (let i = 0; i < cssSelectors.length; i++) {
            this._addSelectable(cssSelectors[i], callbackCtxt, listContext);
        }
    }
    /**
     * Add an object that can be found later on by calling `match`.
     * @param cssSelector A css selector
     * @param callbackCtxt An opaque object that will be given to the callback of the `match` function
     */
    _addSelectable(cssSelector, callbackCtxt, listContext) {
        let matcher = this;
        const element = cssSelector.element;
        const classNames = cssSelector.classNames;
        const attrs = cssSelector.attrs;
        const selectable = new SelectorContext(cssSelector, callbackCtxt, listContext);
        if (element) {
            const isTerminal = attrs.length === 0 && classNames.length === 0;
            if (isTerminal) {
                this._addTerminal(matcher._elementMap, element, selectable);
            }
            else {
                matcher = this._addPartial(matcher._elementPartialMap, element);
            }
        }
        if (classNames) {
            for (let i = 0; i < classNames.length; i++) {
                const isTerminal = attrs.length === 0 && i === classNames.length - 1;
                const className = classNames[i];
                if (isTerminal) {
                    this._addTerminal(matcher._classMap, className, selectable);
                }
                else {
                    matcher = this._addPartial(matcher._classPartialMap, className);
                }
            }
        }
        if (attrs) {
            for (let i = 0; i < attrs.length; i += 2) {
                const isTerminal = i === attrs.length - 2;
                const name = attrs[i];
                const value = attrs[i + 1];
                if (isTerminal) {
                    const terminalMap = matcher._attrValueMap;
                    let terminalValuesMap = terminalMap.get(name);
                    if (!terminalValuesMap) {
                        terminalValuesMap = new Map();
                        terminalMap.set(name, terminalValuesMap);
                    }
                    this._addTerminal(terminalValuesMap, value, selectable);
                }
                else {
                    const partialMap = matcher._attrValuePartialMap;
                    let partialValuesMap = partialMap.get(name);
                    if (!partialValuesMap) {
                        partialValuesMap = new Map();
                        partialMap.set(name, partialValuesMap);
                    }
                    matcher = this._addPartial(partialValuesMap, value);
                }
            }
        }
    }
    _addTerminal(map, name, selectable) {
        let terminalList = map.get(name);
        if (!terminalList) {
            terminalList = [];
            map.set(name, terminalList);
        }
        terminalList.push(selectable);
    }
    _addPartial(map, name) {
        let matcher = map.get(name);
        if (!matcher) {
            matcher = new SelectorMatcher();
            map.set(name, matcher);
        }
        return matcher;
    }
    /**
     * Find the objects that have been added via `addSelectable`
     * whose css selector is contained in the given css selector.
     * @param cssSelector A css selector
     * @param matchedCallback This callback will be called with the object handed into `addSelectable`
     * @return boolean true if a match was found
    */
    match(cssSelector, matchedCallback) {
        let result = false;
        const element = cssSelector.element;
        const classNames = cssSelector.classNames;
        const attrs = cssSelector.attrs;
        for (let i = 0; i < this._listContexts.length; i++) {
            this._listContexts[i].alreadyMatched = false;
        }
        result = this._matchTerminal(this._elementMap, element, cssSelector, matchedCallback) || result;
        result = this._matchPartial(this._elementPartialMap, element, cssSelector, matchedCallback) ||
            result;
        if (classNames) {
            for (let i = 0; i < classNames.length; i++) {
                const className = classNames[i];
                result =
                    this._matchTerminal(this._classMap, className, cssSelector, matchedCallback) || result;
                result =
                    this._matchPartial(this._classPartialMap, className, cssSelector, matchedCallback) ||
                        result;
            }
        }
        if (attrs) {
            for (let i = 0; i < attrs.length; i += 2) {
                const name = attrs[i];
                const value = attrs[i + 1];
                const terminalValuesMap = this._attrValueMap.get(name);
                if (value) {
                    result =
                        this._matchTerminal(terminalValuesMap, '', cssSelector, matchedCallback) || result;
                }
                result =
                    this._matchTerminal(terminalValuesMap, value, cssSelector, matchedCallback) || result;
                const partialValuesMap = this._attrValuePartialMap.get(name);
                if (value) {
                    result = this._matchPartial(partialValuesMap, '', cssSelector, matchedCallback) || result;
                }
                result =
                    this._matchPartial(partialValuesMap, value, cssSelector, matchedCallback) || result;
            }
        }
        return result;
    }
    /** @internal */
    _matchTerminal(map, name, cssSelector, matchedCallback) {
        if (!map || typeof name !== 'string') {
            return false;
        }
        let selectables = map.get(name) || [];
        const starSelectables = map.get('*');
        if (starSelectables) {
            selectables = selectables.concat(starSelectables);
        }
        if (selectables.length === 0) {
            return false;
        }
        let selectable;
        let result = false;
        for (let i = 0; i < selectables.length; i++) {
            selectable = selectables[i];
            result = selectable.finalize(cssSelector, matchedCallback) || result;
        }
        return result;
    }
    /** @internal */
    _matchPartial(map, name, cssSelector, matchedCallback) {
        if (!map || typeof name !== 'string') {
            return false;
        }
        const nestedSelector = map.get(name);
        if (!nestedSelector) {
            return false;
        }
        // TODO(perf): get rid of recursion and measure again
        // TODO(perf): don't pass the whole selector into the recursion,
        // but only the not processed parts
        return nestedSelector.match(cssSelector, matchedCallback);
    }
}
export class SelectorListContext {
    constructor(selectors) {
        this.selectors = selectors;
        this.alreadyMatched = false;
    }
}
// Store context to pass back selector and context when a selector is matched
export class SelectorContext {
    constructor(selector, cbContext, listContext) {
        this.selector = selector;
        this.cbContext = cbContext;
        this.listContext = listContext;
        this.notSelectors = selector.notSelectors;
    }
    finalize(cssSelector, callback) {
        let result = true;
        if (this.notSelectors.length > 0 && (!this.listContext || !this.listContext.alreadyMatched)) {
            const notMatcher = SelectorMatcher.createNotMatcher(this.notSelectors);
            result = !notMatcher.match(cssSelector, null);
        }
        if (result && callback && (!this.listContext || !this.listContext.alreadyMatched)) {
            if (this.listContext) {
                this.listContext.alreadyMatched = true;
            }
            callback(this.selector, this.cbContext);
        }
        return result;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VsZWN0b3IuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvc2VsZWN0b3IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxFQUFDLG9CQUFvQixFQUFDLE1BQU0sdUJBQXVCLENBQUM7QUFFM0QsTUFBTSxnQkFBZ0IsR0FBRyxJQUFJLE1BQU0sQ0FDL0IsY0FBYyxHQUFhLFNBQVM7SUFDaEMsWUFBWSxHQUFXLFFBQVE7SUFDL0IsbUJBQW1CLEdBQUksV0FBVztJQUNsQyxpRkFBaUY7SUFDakYsdURBQXVELEdBQUksNEJBQTRCO0lBQzVCLG9CQUFvQjtJQUNwQixtQkFBbUI7SUFDOUUsUUFBUSxHQUFtRCxNQUFNO0lBQ2pFLGFBQWEsRUFBOEMsTUFBTTtBQUNyRSxHQUFHLENBQUMsQ0FBQztBQUVUOzs7O0dBSUc7QUFDSCxNQUFNLE9BQU8sV0FBVztJQUF4QjtRQUNFLFlBQU8sR0FBZ0IsSUFBSSxDQUFDO1FBQzVCLGVBQVUsR0FBYSxFQUFFLENBQUM7UUFDMUI7Ozs7Ozs7Ozs7V0FVRztRQUNILFVBQUssR0FBYSxFQUFFLENBQUM7UUFDckIsaUJBQVksR0FBa0IsRUFBRSxDQUFDO0lBd0duQyxDQUFDO0lBdEdDLE1BQU0sQ0FBQyxLQUFLLENBQUMsUUFBZ0I7UUFDM0IsTUFBTSxPQUFPLEdBQWtCLEVBQUUsQ0FBQztRQUNsQyxNQUFNLFVBQVUsR0FBRyxDQUFDLEdBQWtCLEVBQUUsTUFBbUIsRUFBRSxFQUFFO1lBQzdELElBQUksTUFBTSxDQUFDLFlBQVksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLE9BQU8sSUFBSSxNQUFNLENBQUMsVUFBVSxDQUFDLE1BQU0sSUFBSSxDQUFDO2dCQUNsRixNQUFNLENBQUMsS0FBSyxDQUFDLE1BQU0sSUFBSSxDQUFDLEVBQUU7Z0JBQzVCLE1BQU0sQ0FBQyxPQUFPLEdB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SUFBSSxNQUFNLEdBQUcsSUFBSSxDQUFDO1FBQ2xCLElBQUksSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxJQUFJLENBQUMsQ0FBQyxJQUFJLENBQUMsV0FBVyxJQUFJLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxjQUFjLENBQUMsRUFBRTtZQUMzRixNQUFNLFVBQVUsR0FBRyxlQUFlLENBQUMsZ0JBQWdCLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxDQUFDO1lBQ3ZFLE1BQU0sR0FBRyxDQUFDLFVBQVUsQ0FBQyxLQUFLLENBQUMsV0FBVyxFQUFFLElBQUksQ0FBQyxDQUFDO1NBQy9DO1FBQ0QsSUFBSSxNQUFNLElBQUksUUFBUSxJQUFJLENBQUMsQ0FBQyxJQUFJLENBQUMsV0FBVyxJQUFJLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxjQUFjLENBQUMsRUFBRTtZQUNqRixJQUFJLElBQUksQ0FBQyxXQUFXLEVBQUU7Z0JBQ3BCLElBQUksQ0FBQyxXQUFXLENBQUMsY0FBYyxHQUFHLElBQUksQ0FBQzthQUN4QztZQUNELFFBQVEsQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUN6QztRQUNELE9BQU8sTUFBTSxDQUFDO0lBQ2hCLENBQUM7Q0FDRiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtnZXRIdG1sVGFnRGVmaW5pdGlvbn0gZnJvbSAnLi9tbF9wYXJzZXIvaHRtbF90YWdzJztcblxuY29uc3QgX1NFTEVDVE9SX1JFR0VYUCA9IG5ldyBSZWdFeHAoXG4gICAgJyhcXFxcOm5vdFxcXFwoKXwnICsgICAgICAgICAgIC8vXCI6bm90KFwiXG4gICAgICAgICcoWy1cXFxcd10rKXwnICsgICAgICAgICAvLyBcInRhZ1wiXG4gICAgICAgICcoPzpcXFxcLihbLVxcXFx3XSspKXwnICsgIC8vIFwiLmNsYXNzXCJcbiAgICAgICAgLy8gXCItXCIgc2hvdWxkIGFwcGVhciBmaXJzdCBpbiB0aGUgcmVnZXhwIGJlbG93IGFzIEZGMzEgcGFyc2VzIFwiWy4tXFx3XVwiIGFzIGEgcmFuZ2VcbiAgICAgICAgJyg/OlxcXFxbKFstLlxcXFx3Kl0rKSg/Oj0oW1xcXCJcXCddPykoW15cXFxcXVxcXCJcXCddKilcXFxcNSk/XFxcXF0pfCcgKyAgLy8gXCJbbmFtZV1cIiwgXCJbbmFtZT12YWx1ZV1cIixcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBcIltuYW1lPVwidmFsdWVcIl1cIixcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBcIltuYW1lPSd2YWx1ZSddXCJcbiAgICAgICAgJyhcXFxcKSl8JyArICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIFwiKVwiXG4gICAgICAgICcoXFxcXHMqLFxcXFxzKiknLCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIFwiLFwiXG4gICAgJ2cnKTtcblxuLyoqXG4gKiBBIGNzcyBzZWxlY3RvciBjb250YWlucyBhbiBlbGVtZW50IG5hbWUsXG4gKiBjc3MgY2xhc3NlcyBhbmQgYXR0cmlidXRlL3ZhbHVlIHBhaXJzIHdpdGggdGhlIHB1cnBvc2VcbiAqIG9mIHNlbGVjdGluZyBzdWJzZXRzIG91dCBvZiB0aGVtLlxuICovXG5leHBvcnQgY2xhc3MgQ3NzU2VsZWN0b3Ige1xuICBlbGVtZW50OiBzdHJpbmd8bnVsbCA9IG51bGw7XG4gIGNsYXNzTmFtZXM6IHN0cmluZ1tdID0gW107XG4gIC8qKlxuICAgKiBUaGUgc2VsZWN0b3JzIGFyZSBlbmNvZGVkIGluIHBhaXJzIHdoZXJlOlxuICAgKiAtIGV2ZW4gbG9jYXRpb25zIGFyZSBhdHRyaWJ1dGUgbmFtZXNcbiAgICogLSBvZGQgbG9jYXRpb25zIGFyZSBhdHRyaWJ1dGUgdmFsdWVzLlxuICAgKlxuICAgKiBFeGFtcGxlOlxuICAgKiBTZWxlY3RvcjogYFtrZXkxPXZhbHVlMV1ba2V5Ml1gIHdvdWxkIHBhcnNlIHRvOlxuICAgKiBgYGBcbiAgICogWydrZXkxJywgJ3ZhbHVlMScsICdrZXkyJywgJyddXG4gICAqIGBgYFxuICAgKi9cbiAgYXR0cnM6IHN0cmluZ1tdID0gW107XG4gIG5vdFNlbGVjdG9yczogQ3NzU2VsZWN0b3JbXSA9IFtdO1xuXG4gIHN0YXRpYyBwYXJzZShzZWxlY3Rvcjogc3RyaW5nKTogQ3NzU2VsZWN0b3JbXSB7XG4gICAgY29uc3QgcmVzdWx0czogQ3NzU2VsZWN0b3JbXSA9IFtdO1xuICAgIGNvbnN0IF9hZGRSZXN1bHQgPSAocmVzOiBDc3NTZWxlY3RvcltdLCBjc3NTZWw6IENzc1NlbGVjdG9yKSA9PiB7XG4gICAgICBpZiAoY3NzU2VsLm5vdFNlbGVjdG9ycy5sZW5ndGggPiAwICYmICFjc3NTZWwuZWxlbWVudCAmJiBjc3NTZWwuY2xhc3NOYW1lcy5sZW5ndGggPT0gMCAmJlxuICAgICAgICAgIGNzc1NlbC5hdHRycy5sZW5ndGggPT0gMCkge1xuICAgICAgICBjc3NTZWwuZWxlbWVudCA9ICcqJztcbiAgICAgIH1cbiAgICAgIHJlcy5wdXNoKGNzc1NlbCk7XG4gICAgfTtcbiAgICBsZXQgY3NzU2VsZWN0b3IgPSBuZXcgQ3NzU2VsZWN0b3IoKTtcbiAgICBsZXQgbWF0Y2g6IHN0cmluZ1tdfG51bGw7XG4gICAgbGV0IGN1cnJlbnQgPSBjc3NTZWxlY3RvcjtcbiAgICBsZXQgaW5Ob3QgPSBmYWxzZTtcbiAgICBfU0VMRUNUT1JfUkVHRVhQLmxhc3RJbmRleCA9IDA7XG4gICAgd2hpbGUgKG1hdGNoID0gX1NFTEVDVE9SX1JFR0VYUC5leGVjKHNlbGVjdG9yKSkge1xuICAgICAgaWYgKG1hdGNoWzFdKSB7XG4gICAgICAgIGlmIChpbk5vdCkge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignTmVzdGluZyA6bm90IGlzIG5vdCBhbGxvd2VkIGluIGEgc2VsZWN0b3InKTtcbiAgICAgICAgfVxuICAgICAgICBpbk5vdCA9IHRydWU7XG4gICAgICAgIGN1cnJlbnQgPSBuZXcgQ3NzU2VsZWN0b3IoKTtcbiAgICAgICAgY3NzU2VsZWN0b3Iubm90U2VsZWN0b3JzLnB1c2goY3VycmVudCk7XG4gICAgICB9XG4gICAgICBpZiAobWF0Y2hbMl0pIHtcbiAgICAgICAgY3VycmVudC5zZXRFbGVtZW50KG1hdGNoWzJdKTtcbiAgICAgIH1cbiAgICAgIGlmIChtYXRjaFszXSkge1xuICAgICAgICBjdXJyZW50LmFkZENsYXNzTmFtZShtYXRjaFszXSk7XG4gICAgICB9XG4gICAgICBpZiAobWF0Y2hbNF0pIHtcbiAgICAgICAgY3VycmVudC5hZGRBdHRyaWJ1dGUobWF0Y2hbNF0sIG1hdGNoWzZdKTtcbiAgICAgIH1cbiAgICAgIGlmIChtYXRjaFs3XSkge1xuICAgICAgICBpbk5vdCA9IGZhbHNlO1xuICAgICAgICBjdXJyZW50ID0gY3NzU2VsZWN0b3I7XG4gICAgICB9XG4gICAgICBpZiAobWF0Y2hbOF0pIHtcbiAgICAgICAgaWYgKGluTm90KSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdNdWx0aXBsZSBzZWxlY3RvcnMgaW4gOm5vdCBhcmUgbm90IHN1cHBvcnRlZCcpO1xuICAgICAgICB9XG4gICAgICAgIF9hZGRSZXN1bHQocmVzdWx0cywgY3NzU2VsZWN0b3IpO1xuICAgICAgICBjc3NTZWxlY3RvciA9IGN1cnJlbnQgPSBuZXcgQ3NzU2VsZWN0b3IoKTtcbiAgICAgIH1cbiAgICB9XG4gICAgX2FkZFJlc3VsdChyZXN1bHRzLCBjc3NTZWxlY3Rvcik7XG4gICAgcmV0dXJuIHJlc3VsdHM7XG4gIH1cblxuICBpc0VsZW1lbnRTZWxlY3RvcigpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5oYXNFbGVtZW50U2VsZWN0b3IoKSAmJiB0aGlzLmNsYXNzTmFtZXMubGVuZ3RoID09IDAgJiYgdGhpcy5hdHRycy5sZW5ndGggPT0gMCAmJlxuICAgICAgICB0aGlzLm5vdFNlbGVjdG9ycy5sZW5ndGggPT09IDA7XG4gIH1cblxuICBoYXNFbGVtZW50U2VsZWN0b3IoKTogYm9vbGVhbiB7IHJldHVybiAhIXRoaXMuZWxlbWVudDsgfVxuXG4gIHNldEVsZW1lbnQoZWxlbWVudDogc3RyaW5nfG51bGwgPSBudWxsKSB7IHRoaXMuZWxlbWVudCA9IGVsZW1lbnQ7IH1cblxuICAvKiogR2V0cyBhIHRlbXBsYXRlIHN0cmluZyBmb3IgYW4gZWxlbWVudCB0aGF0IG1hdGNoZXMgdGhlIHNlbGVjdG9yLiAqL1xuICBnZXRNYXRjaGluZ0VsZW1lbnRUZW1wbGF0ZSgpOiBzdHJpbmcge1xuICAgIGNvbnN0IHRhZ05hbWUgPSB0aGlzLmVsZW1lbnQgfHwgJ2Rpdic7XG4gICAgY29uc3QgY2xhc3NBdHRyID0gdGhpcy5jbGFzc05hbWVzLmxlbmd0aCA+IDAgPyBgIGNsYXNzPVwiJHt0aGlzLmNsYXNzTmFtZXMuam9pbignICcpfVwiYCA6ICcnO1xuXG4gICAgbGV0IGF0dHJzID0gJyc7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLmF0dHJzLmxlbmd0aDsgaSArPSAyKSB7XG4gICAgICBjb25zdCBhdHRyTmFtZSA9IHRoaXMuYXR0cnNbaV07XG4gICAgICBjb25zdCBhdHRyVmFsdWUgPSB0aGlzLmF0dHJzW2kgKyAxXSAhPT0gJycgPyBgPVwiJHt0aGlzLmF0dHJzW2kgKyAxXX1cImAgOiAnJztcbiAgICAgIGF0dHJzICs9IGAgJHthdHRyTmFtZX0ke2F0dHJWYWx1ZX1gO1xuICAgIH1cblxuICAgIHJldHVybiBnZXRIdG1sVGFnRGVmaW5pdGlvbih0YWdOYW1lKS5pc1ZvaWQgPyBgPCR7dGFnTmFtZX0ke2NsYXNzQXR0cn0ke2F0dHJzfS8+YCA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGA8JHt0YWdOYW1lfSR7Y2xhc3NBdHRyfSR7YXR0cnN9PjwvJHt0YWdOYW1lfT5gO1xuICB9XG5cbiAgZ2V0QXR0cnMoKTogc3RyaW5nW10ge1xuICAgIGNvbnN0IHJlc3VsdDogc3RyaW5nW10gPSBbXTtcbiAgICBpZiAodGhpcy5jbGFzc05hbWVzLmxlbmd0aCA+IDApIHtcbiAgICAgIHJlc3VsdC5wdXNoKCdjbGFzcycsIHRoaXMuY2xhc3NOYW1lcy5qb2luKCcgJykpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0LmNvbmNhdCh0aGlzLmF0dHJzKTtcbiAgfVxuXG4gIGFkZEF0dHJpYnV0ZShuYW1lOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcgPSAnJykge1xuICAgIHRoaXMuYXR0cnMucHVzaChuYW1lLCB2YWx1ZSAmJiB2YWx1ZS50b0xvd2VyQ2FzZSgpIHx8ICcnKTtcbiAgfVxuXG4gIGFkZENsYXNzTmFtZShuYW1lOiBzdHJpbmcpIHsgdGhpcy5jbGFzc05hbWVzLnB1c2gobmFtZS50b0xvd2VyQ2FzZSgpKTsgfVxuXG4gIHRvU3RyaW5nKCk6IHN0cmluZyB7XG4gICAgbGV0IHJlczogc3RyaW5nID0gdGhpcy5lbGVtZW50IHx8ICcnO1xuICAgIGlmICh0aGlzLmNsYXNzTmFtZXMpIHtcbiAgICAgIHRoaXMuY2xhc3NOYW1lcy5mb3JFYWNoKGtsYXNzID0+IHJlcyArPSBgLiR7a2xhc3N9YCk7XG4gICAgfVxuICAgIGlmICh0aGlzLmF0dHJzKSB7XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IHRoaXMuYXR0cnMubGVuZ3RoOyBpICs9IDIpIHtcbiAgICAgICAgY29uc3QgbmFtZSA9IHRoaXMuYXR0cnNbaV07XG4gICAgICAgIGNvbnN0IHZhbHVlID0gdGhpcy5hdHRyc1tpICsgMV07XG4gICAgICAgIHJlcyArPSBgWyR7bmFtZX0ke3ZhbHVlID8gJz0nICsgdmFsdWUgOiAnJ31dYDtcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5ub3RTZWxlY3RvcnMuZm9yRWFjaChub3RTZWxlY3RvciA9PiByZXMgKz0gYDpub3QoJHtub3RTZWxlY3Rvcn0pYCk7XG4gICAgcmV0dXJuIHJlcztcbiAgfVxufVxuXG4vKipcbiAqIFJlYWRzIGEgbGlzdCBvZiBDc3NTZWxlY3RvcnMgYW5kIGFsbG93cyB0byBjYWxjdWxhdGUgd2hpY2ggb25lc1xuICogYXJlIGNvbnRhaW5lZCBpbiBhIGdpdmVuIENzc1NlbGVjdG9yLlxuICovXG5leHBvcnQgY2xhc3MgU2VsZWN0b3JNYXRjaGVyPFQgPSBhbnk+IHtcbiAgc3RhdGljIGNyZWF0ZU5vdE1hdGNoZXIobm90U2VsZWN0b3JzOiBDc3NTZWxlY3RvcltdKTogU2VsZWN0b3JNYXRjaGVyPG51bGw+IHtcbiAgICBjb25zdCBub3RNYXRjaGVyID0gbmV3IFNlbGVjdG9yTWF0Y2hlcjxudWxsPigpO1xuICAgIG5vdE1hdGNoZXIuYWRkU2VsZWN0YWJsZXMobm90U2VsZWN0b3JzLCBudWxsKTtcbiAgICByZXR1cm4gbm90TWF0Y2hlcjtcbiAgfVxuXG4gIHByaXZhdGUgX2VsZW1lbnRNYXAgPSBuZXcgTWFwPHN0cmluZywgU2VsZWN0b3JDb250ZXh0PFQ+W10+KCk7XG4gIHByaXZhdGUgX2VsZW1lbnRQYXJ0aWFsTWFwID0gbmV3IE1hcDxzdHJpbmcsIFNlbGVjdG9yTWF0Y2hlcjxUPj4oKTtcbiAgcHJpdmF0ZSBfY2xhc3NNYXAgPSBuZXcgTWFwPHN0cmluZywgU2VsZWN0b3JDb250ZXh0PFQ+W10+KCk7XG4gIHByaXZhdGUgX2NsYXNzUGFydGlhbE1hcCA9IG5ldyBNYXA8c3RyaW5nLCBTZWxlY3Rvck1hdGNoZXI8VD4+KCk7XG4gIHByaXZhdGUgX2F0dHJWYWx1ZU1hcCA9IG5ldyBNYXA8c3RyaW5nLCBNYXA8c3RyaW5nLCBTZWxlY3RvckNvbnRleHQ8VD5bXT4+KCk7XG4gIHByaXZhdGUgX2F0dHJWYWx1ZVBhcnRpYWxNYXAgPSBuZXcgTWFwPHN0cmluZywgTWFwPHN0cmluZywgU2VsZWN0b3JNYXRjaGVyPFQ+Pj4oKTtcbiAgcHJpdmF0ZSBfbGlzdENvbnRleHRzOiBTZWxlY3Rvckxpc3RDb250ZXh0W10gPSBbXTtcblxuICBhZGRTZWxlY3RhYmxlcyhjc3NTZWxlY3RvcnM6IENzc1NlbGVjdG9yW10sIGNhbGxiYWNrQ3R4dD86IFQpIHtcbiAgICBsZXQgbGlzdENvbnRleHQ6IFNlbGVjdG9yTGlzdENvbnRleHQgPSBudWxsICE7XG4gICAgaWYgKGNzc1NlbGVjdG9ycy5sZW5ndGggPiAxKSB7XG4gICAgICBsaXN0Q29udGV4dCA9IG5ldyBTZWxlY3Rvckxpc3RDb250ZXh0KGNzc1NlbGVjdG9ycyk7XG4gICAgICB0aGlzLl9saXN0Q29udGV4dHMucHVzaChsaXN0Q29udGV4dCk7XG4gICAgfVxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY3NzU2VsZWN0b3JzLmxlbmd0aDsgaSsrKSB7XG4gICAgICB0aGlzLl9hZGRTZWxlY3RhYmxlKGNzc1NlbGVjdG9yc1tpXSwgY2FsbGJhY2tDdHh0IGFzIFQsIGxpc3RDb250ZXh0KTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQWRkIGFuIG9iamVjdCB0aGF0IGNhbiBiZSBmb3VuZCBsYXRlciBvbiBieSBjYWxsaW5nIGBtYXRjaGAuXG4gICAqIEBwYXJhbSBjc3NTZWxlY3RvciBBIGNzcyBzZWxlY3RvclxuICAgKiBAcGFyYW0gY2FsbGJhY2tDdHh0IEFuIG9wYXF1ZSBvYmplY3QgdGhhdCB3aWxsIGJlIGdpdmVuIHRvIHRoZSBjYWxsYmFjayBvZiB0aGUgYG1hdGNoYCBmdW5jdGlvblxuICAgKi9cbiAgcHJpdmF0ZSBfYWRkU2VsZWN0YWJsZShcbiAgICAgIGNzc1NlbGVjdG9yOiBDc3NTZWxlY3RvciwgY2FsbGJhY2tDdHh0OiBULCBsaXN0Q29udGV4dDogU2VsZWN0b3JMaXN0Q29udGV4dCkge1xuICAgIGxldCBtYXRjaGVyOiBTZWxlY3Rvck1hdGNoZXI8VD4gPSB0aGlzO1xuICAgIGNvbnN0IGVsZW1lbnQgPSBjc3NTZWxlY3Rvci5lbGVtZW50O1xuICAgIGNvbnN0IGNsYXNzTmFtZXMgPSBjc3NTZWxlY3Rvci5jbGFzc05hbWVzO1xuICAgIGNvbnN0IGF0dHJzID0gY3NzU2VsZWN0b3IuYXR0cnM7XG4gICAgY29uc3Qgc2VsZWN0YWJsZSA9IG5ldyBTZWxlY3RvckNvbnRleHQoY3NzU2VsZWN0b3IsIGNhbGxiYWNrQ3R4dCwgbGlzdENvbnRleHQpO1xuXG4gICAgaWYgKGVsZW1lbnQpIHtcbiAgICAgIGNvbnN0IGlzVGVybWluYWwgPSBhdHRycy5sZW5ndGggPT09IDAgJiYgY2xhc3NOYW1lcy5sZW5ndGggPT09IDA7XG4gICAgICBpZiAoaXNUZXJtaW5hbCkge1xuICAgICAgICB0aGlzLl9hZGRUZXJtaW5hbChtYXRjaGVyLl9lbGVtZW50TWFwLCBlbGVtZW50LCBzZWxlY3RhYmxlKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIG1hdGNoZXIgPSB0aGlzLl9hZGRQYXJ0aWFsKG1hdGNoZXIuX2VsZW1lbnRQYXJ0aWFsTWFwLCBlbGVtZW50KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoY2xhc3NOYW1lcykge1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjbGFzc05hbWVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICAgIGNvbnN0IGlzVGVybWluYWwgPSBhdHRycy5sZW5ndGggPT09IDAgJiYgaSA9PT0gY2xhc3NOYW1lcy5sZW5ndGggLSAxO1xuICAgICAgICBjb25zdCBjbGFzc05hbWUgPSBjbGFzc05hbWVzW2ldO1xuICAgICAgICBpZiAoaXNUZXJtaW5hbCkge1xuICAgICAgICAgIHRoaXMuX2FkZFRlcm1pbmFsKG1hdGNoZXIuX2NsYXNzTWFwLCBjbGFzc05hbWUsIHNlbGVjdGFibGUpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG1hdGNoZXIgPSB0aGlzLl9hZGRQYXJ0aWFsKG1hdGNoZXIuX2NsYXNzUGFydGlhbE1hcCwgY2xhc3NOYW1lKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChhdHRycykge1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhdHRycy5sZW5ndGg7IGkgKz0gMikge1xuICAgICAgICBjb25zdCBpc1Rlcm1pbmFsID0gaSA9PT0gYXR0cnMubGVuZ3RoIC0gMjtcbiAgICAgICAgY29uc3QgbmFtZSA9IGF0dHJzW2ldO1xuICAgICAgICBjb25zdCB2YWx1ZSA9IGF0dHJzW2kgKyAxXTtcbiAgICAgICAgaWYgKGlzVGVybWluYWwpIHtcbiAgICAgICAgICBjb25zdCB0ZXJtaW5hbE1hcCA9IG1hdGNoZXIuX2F0dHJWYWx1ZU1hcDtcbiAgICAgICAgICBsZXQgdGVybWluYWxWYWx1ZXNNYXAgPSB0ZXJtaW5hbE1hcC5nZXQobmFtZSk7XG4gICAgICAgICAgaWYgKCF0ZXJtaW5hbFZhbHVlc01hcCkge1xuICAgICAgICAgICAgdGVybWluYWxWYWx1ZXNNYXAgPSBuZXcgTWFwPHN0cmluZywgU2VsZWN0b3JDb250ZXh0PFQ+W10+KCk7XG4gICAgICAgICAgICB0ZXJtaW5hbE1hcC5zZXQobmFtZSwgdGVybWluYWxWYWx1ZXNNYXApO1xuICAgICAgICAgIH1cbiAgICAgICAgICB0aGlzLl9hZGRUZXJtaW5hbCh0ZXJtaW5hbFZhbHVlc01hcCwgdmFsdWUsIHNlbGVjdGFibGUpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnN0IHBhcnRpYWxNYXAgPSBtYXRjaGVyLl9hdHRyVmFsdWVQYXJ0aWFsTWFwO1xuICAgICAgICAgIGxldCBwYXJ0aWFsVmFsdWVzTWFwID0gcGFydGlhbE1hcC5nZXQobmFtZSk7XG4gICAgICAgICAgaWYgKCFwYXJ0aWFsVmFsdWVzTWFwKSB7XG4gICAgICAgICAgICBwYXJ0aWFsVmFsdWVzTWFwID0gbmV3IE1hcDxzdHJpbmcsIFNlbGVjdG9yTWF0Y2hlcjxUPj4oKTtcbiAgICAgICAgICAgIHBhcnRpYWxNYXAuc2V0KG5hbWUsIHBhcnRpYWxWYWx1ZXNNYXApO1xuICAgICAgICAgIH1cbiAgICAgICAgICBtYXRjaGVyID0gdGhpcy5fYWRkUGFydGlhbChwYXJ0aWFsVmFsdWVzTWFwLCB2YWx1ZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9hZGRUZXJtaW5hbChcbiAgICAgIG1hcDogTWFwPHN0cmluZywgU2VsZWN0b3JDb250ZXh0PFQ+W10+LCBuYW1lOiBzdHJpbmcsIHNlbGVjdGFibGU6IFNlbGVjdG9yQ29udGV4dDxUPikge1xuICAgIGxldCB0ZXJtaW5hbExpc3QgPSBtYXAuZ2V0KG5hbWUpO1xuICAgIGlmICghdGVybWluYWxMaXN0KSB7XG4gICAgICB0ZXJtaW5hbExpc3QgPSBbXTtcbiAgICAgIG1hcC5zZXQobmFtZSwgdGVybWluYWxMaXN0KTtcbiAgICB9XG4gICAgdGVybWluYWxMaXN0LnB1c2goc2VsZWN0YWJsZSk7XG4gIH1cblxuICBwcml2YXRlIF9hZGRQYXJ0aWFsKG1hcDogTWFwPHN0cmluZywgU2VsZWN0b3JNYXRjaGVyPFQ+PiwgbmFtZTogc3RyaW5nKTogU2VsZWN0b3JNYXRjaGVyPFQ+IHtcbiAgICBsZXQgbWF0Y2hlciA9IG1hcC5nZXQobmFtZSk7XG4gICAgaWYgKCFtYXRjaGVyKSB7XG4gICAgICBtYXRjaGVyID0gbmV3IFNlbGVjdG9yTWF0Y2hlcjxUPigpO1xuICAgICAgbWFwLnNldChuYW1lLCBtYXRjaGVyKTtcbiAgICB9XG4gICAgcmV0dXJuIG1hdGNoZXI7XG4gIH1cblxuICAvKipcbiAgICogRmluZCB0aGUgb2JqZWN0cyB0aGF0IGhhdmUgYmVlbiBhZGRlZCB2aWEgYGFkZFNlbGVjdGFibGVgXG4gICAqIHdob3NlIGNzcyBzZWxlY3RvciBpcyBjb250YWluZWQgaW4gdGhlIGdpdmVuIGNzcyBzZWxlY3Rvci5cbiAgICogQHBhcmFtIGNzc1NlbGVjdG9yIEEgY3NzIHNlbGVjdG9yXG4gICAqIEBwYXJhbSBtYXRjaGVkQ2FsbGJhY2sgVGhpcyBjYWxsYmFjayB3aWxsIGJlIGNhbGxlZCB3aXRoIHRoZSBvYmplY3QgaGFuZGVkIGludG8gYGFkZFNlbGVjdGFibGVgXG4gICAqIEByZXR1cm4gYm9vbGVhbiB0cnVlIGlmIGEgbWF0Y2ggd2FzIGZvdW5kXG4gICovXG4gIG1hdGNoKGNzc1NlbGVjdG9yOiBDc3NTZWxlY3RvciwgbWF0Y2hlZENhbGxiYWNrOiAoKGM6IENzc1NlbGVjdG9yLCBhOiBUKSA9PiB2b2lkKXxudWxsKTogYm9vbGVhbiB7XG4gICAgbGV0IHJlc3VsdCA9IGZhbHNlO1xuICAgIGNvbnN0IGVsZW1lbnQgPSBjc3NTZWxlY3Rvci5lbGVtZW50ICE7XG4gICAgY29uc3QgY2xhc3NOYW1lcyA9IGNzc1NlbGVjdG9yLmNsYXNzTmFtZXM7XG4gICAgY29uc3QgYXR0cnMgPSBjc3NTZWxlY3Rvci5hdHRycztcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdGhpcy5fbGlzdENvbnRleHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgICB0aGlzLl9saXN0Q29udGV4dHNbaV0uYWxyZWFkeU1hdGNoZWQgPSBmYWxzZTtcbiAgICB9XG5cbiAgICByZXN1bHQgPSB0aGlzLl9tYXRjaFRlcm1pbmFsKHRoaXMuX2VsZW1lbnRNYXAsIGVsZW1lbnQsIGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spIHx8IHJlc3VsdDtcbiAgICByZXN1bHQgPSB0aGlzLl9tYXRjaFBhcnRpYWwodGhpcy5fZWxlbWVudFBhcnRpYWxNYXAsIGVsZW1lbnQsIGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spIHx8XG4gICAgICAgIHJlc3VsdDtcblxuICAgIGlmIChjbGFzc05hbWVzKSB7XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsYXNzTmFtZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgY29uc3QgY2xhc3NOYW1lID0gY2xhc3NOYW1lc1tpXTtcbiAgICAgICAgcmVzdWx0ID1cbiAgICAgICAgICAgIHRoaXMuX21hdGNoVGVybWluYWwodGhpcy5fY2xhc3NNYXAsIGNsYXNzTmFtZSwgY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHwgcmVzdWx0O1xuICAgICAgICByZXN1bHQgPVxuICAgICAgICAgICAgdGhpcy5fbWF0Y2hQYXJ0aWFsKHRoaXMuX2NsYXNzUGFydGlhbE1hcCwgY2xhc3NOYW1lLCBjc3NTZWxlY3RvciwgbWF0Y2hlZENhbGxiYWNrKSB8fFxuICAgICAgICAgICAgcmVzdWx0O1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChhdHRycykge1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhdHRycy5sZW5ndGg7IGkgKz0gMikge1xuICAgICAgICBjb25zdCBuYW1lID0gYXR0cnNbaV07XG4gICAgICAgIGNvbnN0IHZhbHVlID0gYXR0cnNbaSArIDFdO1xuXG4gICAgICAgIGNvbnN0IHRlcm1pbmFsVmFsdWVzTWFwID0gdGhpcy5fYXR0clZhbHVlTWFwLmdldChuYW1lKSAhO1xuICAgICAgICBpZiAodmFsdWUpIHtcbiAgICAgICAgICByZXN1bHQgPVxuICAgICAgICAgICAgICB0aGlzLl9tYXRjaFRlcm1pbmFsKHRlcm1pbmFsVmFsdWVzTWFwLCAnJywgY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHwgcmVzdWx0O1xuICAgICAgICB9XG4gICAgICAgIHJlc3VsdCA9XG4gICAgICAgICAgICB0aGlzLl9tYXRjaFRlcm1pbmFsKHRlcm1pbmFsVmFsdWVzTWFwLCB2YWx1ZSwgY3NzU2VsZWN0b3IsIG1hdGNoZWRDYWxsYmFjaykgfHwgcmVzdWx0O1xuXG4gICAgICAgIGNvbnN0IHBhcnRpYWxWYWx1ZXNNYXAgPSB0aGlzLl9hdHRyVmFsdWVQYXJ0aWFsTWFwLmdldChuYW1lKSAhO1xuICAgICAgICBpZiAodmFsdWUpIHtcbiAgICAgICAgICByZXN1bHQgPSB0aGlzLl9tYXRjaFBhcnRpYWwocGFydGlhbFZhbHVlc01hcCwgJycsIGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spIHx8IHJlc3VsdDtcbiAgICAgICAgfVxuICAgICAgICByZXN1bHQgPVxuICAgICAgICAgICAgdGhpcy5fbWF0Y2hQYXJ0aWFsKHBhcnRpYWxWYWx1ZXNNYXAsIHZhbHVlLCBjc3NTZWxlY3RvciwgbWF0Y2hlZENhbGxiYWNrKSB8fCByZXN1bHQ7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9tYXRjaFRlcm1pbmFsKFxuICAgICAgbWFwOiBNYXA8c3RyaW5nLCBTZWxlY3RvckNvbnRleHQ8VD5bXT4sIG5hbWU6IHN0cmluZywgY3NzU2VsZWN0b3I6IENzc1NlbGVjdG9yLFxuICAgICAgbWF0Y2hlZENhbGxiYWNrOiAoKGM6IENzc1NlbGVjdG9yLCBhOiBhbnkpID0+IHZvaWQpfG51bGwpOiBib29sZWFuIHtcbiAgICBpZiAoIW1hcCB8fCB0eXBlb2YgbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG5cbiAgICBsZXQgc2VsZWN0YWJsZXM6IFNlbGVjdG9yQ29udGV4dDxUPltdID0gbWFwLmdldChuYW1lKSB8fCBbXTtcbiAgICBjb25zdCBzdGFyU2VsZWN0YWJsZXM6IFNlbGVjdG9yQ29udGV4dDxUPltdID0gbWFwLmdldCgnKicpICE7XG4gICAgaWYgKHN0YXJTZWxlY3RhYmxlcykge1xuICAgICAgc2VsZWN0YWJsZXMgPSBzZWxlY3RhYmxlcy5jb25jYXQoc3RhclNlbGVjdGFibGVzKTtcbiAgICB9XG4gICAgaWYgKHNlbGVjdGFibGVzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICBsZXQgc2VsZWN0YWJsZTogU2VsZWN0b3JDb250ZXh0PFQ+O1xuICAgIGxldCByZXN1bHQgPSBmYWxzZTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHNlbGVjdGFibGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBzZWxlY3RhYmxlID0gc2VsZWN0YWJsZXNbaV07XG4gICAgICByZXN1bHQgPSBzZWxlY3RhYmxlLmZpbmFsaXplKGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spIHx8IHJlc3VsdDtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX21hdGNoUGFydGlhbChcbiAgICAgIG1hcDogTWFwPHN0cmluZywgU2VsZWN0b3JNYXRjaGVyPFQ+PiwgbmFtZTogc3RyaW5nLCBjc3NTZWxlY3RvcjogQ3NzU2VsZWN0b3IsXG4gICAgICBtYXRjaGVkQ2FsbGJhY2s6ICgoYzogQ3NzU2VsZWN0b3IsIGE6IGFueSkgPT4gdm9pZCl8bnVsbCk6IGJvb2xlYW4ge1xuICAgIGlmICghbWFwIHx8IHR5cGVvZiBuYW1lICE9PSAnc3RyaW5nJykge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cblxuICAgIGNvbnN0IG5lc3RlZFNlbGVjdG9yID0gbWFwLmdldChuYW1lKTtcbiAgICBpZiAoIW5lc3RlZFNlbGVjdG9yKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICAgIC8vIFRPRE8ocGVyZik6IGdldCByaWQgb2YgcmVjdXJzaW9uIGFuZCBtZWFzdXJlIGFnYWluXG4gICAgLy8gVE9ETyhwZXJmKTogZG9uJ3QgcGFzcyB0aGUgd2hvbGUgc2VsZWN0b3IgaW50byB0aGUgcmVjdXJzaW9uLFxuICAgIC8vIGJ1dCBvbmx5IHRoZSBub3QgcHJvY2Vzc2VkIHBhcnRzXG4gICAgcmV0dXJuIG5lc3RlZFNlbGVjdG9yLm1hdGNoKGNzc1NlbGVjdG9yLCBtYXRjaGVkQ2FsbGJhY2spO1xuICB9XG59XG5cblxuZXhwb3J0IGNsYXNzIFNlbGVjdG9yTGlzdENvbnRleHQge1xuICBhbHJlYWR5TWF0Y2hlZDogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBzZWxlY3RvcnM6IENzc1NlbGVjdG9yW10pIHt9XG59XG5cbi8vIFN0b3JlIGNvbnRleHQgdG8gcGFzcyBiYWNrIHNlbGVjdG9yIGFuZCBjb250ZXh0IHdoZW4gYSBzZWxlY3RvciBpcyBtYXRjaGVkXG5leHBvcnQgY2xhc3MgU2VsZWN0b3JDb250ZXh0PFQgPSBhbnk+IHtcbiAgbm90U2VsZWN0b3JzOiBDc3NTZWxlY3RvcltdO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIHNlbGVjdG9yOiBDc3NTZWxlY3RvciwgcHVibGljIGNiQ29udGV4dDogVCwgcHVibGljIGxpc3RDb250ZXh0OiBTZWxlY3Rvckxpc3RDb250ZXh0KSB7XG4gICAgdGhpcy5ub3RTZWxlY3RvcnMgPSBzZWxlY3Rvci5ub3RTZWxlY3RvcnM7XG4gIH1cblxuICBmaW5hbGl6ZShjc3NTZWxlY3RvcjogQ3NzU2VsZWN0b3IsIGNhbGxiYWNrOiAoKGM6IENzc1NlbGVjdG9yLCBhOiBUKSA9PiB2b2lkKXxudWxsKTogYm9vbGVhbiB7XG4gICAgbGV0IHJlc3VsdCA9IHRydWU7XG4gICAgaWYgKHRoaXMubm90U2VsZWN0b3JzLmxlbmd0aCA+IDAgJiYgKCF0aGlzLmxpc3RDb250ZXh0IHx8ICF0aGlzLmxpc3RDb250ZXh0LmFscmVhZHlNYXRjaGVkKSkge1xuICAgICAgY29uc3Qgbm90TWF0Y2hlciA9IFNlbGVjdG9yTWF0Y2hlci5jcmVhdGVOb3RNYXRjaGVyKHRoaXMubm90U2VsZWN0b3JzKTtcbiAgICAgIHJlc3VsdCA9ICFub3RNYXRjaGVyLm1hdGNoKGNzc1NlbGVjdG9yLCBudWxsKTtcbiAgICB9XG4gICAgaWYgKHJlc3VsdCAmJiBjYWxsYmFjayAmJiAoIXRoaXMubGlzdENvbnRleHQgfHwgIXRoaXMubGlzdENvbnRleHQuYWxyZWFkeU1hdGNoZWQpKSB7XG4gICAgICBpZiAodGhpcy5saXN0Q29udGV4dCkge1xuICAgICAgICB0aGlzLmxpc3RDb250ZXh0LmFscmVhZHlNYXRjaGVkID0gdHJ1ZTtcbiAgICAgIH1cbiAgICAgIGNhbGxiYWNrKHRoaXMuc2VsZWN0b3IsIHRoaXMuY2JDb250ZXh0KTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxufVxuIl19