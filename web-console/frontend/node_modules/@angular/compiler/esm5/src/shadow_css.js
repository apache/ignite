/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
/**
 * This file is a port of shadowCSS from webcomponents.js to TypeScript.
 *
 * Please make sure to keep to edits in sync with the source file.
 *
 * Source:
 * https://github.com/webcomponents/webcomponentsjs/blob/4efecd7e0e/src/ShadowCSS/ShadowCSS.js
 *
 * The original file level comment is reproduced below
 */
/*
  This is a limited shim for ShadowDOM css styling.
  https://dvcs.w3.org/hg/webcomponents/raw-file/tip/spec/shadow/index.html#styles

  The intention here is to support only the styling features which can be
  relatively simply implemented. The goal is to allow users to avoid the
  most obvious pitfalls and do so without compromising performance significantly.
  For ShadowDOM styling that's not covered here, a set of best practices
  can be provided that should allow users to accomplish more complex styling.

  The following is a list of specific ShadowDOM styling features and a brief
  discussion of the approach used to shim.

  Shimmed features:

  * :host, :host-context: ShadowDOM allows styling of the shadowRoot's host
  element using the :host rule. To shim this feature, the :host styles are
  reformatted and prefixed with a given scope name and promoted to a
  document level stylesheet.
  For example, given a scope name of .foo, a rule like this:

    :host {
        background: red;
      }
    }

  becomes:

    .foo {
      background: red;
    }

  * encapsulation: Styles defined within ShadowDOM, apply only to
  dom inside the ShadowDOM. Polymer uses one of two techniques to implement
  this feature.

  By default, rules are prefixed with the host element tag name
  as a descendant selector. This ensures styling does not leak out of the 'top'
  of the element's ShadowDOM. For example,

  div {
      font-weight: bold;
    }

  becomes:

  x-foo div {
      font-weight: bold;
    }

  becomes:


  Alternatively, if WebComponents.ShadowCSS.strictStyling is set to true then
  selectors are scoped by adding an attribute selector suffix to each
  simple selector that contains the host element tag name. Each element
  in the element's ShadowDOM template is also given the scope attribute.
  Thus, these rules match only elements that have the scope attribute.
  For example, given a scope name of x-foo, a rule like this:

    div {
      font-weight: bold;
    }

  becomes:

    div[x-foo] {
      font-weight: bold;
    }

  Note that elements that are dynamically added to a scope must have the scope
  selector added to them manually.

  * upper/lower bound encapsulation: Styles which are defined outside a
  shadowRoot should not cross the ShadowDOM boundary and should not apply
  inside a shadowRoot.

  This styling behavior is not emulated. Some possible ways to do this that
  were rejected due to complexity and/or performance concerns include: (1) reset
  every possible property for every possible selector for a given scope name;
  (2) re-implement css in javascript.

  As an alternative, users should make sure to use selectors
  specific to the scope in which they are working.

  * ::distributed: This behavior is not emulated. It's often not necessary
  to style the contents of a specific insertion point and instead, descendants
  of the host element can be styled selectively. Users can also create an
  extra node around an insertion point and style that node's contents
  via descendent selectors. For example, with a shadowRoot like this:

    <style>
      ::content(div) {
        background: red;
      }
    </style>
    <content></content>

  could become:

    <style>
      / *@polyfill .content-container div * /
      ::content(div) {
        background: red;
      }
    </style>
    <div class="content-container">
      <content></content>
    </div>

  Note the use of @polyfill in the comment above a ShadowDOM specific style
  declaration. This is a directive to the styling shim to use the selector
  in comments in lieu of the next selector when running under polyfill.
*/
var ShadowCss = /** @class */ (function () {
    function ShadowCss() {
        this.strictStyling = true;
    }
    /*
    * Shim some cssText with the given selector. Returns cssText that can
    * be included in the document via WebComponents.ShadowCSS.addCssToDocument(css).
    *
    * When strictStyling is true:
    * - selector is the attribute added to all elements inside the host,
    * - hostSelector is the attribute added to the host itself.
    */
    ShadowCss.prototype.shimCssText = function (cssText, selector, hostSelector) {
        if (hostSelector === void 0) { hostSelector = ''; }
        var commentsWithHash = extractCommentsWithHash(cssText);
        cssText = stripComments(cssText);
        cssText = this._insertDirectives(cssText);
        var scopedCssText = this._scopeCssText(cssText, selector, hostSelector);
        return tslib_1.__spread([scopedCssText], commentsWithHash).join('\n');
    };
    ShadowCss.prototype._insertDirectives = function (cssText) {
        cssText = this._insertPolyfillDirectivesInCssText(cssText);
        return this._insertPolyfillRulesInCssText(cssText);
    };
    /*
     * Process styles to convert native ShadowDOM rules that will trip
     * up the css parser; we rely on decorating the stylesheet with inert rules.
     *
     * For example, we convert this rule:
     *
     * polyfill-next-selector { content: ':host menu-item'; }
     * ::content menu-item {
     *
     * to this:
     *
     * scopeName menu-item {
     *
    **/
    ShadowCss.prototype._insertPolyfillDirectivesInCssText = function (cssText) {
        // Difference with webcomponents.js: does not handle comments
        return cssText.replace(_cssContentNextSelectorRe, function () {
            var m = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                m[_i] = arguments[_i];
            }
            return m[2] + '{';
        });
    };
    /*
     * Process styles to add rules which will only apply under the polyfill
     *
     * For example, we convert this rule:
     *
     * polyfill-rule {
     *   content: ':host menu-item';
     * ...
     * }
     *
     * to this:
     *
     * scopeName menu-item {...}
     *
    **/
    ShadowCss.prototype._insertPolyfillRulesInCssText = function (cssText) {
        // Difference with webcomponents.js: does not handle comments
        return cssText.replace(_cssContentRuleRe, function () {
            var m = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                m[_i] = arguments[_i];
            }
            var rule = m[0].replace(m[1], '').replace(m[2], '');
            return m[4] + rule;
        });
    };
    /* Ensure styles are scoped. Pseudo-scoping takes a rule like:
     *
     *  .foo {... }
     *
     *  and converts this to
     *
     *  scopeName .foo { ... }
    */
    ShadowCss.prototype._scopeCssText = function (cssText, scopeSelector, hostSelector) {
        var unscopedRules = this._extractUnscopedRulesFromCssText(cssText);
        // replace :host and :host-context -shadowcsshost and -shadowcsshost respectively
        cssText = this._insertPolyfillHostInCssText(cssText);
        cssText = this._convertColonHost(cssText);
        cssText = this._convertColonHostContext(cssText);
        cssText = this._convertShadowDOMSelectors(cssText);
        if (scopeSelector) {
            cssText = this._scopeSelectors(cssText, scopeSelector, hostSelector);
        }
        cssText = cssText + '\n' + unscopedRules;
        return cssText.trim();
    };
    /*
     * Process styles to add rules which will only apply under the polyfill
     * and do not process via CSSOM. (CSSOM is destructive to rules on rare
     * occasions, e.g. -webkit-calc on Safari.)
     * For example, we convert this rule:
     *
     * @polyfill-unscoped-rule {
     *   content: 'menu-item';
     * ... }
     *
     * to this:
     *
     * menu-item {...}
     *
    **/
    ShadowCss.prototype._extractUnscopedRulesFromCssText = function (cssText) {
        // Difference with webcomponents.js: does not handle comments
        var r = '';
        var m;
        _cssContentUnscopedRuleRe.lastIndex = 0;
        while ((m = _cssContentUnscopedRuleRe.exec(cssText)) !== null) {
            var rule = m[0].replace(m[2], '').replace(m[1], m[4]);
            r += rule + '\n\n';
        }
        return r;
    };
    /*
     * convert a rule like :host(.foo) > .bar { }
     *
     * to
     *
     * .foo<scopeName> > .bar
    */
    ShadowCss.prototype._convertColonHost = function (cssText) {
        return this._convertColonRule(cssText, _cssColonHostRe, this._colonHostPartReplacer);
    };
    /*
     * convert a rule like :host-context(.foo) > .bar { }
     *
     * to
     *
     * .foo<scopeName> > .bar, .foo scopeName > .bar { }
     *
     * and
     *
     * :host-context(.foo:host) .bar { ... }
     *
     * to
     *
     * .foo<scopeName> .bar { ... }
    */
    ShadowCss.prototype._convertColonHostContext = function (cssText) {
        return this._convertColonRule(cssText, _cssColonHostContextRe, this._colonHostContextPartReplacer);
    };
    ShadowCss.prototype._convertColonRule = function (cssText, regExp, partReplacer) {
        // m[1] = :host(-context), m[2] = contents of (), m[3] rest of rule
        return cssText.replace(regExp, function () {
            var m = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                m[_i] = arguments[_i];
            }
            if (m[2]) {
                var parts = m[2].split(',');
                var r = [];
                for (var i = 0; i < parts.length; i++) {
                    var p = parts[i].trim();
                    if (!p)
                        break;
                    r.push(partReplacer(_polyfillHostNoCombinator, p, m[3]));
                }
                return r.join(',');
            }
            else {
                return _polyfillHostNoCombinator + m[3];
            }
        });
    };
    ShadowCss.prototype._colonHostContextPartReplacer = function (host, part, suffix) {
        if (part.indexOf(_polyfillHost) > -1) {
            return this._colonHostPartReplacer(host, part, suffix);
        }
        else {
            return host + part + suffix + ', ' + part + ' ' + host + suffix;
        }
    };
    ShadowCss.prototype._colonHostPartReplacer = function (host, part, suffix) {
        return host + part.replace(_polyfillHost, '') + suffix;
    };
    /*
     * Convert combinators like ::shadow and pseudo-elements like ::content
     * by replacing with space.
    */
    ShadowCss.prototype._convertShadowDOMSelectors = function (cssText) {
        return _shadowDOMSelectorsRe.reduce(function (result, pattern) { return result.replace(pattern, ' '); }, cssText);
    };
    // change a selector like 'div' to 'name div'
    ShadowCss.prototype._scopeSelectors = function (cssText, scopeSelector, hostSelector) {
        var _this = this;
        return processRules(cssText, function (rule) {
            var selector = rule.selector;
            var content = rule.content;
            if (rule.selector[0] != '@') {
                selector =
                    _this._scopeSelector(rule.selector, scopeSelector, hostSelector, _this.strictStyling);
            }
            else if (rule.selector.startsWith('@media') || rule.selector.startsWith('@supports') ||
                rule.selector.startsWith('@page') || rule.selector.startsWith('@document')) {
                content = _this._scopeSelectors(rule.content, scopeSelector, hostSelector);
            }
            return new CssRule(selector, content);
        });
    };
    ShadowCss.prototype._scopeSelector = function (selector, scopeSelector, hostSelector, strict) {
        var _this = this;
        return selector.split(',')
            .map(function (part) { return part.trim().split(_shadowDeepSelectors); })
            .map(function (deepParts) {
            var _a = tslib_1.__read(deepParts), shallowPart = _a[0], otherParts = _a.slice(1);
            var applyScope = function (shallowPart) {
                if (_this._selectorNeedsScoping(shallowPart, scopeSelector)) {
                    return strict ?
                        _this._applyStrictSelectorScope(shallowPart, scopeSelector, hostSelector) :
                        _this._applySelectorScope(shallowPart, scopeSelector, hostSelector);
                }
                else {
                    return shallowPart;
                }
            };
            return tslib_1.__spread([applyScope(shallowPart)], otherParts).join(' ');
        })
            .join(', ');
    };
    ShadowCss.prototype._selectorNeedsScoping = function (selector, scopeSelector) {
        var re = this._makeScopeMatcher(scopeSelector);
        return !re.test(selector);
    };
    ShadowCss.prototype._makeScopeMatcher = function (scopeSelector) {
        var lre = /\[/g;
        var rre = /\]/g;
        scopeSelector = scopeSelector.replace(lre, '\\[').replace(rre, '\\]');
        return new RegExp('^(' + scopeSelector + ')' + _selectorReSuffix, 'm');
    };
    ShadowCss.prototype._applySelectorScope = function (selector, scopeSelector, hostSelector) {
        // Difference from webcomponents.js: scopeSelector could not be an array
        return this._applySimpleSelectorScope(selector, scopeSelector, hostSelector);
    };
    // scope via name and [is=name]
    ShadowCss.prototype._applySimpleSelectorScope = function (selector, scopeSelector, hostSelector) {
        // In Android browser, the lastIndex is not reset when the regex is used in String.replace()
        _polyfillHostRe.lastIndex = 0;
        if (_polyfillHostRe.test(selector)) {
            var replaceBy_1 = this.strictStyling ? "[" + hostSelector + "]" : scopeSelector;
            return selector
                .replace(_polyfillHostNoCombinatorRe, function (hnc, selector) {
                return selector.replace(/([^:]*)(:*)(.*)/, function (_, before, colon, after) {
                    return before + replaceBy_1 + colon + after;
                });
            })
                .replace(_polyfillHostRe, replaceBy_1 + ' ');
        }
        return scopeSelector + ' ' + selector;
    };
    // return a selector with [name] suffix on each simple selector
    // e.g. .foo.bar > .zot becomes .foo[name].bar[name] > .zot[name]  /** @internal */
    ShadowCss.prototype._applyStrictSelectorScope = function (selector, scopeSelector, hostSelector) {
        var _this = this;
        var isRe = /\[is=([^\]]*)\]/g;
        scopeSelector = scopeSelector.replace(isRe, function (_) {
            var parts = [];
            for (var _i = 1; _i < arguments.length; _i++) {
                parts[_i - 1] = arguments[_i];
            }
            return parts[0];
        });
        var attrName = '[' + scopeSelector + ']';
        var _scopeSelectorPart = function (p) {
            var scopedP = p.trim();
            if (!scopedP) {
                return '';
            }
            if (p.indexOf(_polyfillHostNoCombinator) > -1) {
                scopedP = _this._applySimpleSelectorScope(p, scopeSelector, hostSelector);
            }
            else {
                // remove :host since it should be unnecessary
                var t = p.replace(_polyfillHostRe, '');
                if (t.length > 0) {
                    var matches = t.match(/([^:]*)(:*)(.*)/);
                    if (matches) {
                        scopedP = matches[1] + attrName + matches[2] + matches[3];
                    }
                }
            }
            return scopedP;
        };
        var safeContent = new SafeSelector(selector);
        selector = safeContent.content();
        var scopedSelector = '';
        var startIndex = 0;
        var res;
        var sep = /( |>|\+|~(?!=))\s*/g;
        // If a selector appears before :host it should not be shimmed as it
        // matches on ancestor elements and not on elements in the host's shadow
        // `:host-context(div)` is transformed to
        // `-shadowcsshost-no-combinatordiv, div -shadowcsshost-no-combinator`
        // the `div` is not part of the component in the 2nd selectors and should not be scoped.
        // Historically `component-tag:host` was matching the component so we also want to preserve
        // this behavior to avoid breaking legacy apps (it should not match).
        // The behavior should be:
        // - `tag:host` -> `tag[h]` (this is to avoid breaking legacy apps, should not match anything)
        // - `tag :host` -> `tag [h]` (`tag` is not scoped because it's considered part of a
        //   `:host-context(tag)`)
        var hasHost = selector.indexOf(_polyfillHostNoCombinator) > -1;
        // Only scope parts after the first `-shadowcsshost-no-combinator` when it is present
        var shouldScope = !hasHost;
        while ((res = sep.exec(selector)) !== null) {
            var separator = res[1];
            var part_1 = selector.slice(startIndex, res.index).trim();
            shouldScope = shouldScope || part_1.indexOf(_polyfillHostNoCombinator) > -1;
            var scopedPart = shouldScope ? _scopeSelectorPart(part_1) : part_1;
            scopedSelector += scopedPart + " " + separator + " ";
            startIndex = sep.lastIndex;
        }
        var part = selector.substring(startIndex);
        shouldScope = shouldScope || part.indexOf(_polyfillHostNoCombinator) > -1;
        scopedSelector += shouldScope ? _scopeSelectorPart(part) : part;
        // replace the placeholders with their original values
        return safeContent.restore(scopedSelector);
    };
    ShadowCss.prototype._insertPolyfillHostInCssText = function (selector) {
        return selector.replace(_colonHostContextRe, _polyfillHostContext)
            .replace(_colonHostRe, _polyfillHost);
    };
    return ShadowCss;
}());
export { ShadowCss };
var SafeSelector = /** @class */ (function () {
    function SafeSelector(selector) {
        var _this = this;
        this.placeholders = [];
        this.index = 0;
        // Replaces attribute selectors with placeholders.
        // The WS in [attr="va lue"] would otherwise be interpreted as a selector separator.
        selector = selector.replace(/(\[[^\]]*\])/g, function (_, keep) {
            var replaceBy = "__ph-" + _this.index + "__";
            _this.placeholders.push(keep);
            _this.index++;
            return replaceBy;
        });
        // Replaces the expression in `:nth-child(2n + 1)` with a placeholder.
        // WS and "+" would otherwise be interpreted as selector separators.
        this._content = selector.replace(/(:nth-[-\w]+)(\([^)]+\))/g, function (_, pseudo, exp) {
            var replaceBy = "__ph-" + _this.index + "__";
            _this.placeholders.push(exp);
            _this.index++;
            return pseudo + replaceBy;
        });
    }
    SafeSelector.prototype.restore = function (content) {
        var _this = this;
        return content.replace(/__ph-(\d+)__/g, function (ph, index) { return _this.placeholders[+index]; });
    };
    SafeSelector.prototype.content = function () { return this._content; };
    return SafeSelector;
}());
var _cssContentNextSelectorRe = /polyfill-next-selector[^}]*content:[\s]*?(['"])(.*?)\1[;\s]*}([^{]*?){/gim;
var _cssContentRuleRe = /(polyfill-rule)[^}]*(content:[\s]*(['"])(.*?)\3)[;\s]*[^}]*}/gim;
var _cssContentUnscopedRuleRe = /(polyfill-unscoped-rule)[^}]*(content:[\s]*(['"])(.*?)\3)[;\s]*[^}]*}/gim;
var _polyfillHost = '-shadowcsshost';
// note: :host-context pre-processed to -shadowcsshostcontext.
var _polyfillHostContext = '-shadowcsscontext';
var _parenSuffix = ')(?:\\((' +
    '(?:\\([^)(]*\\)|[^)(]*)+?' +
    ')\\))?([^,{]*)';
var _cssColonHostRe = new RegExp('(' + _polyfillHost + _parenSuffix, 'gim');
var _cssColonHostContextRe = new RegExp('(' + _polyfillHostContext + _parenSuffix, 'gim');
var _polyfillHostNoCombinator = _polyfillHost + '-no-combinator';
var _polyfillHostNoCombinatorRe = /-shadowcsshost-no-combinator([^\s]*)/;
var _shadowDOMSelectorsRe = [
    /::shadow/g,
    /::content/g,
    // Deprecated selectors
    /\/shadow-deep\//g,
    /\/shadow\//g,
];
// The deep combinator is deprecated in the CSS spec
// Support for `>>>`, `deep`, `::ng-deep` is then also deprecated and will be removed in the future.
// see https://github.com/angular/angular/pull/17677
var _shadowDeepSelectors = /(?:>>>)|(?:\/deep\/)|(?:::ng-deep)/g;
var _selectorReSuffix = '([>\\s~+\[.,{:][\\s\\S]*)?$';
var _polyfillHostRe = /-shadowcsshost/gim;
var _colonHostRe = /:host/gim;
var _colonHostContextRe = /:host-context/gim;
var _commentRe = /\/\*\s*[\s\S]*?\*\//g;
function stripComments(input) {
    return input.replace(_commentRe, '');
}
var _commentWithHashRe = /\/\*\s*#\s*source(Mapping)?URL=[\s\S]+?\*\//g;
function extractCommentsWithHash(input) {
    return input.match(_commentWithHashRe) || [];
}
var _ruleRe = /(\s*)([^;\{\}]+?)(\s*)((?:{%BLOCK%}?\s*;?)|(?:\s*;))/g;
var _curlyRe = /([{}])/g;
var OPEN_CURLY = '{';
var CLOSE_CURLY = '}';
var BLOCK_PLACEHOLDER = '%BLOCK%';
var CssRule = /** @class */ (function () {
    function CssRule(selector, content) {
        this.selector = selector;
        this.content = content;
    }
    return CssRule;
}());
export { CssRule };
export function processRules(input, ruleCallback) {
    var inputWithEscapedBlocks = escapeBlocks(input);
    var nextBlockIndex = 0;
    return inputWithEscapedBlocks.escapedString.replace(_ruleRe, function () {
        var m = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            m[_i] = arguments[_i];
        }
        var selector = m[2];
        var content = '';
        var suffix = m[4];
        var contentPrefix = '';
        if (suffix && suffix.startsWith('{' + BLOCK_PLACEHOLDER)) {
            content = inputWithEscapedBlocks.blocks[nextBlockIndex++];
            suffix = suffix.substring(BLOCK_PLACEHOLDER.length + 1);
            contentPrefix = '{';
        }
        var rule = ruleCallback(new CssRule(selector, content));
        return "" + m[1] + rule.selector + m[3] + contentPrefix + rule.content + suffix;
    });
}
var StringWithEscapedBlocks = /** @class */ (function () {
    function StringWithEscapedBlocks(escapedString, blocks) {
        this.escapedString = escapedString;
        this.blocks = blocks;
    }
    return StringWithEscapedBlocks;
}());
function escapeBlocks(input) {
    var inputParts = input.split(_curlyRe);
    var resultParts = [];
    var escapedBlocks = [];
    var bracketCount = 0;
    var currentBlockParts = [];
    for (var partIndex = 0; partIndex < inputParts.length; partIndex++) {
        var part = inputParts[partIndex];
        if (part == CLOSE_CURLY) {
            bracketCount--;
        }
        if (bracketCount > 0) {
            currentBlockParts.push(part);
        }
        else {
            if (currentBlockParts.length > 0) {
                escapedBlocks.push(currentBlockParts.join(''));
                resultParts.push(BLOCK_PLACEHOLDER);
                currentBlockParts = [];
            }
            resultParts.push(part);
        }
        if (part == OPEN_CURLY) {
            bracketCount++;
        }
    }
    if (currentBlockParts.length > 0) {
        escapedBlocks.push(currentBlockParts.join(''));
        resultParts.push(BLOCK_PLACEHOLDER);
    }
    return new StringWithEscapedBlocks(resultParts.join(''), escapedBlocks);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2hhZG93X2Nzcy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9zaGFkb3dfY3NzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSDs7Ozs7Ozs7O0dBU0c7QUFFSDs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7RUFpSEU7QUFFRjtJQUdFO1FBRkEsa0JBQWEsR0FBWSxJQUFJLENBQUM7SUFFZixDQUFDO0lBRWhCOzs7Ozs7O01BT0U7SUFDRiwrQkFBVyxHQUFYLFVBQVksT0FBZSxFQUFFLFFBQWdCLEVBQUUsWUFBeUI7UUFBekIsNkJBQUEsRUFBQSxpQkFBeUI7UUFDdEUsSUFBTSxnQkFBZ0IsR0FBRyx1QkFBdUIsQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUMxRCxPQUFPLEdBQUcsYUFBYSxDQUFDLE9BQU8sQ0FBQyxDQUFDO1FBQ2pDLE9BQU8sR0FBRyxJQUFJLENBQUMsaUJBQWlCLENBQUMsT0FBTyxDQUFDLENBQUM7UUFFMUMsSUFBTSxhQUFhLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxPQUFPLEVBQUUsUUFBUSxFQUFFLFlBQVksQ0FBQyxDQUFDO1FBQzFFLE9BQU8sa0JBQUMsYUFBYSxHQUFLLGdCQUFnQixFQUFFLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUN6RCxDQUFDO0lBRU8scUNBQWlCLEdBQXpCLFVBQTBCLE9BQWU7UUFDdkMsT0FBTyxHQUFHLElBQUksQ0FBQyxrQ0FBa0MsQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUMzRCxPQUFPLElBQUksQ0FBQyw2QkFBNkIsQ0FBQyxPQUFPLENBQUMsQ0FBQztJQUNyRCxDQUFDO0lBRUQ7Ozs7Ozs7Ozs7Ozs7T0Fh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RUFBRSxTQUFTLEdBQUcsVUFBVSxDQUFDLE1BQU0sRUFBRSxTQUFTLEVBQUUsRUFBRTtRQUNsRSxJQUFNLElBQUksR0FBRyxVQUFVLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDbkMsSUFBSSxJQUFJLElBQUksV0FBVyxFQUFFO1lBQ3ZCLFlBQVksRUFBRSxDQUFDO1NBQ2hCO1FBQ0QsSUFBSSxZQUFZLEdBQUcsQ0FBQyxFQUFFO1lBQ3BCLGlCQUFpQixDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUM5QjthQUFNO1lBQ0wsSUFBSSxpQkFBaUIsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFO2dCQUNoQyxhQUFhLENBQUMsSUFBSSxDQUFDLGlCQUFpQixDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDO2dCQUMvQyxXQUFXLENBQUMsSUFBSSxDQUFDLGlCQUFpQixDQUFDLENBQUM7Z0JBQ3BDLGlCQUFpQixHQUFHLEVBQUUsQ0FBQzthQUN4QjtZQUNELFdBQVcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDeEI7UUFDRCxJQUFJLElBQUksSUFBSSxVQUFVLEVBQUU7WUFDdEIsWUFBWSxFQUFFLENBQUM7U0FDaEI7S0FDRjtJQUNELElBQUksaUJBQWlCLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtRQUNoQyxhQUFhLENBQUMsSUFBSSxDQUFDLGlCQUFpQixDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDO1FBQy9DLFdBQVcsQ0FBQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsQ0FBQztLQUNyQztJQUNELE9BQU8sSUFBSSx1QkFBdUIsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxFQUFFLGFBQWEsQ0FBQyxDQUFDO0FBQzFFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogVGhpcyBmaWxlIGlzIGEgcG9ydCBvZiBzaGFkb3dDU1MgZnJvbSB3ZWJjb21wb25lbnRzLmpzIHRvIFR5cGVTY3JpcHQuXG4gKlxuICogUGxlYXNlIG1ha2Ugc3VyZSB0byBrZWVwIHRvIGVkaXRzIGluIHN5bmMgd2l0aCB0aGUgc291cmNlIGZpbGUuXG4gKlxuICogU291cmNlOlxuICogaHR0cHM6Ly9naXRodWIuY29tL3dlYmNvbXBvbmVudHMvd2ViY29tcG9uZW50c2pzL2Jsb2IvNGVmZWNkN2UwZS9zcmMvU2hhZG93Q1NTL1NoYWRvd0NTUy5qc1xuICpcbiAqIFRoZSBvcmlnaW5hbCBmaWxlIGxldmVsIGNvbW1lbnQgaXMgcmVwcm9kdWNlZCBiZWxvd1xuICovXG5cbi8qXG4gIFRoaXMgaXMgYSBsaW1pdGVkIHNoaW0gZm9yIFNoYWRvd0RPTSBjc3Mgc3R5bGluZy5cbiAgaHR0cHM6Ly9kdmNzLnczLm9yZy9oZy93ZWJjb21wb25lbnRzL3Jhdy1maWxlL3RpcC9zcGVjL3NoYWRvdy9pbmRleC5odG1sI3N0eWxlc1xuXG4gIFRoZSBpbnRlbnRpb24gaGVyZSBpcyB0byBzdXBwb3J0IG9ubHkgdGhlIHN0eWxpbmcgZmVhdHVyZXMgd2hpY2ggY2FuIGJlXG4gIHJlbGF0aXZlbHkgc2ltcGx5IGltcGxlbWVudGVkLiBUaGUgZ29hbCBpcyB0byBhbGxvdyB1c2VycyB0byBhdm9pZCB0aGVcbiAgbW9zdCBvYnZpb3VzIHBpdGZhbGxzIGFuZCBkbyBzbyB3aXRob3V0IGNvbXByb21pc2luZyBwZXJmb3JtYW5jZSBzaWduaWZpY2FudGx5LlxuICBGb3IgU2hhZG93RE9NIHN0eWxpbmcgdGhhdCdzIG5vdCBjb3ZlcmVkIGhlcmUsIGEgc2V0IG9mIGJlc3QgcHJhY3RpY2VzXG4gIGNhbiBiZSBwcm92aWRlZCB0aGF0IHNob3VsZCBhbGxvdyB1c2VycyB0byBhY2NvbXBsaXNoIG1vcmUgY29tcGxleCBzdHlsaW5nLlxuXG4gIFRoZSBmb2xsb3dpbmcgaXMgYSBsaXN0IG9mIHNwZWNpZmljIFNoYWRvd0RPTSBzdHlsaW5nIGZlYXR1cmVzIGFuZCBhIGJyaWVmXG4gIGRpc2N1c3Npb24gb2YgdGhlIGFwcHJvYWNoIHVzZWQgdG8gc2hpbS5cblxuICBTaGltbWVkIGZlYXR1cmVzOlxuXG4gICogOmhvc3QsIDpob3N0LWNvbnRleHQ6IFNoYWRvd0RPTSBhbGxvd3Mgc3R5bGluZyBvZiB0aGUgc2hhZG93Um9vdCdzIGhvc3RcbiAgZWxlbWVudCB1c2luZyB0aGUgOmhvc3QgcnVsZS4gVG8gc2hpbSB0aGlzIGZlYXR1cmUsIHRoZSA6aG9zdCBzdHlsZXMgYXJlXG4gIHJlZm9ybWF0dGVkIGFuZCBwcmVmaXhlZCB3aXRoIGEgZ2l2ZW4gc2NvcGUgbmFtZSBhbmQgcHJvbW90ZWQgdG8gYVxuICBkb2N1bWVudCBsZXZlbCBzdHlsZXNoZWV0LlxuICBGb3IgZXhhbXBsZSwgZ2l2ZW4gYSBzY29wZSBuYW1lIG9mIC5mb28sIGEgcnVsZSBsaWtlIHRoaXM6XG5cbiAgICA6aG9zdCB7XG4gICAgICAgIGJhY2tncm91bmQ6IHJlZDtcbiAgICAgIH1cbiAgICB9XG5cbiAgYmVjb21lczpcblxuICAgIC5mb28ge1xuICAgICAgYmFja2dyb3VuZDogcmVkO1xuICAgIH1cblxuICAqIGVuY2Fwc3VsYXRpb246IFN0eWxlcyBkZWZpbmVkIHdpdGhpbiBTaGFkb3dET00sIGFwcGx5IG9ubHkgdG9cbiAgZG9tIGluc2lkZSB0aGUgU2hhZG93RE9NLiBQb2x5bWVyIHVzZXMgb25lIG9mIHR3byB0ZWNobmlxdWVzIHRvIGltcGxlbWVudFxuICB0aGlzIGZlYXR1cmUuXG5cbiAgQnkgZGVmYXVsdCwgcnVsZXMgYXJlIHByZWZpeGVkIHdpdGggdGhlIGhvc3QgZWxlbWVudCB0YWcgbmFtZVxuICBhcyBhIGRlc2NlbmRhbnQgc2VsZWN0b3IuIFRoaXMgZW5zdXJlcyBzdHlsaW5nIGRvZXMgbm90IGxlYWsgb3V0IG9mIHRoZSAndG9wJ1xuICBvZiB0aGUgZWxlbWVudCdzIFNoYWRvd0RPTS4gRm9yIGV4YW1wbGUsXG5cbiAgZGl2IHtcbiAgICAgIGZvbnQtd2VpZ2h0OiBib2xkO1xuICAgIH1cblxuICBiZWNvbWVzOlxuXG4gIHgtZm9vIGRpdiB7XG4gICAgICBmb250LXdlaWdodDogYm9sZDtcbiAgICB9XG5cbiAgYmVjb21lczpcblxuXG4gIEFsdGVybmF0aXZlbHksIGlmIFdlYkNvbXBvbmVudHMuU2hhZG93Q1NTLnN0cmljdFN0eWxpbmcgaXMgc2V0IHRvIHRydWUgdGhlblxuICBzZWxlY3RvcnMgYXJlIHNjb3BlZCBieSBhZGRpbmcgYW4gYXR0cmlidXRlIHNlbGVjdG9yIHN1ZmZpeCB0byBlYWNoXG4gIHNpbXBsZSBzZWxlY3RvciB0aGF0IGNvbnRhaW5zIHRoZSBob3N0IGVsZW1lbnQgdGFnIG5hbWUuIEVhY2ggZWxlbWVudFxuICBpbiB0aGUgZWxlbWVudCdzIFNoYWRvd0RPTSB0ZW1wbGF0ZSBpcyBhbHNvIGdpdmVuIHRoZSBzY29wZSBhdHRyaWJ1dGUuXG4gIFRodXMsIHRoZXNlIHJ1bGVzIG1hdGNoIG9ubHkgZWxlbWVudHMgdGhhdCBoYXZlIHRoZSBzY29wZSBhdHRyaWJ1dGUuXG4gIEZvciBleGFtcGxlLCBnaXZlbiBhIHNjb3BlIG5hbWUgb2YgeC1mb28sIGEgcnVsZSBsaWtlIHRoaXM6XG5cbiAgICBkaXYge1xuICAgICAgZm9udC13ZWlnaHQ6IGJvbGQ7XG4gICAgfVxuXG4gIGJlY29tZXM6XG5cbiAgICBkaXZbeC1mb29dIHtcbiAgICAgIGZvbnQtd2VpZ2h0OiBib2xkO1xuICAgIH1cblxuICBOb3RlIHRoYXQgZWxlbWVudHMgdGhhdCBhcmUgZHluYW1pY2FsbHkgYWRkZWQgdG8gYSBzY29wZSBtdXN0IGhhdmUgdGhlIHNjb3BlXG4gIHNlbGVjdG9yIGFkZGVkIHRvIHRoZW0gbWFudWFsbHkuXG5cbiAgKiB1cHBlci9sb3dlciBib3VuZCBlbmNhcHN1bGF0aW9uOiBTdHlsZXMgd2hpY2ggYXJlIGRlZmluZWQgb3V0c2lkZSBhXG4gIHNoYWRvd1Jvb3Qgc2hvdWxkIG5vdCBjcm9zcyB0aGUgU2hhZG93RE9NIGJvdW5kYXJ5IGFuZCBzaG91bGQgbm90IGFwcGx5XG4gIGluc2lkZSBhIHNoYWRvd1Jvb3QuXG5cbiAgVGhpcyBzdHlsaW5nIGJlaGF2aW9yIGlzIG5vdCBlbXVsYXRlZC4gU29tZSBwb3NzaWJsZSB3YXlzIHRvIGRvIHRoaXMgdGhhdFxuICB3ZXJlIHJlamVjdGVkIGR1ZSB0byBjb21wbGV4aXR5IGFuZC9vciBwZXJmb3JtYW5jZSBjb25jZXJucyBpbmNsdWRlOiAoMSkgcmVzZXRcbiAgZXZlcnkgcG9zc2libGUgcHJvcGVydHkgZm9yIGV2ZXJ5IHBvc3NpYmxlIHNlbGVjdG9yIGZvciBhIGdpdmVuIHNjb3BlIG5hbWU7XG4gICgyKSByZS1pbXBsZW1lbnQgY3NzIGluIGphdmFzY3JpcHQuXG5cbiAgQXMgYW4gYWx0ZXJuYXRpdmUsIHVzZXJzIHNob3VsZCBtYWtlIHN1cmUgdG8gdXNlIHNlbGVjdG9yc1xuICBzcGVjaWZpYyB0byB0aGUgc2NvcGUgaW4gd2hpY2ggdGhleSBhcmUgd29ya2luZy5cblxuICAqIDo6ZGlzdHJpYnV0ZWQ6IFRoaXMgYmVoYXZpb3IgaXMgbm90IGVtdWxhdGVkLiBJdCdzIG9mdGVuIG5vdCBuZWNlc3NhcnlcbiAgdG8gc3R5bGUgdGhlIGNvbnRlbnRzIG9mIGEgc3BlY2lmaWMgaW5zZXJ0aW9uIHBvaW50IGFuZCBpbnN0ZWFkLCBkZXNjZW5kYW50c1xuICBvZiB0aGUgaG9zdCBlbGVtZW50IGNhbiBiZSBzdHlsZWQgc2VsZWN0aXZlbHkuIFVzZXJzIGNhbiBhbHNvIGNyZWF0ZSBhblxuICBleHRyYSBub2RlIGFyb3VuZCBhbiBpbnNlcnRpb24gcG9pbnQgYW5kIHN0eWxlIHRoYXQgbm9kZSdzIGNvbnRlbnRzXG4gIHZpYSBkZXNjZW5kZW50IHNlbGVjdG9ycy4gRm9yIGV4YW1wbGUsIHdpdGggYSBzaGFkb3dSb290IGxpa2UgdGhpczpcblxuICAgIDxzdHlsZT5cbiAgICAgIDo6Y29udGVudChkaXYpIHtcbiAgICAgICAgYmFja2dyb3VuZDogcmVkO1xuICAgICAgfVxuICAgIDwvc3R5bGU+XG4gICAgPGNvbnRlbnQ+PC9jb250ZW50PlxuXG4gIGNvdWxkIGJlY29tZTpcblxuICAgIDxzdHlsZT5cbiAgICAgIC8gKkBwb2x5ZmlsbCAuY29udGVudC1jb250YWluZXIgZGl2ICogL1xuICAgICAgOjpjb250ZW50KGRpdikge1xuICAgICAgICBiYWNrZ3JvdW5kOiByZWQ7XG4gICAgICB9XG4gICAgPC9zdHlsZT5cbiAgICA8ZGl2IGNsYXNzPVwiY29udGVudC1jb250YWluZXJcIj5cbiAgICAgIDxjb250ZW50PjwvY29udGVudD5cbiAgICA8L2Rpdj5cblxuICBOb3RlIHRoZSB1c2Ugb2YgQHBvbHlmaWxsIGluIHRoZSBjb21tZW50IGFib3ZlIGEgU2hhZG93RE9NIHNwZWNpZmljIHN0eWxlXG4gIGRlY2xhcmF0aW9uLiBUaGlzIGlzIGEgZGlyZWN0aXZlIHRvIHRoZSBzdHlsaW5nIHNoaW0gdG8gdXNlIHRoZSBzZWxlY3RvclxuICBpbiBjb21tZW50cyBpbiBsaWV1IG9mIHRoZSBuZXh0IHNlbGVjdG9yIHdoZW4gcnVubmluZyB1bmRlciBwb2x5ZmlsbC5cbiovXG5cbmV4cG9ydCBjbGFzcyBTaGFkb3dDc3Mge1xuICBzdHJpY3RTdHlsaW5nOiBib29sZWFuID0gdHJ1ZTtcblxuICBjb25zdHJ1Y3RvcigpIHt9XG5cbiAgLypcbiAgKiBTaGltIHNvbWUgY3NzVGV4dCB3aXRoIHRoZSBnaXZlbiBzZWxlY3Rvci4gUmV0dXJucyBjc3NUZXh0IHRoYXQgY2FuXG4gICogYmUgaW5jbHVkZWQgaW4gdGhlIGRvY3VtZW50IHZpYSBXZWJDb21wb25lbnRzLlNoYWRvd0NTUy5hZGRDc3NUb0RvY3VtZW50KGNzcykuXG4gICpcbiAgKiBXaGVuIHN0cmljdFN0eWxpbmcgaXMgdHJ1ZTpcbiAgKiAtIHNlbGVjdG9yIGlzIHRoZSBhdHRyaWJ1dGUgYWRkZWQgdG8gYWxsIGVsZW1lbnRzIGluc2lkZSB0aGUgaG9zdCxcbiAgKiAtIGhvc3RTZWxlY3RvciBpcyB0aGUgYXR0cmlidXRlIGFkZGVkIHRvIHRoZSBob3N0IGl0c2VsZi5cbiAgKi9cbiAgc2hpbUNzc1RleHQoY3NzVGV4dDogc3RyaW5nLCBzZWxlY3Rvcjogc3RyaW5nLCBob3N0U2VsZWN0b3I6IHN0cmluZyA9ICcnKTogc3RyaW5nIHtcbiAgICBjb25zdCBjb21tZW50c1dpdGhIYXNoID0gZXh0cmFjdENvbW1lbnRzV2l0aEhhc2goY3NzVGV4dCk7XG4gICAgY3NzVGV4dCA9IHN0cmlwQ29tbWVudHMoY3NzVGV4dCk7XG4gICAgY3NzVGV4dCA9IHRoaXMuX2luc2VydERpcmVjdGl2ZXMoY3NzVGV4dCk7XG5cbiAgICBjb25zdCBzY29wZWRDc3NUZXh0ID0gdGhpcy5fc2NvcGVDc3NUZXh0KGNzc1RleHQsIHNlbGVjdG9yLCBob3N0U2VsZWN0b3IpO1xuICAgIHJldHVybiBbc2NvcGVkQ3NzVGV4dCwgLi4uY29tbWVudHNXaXRoSGFzaF0uam9pbignXFxuJyk7XG4gIH1cblxuICBwcml2YXRlIF9pbnNlcnREaXJlY3RpdmVzKGNzc1RleHQ6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgY3NzVGV4dCA9IHRoaXMuX2luc2VydFBvbHlmaWxsRGlyZWN0aXZlc0luQ3NzVGV4dChjc3NUZXh0KTtcbiAgICByZXR1cm4gdGhpcy5faW5zZXJ0UG9seWZpbGxSdWxlc0luQ3NzVGV4dChjc3NUZXh0KTtcbiAgfVxuXG4gIC8qXG4gICAqIFByb2Nlc3Mgc3R5bGVzIHRvIGNvbnZlcnQgbmF0aXZlIFNoYWRvd0RPTSBydWxlcyB0aGF0IHdpbGwgdHJpcFxuICAgKiB1cCB0aGUgY3NzIHBhcnNlcjsgd2UgcmVseSBvbiBkZWNvcmF0aW5nIHRoZSBzdHlsZXNoZWV0IHdpdGggaW5lcnQgcnVsZXMuXG4gICAqXG4gICAqIEZvciBleGFtcGxlLCB3ZSBjb252ZXJ0IHRoaXMgcnVsZTpcbiAgICpcbiAgICogcG9seWZpbGwtbmV4dC1zZWxlY3RvciB7IGNvbnRlbnQ6ICc6aG9zdCBtZW51LWl0ZW0nOyB9XG4gICAqIDo6Y29udGVudCBtZW51LWl0ZW0ge1xuICAgKlxuICAgKiB0byB0aGlzOlxuICAgKlxuICAgKiBzY29wZU5hbWUgbWVudS1pdGVtIHtcbiAgICpcbiAgKiovXG4gIHByaXZhdGUgX2luc2VydFBvbHlmaWxsRGlyZWN0aXZlc0luQ3NzVGV4dChjc3NUZXh0OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIC8vIERpZmZlcmVuY2Ugd2l0aCB3ZWJjb21wb25lbnRzLmpzOiBkb2VzIG5vdCBoYW5kbGUgY29tbWVudHNcbiAgICByZXR1cm4gY3NzVGV4dC5yZXBsYWNlKFxuICAgICAgICBfY3NzQ29udGVudE5leHRTZWxlY3RvclJlLCBmdW5jdGlvbiguLi5tOiBzdHJpbmdbXSkgeyByZXR1cm4gbVsyXSArICd7JzsgfSk7XG4gIH1cblxuICAvKlxuICAgKiBQcm9jZXNzIHN0eWxlcyB0byBhZGQgcnVsZXMgd2hpY2ggd2lsbCBvbmx5IGFwcGx5IHVuZGVyIHRoZSBwb2x5ZmlsbFxuICAgKlxuICAgKiBGb3IgZXhhbXBsZSwgd2UgY29udmVydCB0aGlzIHJ1bGU6XG4gICAqXG4gICAqIHBvbHlmaWxsLXJ1bGUge1xuICAgKiAgIGNvbnRlbnQ6ICc6aG9zdCBtZW51LWl0ZW0nO1xuICAgKiAuLi5cbiAgICogfVxuICAgKlxuICAgKiB0byB0aGlzOlxuICAgKlxuICAgKiBzY29wZU5hbWUgbWVudS1pdGVtIHsuLi59XG4gICAqXG4gICoqL1xuICBwcml2YXRlIF9pbnNlcnRQb2x5ZmlsbFJ1bGVzSW5Dc3NUZXh0KGNzc1RleHQ6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgLy8gRGlmZmVyZW5jZSB3aXRoIHdlYmNvbXBvbmVudHMuanM6IGRvZXMgbm90IGhhbmRsZSBjb21tZW50c1xuICAgIHJldHVybiBjc3NUZXh0LnJlcGxhY2UoX2Nzc0NvbnRlbnRSdWxlUmUsICguLi5tOiBzdHJpbmdbXSkgPT4ge1xuICAgICAgY29uc3QgcnVsZSA9IG1bMF0ucmVwbGFjZShtWzFdLCAnJykucmVwbGFjZShtWzJdLCAnJyk7XG4gICAgICByZXR1cm4gbVs0XSArIHJ1bGU7XG4gICAgfSk7XG4gIH1cblxuICAvKiBFbnN1cmUgc3R5bGVzIGFyZSBzY29wZWQuIFBzZXVkby1zY29waW5nIHRha2VzIGEgcnVsZSBsaWtlOlxuICAgKlxuICAgKiAgLmZvbyB7Li4uIH1cbiAgICpcbiAgICogIGFuZCBjb252ZXJ0cyB0aGlzIHRvXG4gICAqXG4gICAqICBzY29wZU5hbWUgLmZvbyB7IC4uLiB9XG4gICovXG4gIHByaXZhdGUgX3Njb3BlQ3NzVGV4dChjc3NUZXh0OiBzdHJpbmcsIHNjb3BlU2VsZWN0b3I6IHN0cmluZywgaG9zdFNlbGVjdG9yOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGNvbnN0IHVuc2NvcGVkUnVsZXMgPSB0aGlzLl9leHRyYWN0VW5zY29wZWRSdWxlc0Zyb21Dc3NUZXh0KGNzc1RleHQpO1xuICAgIC8vIHJlcGxhY2UgOmhvc3QgYW5kIDpob3N0LWNvbnRleHQgLXNoYWRvd2Nzc2hvc3QgYW5kIC1zaGFkb3djc3Nob3N0IHJlc3BlY3RpdmVseVxuICAgIGNzc1RleHQgPSB0aGlzLl9pbnNlcnRQb2x5ZmlsbEhvc3RJbkNzc1RleHQoY3NzVGV4dCk7XG4gICAgY3NzVGV4dCA9IHRoaXMuX2NvbnZlcnRDb2xvbkhvc3QoY3NzVGV4dCk7XG4gICAgY3NzVGV4dCA9IHRoaXMuX2NvbnZlcnRDb2xvbkhvc3RDb250ZXh0KGNzc1RleHQpO1xuICAgIGNzc1RleHQgPSB0aGlzLl9jb252ZXJ0U2hhZG93RE9NU2VsZWN0b3JzKGNzc1RleHQpO1xuICAgIGlmIChzY29wZVNlbGVjdG9yKSB7XG4gICAgICBjc3NUZXh0ID0gdGhpcy5fc2NvcGVTZWxlY3RvcnMoY3NzVGV4dCwgc2NvcGVTZWxlY3RvciwgaG9zdFNlbGVjdG9yKTtcbiAgICB9XG4gICAgY3NzVGV4dCA9IGNzc1RleHQgKyAnXFxuJyArIHVuc2NvcGVkUnVsZXM7XG4gICAgcmV0dXJuIGNzc1RleHQudHJpbSgpO1xuICB9XG5cbiAgLypcbiAgICogUHJvY2VzcyBzdHlsZXMgdG8gYWRkIHJ1bGVzIHdoaWNoIHdpbGwgb25seSBhcHBseSB1bmRlciB0aGUgcG9seWZpbGxcbiAgICogYW5kIGRvIG5vdCBwcm9jZXNzIHZpYSBDU1NPTS4gKENTU09NIGlzIGRlc3RydWN0aXZlIHRvIHJ1bGVzIG9uIHJhcmVcbiAgICogb2NjYXNpb25zLCBlLmcuIC13ZWJraXQtY2FsYyBvbiBTYWZhcmkuKVxuICAgKiBGb3IgZXhhbXBsZSwgd2UgY29udmVydCB0aGlzIHJ1bGU6XG4gICAqXG4gICAqIEBwb2x5ZmlsbC11bnNjb3BlZC1ydWxlIHtcbiAgICogICBjb250ZW50OiAnbWVudS1pdGVtJztcbiAgICogLi4uIH1cbiAgICpcbiAgICogdG8gdGhpczpcbiAgICpcbiAgICogbWVudS1pdGVtIHsuLi59XG4gICAqXG4gICoqL1xuICBwcml2YXRlIF9leHRyYWN0VW5zY29wZWRSdWxlc0Zyb21Dc3NUZXh0KGNzc1RleHQ6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgLy8gRGlmZmVyZW5jZSB3aXRoIHdlYmNvbXBvbmVudHMuanM6IGRvZXMgbm90IGhhbmRsZSBjb21tZW50c1xuICAgIGxldCByID0gJyc7XG4gICAgbGV0IG06IFJlZ0V4cEV4ZWNBcnJheXxudWxsO1xuICAgIF9jc3NDb250ZW50VW5zY29wZWRSdWxlUmUubGFzdEluZGV4ID0gMDtcbiAgICB3aGlsZSAoKG0gPSBfY3NzQ29udGVudFVuc2NvcGVkUnVsZVJlLmV4ZWMoY3NzVGV4dCkpICE9PSBudWxsKSB7XG4gICAgICBjb25zdCBydWxlID0gbVswXS5yZXBsYWNlKG1bMl0sICcnKS5yZXBsYWNlKG1bMV0sIG1bNF0pO1xuICAgICAgciArPSBydWxlICsgJ1xcblxcbic7XG4gICAgfVxuICAgIHJldHVybiByO1xuICB9XG5cbiAgLypcbiAgICogY29udmVydCBhIHJ1bGUgbGlrZSA6aG9zdCguZm9vKSA+IC5iYXIgeyB9XG4gICAqXG4gICAqIHRvXG4gICAqXG4gICAqIC5mb288c2NvcGVOYW1lPiA+IC5iYXJcbiAgKi9cbiAgcHJpdmF0ZSBfY29udmVydENvbG9uSG9zdChjc3NUZXh0OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl9jb252ZXJ0Q29sb25SdWxlKGNzc1RleHQsIF9jc3NDb2xvbkhvc3RSZSwgdGhpcy5fY29sb25Ib3N0UGFydFJlcGxhY2VyKTtcbiAgfVxuXG4gIC8qXG4gICAqIGNvbnZlcnQgYSBydWxlIGxpa2UgOmhvc3QtY29udGV4dCguZm9vKSA+IC5iYXIgeyB9XG4gICAqXG4gICAqIHRvXG4gICAqXG4gICAqIC5mb288c2NvcGVOYW1lPiA+IC5iYXIsIC5mb28gc2NvcGVOYW1lID4gLmJhciB7IH1cbiAgICpcbiAgICogYW5kXG4gICAqXG4gICAqIDpob3N0LWNvbnRleHQoLmZvbzpob3N0KSAuYmFyIHsgLi4uIH1cbiAgICpcbiAgICogdG9cbiAgICpcbiAgICogLmZvbzxzY29wZU5hbWU+IC5iYXIgeyAuLi4gfVxuICAqL1xuICBwcml2YXRlIF9jb252ZXJ0Q29sb25Ib3N0Q29udGV4dChjc3NUZXh0OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl9jb252ZXJ0Q29sb25SdWxlKFxuICAgICAgICBjc3NUZXh0LCBfY3NzQ29sb25Ib3N0Q29udGV4dFJlLCB0aGlzLl9jb2xvbkhvc3RDb250ZXh0UGFydFJlcGxhY2VyKTtcbiAgfVxuXG4gIHByaXZhdGUgX2NvbnZlcnRDb2xvblJ1bGUoY3NzVGV4dDogc3RyaW5nLCByZWdFeHA6IFJlZ0V4cCwgcGFydFJlcGxhY2VyOiBGdW5jdGlvbik6IHN0cmluZyB7XG4gICAgLy8gbVsxXSA9IDpob3N0KC1jb250ZXh0KSwgbVsyXSA9IGNvbnRlbnRzIG9mICgpLCBtWzNdIHJlc3Qgb2YgcnVsZVxuICAgIHJldHVybiBjc3NUZXh0LnJlcGxhY2UocmVnRXhwLCBmdW5jdGlvbiguLi5tOiBzdHJpbmdbXSkge1xuICAgICAgaWYgKG1bMl0pIHtcbiAgICAgICAgY29uc3QgcGFydHMgPSBtWzJdLnNwbGl0KCcsJyk7XG4gICAgICAgIGNvbnN0IHI6IHN0cmluZ1tdID0gW107XG4gICAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgcGFydHMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgICBjb25zdCBwID0gcGFydHNbaV0udHJpbSgpO1xuICAgICAgICAgIGlmICghcCkgYnJlYWs7XG4gICAgICAgICAgci5wdXNoKHBhcnRSZXBsYWNlcihfcG9seWZpbGxIb3N0Tm9Db21iaW5hdG9yLCBwLCBtWzNdKSk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHIuam9pbignLCcpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIF9wb2x5ZmlsbEhvc3ROb0NvbWJpbmF0b3IgKyBtWzNdO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfY29sb25Ib3N0Q29udGV4dFBhcnRSZXBsYWNlcihob3N0OiBzdHJpbmcsIHBhcnQ6IHN0cmluZywgc3VmZml4OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGlmIChwYXJ0LmluZGV4T2YoX3BvbHlmaWxsSG9zdCkgPiAtMSkge1xuICAgICAgcmV0dXJuIHRoaXMuX2NvbG9uSG9zdFBhcnRSZXBsYWNlcihob3N0LCBwYXJ0LCBzdWZmaXgpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gaG9zdCArIHBhcnQgKyBzdWZmaXggKyAnLCAnICsgcGFydCArICcgJyArIGhvc3QgKyBzdWZmaXg7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfY29sb25Ib3N0UGFydFJlcGxhY2VyKGhvc3Q6IHN0cmluZywgcGFydDogc3RyaW5nLCBzdWZmaXg6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgcmV0dXJuIGhvc3QgKyBwYXJ0LnJlcGxhY2UoX3BvbHlmaWxsSG9zdCwgJycpICsgc3VmZml4O1xuICB9XG5cbiAgLypcbiAgICogQ29udmVydCBjb21iaW5hdG9ycyBsaWtlIDo6c2hhZG93IGFuZCBwc2V1ZG8tZWxlbWVudHMgbGlrZSA6OmNvbnRlbnRcbiAgICogYnkgcmVwbGFjaW5nIHdpdGggc3BhY2UuXG4gICovXG4gIHByaXZhdGUgX2NvbnZlcnRTaGFkb3dET01TZWxlY3RvcnMoY3NzVGV4dDogc3RyaW5nKTogc3RyaW5nIHtcbiAgICByZXR1cm4gX3NoYWRvd0RPTVNlbGVjdG9yc1JlLnJlZHVjZSgocmVzdWx0LCBwYXR0ZXJuKSA9PiByZXN1bHQucmVwbGFjZShwYXR0ZXJuLCAnICcpLCBjc3NUZXh0KTtcbiAgfVxuXG4gIC8vIGNoYW5nZSBhIHNlbGVjdG9yIGxpa2UgJ2RpdicgdG8gJ25hbWUgZGl2J1xuICBwcml2YXRlIF9zY29wZVNlbGVjdG9ycyhjc3NUZXh0OiBzdHJpbmcsIHNjb3BlU2VsZWN0b3I6IHN0cmluZywgaG9zdFNlbGVjdG9yOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIHJldHVybiBwcm9jZXNzUnVsZXMoY3NzVGV4dCwgKHJ1bGU6IENzc1J1bGUpID0+IHtcbiAgICAgIGxldCBzZWxlY3RvciA9IHJ1bGUuc2VsZWN0b3I7XG4gICAgICBsZXQgY29udGVudCA9IHJ1bGUuY29udGVudDtcbiAgICAgIGlmIChydWxlLnNlbGVjdG9yWzBdICE9ICdAJykge1xuICAgICAgICBzZWxlY3RvciA9XG4gICAgICAgICAgICB0aGlzLl9zY29wZVNlbGVjdG9yKHJ1bGUuc2VsZWN0b3IsIHNjb3BlU2VsZWN0b3IsIGhvc3RTZWxlY3RvciwgdGhpcy5zdHJpY3RTdHlsaW5nKTtcbiAgICAgIH0gZWxzZSBpZiAoXG4gICAgICAgICAgcnVsZS5zZWxlY3Rvci5zdGFydHNXaXRoKCdAbWVkaWEnKSB8fCBydWxlLnNlbGVjdG9yLnN0YXJ0c1dpdGgoJ0BzdXBwb3J0cycpIHx8XG4gICAgICAgICAgcnVsZS5zZWxlY3Rvci5zdGFydHNXaXRoKCdAcGFnZScpIHx8IHJ1bGUuc2VsZWN0b3Iuc3RhcnRzV2l0aCgnQGRvY3VtZW50JykpIHtcbiAgICAgICAgY29udGVudCA9IHRoaXMuX3Njb3BlU2VsZWN0b3JzKHJ1bGUuY29udGVudCwgc2NvcGVTZWxlY3RvciwgaG9zdFNlbGVjdG9yKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBuZXcgQ3NzUnVsZShzZWxlY3RvciwgY29udGVudCk7XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIF9zY29wZVNlbGVjdG9yKFxuICAgICAgc2VsZWN0b3I6IHN0cmluZywgc2NvcGVTZWxlY3Rvcjogc3RyaW5nLCBob3N0U2VsZWN0b3I6IHN0cmluZywgc3RyaWN0OiBib29sZWFuKTogc3RyaW5nIHtcbiAgICByZXR1cm4gc2VsZWN0b3Iuc3BsaXQoJywnKVxuICAgICAgICAubWFwKHBhcnQgPT4gcGFydC50cmltKCkuc3BsaXQoX3NoYWRvd0RlZXBTZWxlY3RvcnMpKVxuICAgICAgICAubWFwKChkZWVwUGFydHMpID0+IHtcbiAgICAgICAgICBjb25zdCBbc2hhbGxvd1BhcnQsIC4uLm90aGVyUGFydHNdID0gZGVlcFBhcnRzO1xuICAgICAgICAgIGNvbnN0IGFwcGx5U2NvcGUgPSAoc2hhbGxvd1BhcnQ6IHN0cmluZykgPT4ge1xuICAgICAgICAgICAgaWYgKHRoaXMuX3NlbGVjdG9yTmVlZHNTY29waW5nKHNoYWxsb3dQYXJ0LCBzY29wZVNlbGVjdG9yKSkge1xuICAgICAgICAgICAgICByZXR1cm4gc3RyaWN0ID9cbiAgICAgICAgICAgICAgICAgIHRoaXMuX2FwcGx5U3RyaWN0U2VsZWN0b3JTY29wZShzaGFsbG93UGFydCwgc2NvcGVTZWxlY3RvciwgaG9zdFNlbGVjdG9yKSA6XG4gICAgICAgICAgICAgICAgICB0aGlzLl9hcHBseVNlbGVjdG9yU2NvcGUoc2hhbGxvd1BhcnQsIHNjb3BlU2VsZWN0b3IsIGhvc3RTZWxlY3Rvcik7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICByZXR1cm4gc2hhbGxvd1BhcnQ7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfTtcbiAgICAgICAgICByZXR1cm4gW2FwcGx5U2NvcGUoc2hhbGxvd1BhcnQpLCAuLi5vdGhlclBhcnRzXS5qb2luKCcgJyk7XG4gICAgICAgIH0pXG4gICAgICAgIC5qb2luKCcsICcpO1xuICB9XG5cbiAgcHJpdmF0ZSBfc2VsZWN0b3JOZWVkc1Njb3Bpbmcoc2VsZWN0b3I6IHN0cmluZywgc2NvcGVTZWxlY3Rvcjogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgY29uc3QgcmUgPSB0aGlzLl9tYWtlU2NvcGVNYXRjaGVyKHNjb3BlU2VsZWN0b3IpO1xuICAgIHJldHVybiAhcmUudGVzdChzZWxlY3Rvcik7XG4gIH1cblxuICBwcml2YXRlIF9tYWtlU2NvcGVNYXRjaGVyKHNjb3BlU2VsZWN0b3I6IHN0cmluZyk6IFJlZ0V4cCB7XG4gICAgY29uc3QgbHJlID0gL1xcWy9nO1xuICAgIGNvbnN0IHJyZSA9IC9cXF0vZztcbiAgICBzY29wZVNlbGVjdG9yID0gc2NvcGVTZWxlY3Rvci5yZXBsYWNlKGxyZSwgJ1xcXFxbJykucmVwbGFjZShycmUsICdcXFxcXScpO1xuICAgIHJldHVybiBuZXcgUmVnRXhwKCdeKCcgKyBzY29wZVNlbGVjdG9yICsgJyknICsgX3NlbGVjdG9yUmVTdWZmaXgsICdtJyk7XG4gIH1cblxuICBwcml2YXRlIF9hcHBseVNlbGVjdG9yU2NvcGUoc2VsZWN0b3I6IHN0cmluZywgc2NvcGVTZWxlY3Rvcjogc3RyaW5nLCBob3N0U2VsZWN0b3I6IHN0cmluZyk6XG4gICAgICBzdHJpbmcge1xuICAgIC8vIERpZmZlcmVuY2UgZnJvbSB3ZWJjb21wb25lbnRzLmpzOiBzY29wZVNlbGVjdG9yIGNvdWxkIG5vdCBiZSBhbiBhcnJheVxuICAgIHJldHVybiB0aGlzLl9hcHBseVNpbXBsZVNlbGVjdG9yU2NvcGUoc2VsZWN0b3IsIHNjb3BlU2VsZWN0b3IsIGhvc3RTZWxlY3Rvcik7XG4gIH1cblxuICAvLyBzY29wZSB2aWEgbmFtZSBhbmQgW2lzPW5hbWVdXG4gIHByaXZhdGUgX2FwcGx5U2ltcGxlU2VsZWN0b3JTY29wZShzZWxlY3Rvcjogc3RyaW5nLCBzY29wZVNlbGVjdG9yOiBzdHJpbmcsIGhvc3RTZWxlY3Rvcjogc3RyaW5nKTpcbiAgICAgIHN0cmluZyB7XG4gICAgLy8gSW4gQW5kcm9pZCBicm93c2VyLCB0aGUgbGFzdEluZGV4IGlzIG5vdCByZXNldCB3aGVuIHRoZSByZWdleCBpcyB1c2VkIGluIFN0cmluZy5yZXBsYWNlKClcbiAgICBfcG9seWZpbGxIb3N0UmUubGFzdEluZGV4ID0gMDtcbiAgICBpZiAoX3BvbHlmaWxsSG9zdFJlLnRlc3Qoc2VsZWN0b3IpKSB7XG4gICAgICBjb25zdCByZXBsYWNlQnkgPSB0aGlzLnN0cmljdFN0eWxpbmcgPyBgWyR7aG9zdFNlbGVjdG9yfV1gIDogc2NvcGVTZWxlY3RvcjtcbiAgICAgIHJldHVybiBzZWxlY3RvclxuICAgICAgICAgIC5yZXBsYWNlKFxuICAgICAgICAgICAgICBfcG9seWZpbGxIb3N0Tm9Db21iaW5hdG9yUmUsXG4gICAgICAgICAgICAgIChobmMsIHNlbGVjdG9yKSA9PiB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHNlbGVjdG9yLnJlcGxhY2UoXG4gICAgICAgICAgICAgICAgICAgIC8oW146XSopKDoqKSguKikvLFxuICAgICAgICAgICAgICAgICAgICAoXzogc3RyaW5nLCBiZWZvcmU6IHN0cmluZywgY29sb246IHN0cmluZywgYWZ0ZXI6IHN0cmluZykgPT4ge1xuICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBiZWZvcmUgKyByZXBsYWNlQnkgKyBjb2xvbiArIGFmdGVyO1xuICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgfSlcbiAgICAgICAgICAucmVwbGFjZShfcG9seWZpbGxIb3N0UmUsIHJlcGxhY2VCeSArICcgJyk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHNjb3BlU2VsZWN0b3IgKyAnICcgKyBzZWxlY3RvcjtcbiAgfVxuXG4gIC8vIHJldHVybiBhIHNlbGVjdG9yIHdpdGggW25hbWVdIHN1ZmZpeCBvbiBlYWNoIHNpbXBsZSBzZWxlY3RvclxuICAvLyBlLmcuIC5mb28uYmFyID4gLnpvdCBiZWNvbWVzIC5mb29bbmFtZV0uYmFyW25hbWVdID4gLnpvdFtuYW1lXSAgLyoqIEBpbnRlcm5hbCAqL1xuICBwcml2YXRlIF9hcHBseVN0cmljdFNlbGVjdG9yU2NvcGUoc2VsZWN0b3I6IHN0cmluZywgc2NvcGVTZWxlY3Rvcjogc3RyaW5nLCBob3N0U2VsZWN0b3I6IHN0cmluZyk6XG4gICAgICBzdHJpbmcge1xuICAgIGNvbnN0IGlzUmUgPSAvXFxbaXM9KFteXFxdXSopXFxdL2c7XG4gICAgc2NvcGVTZWxlY3RvciA9IHNjb3BlU2VsZWN0b3IucmVwbGFjZShpc1JlLCAoXzogc3RyaW5nLCAuLi5wYXJ0czogc3RyaW5nW10pID0+IHBhcnRzWzBdKTtcblxuICAgIGNvbnN0IGF0dHJOYW1lID0gJ1snICsgc2NvcGVTZWxlY3RvciArICddJztcblxuICAgIGNvbnN0IF9zY29wZVNlbGVjdG9yUGFydCA9IChwOiBzdHJpbmcpID0+IHtcbiAgICAgIGxldCBzY29wZWRQID0gcC50cmltKCk7XG5cbiAgICAgIGlmICghc2NvcGVkUCkge1xuICAgICAgICByZXR1cm4gJyc7XG4gICAgICB9XG5cbiAgICAgIGlmIChwLmluZGV4T2YoX3BvbHlmaWxsSG9zdE5vQ29tYmluYXRvcikgPiAtMSkge1xuICAgICAgICBzY29wZWRQID0gdGhpcy5fYXBwbHlTaW1wbGVTZWxlY3RvclNjb3BlKHAsIHNjb3BlU2VsZWN0b3IsIGhvc3RTZWxlY3Rvcik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICAvLyByZW1vdmUgOmhvc3Qgc2luY2UgaXQgc2hvdWxkIGJlIHVubmVjZXNzYXJ5XG4gICAgICAgIGNvbnN0IHQgPSBwLnJlcGxhY2UoX3BvbHlmaWxsSG9zdFJlLCAnJyk7XG4gICAgICAgIGlmICh0Lmxlbmd0aCA+IDApIHtcbiAgICAgICAgICBjb25zdCBtYXRjaGVzID0gdC5tYXRjaCgvKFteOl0qKSg6KikoLiopLyk7XG4gICAgICAgICAgaWYgKG1hdGNoZXMpIHtcbiAgICAgICAgICAgIHNjb3BlZFAgPSBtYXRjaGVzWzFdICsgYXR0ck5hbWUgKyBtYXRjaGVzWzJdICsgbWF0Y2hlc1szXTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHNjb3BlZFA7XG4gICAgfTtcblxuICAgIGNvbnN0IHNhZmVDb250ZW50ID0gbmV3IFNhZmVTZWxlY3RvcihzZWxlY3Rvcik7XG4gICAgc2VsZWN0b3IgPSBzYWZlQ29udGVudC5jb250ZW50KCk7XG5cbiAgICBsZXQgc2NvcGVkU2VsZWN0b3IgPSAnJztcbiAgICBsZXQgc3RhcnRJbmRleCA9IDA7XG4gICAgbGV0IHJlczogUmVnRXhwRXhlY0FycmF5fG51bGw7XG4gICAgY29uc3Qgc2VwID0gLyggfD58XFwrfH4oPyE9KSlcXHMqL2c7XG5cbiAgICAvLyBJZiBhIHNlbGVjdG9yIGFwcGVhcnMgYmVmb3JlIDpob3N0IGl0IHNob3VsZCBub3QgYmUgc2hpbW1lZCBhcyBpdFxuICAgIC8vIG1hdGNoZXMgb24gYW5jZXN0b3IgZWxlbWVudHMgYW5kIG5vdCBvbiBlbGVtZW50cyBpbiB0aGUgaG9zdCdzIHNoYWRvd1xuICAgIC8vIGA6aG9zdC1jb250ZXh0KGRpdilgIGlzIHRyYW5zZm9ybWVkIHRvXG4gICAgLy8gYC1zaGFkb3djc3Nob3N0LW5vLWNvbWJpbmF0b3JkaXYsIGRpdiAtc2hhZG93Y3NzaG9zdC1uby1jb21iaW5hdG9yYFxuICAgIC8vIHRoZSBgZGl2YCBpcyBub3QgcGFydCBvZiB0aGUgY29tcG9uZW50IGluIHRoZSAybmQgc2VsZWN0b3JzIGFuZCBzaG91bGQgbm90IGJlIHNjb3BlZC5cbiAgICAvLyBIaXN0b3JpY2FsbHkgYGNvbXBvbmVudC10YWc6aG9zdGAgd2FzIG1hdGNoaW5nIHRoZSBjb21wb25lbnQgc28gd2UgYWxzbyB3YW50IHRvIHByZXNlcnZlXG4gICAgLy8gdGhpcyBiZWhhdmlvciB0byBhdm9pZCBicmVha2luZyBsZWdhY3kgYXBwcyAoaXQgc2hvdWxkIG5vdCBtYXRjaCkuXG4gICAgLy8gVGhlIGJlaGF2aW9yIHNob3VsZCBiZTpcbiAgICAvLyAtIGB0YWc6aG9zdGAgLT4gYHRhZ1toXWAgKHRoaXMgaXMgdG8gYXZvaWQgYnJlYWtpbmcgbGVnYWN5IGFwcHMsIHNob3VsZCBub3QgbWF0Y2ggYW55dGhpbmcpXG4gICAgLy8gLSBgdGFnIDpob3N0YCAtPiBgdGFnIFtoXWAgKGB0YWdgIGlzIG5vdCBzY29wZWQgYmVjYXVzZSBpdCdzIGNvbnNpZGVyZWQgcGFydCBvZiBhXG4gICAgLy8gICBgOmhvc3QtY29udGV4dCh0YWcpYClcbiAgICBjb25zdCBoYXNIb3N0ID0gc2VsZWN0b3IuaW5kZXhPZihfcG9seWZpbGxIb3N0Tm9Db21iaW5hdG9yKSA+IC0xO1xuICAgIC8vIE9ubHkgc2NvcGUgcGFydHMgYWZ0ZXIgdGhlIGZpcnN0IGAtc2hhZG93Y3NzaG9zdC1uby1jb21iaW5hdG9yYCB3aGVuIGl0IGlzIHByZXNlbnRcbiAgICBsZXQgc2hvdWxkU2NvcGUgPSAhaGFzSG9zdDtcblxuICAgIHdoaWxlICgocmVzID0gc2VwLmV4ZWMoc2VsZWN0b3IpKSAhPT0gbnVsbCkge1xuICAgICAgY29uc3Qgc2VwYXJhdG9yID0gcmVzWzFdO1xuICAgICAgY29uc3QgcGFydCA9IHNlbGVjdG9yLnNsaWNlKHN0YXJ0SW5kZXgsIHJlcy5pbmRleCkudHJpbSgpO1xuICAgICAgc2hvdWxkU2NvcGUgPSBzaG91bGRTY29wZSB8fCBwYXJ0LmluZGV4T2YoX3BvbHlmaWxsSG9zdE5vQ29tYmluYXRvcikgPiAtMTtcbiAgICAgIGNvbnN0IHNjb3BlZFBhcnQgPSBzaG91bGRTY29wZSA/IF9zY29wZVNlbGVjdG9yUGFydChwYXJ0KSA6IHBhcnQ7XG4gICAgICBzY29wZWRTZWxlY3RvciArPSBgJHtzY29wZWRQYXJ0fSAke3NlcGFyYXRvcn0gYDtcbiAgICAgIHN0YXJ0SW5kZXggPSBzZXAubGFzdEluZGV4O1xuICAgIH1cblxuICAgIGNvbnN0IHBhcnQgPSBzZWxlY3Rvci5zdWJzdHJpbmcoc3RhcnRJbmRleCk7XG4gICAgc2hvdWxkU2NvcGUgPSBzaG91bGRTY29wZSB8fCBwYXJ0LmluZGV4T2YoX3BvbHlmaWxsSG9zdE5vQ29tYmluYXRvcikgPiAtMTtcbiAgICBzY29wZWRTZWxlY3RvciArPSBzaG91bGRTY29wZSA/IF9zY29wZVNlbGVjdG9yUGFydChwYXJ0KSA6IHBhcnQ7XG5cbiAgICAvLyByZXBsYWNlIHRoZSBwbGFjZWhvbGRlcnMgd2l0aCB0aGVpciBvcmlnaW5hbCB2YWx1ZXNcbiAgICByZXR1cm4gc2FmZUNvbnRlbnQucmVzdG9yZShzY29wZWRTZWxlY3Rvcik7XG4gIH1cblxuICBwcml2YXRlIF9pbnNlcnRQb2x5ZmlsbEhvc3RJbkNzc1RleHQoc2VsZWN0b3I6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHNlbGVjdG9yLnJlcGxhY2UoX2NvbG9uSG9zdENvbnRleHRSZSwgX3BvbHlmaWxsSG9zdENvbnRleHQpXG4gICAgICAgIC5yZXBsYWNlKF9jb2xvbkhvc3RSZSwgX3BvbHlmaWxsSG9zdCk7XG4gIH1cbn1cblxuY2xhc3MgU2FmZVNlbGVjdG9yIHtcbiAgcHJpdmF0ZSBwbGFjZWhvbGRlcnM6IHN0cmluZ1tdID0gW107XG4gIHByaXZhdGUgaW5kZXggPSAwO1xuICBwcml2YXRlIF9jb250ZW50OiBzdHJpbmc7XG5cbiAgY29uc3RydWN0b3Ioc2VsZWN0b3I6IHN0cmluZykge1xuICAgIC8vIFJlcGxhY2VzIGF0dHJpYnV0ZSBzZWxlY3RvcnMgd2l0aCBwbGFjZWhvbGRlcnMuXG4gICAgLy8gVGhlIFdTIGluIFthdHRyPVwidmEgbHVlXCJdIHdvdWxkIG90aGVyd2lzZSBiZSBpbnRlcnByZXRlZCBhcyBhIHNlbGVjdG9yIHNlcGFyYXRvci5cbiAgICBzZWxlY3RvciA9IHNlbGVjdG9yLnJlcGxhY2UoLyhcXFtbXlxcXV0qXFxdKS9nLCAoXywga2VlcCkgPT4ge1xuICAgICAgY29uc3QgcmVwbGFjZUJ5ID0gYF9fcGgtJHt0aGlzLmluZGV4fV9fYDtcbiAgICAgIHRoaXMucGxhY2Vob2xkZXJzLnB1c2goa2VlcCk7XG4gICAgICB0aGlzLmluZGV4Kys7XG4gICAgICByZXR1cm4gcmVwbGFjZUJ5O1xuICAgIH0pO1xuXG4gICAgLy8gUmVwbGFjZXMgdGhlIGV4cHJlc3Npb24gaW4gYDpudGgtY2hpbGQoMm4gKyAxKWAgd2l0aCBhIHBsYWNlaG9sZGVyLlxuICAgIC8vIFdTIGFuZCBcIitcIiB3b3VsZCBvdGhlcndpc2UgYmUgaW50ZXJwcmV0ZWQgYXMgc2VsZWN0b3Igc2VwYXJhdG9ycy5cbiAgICB0aGlzLl9jb250ZW50ID0gc2VsZWN0b3IucmVwbGFjZSgvKDpudGgtWy1cXHddKykoXFwoW14pXStcXCkpL2csIChfLCBwc2V1ZG8sIGV4cCkgPT4ge1xuICAgICAgY29uc3QgcmVwbGFjZUJ5ID0gYF9fcGgtJHt0aGlzLmluZGV4fV9fYDtcbiAgICAgIHRoaXMucGxhY2Vob2xkZXJzLnB1c2goZXhwKTtcbiAgICAgIHRoaXMuaW5kZXgrKztcbiAgICAgIHJldHVybiBwc2V1ZG8gKyByZXBsYWNlQnk7XG4gICAgfSk7XG4gIH1cblxuICByZXN0b3JlKGNvbnRlbnQ6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgcmV0dXJuIGNvbnRlbnQucmVwbGFjZSgvX19waC0oXFxkKylfXy9nLCAocGgsIGluZGV4KSA9PiB0aGlzLnBsYWNlaG9sZGVyc1sraW5kZXhdKTtcbiAgfVxuXG4gIGNvbnRlbnQoKTogc3RyaW5nIHsgcmV0dXJuIHRoaXMuX2NvbnRlbnQ7IH1cbn1cblxuY29uc3QgX2Nzc0NvbnRlbnROZXh0U2VsZWN0b3JSZSA9XG4gICAgL3BvbHlmaWxsLW5leHQtc2VsZWN0b3JbXn1dKmNvbnRlbnQ6W1xcc10qPyhbJ1wiXSkoLio/KVxcMVs7XFxzXSp9KFtee10qPyl7L2dpbTtcbmNvbnN0IF9jc3NDb250ZW50UnVsZVJlID0gLyhwb2x5ZmlsbC1ydWxlKVtefV0qKGNvbnRlbnQ6W1xcc10qKFsnXCJdKSguKj8pXFwzKVs7XFxzXSpbXn1dKn0vZ2ltO1xuY29uc3QgX2Nzc0NvbnRlbnRVbnNjb3BlZFJ1bGVSZSA9XG4gICAgLyhwb2x5ZmlsbC11bnNjb3BlZC1ydWxlKVtefV0qKGNvbnRlbnQ6W1xcc10qKFsnXCJdKSguKj8pXFwzKVs7XFxzXSpbXn1dKn0vZ2ltO1xuY29uc3QgX3BvbHlmaWxsSG9zdCA9ICctc2hhZG93Y3NzaG9zdCc7XG4vLyBub3RlOiA6aG9zdC1jb250ZXh0IHByZS1wcm9jZXNzZWQgdG8gLXNoYWRvd2Nzc2hvc3Rjb250ZXh0LlxuY29uc3QgX3BvbHlmaWxsSG9zdENvbnRleHQgPSAnLXNoYWRvd2Nzc2NvbnRleHQnO1xuY29uc3QgX3BhcmVuU3VmZml4ID0gJykoPzpcXFxcKCgnICtcbiAgICAnKD86XFxcXChbXikoXSpcXFxcKXxbXikoXSopKz8nICtcbiAgICAnKVxcXFwpKT8oW14se10qKSc7XG5jb25zdCBfY3NzQ29sb25Ib3N0UmUgPSBuZXcgUmVnRXhwKCcoJyArIF9wb2x5ZmlsbEhvc3QgKyBfcGFyZW5TdWZmaXgsICdnaW0nKTtcbmNvbnN0IF9jc3NDb2xvbkhvc3RDb250ZXh0UmUgPSBuZXcgUmVnRXhwKCcoJyArIF9wb2x5ZmlsbEhvc3RDb250ZXh0ICsgX3BhcmVuU3VmZml4LCAnZ2ltJyk7XG5jb25zdCBfcG9seWZpbGxIb3N0Tm9Db21iaW5hdG9yID0gX3BvbHlmaWxsSG9zdCArICctbm8tY29tYmluYXRvcic7XG5jb25zdCBfcG9seWZpbGxIb3N0Tm9Db21iaW5hdG9yUmUgPSAvLXNoYWRvd2Nzc2hvc3Qtbm8tY29tYmluYXRvcihbXlxcc10qKS87XG5jb25zdCBfc2hhZG93RE9NU2VsZWN0b3JzUmUgPSBbXG4gIC86OnNoYWRvdy9nLFxuICAvOjpjb250ZW50L2csXG4gIC8vIERlcHJlY2F0ZWQgc2VsZWN0b3JzXG4gIC9cXC9zaGFkb3ctZGVlcFxcLy9nLFxuICAvXFwvc2hhZG93XFwvL2csXG5dO1xuXG4vLyBUaGUgZGVlcCBjb21iaW5hdG9yIGlzIGRlcHJlY2F0ZWQgaW4gdGhlIENTUyBzcGVjXG4vLyBTdXBwb3J0IGZvciBgPj4+YCwgYGRlZXBgLCBgOjpuZy1kZWVwYCBpcyB0aGVuIGFsc28gZGVwcmVjYXRlZCBhbmQgd2lsbCBiZSByZW1vdmVkIGluIHRoZSBmdXR1cmUuXG4vLyBzZWUgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9wdWxsLzE3Njc3XG5jb25zdCBfc2hhZG93RGVlcFNlbGVjdG9ycyA9IC8oPzo+Pj4pfCg/OlxcL2RlZXBcXC8pfCg/Ojo6bmctZGVlcCkvZztcbmNvbnN0IF9zZWxlY3RvclJlU3VmZml4ID0gJyhbPlxcXFxzfitcXFsuLHs6XVtcXFxcc1xcXFxTXSopPyQnO1xuY29uc3QgX3BvbHlmaWxsSG9zdFJlID0gLy1zaGFkb3djc3Nob3N0L2dpbTtcbmNvbnN0IF9jb2xvbkhvc3RSZSA9IC86aG9zdC9naW07XG5jb25zdCBfY29sb25Ib3N0Q29udGV4dFJlID0gLzpob3N0LWNvbnRleHQvZ2ltO1xuXG5jb25zdCBfY29tbWVudFJlID0gL1xcL1xcKlxccypbXFxzXFxTXSo/XFwqXFwvL2c7XG5cbmZ1bmN0aW9uIHN0cmlwQ29tbWVudHMoaW5wdXQ6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBpbnB1dC5yZXBsYWNlKF9jb21tZW50UmUsICcnKTtcbn1cblxuY29uc3QgX2NvbW1lbnRXaXRoSGFzaFJlID0gL1xcL1xcKlxccyojXFxzKnNvdXJjZShNYXBwaW5nKT9VUkw9W1xcc1xcU10rP1xcKlxcLy9nO1xuXG5mdW5jdGlvbiBleHRyYWN0Q29tbWVudHNXaXRoSGFzaChpbnB1dDogc3RyaW5nKTogc3RyaW5nW10ge1xuICByZXR1cm4gaW5wdXQubWF0Y2goX2NvbW1lbnRXaXRoSGFzaFJlKSB8fCBbXTtcbn1cblxuY29uc3QgX3J1bGVSZSA9IC8oXFxzKikoW147XFx7XFx9XSs/KShcXHMqKSgoPzp7JUJMT0NLJX0/XFxzKjs/KXwoPzpcXHMqOykpL2c7XG5jb25zdCBfY3VybHlSZSA9IC8oW3t9XSkvZztcbmNvbnN0IE9QRU5fQ1VSTFkgPSAneyc7XG5jb25zdCBDTE9TRV9DVVJMWSA9ICd9JztcbmNvbnN0IEJMT0NLX1BMQUNFSE9MREVSID0gJyVCTE9DSyUnO1xuXG5leHBvcnQgY2xhc3MgQ3NzUnVsZSB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBzZWxlY3Rvcjogc3RyaW5nLCBwdWJsaWMgY29udGVudDogc3RyaW5nKSB7fVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcHJvY2Vzc1J1bGVzKGlucHV0OiBzdHJpbmcsIHJ1bGVDYWxsYmFjazogKHJ1bGU6IENzc1J1bGUpID0+IENzc1J1bGUpOiBzdHJpbmcge1xuICBjb25zdCBpbnB1dFdpdGhFc2NhcGVkQmxvY2tzID0gZXNjYXBlQmxvY2tzKGlucHV0KTtcbiAgbGV0IG5leHRCbG9ja0luZGV4ID0gMDtcbiAgcmV0dXJuIGlucHV0V2l0aEVzY2FwZWRCbG9ja3MuZXNjYXBlZFN0cmluZy5yZXBsYWNlKF9ydWxlUmUsIGZ1bmN0aW9uKC4uLm06IHN0cmluZ1tdKSB7XG4gICAgY29uc3Qgc2VsZWN0b3IgPSBtWzJdO1xuICAgIGxldCBjb250ZW50ID0gJyc7XG4gICAgbGV0IHN1ZmZpeCA9IG1bNF07XG4gICAgbGV0IGNvbnRlbnRQcmVmaXggPSAnJztcbiAgICBpZiAoc3VmZml4ICYmIHN1ZmZpeC5zdGFydHNXaXRoKCd7JyArIEJMT0NLX1BMQUNFSE9MREVSKSkge1xuICAgICAgY29udGVudCA9IGlucHV0V2l0aEVzY2FwZWRCbG9ja3MuYmxvY2tzW25leHRCbG9ja0luZGV4KytdO1xuICAgICAgc3VmZml4ID0gc3VmZml4LnN1YnN0cmluZyhCTE9DS19QTEFDRUhPTERFUi5sZW5ndGggKyAxKTtcbiAgICAgIGNvbnRlbnRQcmVmaXggPSAneyc7XG4gICAgfVxuICAgIGNvbnN0IHJ1bGUgPSBydWxlQ2FsbGJhY2sobmV3IENzc1J1bGUoc2VsZWN0b3IsIGNvbnRlbnQpKTtcbiAgICByZXR1cm4gYCR7bVsxXX0ke3J1bGUuc2VsZWN0b3J9JHttWzNdfSR7Y29udGVudFByZWZpeH0ke3J1bGUuY29udGVudH0ke3N1ZmZpeH1gO1xuICB9KTtcbn1cblxuY2xhc3MgU3RyaW5nV2l0aEVzY2FwZWRCbG9ja3Mge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgZXNjYXBlZFN0cmluZzogc3RyaW5nLCBwdWJsaWMgYmxvY2tzOiBzdHJpbmdbXSkge31cbn1cblxuZnVuY3Rpb24gZXNjYXBlQmxvY2tzKGlucHV0OiBzdHJpbmcpOiBTdHJpbmdXaXRoRXNjYXBlZEJsb2NrcyB7XG4gIGNvbnN0IGlucHV0UGFydHMgPSBpbnB1dC5zcGxpdChfY3VybHlSZSk7XG4gIGNvbnN0IHJlc3VsdFBhcnRzOiBzdHJpbmdbXSA9IFtdO1xuICBjb25zdCBlc2NhcGVkQmxvY2tzOiBzdHJpbmdbXSA9IFtdO1xuICBsZXQgYnJhY2tldENvdW50ID0gMDtcbiAgbGV0IGN1cnJlbnRCbG9ja1BhcnRzOiBzdHJpbmdbXSA9IFtdO1xuICBmb3IgKGxldCBwYXJ0SW5kZXggPSAwOyBwYXJ0SW5kZXggPCBpbnB1dFBhcnRzLmxlbmd0aDsgcGFydEluZGV4KyspIHtcbiAgICBjb25zdCBwYXJ0ID0gaW5wdXRQYXJ0c1twYXJ0SW5kZXhdO1xuICAgIGlmIChwYXJ0ID09IENMT1NFX0NVUkxZKSB7XG4gICAgICBicmFja2V0Q291bnQtLTtcbiAgICB9XG4gICAgaWYgKGJyYWNrZXRDb3VudCA+IDApIHtcbiAgICAgIGN1cnJlbnRCbG9ja1BhcnRzLnB1c2gocGFydCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChjdXJyZW50QmxvY2tQYXJ0cy5sZW5ndGggPiAwKSB7XG4gICAgICAgIGVzY2FwZWRCbG9ja3MucHVzaChjdXJyZW50QmxvY2tQYXJ0cy5qb2luKCcnKSk7XG4gICAgICAgIHJlc3VsdFBhcnRzLnB1c2goQkxPQ0tfUExBQ0VIT0xERVIpO1xuICAgICAgICBjdXJyZW50QmxvY2tQYXJ0cyA9IFtdO1xuICAgICAgfVxuICAgICAgcmVzdWx0UGFydHMucHVzaChwYXJ0KTtcbiAgICB9XG4gICAgaWYgKHBhcnQgPT0gT1BFTl9DVVJMWSkge1xuICAgICAgYnJhY2tldENvdW50Kys7XG4gICAgfVxuICB9XG4gIGlmIChjdXJyZW50QmxvY2tQYXJ0cy5sZW5ndGggPiAwKSB7XG4gICAgZXNjYXBlZEJsb2Nrcy5wdXNoKGN1cnJlbnRCbG9ja1BhcnRzLmpvaW4oJycpKTtcbiAgICByZXN1bHRQYXJ0cy5wdXNoKEJMT0NLX1BMQUNFSE9MREVSKTtcbiAgfVxuICByZXR1cm4gbmV3IFN0cmluZ1dpdGhFc2NhcGVkQmxvY2tzKHJlc3VsdFBhcnRzLmpvaW4oJycpLCBlc2NhcGVkQmxvY2tzKTtcbn1cbiJdfQ==