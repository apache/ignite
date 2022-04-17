/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
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
export declare class ShadowCss {
    strictStyling: boolean;
    constructor();
    shimCssText(cssText: string, selector: string, hostSelector?: string): string;
    private _insertDirectives;
    private _insertPolyfillDirectivesInCssText;
    private _insertPolyfillRulesInCssText;
    private _scopeCssText;
    private _extractUnscopedRulesFromCssText;
    private _convertColonHost;
    private _convertColonHostContext;
    private _convertColonRule;
    private _colonHostContextPartReplacer;
    private _colonHostPartReplacer;
    private _convertShadowDOMSelectors;
    private _scopeSelectors;
    private _scopeSelector;
    private _selectorNeedsScoping;
    private _makeScopeMatcher;
    private _applySelectorScope;
    private _applySimpleSelectorScope;
    private _insertPolyfillHostInCssText;
}
export declare class CssRule {
    selector: string;
    content: string;
    constructor(selector: string, content: string);
}
export declare function processRules(input: string, ruleCallback: (rule: CssRule) => CssRule): string;
