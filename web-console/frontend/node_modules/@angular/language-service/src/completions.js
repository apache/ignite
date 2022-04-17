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
        define("@angular/language-service/src/completions", ["require", "exports", "tslib", "@angular/compiler", "@angular/compiler-cli/src/language_services", "@angular/language-service/src/expressions", "@angular/language-service/src/html_info", "@angular/language-service/src/utils"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compiler_1 = require("@angular/compiler");
    var language_services_1 = require("@angular/compiler-cli/src/language_services");
    var expressions_1 = require("@angular/language-service/src/expressions");
    var html_info_1 = require("@angular/language-service/src/html_info");
    var utils_1 = require("@angular/language-service/src/utils");
    var TEMPLATE_ATTR_PREFIX = '*';
    var hiddenHtmlElements = {
        html: true,
        script: true,
        noscript: true,
        base: true,
        body: true,
        title: true,
        head: true,
        link: true,
    };
    function getTemplateCompletions(templateInfo) {
        var result = undefined;
        var htmlAst = templateInfo.htmlAst, templateAst = templateInfo.templateAst, template = templateInfo.template;
        // The templateNode starts at the delimiter character so we add 1 to skip it.
        if (templateInfo.position != null) {
            var templatePosition_1 = templateInfo.position - template.span.start;
            var path_1 = compiler_1.findNode(htmlAst, templatePosition_1);
            var mostSpecific = path_1.tail;
            if (path_1.empty || !mostSpecific) {
                result = elementCompletions(templateInfo, path_1);
            }
            else {
                var astPosition_1 = templatePosition_1 - mostSpecific.sourceSpan.start.offset;
                mostSpecific.visit({
                    visitElement: function (ast) {
                        var startTagSpan = utils_1.spanOf(ast.sourceSpan);
                        var tagLen = ast.name.length;
                        if (templatePosition_1 <=
                            startTagSpan.start + tagLen + 1 /* 1 for the opening angle bracked */) {
                            // If we are in the tag then return the element completions.
                            result = elementCompletions(templateInfo, path_1);
                        }
                        else if (templatePosition_1 < startTagSpan.end) {
                            // We are in the attribute section of the element (but not in an attribute).
                            // Return the attribute completions.
                            result = attributeCompletions(templateInfo, path_1);
                        }
                    },
                    visitAttribute: function (ast) {
                        if (!ast.valueSpan || !utils_1.inSpan(templatePosition_1, utils_1.spanOf(ast.valueSpan))) {
                            // We are in the name of an attribute. Show attribute completions.
                            result = attributeCompletions(templateInfo, path_1);
                        }
                        else if (ast.valueSpan && utils_1.inSpan(templatePosition_1, utils_1.spanOf(ast.valueSpan))) {
                            result = attributeValueCompletions(templateInfo, templatePosition_1, ast);
                        }
                    },
                    visitText: function (ast) {
                        // Check if we are in a entity.
                        result = entityCompletions(getSourceText(template, utils_1.spanOf(ast)), astPosition_1);
                        if (result)
                            return result;
                        result = interpolationCompletions(templateInfo, templatePosition_1);
                        if (result)
                            return result;
                        var element = path_1.first(compiler_1.Element);
                        if (element) {
                            var definition = compiler_1.getHtmlTagDefinition(element.name);
                            if (definition.contentType === compiler_1.TagContentType.PARSABLE_DATA) {
                                result = voidElementAttributeCompletions(templateInfo, path_1);
                                if (!result) {
                                    // If the element can hold content Show element completions.
                                    result = elementCompletions(templateInfo, path_1);
                                }
                            }
                        }
                        else {
                            // If no element container, implies parsable data so show elements.
                            result = voidElementAttributeCompletions(templateInfo, path_1);
                            if (!result) {
                                result = elementCompletions(templateInfo, path_1);
                            }
                        }
                    },
                    visitComment: function (ast) { },
                    visitExpansion: function (ast) { },
                    visitExpansionCase: function (ast) { }
                }, null);
            }
        }
        return result;
    }
    exports.getTemplateCompletions = getTemplateCompletions;
    function attributeCompletions(info, path) {
        var item = path.tail instanceof compiler_1.Element ? path.tail : path.parentOf(path.tail);
        if (item instanceof compiler_1.Element) {
            return attributeCompletionsForElement(info, item.name, item);
        }
        return undefined;
    }
    function attributeCompletionsForElement(info, elementName, element) {
        var attributes = getAttributeInfosForElement(info, elementName, element);
        // Map all the attributes to a completion
        return attributes.map(function (attr) { return ({
            kind: attr.fromHtml ? 'html attribute' : 'attribute',
            name: nameOfAttr(attr),
            sort: attr.name
        }); });
    }
    function getAttributeInfosForElement(info, elementName, element) {
        var attributes = [];
        // Add html attributes
        var htmlAttributes = html_info_1.attributeNames(elementName) || [];
        if (htmlAttributes) {
            attributes.push.apply(attributes, tslib_1.__spread(htmlAttributes.map(function (name) { return ({ name: name, fromHtml: true }); })));
        }
        // Add html properties
        var htmlProperties = html_info_1.propertyNames(elementName);
        if (htmlProperties) {
            attributes.push.apply(attributes, tslib_1.__spread(htmlProperties.map(function (name) { return ({ name: name, input: true }); })));
        }
        // Add html events
        var htmlEvents = html_info_1.eventNames(elementName);
        if (htmlEvents) {
            attributes.push.apply(attributes, tslib_1.__spread(htmlEvents.map(function (name) { return ({ name: name, output: true }); })));
        }
        var _a = utils_1.getSelectors(info), selectors = _a.selectors, selectorMap = _a.map;
        if (selectors && selectors.length) {
            // All the attributes that are selectable should be shown.
            var applicableSelectors = selectors.filter(function (selector) { return !selector.element || selector.element == elementName; });
            var selectorAndAttributeNames = applicableSelectors.map(function (selector) { return ({ selector: selector, attrs: selector.attrs.filter(function (a) { return !!a; }) }); });
            var attrs_1 = utils_1.flatten(selectorAndAttributeNames.map(function (selectorAndAttr) {
                var directive = selectorMap.get(selectorAndAttr.selector);
                var result = selectorAndAttr.attrs.map(function (name) { return ({ name: name, input: name in directive.inputs, output: name in directive.outputs }); });
                return result;
            }));
            // Add template attribute if a directive contains a template reference
            selectorAndAttributeNames.forEach(function (selectorAndAttr) {
                var selector = selectorAndAttr.selector;
                var directive = selectorMap.get(selector);
                if (directive && utils_1.hasTemplateReference(directive.type) && selector.attrs.length &&
                    selector.attrs[0]) {
                    attrs_1.push({ name: selector.attrs[0], template: true });
                }
            });
            // All input and output properties of the matching directives should be added.
            var elementSelector = element ?
                createElementCssSelector(element) :
                createElementCssSelector(new compiler_1.Element(elementName, [], [], null, null, null));
            var matcher = new compiler_1.SelectorMatcher();
            matcher.addSelectables(selectors);
            matcher.match(elementSelector, function (selector) {
                var directive = selectorMap.get(selector);
                if (directive) {
                    var inputs_1 = directive.inputs, outputs_1 = directive.outputs;
                    attrs_1.push.apply(attrs_1, tslib_1.__spread(Object.keys(inputs_1).map(function (name) { return ({ name: inputs_1[name], input: true }); })));
                    attrs_1.push.apply(attrs_1, tslib_1.__spread(Object.keys(outputs_1).map(function (name) { return ({ name: outputs_1[name], output: true }); })));
                }
            });
            // If a name shows up twice, fold it into a single value.
            attrs_1 = foldAttrs(attrs_1);
            // Now expand them back out to ensure that input/output shows up as well as input and
            // output.
            attributes.push.apply(attributes, tslib_1.__spread(utils_1.flatten(attrs_1.map(expandedAttr))));
        }
        return attributes;
    }
    function attributeValueCompletions(info, position, attr) {
        var path = utils_1.findTemplateAstAt(info.templateAst, position);
        var mostSpecific = path.tail;
        var dinfo = utils_1.diagnosticInfoFromTemplateInfo(info);
        if (mostSpecific) {
            var visitor = new ExpressionVisitor(info, position, attr, function () { return language_services_1.getExpressionScope(dinfo, path, false); });
            mostSpecific.visit(visitor, null);
            if (!visitor.result || !visitor.result.length) {
                // Try allwoing widening the path
                var widerPath_1 = utils_1.findTemplateAstAt(info.templateAst, position, /* allowWidening */ true);
                if (widerPath_1.tail) {
                    var widerVisitor = new ExpressionVisitor(info, position, attr, function () { return language_services_1.getExpressionScope(dinfo, widerPath_1, false); });
                    widerPath_1.tail.visit(widerVisitor, null);
                    return widerVisitor.result;
                }
            }
            return visitor.result;
        }
    }
    function elementCompletions(info, path) {
        var htmlNames = html_info_1.elementNames().filter(function (name) { return !(name in hiddenHtmlElements); });
        // Collect the elements referenced by the selectors
        var directiveElements = utils_1.getSelectors(info)
            .selectors.map(function (selector) { return selector.element; })
            .filter(function (name) { return !!name; });
        var components = directiveElements.map(function (name) { return ({ kind: 'component', name: name, sort: name }); });
        var htmlElements = htmlNames.map(function (name) { return ({ kind: 'element', name: name, sort: name }); });
        // Return components and html elements
        return utils_1.uniqueByName(htmlElements.concat(components));
    }
    function entityCompletions(value, position) {
        // Look for entity completions
        var re = /&[A-Za-z]*;?(?!\d)/g;
        var found;
        var result = undefined;
        while (found = re.exec(value)) {
            var len = found[0].length;
            if (position >= found.index && position < (found.index + len)) {
                result = Object.keys(compiler_1.NAMED_ENTITIES)
                    .map(function (name) { return ({ kind: 'entity', name: "&" + name + ";", sort: name }); });
                break;
            }
        }
        return result;
    }
    function interpolationCompletions(info, position) {
        // Look for an interpolation in at the position.
        var templatePath = utils_1.findTemplateAstAt(info.templateAst, position);
        var mostSpecific = templatePath.tail;
        if (mostSpecific) {
            var visitor = new ExpressionVisitor(info, position, undefined, function () { return language_services_1.getExpressionScope(utils_1.diagnosticInfoFromTemplateInfo(info), templatePath, false); });
            mostSpecific.visit(visitor, null);
            return utils_1.uniqueByName(visitor.result);
        }
    }
    // There is a special case of HTML where text that contains a unclosed tag is treated as
    // text. For exaple '<h1> Some <a text </h1>' produces a text nodes inside of the H1
    // element "Some <a text". We, however, want to treat this as if the user was requesting
    // the attributes of an "a" element, not requesting completion in the a text element. This
    // code checks for this case and returns element completions if it is detected or undefined
    // if it is not.
    function voidElementAttributeCompletions(info, path) {
        var tail = path.tail;
        if (tail instanceof compiler_1.Text) {
            var match = tail.value.match(/<(\w(\w|\d|-)*:)?(\w(\w|\d|-)*)\s/);
            // The position must be after the match, otherwise we are still in a place where elements
            // are expected (such as `<|a` or `<a|`; we only want attributes for `<a |` or after).
            if (match &&
                path.position >= (match.index || 0) + match[0].length + tail.sourceSpan.start.offset) {
                return attributeCompletionsForElement(info, match[3]);
            }
        }
    }
    var ExpressionVisitor = /** @class */ (function (_super) {
        tslib_1.__extends(ExpressionVisitor, _super);
        function ExpressionVisitor(info, position, attr, getExpressionScope) {
            var _this = _super.call(this) || this;
            _this.info = info;
            _this.position = position;
            _this.attr = attr;
            _this.getExpressionScope = getExpressionScope || (function () { return info.template.members; });
            return _this;
        }
        ExpressionVisitor.prototype.visitDirectiveProperty = function (ast) {
            this.attributeValueCompletions(ast.value);
        };
        ExpressionVisitor.prototype.visitElementProperty = function (ast) {
            this.attributeValueCompletions(ast.value);
        };
        ExpressionVisitor.prototype.visitEvent = function (ast) { this.attributeValueCompletions(ast.handler); };
        ExpressionVisitor.prototype.visitElement = function (ast) {
            var _this = this;
            if (this.attr && utils_1.getSelectors(this.info) && this.attr.name.startsWith(TEMPLATE_ATTR_PREFIX)) {
                // The value is a template expression but the expression AST was not produced when the
                // TemplateAst was produce so
                // do that now.
                var key_1 = this.attr.name.substr(TEMPLATE_ATTR_PREFIX.length);
                // Find the selector
                var selectorInfo = utils_1.getSelectors(this.info);
                var selectors = selectorInfo.selectors;
                var selector_1 = selectors.filter(function (s) { return s.attrs.some(function (attr, i) { return i % 2 == 0 && attr == key_1; }); })[0];
                var templateBindingResult = this.info.expressionParser.parseTemplateBindings(key_1, this.attr.value, null, 0);
                // find the template binding that contains the position
                if (!this.attr.valueSpan)
                    return;
                var valueRelativePosition_1 = this.position - this.attr.valueSpan.start.offset;
                var bindings = templateBindingResult.templateBindings;
                var binding = bindings.find(function (binding) { return utils_1.inSpan(valueRelativePosition_1, binding.span, /* exclusive */ true); }) ||
                    bindings.find(function (binding) { return utils_1.inSpan(valueRelativePosition_1, binding.span); });
                var keyCompletions = function () {
                    var keys = [];
                    if (selector_1) {
                        var attrNames = selector_1.attrs.filter(function (_, i) { return i % 2 == 0; });
                        keys = attrNames.filter(function (name) { return name.startsWith(key_1) && name != key_1; })
                            .map(function (name) { return lowerName(name.substr(key_1.length)); });
                    }
                    keys.push('let');
                    _this.result = keys.map(function (key) { return ({ kind: 'key', name: key, sort: key }); });
                };
                if (!binding || (binding.key == key_1 && !binding.expression)) {
                    // We are in the root binding. We should return `let` and keys that are left in the
                    // selector.
                    keyCompletions();
                }
                else if (binding.keyIsVar) {
                    var equalLocation = this.attr.value.indexOf('=');
                    this.result = [];
                    if (equalLocation >= 0 && valueRelativePosition_1 >= equalLocation) {
                        // We are after the '=' in a let clause. The valid values here are the members of the
                        // template reference's type parameter.
                        var directiveMetadata = selectorInfo.map.get(selector_1);
                        if (directiveMetadata) {
                            var contextTable = this.info.template.query.getTemplateContext(directiveMetadata.type.reference);
                            if (contextTable) {
                                this.result = this.symbolsToCompletions(contextTable.values());
                            }
                        }
                    }
                    else if (binding.key && valueRelativePosition_1 <= (binding.key.length - key_1.length)) {
                        keyCompletions();
                    }
                }
                else {
                    // If the position is in the expression or after the key or there is no key, return the
                    // expression completions
                    if ((binding.expression && utils_1.inSpan(valueRelativePosition_1, binding.expression.ast.span)) ||
                        (binding.key &&
                            valueRelativePosition_1 > binding.span.start + (binding.key.length - key_1.length)) ||
                        !binding.key) {
                        var span = new compiler_1.ParseSpan(0, this.attr.value.length);
                        this.attributeValueCompletions(binding.expression ? binding.expression.ast :
                            new compiler_1.PropertyRead(span, new compiler_1.ImplicitReceiver(span), ''), valueRelativePosition_1);
                    }
                    else {
                        keyCompletions();
                    }
                }
            }
        };
        ExpressionVisitor.prototype.visitBoundText = function (ast) {
            var expressionPosition = this.position - ast.sourceSpan.start.offset;
            if (utils_1.inSpan(expressionPosition, ast.value.span)) {
                var completions = expressions_1.getExpressionCompletions(this.getExpressionScope(), ast.value, expressionPosition, this.info.template.query);
                if (completions) {
                    this.result = this.symbolsToCompletions(completions);
                }
            }
        };
        ExpressionVisitor.prototype.attributeValueCompletions = function (value, position) {
            var symbols = expressions_1.getExpressionCompletions(this.getExpressionScope(), value, position == null ? this.attributeValuePosition : position, this.info.template.query);
            if (symbols) {
                this.result = this.symbolsToCompletions(symbols);
            }
        };
        ExpressionVisitor.prototype.symbolsToCompletions = function (symbols) {
            return symbols.filter(function (s) { return !s.name.startsWith('__') && s.public; })
                .map(function (symbol) { return ({ kind: symbol.kind, name: symbol.name, sort: symbol.name }); });
        };
        Object.defineProperty(ExpressionVisitor.prototype, "attributeValuePosition", {
            get: function () {
                if (this.attr && this.attr.valueSpan) {
                    return this.position - this.attr.valueSpan.start.offset;
                }
                return 0;
            },
            enumerable: true,
            configurable: true
        });
        return ExpressionVisitor;
    }(compiler_1.NullTemplateVisitor));
    function getSourceText(template, span) {
        return template.source.substring(span.start, span.end);
    }
    function nameOfAttr(attr) {
        var name = attr.name;
        if (attr.output) {
            name = utils_1.removeSuffix(name, 'Events');
            name = utils_1.removeSuffix(name, 'Changed');
        }
        var result = [name];
        if (attr.input) {
            result.unshift('[');
            result.push(']');
        }
        if (attr.output) {
            result.unshift('(');
            result.push(')');
        }
        if (attr.template) {
            result.unshift('*');
        }
        return result.join('');
    }
    var templateAttr = /^(\w+:)?(template$|^\*)/;
    function createElementCssSelector(element) {
        var e_1, _a;
        var cssSelector = new compiler_1.CssSelector();
        var elNameNoNs = compiler_1.splitNsName(element.name)[1];
        cssSelector.setElement(elNameNoNs);
        try {
            for (var _b = tslib_1.__values(element.attrs), _c = _b.next(); !_c.done; _c = _b.next()) {
                var attr = _c.value;
                if (!attr.name.match(templateAttr)) {
                    var _d = tslib_1.__read(compiler_1.splitNsName(attr.name), 2), _ = _d[0], attrNameNoNs = _d[1];
                    cssSelector.addAttribute(attrNameNoNs, attr.value);
                    if (attr.name.toLowerCase() == 'class') {
                        var classes = attr.value.split(/s+/g);
                        classes.forEach(function (className) { return cssSelector.addClassName(className); });
                    }
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return cssSelector;
    }
    function foldAttrs(attrs) {
        var inputOutput = new Map();
        var templates = new Map();
        var result = [];
        attrs.forEach(function (attr) {
            if (attr.fromHtml) {
                return attr;
            }
            if (attr.template) {
                var duplicate = templates.get(attr.name);
                if (!duplicate) {
                    result.push({ name: attr.name, template: true });
                    templates.set(attr.name, attr);
                }
            }
            if (attr.input || attr.output) {
                var duplicate = inputOutput.get(attr.name);
                if (duplicate) {
                    duplicate.input = duplicate.input || attr.input;
                    duplicate.output = duplicate.output || attr.output;
                }
                else {
                    var cloneAttr = { name: attr.name };
                    if (attr.input)
                        cloneAttr.input = true;
                    if (attr.output)
                        cloneAttr.output = true;
                    result.push(cloneAttr);
                    inputOutput.set(attr.name, cloneAttr);
                }
            }
        });
        return result;
    }
    function expandedAttr(attr) {
        if (attr.input && attr.output) {
            return [
                attr, { name: attr.name, input: true, output: false },
                { name: attr.name, input: false, output: true }
            ];
        }
        return [attr];
    }
    function lowerName(name) {
        return name && (name[0].toLowerCase() + name.substr(1));
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGxldGlvbnMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9sYW5ndWFnZS1zZXJ2aWNlL3NyYy9jb21wbGV0aW9ucy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCw4Q0FBa1c7SUFDbFcsaUZBQStFO0lBRy9FLHlFQUF1RDtJQUN2RCxxRUFBb0Y7SUFFcEYsNkRBQW1LO0lBRW5LLElBQU0sb0JBQW9CLEdBQUcsR0FBRyxDQUFDO0lBRWpDLElBQU0sa0JBQWtCLEdBQUc7UUFDekIsSUFBSSxFQUFFLElBQUk7UUFDVixNQUFNLEVBQUUsSUFBSTtRQUNaLFFBQVEsRUFBRSxJQUFJO1FBQ2QsSUFBSSxFQUFFLElBQUk7UUFDVixJQUFJLEVBQUUsSUFBSTtRQUNWLEtBQUssRUFBRSxJQUFJO1FBQ1gsSUFBSSxFQUFFLElBQUk7UUFDVixJQUFJLEVBQUUsSUFBSTtLQUNYLENBQUM7SUFFRixTQUFnQixzQkFBc0IsQ0FBQyxZQUEwQjtRQUMvRCxJQUFJLE1BQU0sR0FBMEIsU0FBUyxDQUFDO1FBQ3pDLElBQUEsOEJBQU8sRUFBRSxzQ0FBVyxFQUFFLGdDQUFRLENBQWlCO1FBQ3BELDZFQUE2RTtRQUM3RSxJQUFJLFlBQVksQ0FBQyxRQUFRLElBQUksSUFBSSxFQUFFO1lBQ2pDLElBQUksa0JBQWdCLEdBQUcsWUFBWSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQztZQUNuRSxJQUFJLE1BQUksR0FBRyxtQkFBUSxDQUFDLE9BQU8sRUFBRSxrQkFBZ0IsQ0FBQyxDQUFDO1lBQy9DLElBQUksWUFBWSxHQUFHLE1BQUksQ0FBQyxJQUFJLENBQUM7WUFDN0IsSUFBSSxNQUFJLENBQUMsS0FBSyxJQUFJLENBQUMsWUFBWSxFQUFFO2dCQUMvQixNQUFNLEdBQUcsa0JBQWtCLENBQUMsWUFBWSxFQUFFLE1BQUksQ0FBQyxD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QUUsTUFBTSxFQUFFLElBQUksRUFBQzthQUM5QyxDQUFDO1NBQ0g7UUFDRCxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDaEIsQ0FBQztJQUVELFNBQVMsU0FBUyxDQUFDLElBQVk7UUFDN0IsT0FBTyxJQUFJLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsV0FBVyxFQUFFLEdBQUcsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQzFELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QVNULCBBc3RQYXRoLCBBdHRyaWJ1dGUsIEJvdW5kRGlyZWN0aXZlUHJvcGVydHlBc3QsIEJvdW5kRWxlbWVudFByb3BlcnR5QXN0LCBCb3VuZEV2ZW50QXN0LCBCb3VuZFRleHRBc3QsIENzc1NlbGVjdG9yLCBFbGVtZW50LCBFbGVtZW50QXN0LCBJbXBsaWNpdFJlY2VpdmVyLCBOQU1FRF9FTlRJVElFUywgTm9kZSBhcyBIdG1sQXN0LCBOdWxsVGVtcGxhdGVWaXNpdG9yLCBQYXJzZVNwYW4sIFByb3BlcnR5UmVhZCwgU2VsZWN0b3JNYXRjaGVyLCBUYWdDb250ZW50VHlwZSwgVGV4dCwgZmluZE5vZGUsIGdldEh0bWxUYWdEZWZpbml0aW9uLCBzcGxpdE5zTmFtZX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuaW1wb3J0IHtnZXRFeHByZXNzaW9uU2NvcGV9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyLWNsaS9zcmMvbGFuZ3VhZ2Vfc2VydmljZXMnO1xuXG5pbXBvcnQge0F0dHJJbmZvLCBUZW1wbGF0ZUluZm99IGZyb20gJy4vY29tbW9uJztcbmltcG9ydCB7Z2V0RXhwcmVzc2lvbkNvbXBsZXRpb25zfSBmcm9tICcuL2V4cHJlc3Npb25zJztcbmltcG9ydCB7YXR0cmlidXRlTmFtZXMsIGVsZW1lbnROYW1lcywgZXZlbnROYW1lcywgcHJvcGVydHlOYW1lc30gZnJvbSAnLi9odG1sX2luZm8nO1xuaW1wb3J0IHtDb21wbGV0aW9uLCBDb21wbGV0aW9ucywgU3BhbiwgU3ltYm9sLCBTeW1ib2xUYWJsZSwgVGVtcGxhdGVTb3VyY2V9IGZyb20gJy4vdHlwZXMnO1xuaW1wb3J0IHtkaWFnbm9zdGljSW5mb0Zyb21UZW1wbGF0ZUluZm8sIGZpbmRUZW1wbGF0ZUFzdEF0LCBmbGF0dGVuLCBnZXRTZWxlY3RvcnMsIGhhc1RlbXBsYXRlUmVmZXJlbmNlLCBpblNwYW4sIHJlbW92ZVN1ZmZpeCwgc3Bhbk9mLCB1bmlxdWVCeU5hbWV9IGZyb20gJy4vdXRpbHMnO1xuXG5jb25zdCBURU1QTEFURV9BVFRSX1BSRUZJWCA9ICcqJztcblxuY29uc3QgaGlkZGVuSHRtbEVsZW1lbnRzID0ge1xuICBodG1sOiB0cnVlLFxuICBzY3JpcHQ6IHRydWUsXG4gIG5vc2NyaXB0OiB0cnVlLFxuICBiYXNlOiB0cnVlLFxuICBib2R5OiB0cnVlLFxuICB0aXRsZTogdHJ1ZSxcbiAgaGVhZDogdHJ1ZSxcbiAgbGluazogdHJ1ZSxcbn07XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRUZW1wbGF0ZUNvbXBsZXRpb25zKHRlbXBsYXRlSW5mbzogVGVtcGxhdGVJbmZvKTogQ29tcGxldGlvbnN8dW5kZWZpbmVkIHtcbiAgbGV0IHJlc3VsdDogQ29tcGxldGlvbnN8dW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuICBsZXQge2h0bWxBc3QsIHRlbXBsYXRlQXN0LCB0ZW1wbGF0ZX0gPSB0ZW1wbGF0ZUluZm87XG4gIC8vIFRoZSB0ZW1wbGF0ZU5vZGUgc3RhcnRzIGF0IHRoZSBkZWxpbWl0ZXIgY2hhcmFjdGVyIHNvIHdlIGFkZCAxIHRvIHNraXAgaXQuXG4gIGlmICh0ZW1wbGF0ZUluZm8ucG9zaXRpb24gIT0gbnVsbCkge1xuICAgIGxldCB0ZW1wbGF0ZVBvc2l0aW9uID0gdGVtcGxhdGVJbmZvLnBvc2l0aW9uIC0gdGVtcGxhdGUuc3Bhbi5zdGFydDtcbiAgICBsZXQgcGF0aCA9IGZpbmROb2RlKGh0bWxBc3QsIHRlbXBsYXRlUG9zaXRpb24pO1xuICAgIGxldCBtb3N0U3BlY2lmaWMgPSBwYXRoLnRhaWw7XG4gICAgaWYgKHBhdGguZW1wdHkgfHwgIW1vc3RTcGVjaWZpYykge1xuICAgICAgcmVzdWx0ID0gZWxlbWVudENvbXBsZXRpb25zKHRlbXBsYXRlSW5mbywgcGF0aCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCBhc3RQb3NpdGlvbiA9IHRlbXBsYXRlUG9zaXRpb24gLSBtb3N0U3BlY2lmaWMuc291cmNlU3Bhbi5zdGFydC5vZmZzZXQ7XG4gICAgICBtb3N0U3BlY2lmaWMudmlzaXQoXG4gICAgICAgICAge1xuICAgICAgICAgICAgdmlzaXRFbGVtZW50KGFzdCkge1xuICAgICAgICAgICAgICBsZXQgc3RhcnRUYWdTcGFuID0gc3Bhbk9mKGFzdC5zb3VyY2VTcGFuKTtcbiAgICAgICAgICAgICAgbGV0IHRhZ0xlbiA9IGFzdC5uYW1lLmxlbmd0aDtcbiAgICAgICAgICAgICAgaWYgKHRlbXBsYXRlUG9zaXRpb24gPD1cbiAgICAgICAgICAgICAgICAgIHN0YXJ0VGFnU3Bhbi5zdGFydCArIHRhZ0xlbiArIDEgLyogMSBmb3IgdGhlIG9wZW5pbmcgYW5nbGUgYnJhY2tlZCAqLykge1xuICAgICAgICAgICAgICAgIC8vIElmIHdlIGFyZSBpbiB0aGUgdGFnIHRoZW4gcmV0dXJuIHRoZSBlbGVtZW50IGNvbXBsZXRpb25zLlxuICAgICAgICAgICAgICAgIHJlc3VsdCA9IGVsZW1lbnRDb21wbGV0aW9ucyh0ZW1wbGF0ZUluZm8sIHBhdGgpO1xuICAgICAgICAgICAgICB9IGVsc2UgaWYgKHRlbXBsYXRlUG9zaXRpb24gPCBzdGFydFRhZ1NwYW4uZW5kKSB7XG4gICAgICAgICAgICAgICAgLy8gV2UgYXJlIGluIHRoZSBhdHRyaWJ1dGUgc2VjdGlvbiBvZiB0aGUgZWxlbWVudCAoYnV0IG5vdCBpbiBhbiBhdHRyaWJ1dGUpLlxuICAgICAgICAgICAgICAgIC8vIFJldHVybiB0aGUgYXR0cmlidXRlIGNvbXBsZXRpb25zLlxuICAgICAgICAgICAgICAgIHJlc3VsdCA9IGF0dHJpYnV0ZUNvbXBsZXRpb25zKHRlbXBsYXRlSW5mbywgcGF0aCk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB2aXNpdEF0dHJpYnV0ZShhc3QpIHtcbiAgICAgICAgICAgICAgaWYgKCFhc3QudmFsdWVTcGFuIHx8ICFpblNwYW4odGVtcGxhdGVQb3NpdGlvbiwgc3Bhbk9mKGFzdC52YWx1ZVNwYW4pKSkge1xuICAgICAgICAgICAgICAgIC8vIFdlIGFyZSBpbiB0aGUgbmFtZSBvZiBhbiBhdHRyaWJ1dGUuIFNob3cgYXR0cmlidXRlIGNvbXBsZXRpb25zLlxuICAgICAgICAgICAgICAgIHJlc3VsdCA9IGF0dHJpYnV0ZUNvbXBsZXRpb25zKHRlbXBsYXRlSW5mbywgcGF0aCk7XG4gICAgICAgICAgICAgIH0gZWxzZSBpZiAoYXN0LnZhbHVlU3BhbiAmJiBpblNwYW4odGVtcGxhdGVQb3NpdGlvbiwgc3Bhbk9mKGFzdC52YWx1ZVNwYW4pKSkge1xuICAgICAgICAgICAgICAgIHJlc3VsdCA9IGF0dHJpYnV0ZVZhbHVlQ29tcGxldGlvbnModGVtcGxhdGVJbmZvLCB0ZW1wbGF0ZVBvc2l0aW9uLCBhc3QpO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgdmlzaXRUZXh0KGFzdCkge1xuICAgICAgICAgICAgICAvLyBDaGVjayBpZiB3ZSBhcmUgaW4gYSBlbnRpdHkuXG4gICAgICAgICAgICAgIHJlc3VsdCA9IGVudGl0eUNvbXBsZXRpb25zKGdldFNvdXJjZVRleHQodGVtcGxhdGUsIHNwYW5PZihhc3QpKSwgYXN0UG9zaXRpb24pO1xuICAgICAgICAgICAgICBpZiAocmVzdWx0KSByZXR1cm4gcmVzdWx0O1xuICAgICAgICAgICAgICByZXN1bHQgPSBpbnRlcnBvbGF0aW9uQ29tcGxldGlvbnModGVtcGxhdGVJbmZvLCB0ZW1wbGF0ZVBvc2l0aW9uKTtcbiAgICAgICAgICAgICAgaWYgKHJlc3VsdCkgcmV0dXJuIHJlc3VsdDtcbiAgICAgICAgICAgICAgbGV0IGVsZW1lbnQgPSBwYXRoLmZpcnN0KEVsZW1lbnQpO1xuICAgICAgICAgICAgICBpZiAoZWxlbWVudCkge1xuICAgICAgICAgICAgICAgIGxldCBkZWZpbml0aW9uID0gZ2V0SHRtbFRhZ0RlZmluaXRpb24oZWxlbWVudC5uYW1lKTtcbiAgICAgICAgICAgICAgICBpZiAoZGVmaW5pdGlvbi5jb250ZW50VHlwZSA9PT0gVGFnQ29udGVudFR5cGUuUEFSU0FCTEVfREFUQSkge1xuICAgICAgICAgICAgICAgICAgcmVzdWx0ID0gdm9pZEVsZW1lbnRBdHRyaWJ1dGVDb21wbGV0aW9ucyh0ZW1wbGF0ZUluZm8sIHBhdGgpO1xuICAgICAgICAgICAgICAgICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgICAgICAgICAgICAgICAgLy8gSWYgdGhlIGVsZW1lbnQgY2FuIGhvbGQgY29udGVudCBTaG93IGVsZW1lbnQgY29tcGxldGlvbnMuXG4gICAgICAgICAgICAgICAgICAgIHJlc3VsdCA9IGVsZW1lbnRDb21wbGV0aW9ucyh0ZW1wbGF0ZUluZm8sIHBhdGgpO1xuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAvLyBJZiBubyBlbGVtZW50IGNvbnRhaW5lciwgaW1wbGllcyBwYXJzYWJsZSBkYXRhIHNvIHNob3cgZWxlbWVudHMuXG4gICAgICAgICAgICAgICAgcmVzdWx0ID0gdm9pZEVsZW1lbnRBdHRyaWJ1dGVDb21wbGV0aW9ucyh0ZW1wbGF0ZUluZm8sIHBhdGgpO1xuICAgICAgICAgICAgICAgIGlmICghcmVzdWx0KSB7XG4gICAgICAgICAgICAgICAgICByZXN1bHQgPSBlbGVtZW50Q29tcGxldGlvbnModGVtcGxhdGVJbmZvLCBwYXRoKTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB2aXNpdENvbW1lbnQoYXN0KSB7fSxcbiAgICAgICAgICAgIHZpc2l0RXhwYW5zaW9uKGFzdCkge30sXG4gICAgICAgICAgICB2aXNpdEV4cGFuc2lvbkNhc2UoYXN0KSB7fVxuICAgICAgICAgIH0sXG4gICAgICAgICAgbnVsbCk7XG4gICAgfVxuICB9XG4gIHJldHVybiByZXN1bHQ7XG59XG5cbmZ1bmN0aW9uIGF0dHJpYnV0ZUNvbXBsZXRpb25zKGluZm86IFRlbXBsYXRlSW5mbywgcGF0aDogQXN0UGF0aDxIdG1sQXN0Pik6IENvbXBsZXRpb25zfHVuZGVmaW5lZCB7XG4gIGxldCBpdGVtID0gcGF0aC50YWlsIGluc3RhbmNlb2YgRWxlbWVudCA/IHBhdGgudGFpbCA6IHBhdGgucGFyZW50T2YocGF0aC50YWlsKTtcbiAgaWYgKGl0ZW0gaW5zdGFuY2VvZiBFbGVtZW50KSB7XG4gICAgcmV0dXJuIGF0dHJpYnV0ZUNvbXBsZXRpb25zRm9yRWxlbWVudChpbmZvLCBpdGVtLm5hbWUsIGl0ZW0pO1xuICB9XG4gIHJldHVybiB1bmRlZmluZWQ7XG59XG5cbmZ1bmN0aW9uIGF0dHJpYnV0ZUNvbXBsZXRpb25zRm9yRWxlbWVudChcbiAgICBpbmZvOiBUZW1wbGF0ZUluZm8sIGVsZW1lbnROYW1lOiBzdHJpbmcsIGVsZW1lbnQ/OiBFbGVtZW50KTogQ29tcGxldGlvbnMge1xuICBjb25zdCBhdHRyaWJ1dGVzID0gZ2V0QXR0cmlidXRlSW5mb3NGb3JFbGVtZW50KGluZm8sIGVsZW1lbnROYW1lLCBlbGVtZW50KTtcblxuICAvLyBNYXAgYWxsIHRoZSBhdHRyaWJ1dGVzIHRvIGEgY29tcGxldGlvblxuICByZXR1cm4gYXR0cmlidXRlcy5tYXA8Q29tcGxldGlvbj4oYXR0ciA9PiAoe1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBraW5kOiBhdHRyLmZyb21IdG1sID8gJ2h0bWwgYXR0cmlidXRlJyA6ICdhdHRyaWJ1dGUnLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBuYW1lOiBuYW1lT2ZBdHRyKGF0dHIpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBzb3J0OiBhdHRyLm5hbWVcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pKTtcbn1cblxuZnVuY3Rpb24gZ2V0QXR0cmlidXRlSW5mb3NGb3JFbGVtZW50KFxuICAgIGluZm86IFRlbXBsYXRlSW5mbywgZWxlbWVudE5hbWU6IHN0cmluZywgZWxlbWVudD86IEVsZW1lbnQpOiBBdHRySW5mb1tdIHtcbiAgbGV0IGF0dHJpYnV0ZXM6IEF0dHJJbmZvW10gPSBbXTtcblxuICAvLyBBZGQgaHRtbCBhdHRyaWJ1dGVzXG4gIGxldCBodG1sQXR0cmlidXRlcyA9IGF0dHJpYnV0ZU5hbWVzKGVsZW1lbnROYW1lKSB8fCBbXTtcbiAgaWYgKGh0bWxBdHRyaWJ1dGVzKSB7XG4gICAgYXR0cmlidXRlcy5wdXNoKC4uLmh0bWxBdHRyaWJ1dGVzLm1hcDxBdHRySW5mbz4obmFtZSA9PiAoe25hbWUsIGZyb21IdG1sOiB0cnVlfSkpKTtcbiAgfVxuXG4gIC8vIEFkZCBodG1sIHByb3BlcnRpZXNcbiAgbGV0IGh0bWxQcm9wZXJ0aWVzID0gcHJvcGVydHlOYW1lcyhlbGVtZW50TmFtZSk7XG4gIGlmIChodG1sUHJvcGVydGllcykge1xuICAgIGF0dHJpYnV0ZXMucHVzaCguLi5odG1sUHJvcGVydGllcy5tYXA8QXR0ckluZm8+KG5hbWUgPT4gKHtuYW1lLCBpbnB1dDogdHJ1ZX0pKSk7XG4gIH1cblxuICAvLyBBZGQgaHRtbCBldmVudHNcbiAgbGV0IGh0bWxFdmVudHMgPSBldmVudE5hbWVzKGVsZW1lbnROYW1lKTtcbiAgaWYgKGh0bWxFdmVudHMpIHtcbiAgICBhdHRyaWJ1dGVzLnB1c2goLi4uaHRtbEV2ZW50cy5tYXA8QXR0ckluZm8+KG5hbWUgPT4gKHtuYW1lLCBvdXRwdXQ6IHRydWV9KSkpO1xuICB9XG5cbiAgbGV0IHtzZWxlY3RvcnMsIG1hcDogc2VsZWN0b3JNYXB9ID0gZ2V0U2VsZWN0b3JzKGluZm8pO1xuICBpZiAoc2VsZWN0b3JzICYmIHNlbGVjdG9ycy5sZW5ndGgpIHtcbiAgICAvLyBBbGwgdGhlIGF0dHJpYnV0ZXMgdGhhdCBhcmUgc2VsZWN0YWJsZSBzaG91bGQgYmUgc2hvd24uXG4gICAgY29uc3QgYXBwbGljYWJsZVNlbGVjdG9ycyA9XG4gICAgICAgIHNlbGVjdG9ycy5maWx0ZXIoc2VsZWN0b3IgPT4gIXNlbGVjdG9yLmVsZW1lbnQgfHwgc2VsZWN0b3IuZWxlbWVudCA9PSBlbGVtZW50TmFtZSk7XG4gICAgY29uc3Qgc2VsZWN0b3JBbmRBdHRyaWJ1dGVOYW1lcyA9XG4gICAgICAgIGFwcGxpY2FibGVTZWxlY3RvcnMubWFwKHNlbGVjdG9yID0+ICh7c2VsZWN0b3IsIGF0dHJzOiBzZWxlY3Rvci5hdHRycy5maWx0ZXIoYSA9PiAhIWEpfSkpO1xuICAgIGxldCBhdHRycyA9IGZsYXR0ZW4oc2VsZWN0b3JBbmRBdHRyaWJ1dGVOYW1lcy5tYXA8QXR0ckluZm9bXT4oc2VsZWN0b3JBbmRBdHRyID0+IHtcbiAgICAgIGNvbnN0IGRpcmVjdGl2ZSA9IHNlbGVjdG9yTWFwLmdldChzZWxlY3RvckFuZEF0dHIuc2VsZWN0b3IpICE7XG4gICAgICBjb25zdCByZXN1bHQgPSBzZWxlY3RvckFuZEF0dHIuYXR0cnMubWFwPEF0dHJJbmZvPihcbiAgICAgICAgICBuYW1lID0+ICh7bmFtZSwgaW5wdXQ6IG5hbWUgaW4gZGlyZWN0aXZlLmlucHV0cywgb3V0cHV0OiBuYW1lIGluIGRpcmVjdGl2ZS5vdXRwdXRzfSkpO1xuICAgICAgcmV0dXJuIHJlc3VsdDtcbiAgICB9KSk7XG5cbiAgICAvLyBBZGQgdGVtcGxhdGUgYXR0cmlidXRlIGlmIGEgZGlyZWN0aXZlIGNvbnRhaW5zIGEgdGVtcGxhdGUgcmVmZXJlbmNlXG4gICAgc2VsZWN0b3JBbmRBdHRyaWJ1dGVOYW1lcy5mb3JFYWNoKHNlbGVjdG9yQW5kQXR0ciA9PiB7XG4gICAgICBjb25zdCBzZWxlY3RvciA9IHNlbGVjdG9yQW5kQXR0ci5zZWxlY3RvcjtcbiAgICAgIGNvbnN0IGRpcmVjdGl2ZSA9IHNlbGVjdG9yTWFwLmdldChzZWxlY3Rvcik7XG4gICAgICBpZiAoZGlyZWN0aXZlICYmIGhhc1RlbXBsYXRlUmVmZXJlbmNlKGRpcmVjdGl2ZS50eXBlKSAmJiBzZWxlY3Rvci5hdHRycy5sZW5ndGggJiZcbiAgICAgICAgICBzZWxlY3Rvci5hdHRyc1swXSkge1xuICAgICAgICBhdHRycy5wdXNoKHtuYW1lOiBzZWxlY3Rvci5hdHRyc1swXSwgdGVtcGxhdGU6IHRydWV9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIC8vIEFsbCBpbnB1dCBhbmQgb3V0cHV0IHByb3BlcnRpZXMgb2YgdGhlIG1hdGNoaW5nIGRpcmVjdGl2ZXMgc2hvdWxkIGJlIGFkZGVkLlxuICAgIGxldCBlbGVtZW50U2VsZWN0b3IgPSBlbGVtZW50ID9cbiAgICAgICAgY3JlYXRlRWxlbWVudENzc1NlbGVjdG9yKGVsZW1lbnQpIDpcbiAgICAgICAgY3JlYXRlRWxlbWVudENzc1NlbGVjdG9yKG5ldyBFbGVtZW50KGVsZW1lbnROYW1lLCBbXSwgW10sIG51bGwgISwgbnVsbCwgbnVsbCkpO1xuXG4gICAgbGV0IG1hdGNoZXIgPSBuZXcgU2VsZWN0b3JNYXRjaGVyKCk7XG4gICAgbWF0Y2hlci5hZGRTZWxlY3RhYmxlcyhzZWxlY3RvcnMpO1xuICAgIG1hdGNoZXIubWF0Y2goZWxlbWVudFNlbGVjdG9yLCBzZWxlY3RvciA9PiB7XG4gICAgICBsZXQgZGlyZWN0aXZlID0gc2VsZWN0b3JNYXAuZ2V0KHNlbGVjdG9yKTtcbiAgICAgIGlmIChkaXJlY3RpdmUpIHtcbiAgICAgICAgY29uc3Qge2lucHV0cywgb3V0cHV0c30gPSBkaXJlY3RpdmU7XG4gICAgICAgIGF0dHJzLnB1c2goLi4uT2JqZWN0LmtleXMoaW5wdXRzKS5tYXAobmFtZSA9PiAoe25hbWU6IGlucHV0c1tuYW1lXSwgaW5wdXQ6IHRydWV9KSkpO1xuICAgICAgICBhdHRycy5wdXNoKC4uLk9iamVjdC5rZXlzKG91dHB1dHMpLm1hcChuYW1lID0+ICh7bmFtZTogb3V0cHV0c1tuYW1lXSwgb3V0cHV0OiB0cnVlfSkpKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIC8vIElmIGEgbmFtZSBzaG93cyB1cCB0d2ljZSwgZm9sZCBpdCBpbnRvIGEgc2luZ2xlIHZhbHVlLlxuICAgIGF0dHJzID0gZm9sZEF0dHJzKGF0dHJzKTtcblxuICAgIC8vIE5vdyBleHBhbmQgdGhlbSBiYWNrIG91dCB0byBlbnN1cmUgdGhhdCBpbnB1dC9vdXRwdXQgc2hvd3MgdXAgYXMgd2VsbCBhcyBpbnB1dCBhbmRcbiAgICAvLyBvdXRwdXQuXG4gICAgYXR0cmlidXRlcy5wdXNoKC4uLmZsYXR0ZW4oYXR0cnMubWFwKGV4cGFuZGVkQXR0cikpKTtcbiAgfVxuICByZXR1cm4gYXR0cmlidXRlcztcbn1cblxuZnVuY3Rpb24gYXR0cmlidXRlVmFsdWVDb21wbGV0aW9ucyhcbiAgICBpbmZvOiBUZW1wbGF0ZUluZm8sIHBvc2l0aW9uOiBudW1iZXIsIGF0dHI6IEF0dHJpYnV0ZSk6IENvbXBsZXRpb25zfHVuZGVmaW5lZCB7XG4gIGNvbnN0IHBhdGggPSBmaW5kVGVtcGxhdGVBc3RBdChpbmZvLnRlbXBsYXRlQXN0LCBwb3NpdGlvbik7XG4gIGNvbnN0IG1vc3RTcGVjaWZpYyA9IHBhdGgudGFpbDtcbiAgY29uc3QgZGluZm8gPSBkaWFnbm9zdGljSW5mb0Zyb21UZW1wbGF0ZUluZm8oaW5mbyk7XG4gIGlmIChtb3N0U3BlY2lmaWMpIHtcbiAgICBjb25zdCB2aXNpdG9yID1cbiAgICAgICAgbmV3IEV4cHJlc3Npb25WaXNpdG9yKGluZm8sIHBvc2l0aW9uLCBhdHRyLCAoKSA9PiBnZXRFeHByZXNzaW9uU2NvcGUoZGluZm8sIHBhdGgsIGZhbHNlKSk7XG4gICAgbW9zdFNwZWNpZmljLnZpc2l0KHZpc2l0b3IsIG51bGwpO1xuICAgIGlmICghdmlzaXRvci5yZXN1bHQgfHwgIXZpc2l0b3IucmVzdWx0Lmxlbmd0aCkge1xuICAgICAgLy8gVHJ5IGFsbHdvaW5nIHdpZGVuaW5nIHRoZSBwYXRoXG4gICAgICBjb25zdCB3aWRlclBhdGggPSBmaW5kVGVtcGxhdGVBc3RBdChpbmZvLnRlbXBsYXRlQXN0LCBwb3NpdGlvbiwgLyogYWxsb3dXaWRlbmluZyAqLyB0cnVlKTtcbiAgICAgIGlmICh3aWRlclBhdGgudGFpbCkge1xuICAgICAgICBjb25zdCB3aWRlclZpc2l0b3IgPSBuZXcgRXhwcmVzc2lvblZpc2l0b3IoXG4gICAgICAgICAgICBpbmZvLCBwb3NpdGlvbiwgYXR0ciwgKCkgPT4gZ2V0RXhwcmVzc2lvblNjb3BlKGRpbmZvLCB3aWRlclBhdGgsIGZhbHNlKSk7XG4gICAgICAgIHdpZGVyUGF0aC50YWlsLnZpc2l0KHdpZGVyVmlzaXRvciwgbnVsbCk7XG4gICAgICAgIHJldHVybiB3aWRlclZpc2l0b3IucmVzdWx0O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gdmlzaXRvci5yZXN1bHQ7XG4gIH1cbn1cblxuZnVuY3Rpb24gZWxlbWVudENvbXBsZXRpb25zKGluZm86IFRlbXBsYXRlSW5mbywgcGF0aDogQXN0UGF0aDxIdG1sQXN0Pik6IENvbXBsZXRpb25zfHVuZGVmaW5lZCB7XG4gIGxldCBodG1sTmFtZXMgPSBlbGVtZW50TmFtZXMoKS5maWx0ZXIobmFtZSA9PiAhKG5hbWUgaW4gaGlkZGVuSHRtbEVsZW1lbnRzKSk7XG5cbiAgLy8gQ29sbGVjdCB0aGUgZWxlbWVudHMgcmVmZXJlbmNlZCBieSB0aGUgc2VsZWN0b3JzXG4gIGxldCBkaXJlY3RpdmVFbGVtZW50cyA9IGdldFNlbGVjdG9ycyhpbmZvKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLnNlbGVjdG9ycy5tYXAoc2VsZWN0b3IgPT4gc2VsZWN0b3IuZWxlbWVudClcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5maWx0ZXIobmFtZSA9PiAhIW5hbWUpIGFzIHN0cmluZ1tdO1xuXG4gIGxldCBjb21wb25lbnRzID1cbiAgICAgIGRpcmVjdGl2ZUVsZW1lbnRzLm1hcDxDb21wbGV0aW9uPihuYW1lID0+ICh7a2luZDogJ2NvbXBvbmVudCcsIG5hbWUsIHNvcnQ6IG5hbWV9KSk7XG4gIGxldCBodG1sRWxlbWVudHMgPSBodG1sTmFtZXMubWFwPENvbXBsZXRpb24+KG5hbWUgPT4gKHtraW5kOiAnZWxlbWVudCcsIG5hbWU6IG5hbWUsIHNvcnQ6IG5hbWV9KSk7XG5cbiAgLy8gUmV0dXJuIGNvbXBvbmVudHMgYW5kIGh0bWwgZWxlbWVudHNcbiAgcmV0dXJuIHVuaXF1ZUJ5TmFtZShodG1sRWxlbWVudHMuY29uY2F0KGNvbXBvbmVudHMpKTtcbn1cblxuZnVuY3Rpb24gZW50aXR5Q29tcGxldGlvbnModmFsdWU6IHN0cmluZywgcG9zaXRpb246IG51bWJlcik6IENvbXBsZXRpb25zfHVuZGVmaW5lZCB7XG4gIC8vIExvb2sgZm9yIGVudGl0eSBjb21wbGV0aW9uc1xuICBjb25zdCByZSA9IC8mW0EtWmEtel0qOz8oPyFcXGQpL2c7XG4gIGxldCBmb3VuZDogUmVnRXhwRXhlY0FycmF5fG51bGw7XG4gIGxldCByZXN1bHQ6IENvbXBsZXRpb25zfHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgd2hpbGUgKGZvdW5kID0gcmUuZXhlYyh2YWx1ZSkpIHtcbiAgICBsZXQgbGVuID0gZm91bmRbMF0ubGVuZ3RoO1xuICAgIGlmIChwb3NpdGlvbiA+PSBmb3VuZC5pbmRleCAmJiBwb3NpdGlvbiA8IChmb3VuZC5pbmRleCArIGxlbikpIHtcbiAgICAgIHJlc3VsdCA9IE9iamVjdC5rZXlzKE5BTUVEX0VOVElUSUVTKVxuICAgICAgICAgICAgICAgICAgIC5tYXA8Q29tcGxldGlvbj4obmFtZSA9PiAoe2tpbmQ6ICdlbnRpdHknLCBuYW1lOiBgJiR7bmFtZX07YCwgc29ydDogbmFtZX0pKTtcbiAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gcmVzdWx0O1xufVxuXG5mdW5jdGlvbiBpbnRlcnBvbGF0aW9uQ29tcGxldGlvbnMoaW5mbzogVGVtcGxhdGVJbmZvLCBwb3NpdGlvbjogbnVtYmVyKTogQ29tcGxldGlvbnN8dW5kZWZpbmVkIHtcbiAgLy8gTG9vayBmb3IgYW4gaW50ZXJwb2xhdGlvbiBpbiBhdCB0aGUgcG9zaXRpb24uXG4gIGNvbnN0IHRlbXBsYXRlUGF0aCA9IGZpbmRUZW1wbGF0ZUFzdEF0KGluZm8udGVtcGxhdGVBc3QsIHBvc2l0aW9uKTtcbiAgY29uc3QgbW9zdFNwZWNpZmljID0gdGVtcGxhdGVQYXRoLnRhaWw7XG4gIGlmIChtb3N0U3BlY2lmaWMpIHtcbiAgICBsZXQgdmlzaXRvciA9IG5ldyBFeHByZXNzaW9uVmlzaXRvcihcbiAgICAgICAgaW5mbywgcG9zaXRpb24sIHVuZGVmaW5lZCxcbiAgICAgICAgKCkgPT4gZ2V0RXhwcmVzc2lvblNjb3BlKGRpYWdub3N0aWNJbmZvRnJvbVRlbXBsYXRlSW5mbyhpbmZvKSwgdGVtcGxhdGVQYXRoLCBmYWxzZSkpO1xuICAgIG1vc3RTcGVjaWZpYy52aXNpdCh2aXNpdG9yLCBudWxsKTtcbiAgICByZXR1cm4gdW5pcXVlQnlOYW1lKHZpc2l0b3IucmVzdWx0KTtcbiAgfVxufVxuXG4vLyBUaGVyZSBpcyBhIHNwZWNpYWwgY2FzZSBvZiBIVE1MIHdoZXJlIHRleHQgdGhhdCBjb250YWlucyBhIHVuY2xvc2VkIHRhZyBpcyB0cmVhdGVkIGFzXG4vLyB0ZXh0LiBGb3IgZXhhcGxlICc8aDE+IFNvbWUgPGEgdGV4dCA8L2gxPicgcHJvZHVjZXMgYSB0ZXh0IG5vZGVzIGluc2lkZSBvZiB0aGUgSDFcbi8vIGVsZW1lbnQgXCJTb21lIDxhIHRleHRcIi4gV2UsIGhvd2V2ZXIsIHdhbnQgdG8gdHJlYXQgdGhpcyBhcyBpZiB0aGUgdXNlciB3YXMgcmVxdWVzdGluZ1xuLy8gdGhlIGF0dHJpYnV0ZXMgb2YgYW4gXCJhXCIgZWxlbWVudCwgbm90IHJlcXVlc3RpbmcgY29tcGxldGlvbiBpbiB0aGUgYSB0ZXh0IGVsZW1lbnQuIFRoaXNcbi8vIGNvZGUgY2hlY2tzIGZvciB0aGlzIGNhc2UgYW5kIHJldHVybnMgZWxlbWVudCBjb21wbGV0aW9ucyBpZiBpdCBpcyBkZXRlY3RlZCBvciB1bmRlZmluZWRcbi8vIGlmIGl0IGlzIG5vdC5cbmZ1bmN0aW9uIHZvaWRFbGVtZW50QXR0cmlidXRlQ29tcGxldGlvbnMoaW5mbzogVGVtcGxhdGVJbmZvLCBwYXRoOiBBc3RQYXRoPEh0bWxBc3Q+KTogQ29tcGxldGlvbnN8XG4gICAgdW5kZWZpbmVkIHtcbiAgbGV0IHRhaWwgPSBwYXRoLnRhaWw7XG4gIGlmICh0YWlsIGluc3RhbmNlb2YgVGV4dCkge1xuICAgIGxldCBtYXRjaCA9IHRhaWwudmFsdWUubWF0Y2goLzwoXFx3KFxcd3xcXGR8LSkqOik/KFxcdyhcXHd8XFxkfC0pKilcXHMvKTtcbiAgICAvLyBUaGUgcG9zaXRpb24gbXVzdCBiZSBhZnRlciB0aGUgbWF0Y2gsIG90aGVyd2lzZSB3ZSBhcmUgc3RpbGwgaW4gYSBwbGFjZSB3aGVyZSBlbGVtZW50c1xuICAgIC8vIGFyZSBleHBlY3RlZCAoc3VjaCBhcyBgPHxhYCBvciBgPGF8YDsgd2Ugb25seSB3YW50IGF0dHJpYnV0ZXMgZm9yIGA8YSB8YCBvciBhZnRlcikuXG4gICAgaWYgKG1hdGNoICYmXG4gICAgICAgIHBhdGgucG9zaXRpb24gPj0gKG1hdGNoLmluZGV4IHx8IDApICsgbWF0Y2hbMF0ubGVuZ3RoICsgdGFpbC5zb3VyY2VTcGFuLnN0YXJ0Lm9mZnNldCkge1xuICAgICAgcmV0dXJuIGF0dHJpYnV0ZUNvbXBsZXRpb25zRm9yRWxlbWVudChpbmZvLCBtYXRjaFszXSk7XG4gICAgfVxuICB9XG59XG5cbmNsYXNzIEV4cHJlc3Npb25WaXNpdG9yIGV4dGVuZHMgTnVsbFRlbXBsYXRlVmlzaXRvciB7XG4gIHByaXZhdGUgZ2V0RXhwcmVzc2lvblNjb3BlOiAoKSA9PiBTeW1ib2xUYWJsZTtcbiAgcmVzdWx0OiBDb21wbGV0aW9ucztcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgaW5mbzogVGVtcGxhdGVJbmZvLCBwcml2YXRlIHBvc2l0aW9uOiBudW1iZXIsIHByaXZhdGUgYXR0cj86IEF0dHJpYnV0ZSxcbiAgICAgIGdldEV4cHJlc3Npb25TY29wZT86ICgpID0+IFN5bWJvbFRhYmxlKSB7XG4gICAgc3VwZXIoKTtcbiAgICB0aGlzLmdldEV4cHJlc3Npb25TY29wZSA9IGdldEV4cHJlc3Npb25TY29wZSB8fCAoKCkgPT4gaW5mby50ZW1wbGF0ZS5tZW1iZXJzKTtcbiAgfVxuXG4gIHZpc2l0RGlyZWN0aXZlUHJvcGVydHkoYXN0OiBCb3VuZERpcmVjdGl2ZVByb3BlcnR5QXN0KTogdm9pZCB7XG4gICAgdGhpcy5hdHRyaWJ1dGVWYWx1ZUNvbXBsZXRpb25zKGFzdC52YWx1ZSk7XG4gIH1cblxuICB2aXNpdEVsZW1lbnRQcm9wZXJ0eShhc3Q6IEJvdW5kRWxlbWVudFByb3BlcnR5QXN0KTogdm9pZCB7XG4gICAgdGhpcy5hdHRyaWJ1dGVWYWx1ZUNvbXBsZXRpb25zKGFzdC52YWx1ZSk7XG4gIH1cblxuICB2aXNpdEV2ZW50KGFzdDogQm91bmRFdmVudEFzdCk6IHZvaWQgeyB0aGlzLmF0dHJpYnV0ZVZhbHVlQ29tcGxldGlvbnMoYXN0LmhhbmRsZXIpOyB9XG5cbiAgdmlzaXRFbGVtZW50KGFzdDogRWxlbWVudEFzdCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmF0dHIgJiYgZ2V0U2VsZWN0b3JzKHRoaXMuaW5mbykgJiYgdGhpcy5hdHRyLm5hbWUuc3RhcnRzV2l0aChURU1QTEFURV9BVFRSX1BSRUZJWCkpIHtcbiAgICAgIC8vIFRoZSB2YWx1ZSBpcyBhIHRlbXBsYXRlIGV4cHJlc3Npb24gYnV0IHRoZSBleHByZXNzaW9uIEFTVCB3YXMgbm90IHByb2R1Y2VkIHdoZW4gdGhlXG4gICAgICAvLyBUZW1wbGF0ZUFzdCB3YXMgcHJvZHVjZSBzb1xuICAgICAgLy8gZG8gdGhhdCBub3cuXG5cbiAgICAgIGNvbnN0IGtleSA9IHRoaXMuYXR0ci5uYW1lLnN1YnN0cihURU1QTEFURV9BVFRSX1BSRUZJWC5sZW5ndGgpO1xuXG4gICAgICAvLyBGaW5kIHRoZSBzZWxlY3RvclxuICAgICAgY29uc3Qgc2VsZWN0b3JJbmZvID0gZ2V0U2VsZWN0b3JzKHRoaXMuaW5mbyk7XG4gICAgICBjb25zdCBzZWxlY3RvcnMgPSBzZWxlY3RvckluZm8uc2VsZWN0b3JzO1xuICAgICAgY29uc3Qgc2VsZWN0b3IgPVxuICAgICAgICAgIHNlbGVjdG9ycy5maWx0ZXIocyA9PiBzLmF0dHJzLnNvbWUoKGF0dHIsIGkpID0+IGkgJSAyID09IDAgJiYgYXR0ciA9PSBrZXkpKVswXTtcblxuICAgICAgY29uc3QgdGVtcGxhdGVCaW5kaW5nUmVzdWx0ID1cbiAgICAgICAgICB0aGlzLmluZm8uZXhwcmVzc2lvblBhcnNlci5wYXJzZVRlbXBsYXRlQmluZGluZ3Moa2V5LCB0aGlzLmF0dHIudmFsdWUsIG51bGwsIDApO1xuXG4gICAgICAvLyBmaW5kIHRoZSB0ZW1wbGF0ZSBiaW5kaW5nIHRoYXQgY29udGFpbnMgdGhlIHBvc2l0aW9uXG4gICAgICBpZiAoIXRoaXMuYXR0ci52YWx1ZVNwYW4pIHJldHVybjtcbiAgICAgIGNvbnN0IHZhbHVlUmVsYXRpdmVQb3NpdGlvbiA9IHRoaXMucG9zaXRpb24gLSB0aGlzLmF0dHIudmFsdWVTcGFuLnN0YXJ0Lm9mZnNldDtcbiAgICAgIGNvbnN0IGJpbmRpbmdzID0gdGVtcGxhdGVCaW5kaW5nUmVzdWx0LnRlbXBsYXRlQmluZGluZ3M7XG4gICAgICBjb25zdCBiaW5kaW5nID1cbiAgICAgICAgICBiaW5kaW5ncy5maW5kKFxuICAgICAgICAgICAgICBiaW5kaW5nID0+IGluU3Bhbih2YWx1ZVJlbGF0aXZlUG9zaXRpb24sIGJpbmRpbmcuc3BhbiwgLyogZXhjbHVzaXZlICovIHRydWUpKSB8fFxuICAgICAgICAgIGJpbmRpbmdzLmZpbmQoYmluZGluZyA9PiBpblNwYW4odmFsdWVSZWxhdGl2ZVBvc2l0aW9uLCBiaW5kaW5nLnNwYW4pKTtcblxuICAgICAgY29uc3Qga2V5Q29tcGxldGlvbnMgPSAoKSA9PiB7XG4gICAgICAgIGxldCBrZXlzOiBzdHJpbmdbXSA9IFtdO1xuICAgICAgICBpZiAoc2VsZWN0b3IpIHtcbiAgICAgICAgICBjb25zdCBhdHRyTmFtZXMgPSBzZWxlY3Rvci5hdHRycy5maWx0ZXIoKF8sIGkpID0+IGkgJSAyID09IDApO1xuICAgICAgICAgIGtleXMgPSBhdHRyTmFtZXMuZmlsdGVyKG5hbWUgPT4gbmFtZS5zdGFydHNXaXRoKGtleSkgJiYgbmFtZSAhPSBrZXkpXG4gICAgICAgICAgICAgICAgICAgICAubWFwKG5hbWUgPT4gbG93ZXJOYW1lKG5hbWUuc3Vic3RyKGtleS5sZW5ndGgpKSk7XG4gICAgICAgIH1cbiAgICAgICAga2V5cy5wdXNoKCdsZXQnKTtcbiAgICAgICAgdGhpcy5yZXN1bHQgPSBrZXlzLm1hcChrZXkgPT4gPENvbXBsZXRpb24+e2tpbmQ6ICdrZXknLCBuYW1lOiBrZXksIHNvcnQ6IGtleX0pO1xuICAgICAgfTtcblxuICAgICAgaWYgKCFiaW5kaW5nIHx8IChiaW5kaW5nLmtleSA9PSBrZXkgJiYgIWJpbmRpbmcuZXhwcmVzc2lvbikpIHtcbiAgICAgICAgLy8gV2UgYXJlIGluIHRoZSByb290IGJpbmRpbmcuIFdlIHNob3VsZCByZXR1cm4gYGxldGAgYW5kIGtleXMgdGhhdCBhcmUgbGVmdCBpbiB0aGVcbiAgICAgICAgLy8gc2VsZWN0b3IuXG4gICAgICAgIGtleUNvbXBsZXRpb25zKCk7XG4gICAgICB9IGVsc2UgaWYgKGJpbmRpbmcua2V5SXNWYXIpIHtcbiAgICAgICAgY29uc3QgZXF1YWxMb2NhdGlvbiA9IHRoaXMuYXR0ci52YWx1ZS5pbmRleE9mKCc9Jyk7XG4gICAgICAgIHRoaXMucmVzdWx0ID0gW107XG4gICAgICAgIGlmIChlcXVhbExvY2F0aW9uID49IDAgJiYgdmFsdWVSZWxhdGl2ZVBvc2l0aW9uID49IGVxdWFsTG9jYXRpb24pIHtcbiAgICAgICAgICAvLyBXZSBhcmUgYWZ0ZXIgdGhlICc9JyBpbiBhIGxldCBjbGF1c2UuIFRoZSB2YWxpZCB2YWx1ZXMgaGVyZSBhcmUgdGhlIG1lbWJlcnMgb2YgdGhlXG4gICAgICAgICAgLy8gdGVtcGxhdGUgcmVmZXJlbmNlJ3MgdHlwZSBwYXJhbWV0ZXIuXG4gICAgICAgICAgY29uc3QgZGlyZWN0aXZlTWV0YWRhdGEgPSBzZWxlY3RvckluZm8ubWFwLmdldChzZWxlY3Rvcik7XG4gICAgICAgICAgaWYgKGRpcmVjdGl2ZU1ldGFkYXRhKSB7XG4gICAgICAgICAgICBjb25zdCBjb250ZXh0VGFibGUgPVxuICAgICAgICAgICAgICAgIHRoaXMuaW5mby50ZW1wbGF0ZS5xdWVyeS5nZXRUZW1wbGF0ZUNvbnRleHQoZGlyZWN0aXZlTWV0YWRhdGEudHlwZS5yZWZlcmVuY2UpO1xuICAgICAgICAgICAgaWYgKGNvbnRleHRUYWJsZSkge1xuICAgICAgICAgICAgICB0aGlzLnJlc3VsdCA9IHRoaXMuc3ltYm9sc1RvQ29tcGxldGlvbnMoY29udGV4dFRhYmxlLnZhbHVlcygpKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSBpZiAoYmluZGluZy5rZXkgJiYgdmFsdWVSZWxhdGl2ZVBvc2l0aW9uIDw9IChiaW5kaW5nLmtleS5sZW5ndGggLSBrZXkubGVuZ3RoKSkge1xuICAgICAgICAgIGtleUNvbXBsZXRpb25zKCk7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIElmIHRoZSBwb3NpdGlvbiBpcyBpbiB0aGUgZXhwcmVzc2lvbiBvciBhZnRlciB0aGUga2V5IG9yIHRoZXJlIGlzIG5vIGtleSwgcmV0dXJuIHRoZVxuICAgICAgICAvLyBleHByZXNzaW9uIGNvbXBsZXRpb25zXG4gICAgICAgIGlmICgoYmluZGluZy5leHByZXNzaW9uICYmIGluU3Bhbih2YWx1ZVJlbGF0aXZlUG9zaXRpb24sIGJpbmRpbmcuZXhwcmVzc2lvbi5hc3Quc3BhbikpIHx8XG4gICAgICAgICAgICAoYmluZGluZy5rZXkgJiZcbiAgICAgICAgICAgICB2YWx1ZVJlbGF0aXZlUG9zaXRpb24gPiBiaW5kaW5nLnNwYW4uc3RhcnQgKyAoYmluZGluZy5rZXkubGVuZ3RoIC0ga2V5Lmxlbmd0aCkpIHx8XG4gICAgICAgICAgICAhYmluZGluZy5rZXkpIHtcbiAgICAgICAgICBjb25zdCBzcGFuID0gbmV3IFBhcnNlU3BhbigwLCB0aGlzLmF0dHIudmFsdWUubGVuZ3RoKTtcbiAgICAgICAgICB0aGlzLmF0dHJpYnV0ZVZhbHVlQ29tcGxldGlvbnMoXG4gICAgICAgICAgICAgIGJpbmRpbmcuZXhwcmVzc2lvbiA/IGJpbmRpbmcuZXhwcmVzc2lvbi5hc3QgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBuZXcgUHJvcGVydHlSZWFkKHNwYW4sIG5ldyBJbXBsaWNpdFJlY2VpdmVyKHNwYW4pLCAnJyksXG4gICAgICAgICAgICAgIHZhbHVlUmVsYXRpdmVQb3NpdGlvbik7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAga2V5Q29tcGxldGlvbnMoKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHZpc2l0Qm91bmRUZXh0KGFzdDogQm91bmRUZXh0QXN0KSB7XG4gICAgY29uc3QgZXhwcmVzc2lvblBvc2l0aW9uID0gdGhpcy5wb3NpdGlvbiAtIGFzdC5zb3VyY2VTcGFuLnN0YXJ0Lm9mZnNldDtcbiAgICBpZiAoaW5TcGFuKGV4cHJlc3Npb25Qb3NpdGlvbiwgYXN0LnZhbHVlLnNwYW4pKSB7XG4gICAgICBjb25zdCBjb21wbGV0aW9ucyA9IGdldEV4cHJlc3Npb25Db21wbGV0aW9ucyhcbiAgICAgICAgICB0aGlzLmdldEV4cHJlc3Npb25TY29wZSgpLCBhc3QudmFsdWUsIGV4cHJlc3Npb25Qb3NpdGlvbiwgdGhpcy5pbmZvLnRlbXBsYXRlLnF1ZXJ5KTtcbiAgICAgIGlmIChjb21wbGV0aW9ucykge1xuICAgICAgICB0aGlzLnJlc3VsdCA9IHRoaXMuc3ltYm9sc1RvQ29tcGxldGlvbnMoY29tcGxldGlvbnMpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgYXR0cmlidXRlVmFsdWVDb21wbGV0aW9ucyh2YWx1ZTogQVNULCBwb3NpdGlvbj86IG51bWJlcikge1xuICAgIGNvbnN0IHN5bWJvbHMgPSBnZXRFeHByZXNzaW9uQ29tcGxldGlvbnMoXG4gICAgICAgIHRoaXMuZ2V0RXhwcmVzc2lvblNjb3BlKCksIHZhbHVlLCBwb3NpdGlvbiA9PSBudWxsID8gdGhpcy5hdHRyaWJ1dGVWYWx1ZVBvc2l0aW9uIDogcG9zaXRpb24sXG4gICAgICAgIHRoaXMuaW5mby50ZW1wbGF0ZS5xdWVyeSk7XG4gICAgaWYgKHN5bWJvbHMpIHtcbiAgICAgIHRoaXMucmVzdWx0ID0gdGhpcy5zeW1ib2xzVG9Db21wbGV0aW9ucyhzeW1ib2xzKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHN5bWJvbHNUb0NvbXBsZXRpb25zKHN5bWJvbHM6IFN5bWJvbFtdKTogQ29tcGxldGlvbnMge1xuICAgIHJldHVybiBzeW1ib2xzLmZpbHRlcihzID0+ICFzLm5hbWUuc3RhcnRzV2l0aCgnX18nKSAmJiBzLnB1YmxpYylcbiAgICAgICAgLm1hcChzeW1ib2wgPT4gPENvbXBsZXRpb24+e2tpbmQ6IHN5bWJvbC5raW5kLCBuYW1lOiBzeW1ib2wubmFtZSwgc29ydDogc3ltYm9sLm5hbWV9KTtcbiAgfVxuXG4gIHByaXZhdGUgZ2V0IGF0dHJpYnV0ZVZhbHVlUG9zaXRpb24oKSB7XG4gICAgaWYgKHRoaXMuYXR0ciAmJiB0aGlzLmF0dHIudmFsdWVTcGFuKSB7XG4gICAgICByZXR1cm4gdGhpcy5wb3NpdGlvbiAtIHRoaXMuYXR0ci52YWx1ZVNwYW4uc3RhcnQub2Zmc2V0O1xuICAgIH1cbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRTb3VyY2VUZXh0KHRlbXBsYXRlOiBUZW1wbGF0ZVNvdXJjZSwgc3BhbjogU3Bhbik6IHN0cmluZyB7XG4gIHJldHVybiB0ZW1wbGF0ZS5zb3VyY2Uuc3Vic3RyaW5nKHNwYW4uc3RhcnQsIHNwYW4uZW5kKTtcbn1cblxuZnVuY3Rpb24gbmFtZU9mQXR0cihhdHRyOiBBdHRySW5mbyk6IHN0cmluZyB7XG4gIGxldCBuYW1lID0gYXR0ci5uYW1lO1xuICBpZiAoYXR0ci5vdXRwdXQpIHtcbiAgICBuYW1lID0gcmVtb3ZlU3VmZml4KG5hbWUsICdFdmVudHMnKTtcbiAgICBuYW1lID0gcmVtb3ZlU3VmZml4KG5hbWUsICdDaGFuZ2VkJyk7XG4gIH1cbiAgbGV0IHJlc3VsdCA9IFtuYW1lXTtcbiAgaWYgKGF0dHIuaW5wdXQpIHtcbiAgICByZXN1bHQudW5zaGlmdCgnWycpO1xuICAgIHJlc3VsdC5wdXNoKCddJyk7XG4gIH1cbiAgaWYgKGF0dHIub3V0cHV0KSB7XG4gICAgcmVzdWx0LnVuc2hpZnQoJygnKTtcbiAgICByZXN1bHQucHVzaCgnKScpO1xuICB9XG4gIGlmIChhdHRyLnRlbXBsYXRlKSB7XG4gICAgcmVzdWx0LnVuc2hpZnQoJyonKTtcbiAgfVxuICByZXR1cm4gcmVzdWx0LmpvaW4oJycpO1xufVxuXG5jb25zdCB0ZW1wbGF0ZUF0dHIgPSAvXihcXHcrOik/KHRlbXBsYXRlJHxeXFwqKS87XG5mdW5jdGlvbiBjcmVhdGVFbGVtZW50Q3NzU2VsZWN0b3IoZWxlbWVudDogRWxlbWVudCk6IENzc1NlbGVjdG9yIHtcbiAgY29uc3QgY3NzU2VsZWN0b3IgPSBuZXcgQ3NzU2VsZWN0b3IoKTtcbiAgbGV0IGVsTmFtZU5vTnMgPSBzcGxpdE5zTmFtZShlbGVtZW50Lm5hbWUpWzFdO1xuXG4gIGNzc1NlbGVjdG9yLnNldEVsZW1lbnQoZWxOYW1lTm9Ocyk7XG5cbiAgZm9yIChsZXQgYXR0ciBvZiBlbGVtZW50LmF0dHJzKSB7XG4gICAgaWYgKCFhdHRyLm5hbWUubWF0Y2godGVtcGxhdGVBdHRyKSkge1xuICAgICAgbGV0IFtfLCBhdHRyTmFtZU5vTnNdID0gc3BsaXROc05hbWUoYXR0ci5uYW1lKTtcbiAgICAgIGNzc1NlbGVjdG9yLmFkZEF0dHJpYnV0ZShhdHRyTmFtZU5vTnMsIGF0dHIudmFsdWUpO1xuICAgICAgaWYgKGF0dHIubmFtZS50b0xvd2VyQ2FzZSgpID09ICdjbGFzcycpIHtcbiAgICAgICAgY29uc3QgY2xhc3NlcyA9IGF0dHIudmFsdWUuc3BsaXQoL3MrL2cpO1xuICAgICAgICBjbGFzc2VzLmZvckVhY2goY2xhc3NOYW1lID0+IGNzc1NlbGVjdG9yLmFkZENsYXNzTmFtZShjbGFzc05hbWUpKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbiAgcmV0dXJuIGNzc1NlbGVjdG9yO1xufVxuXG5mdW5jdGlvbiBmb2xkQXR0cnMoYXR0cnM6IEF0dHJJbmZvW10pOiBBdHRySW5mb1tdIHtcbiAgbGV0IGlucHV0T3V0cHV0ID0gbmV3IE1hcDxzdHJpbmcsIEF0dHJJbmZvPigpO1xuICBsZXQgdGVtcGxhdGVzID0gbmV3IE1hcDxzdHJpbmcsIEF0dHJJbmZvPigpO1xuICBsZXQgcmVzdWx0OiBBdHRySW5mb1tdID0gW107XG4gIGF0dHJzLmZvckVhY2goYXR0ciA9PiB7XG4gICAgaWYgKGF0dHIuZnJvbUh0bWwpIHtcbiAgICAgIHJldHVybiBhdHRyO1xuICAgIH1cbiAgICBpZiAoYXR0ci50ZW1wbGF0ZSkge1xuICAgICAgbGV0IGR1cGxpY2F0ZSA9IHRlbXBsYXRlcy5nZXQoYXR0ci5uYW1lKTtcbiAgICAgIGlmICghZHVwbGljYXRlKSB7XG4gICAgICAgIHJlc3VsdC5wdXNoKHtuYW1lOiBhdHRyLm5hbWUsIHRlbXBsYXRlOiB0cnVlfSk7XG4gICAgICAgIHRlbXBsYXRlcy5zZXQoYXR0ci5uYW1lLCBhdHRyKTtcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKGF0dHIuaW5wdXQgfHwgYXR0ci5vdXRwdXQpIHtcbiAgICAgIGxldCBkdXBsaWNhdGUgPSBpbnB1dE91dHB1dC5nZXQoYXR0ci5uYW1lKTtcbiAgICAgIGlmIChkdXBsaWNhdGUpIHtcbiAgICAgICAgZHVwbGljYXRlLmlucHV0ID0gZHVwbGljYXRlLmlucHV0IHx8IGF0dHIuaW5wdXQ7XG4gICAgICAgIGR1cGxpY2F0ZS5vdXRwdXQgPSBkdXBsaWNhdGUub3V0cHV0IHx8IGF0dHIub3V0cHV0O1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbGV0IGNsb25lQXR0cjogQXR0ckluZm8gPSB7bmFtZTogYXR0ci5uYW1lfTtcbiAgICAgICAgaWYgKGF0dHIuaW5wdXQpIGNsb25lQXR0ci5pbnB1dCA9IHRydWU7XG4gICAgICAgIGlmIChhdHRyLm91dHB1dCkgY2xvbmVBdHRyLm91dHB1dCA9IHRydWU7XG4gICAgICAgIHJlc3VsdC5wdXNoKGNsb25lQXR0cik7XG4gICAgICAgIGlucHV0T3V0cHV0LnNldChhdHRyLm5hbWUsIGNsb25lQXR0cik7XG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIHJlc3VsdDtcbn1cblxuZnVuY3Rpb24gZXhwYW5kZWRBdHRyKGF0dHI6IEF0dHJJbmZvKTogQXR0ckluZm9bXSB7XG4gIGlmIChhdHRyLmlucHV0ICYmIGF0dHIub3V0cHV0KSB7XG4gICAgcmV0dXJuIFtcbiAgICAgIGF0dHIsIHtuYW1lOiBhdHRyLm5hbWUsIGlucHV0OiB0cnVlLCBvdXRwdXQ6IGZhbHNlfSxcbiAgICAgIHtuYW1lOiBhdHRyLm5hbWUsIGlucHV0OiBmYWxzZSwgb3V0cHV0OiB0cnVlfVxuICAgIF07XG4gIH1cbiAgcmV0dXJuIFthdHRyXTtcbn1cblxuZnVuY3Rpb24gbG93ZXJOYW1lKG5hbWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBuYW1lICYmIChuYW1lWzBdLnRvTG93ZXJDYXNlKCkgKyBuYW1lLnN1YnN0cigxKSk7XG59XG4iXX0=