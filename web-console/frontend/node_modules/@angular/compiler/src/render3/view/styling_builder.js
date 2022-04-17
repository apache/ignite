(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/render3/view/styling_builder", ["require", "exports", "tslib", "@angular/compiler/src/expression_parser/ast", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/template_parser/template_parser", "@angular/compiler/src/render3/r3_identifiers", "@angular/compiler/src/render3/view/style_parser", "@angular/compiler/src/render3/view/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var ast_1 = require("@angular/compiler/src/expression_parser/ast");
    var o = require("@angular/compiler/src/output/output_ast");
    var template_parser_1 = require("@angular/compiler/src/template_parser/template_parser");
    var r3_identifiers_1 = require("@angular/compiler/src/render3/r3_identifiers");
    var style_parser_1 = require("@angular/compiler/src/render3/view/style_parser");
    var util_1 = require("@angular/compiler/src/render3/view/util");
    var IMPORTANT_FLAG = '!important';
    /**
     * Produces creation/update instructions for all styling bindings (class and style)
     *
     * It also produces the creation instruction to register all initial styling values
     * (which are all the static class="..." and style="..." attribute values that exist
     * on an element within a template).
     *
     * The builder class below handles producing instructions for the following cases:
     *
     * - Static style/class attributes (style="..." and class="...")
     * - Dynamic style/class map bindings ([style]="map" and [class]="map|string")
     * - Dynamic style/class property bindings ([style.prop]="exp" and [class.name]="exp")
     *
     * Due to the complex relationship of all of these cases, the instructions generated
     * for these attributes/properties/bindings must be done so in the correct order. The
     * order which these must be generated is as follows:
     *
     * if (createMode) {
     *   styling(...)
     * }
     * if (updateMode) {
     *   styleMap(...)
     *   classMap(...)
     *   styleProp(...)
     *   classProp(...)
     *   stylingApply(...)
     * }
     *
     * The creation/update methods within the builder class produce these instructions.
     */
    var StylingBuilder = /** @class */ (function () {
        function StylingBuilder(_elementIndexExpr, _directiveExpr) {
            this._elementIndexExpr = _elementIndexExpr;
            this._directiveExpr = _directiveExpr;
            /** Whether or not there are any static styling values present */
            this._hasInitialValues = false;
            /**
             *  Whether or not there are any styling bindings present
             *  (i.e. `[style]`, `[class]`, `[style.prop]` or `[class.name]`)
             */
            this.hasBindings = false;
            /** the input for [class] (if it exists) */
            this._classMapInput = null;
            /** the input for [style] (if it exists) */
            this._styleMapInput = null;
            /** an array of each [style.prop] input */
            this._singleStyleInputs = null;
            /** an array of each [class.name] input */
            this._singleClassInputs = null;
            this._lastStylingInput = null;
            this._firstStylingInput = null;
            // maps are used instead of hash maps because a Map will
            // retain the ordering of the keys
            /**
             * Represents the location of each style binding in the template
             * (e.g. `<div [style.width]="w" [style.height]="h">` implies
             * that `width=0` and `height=1`)
             */
            this._stylesIndex = new Map();
            /**
             * Represents the location of each class binding in the template
             * (e.g. `<div [class.big]="b" [class.hidden]="h">` implies
             * that `big=0` and `hidden=1`)
             */
            this._classesIndex = new Map();
            this._initialStyleValues = [];
            this._initialClassValues = [];
            // certain style properties ALWAYS need sanitization
            // this is checked each time new styles are encountered
            this._useDefaultSanitizer = false;
        }
        /**
         * Registers a given input to the styling builder to be later used when producing AOT code.
         *
         * The code below will only accept the input if it is somehow tied to styling (whether it be
         * style/class bindings or static style/class attributes).
         */
        StylingBuilder.prototype.registerBoundInput = function (input) {
            // [attr.style] or [attr.class] are skipped in the code below,
            // they should not be treated as styling-based bindings since
            // they are intended to be written directly to the attr and
            // will therefore skip all style/class resolution that is present
            // with style="", [style]="" and [style.prop]="", class="",
            // [class.prop]="". [class]="" assignments
            var binding = null;
            var name = input.name;
            switch (input.type) {
                case 0 /* Property */:
                    binding = this.registerInputBasedOnName(name, input.value, input.sourceSpan);
                    break;
                case 3 /* Style */:
                    binding = this.registerStyleInput(name, false, input.value, input.sourceSpan, input.unit);
                    break;
                case 2 /* Class */:
                    binding = this.registerClassInput(name, false, input.value, input.sourceSpan);
                    break;
            }
            return binding ? true : false;
        };
        StylingBuilder.prototype.registerInputBasedOnName = function (name, expression, sourceSpan) {
            var binding = null;
            var nameToMatch = name.substring(0, 5); // class | style
            var isStyle = nameToMatch === 'style';
            var isClass = isStyle ? false : (nameToMatch === 'class');
            if (isStyle || isClass) {
                var isMapBased = name.charAt(5) !== '.'; // style.prop or class.prop makes this a no
                var property = name.substr(isMapBased ? 5 : 6); // the dot explains why there's a +1
                if (isStyle) {
                    binding = this.registerStyleInput(property, isMapBased, expression, sourceSpan);
                }
                else {
                    binding = this.registerClassInput(property, isMapBased, expression, sourceSpan);
                }
            }
            return binding;
        };
        StylingBuilder.prototype.registerStyleInput = function (name, isMapBased, value, sourceSpan, unit) {
            if (template_parser_1.isEmptyExpression(value)) {
                return null;
            }
            name = normalizePropName(name);
            var _a = parseProperty(name), property = _a.property, hasOverrideFlag = _a.hasOverrideFlag, bindingUnit = _a.unit;
            var entry = {
                name: property,
                unit: unit || bindingUnit, value: value, sourceSpan: sourceSpan, hasOverrideFlag: hasOverrideFlag
            };
            if (isMapBased) {
                this._useDefaultSanitizer = true;
                this._styleMapInput = entry;
            }
            else {
                (this._singleStyleInputs = this._singleStyleInputs || []).push(entry);
                this._useDefaultSanitizer = this._useDefaultSanitizer || isStyleSanitizable(name);
                registerIntoMap(this._stylesIndex, property);
            }
            this._lastStylingInput = entry;
            this._firstStylingInput = this._firstStylingInput || entry;
            this.hasBindings = true;
            return entry;
        };
        StylingBuilder.prototype.registerClassInput = function (name, isMapBased, value, sourceSpan) {
            if (template_parser_1.isEmptyExpression(value)) {
                return null;
            }
            var _a = parseProperty(name), property = _a.property, hasOverrideFlag = _a.hasOverrideFlag;
            var entry = { name: property, value: value, sourceSpan: sourceSpan, hasOverrideFlag: hasOverrideFlag, unit: null };
            if (isMapBased) {
                this._classMapInput = entry;
            }
            else {
                (this._singleClassInputs = this._singleClassInputs || []).push(entry);
                registerIntoMap(this._classesIndex, property);
            }
            this._lastStylingInput = entry;
            this._firstStylingInput = this._firstStylingInput || entry;
            this.hasBindings = true;
            return entry;
        };
        /**
         * Registers the element's static style string value to the builder.
         *
         * @param value the style string (e.g. `width:100px; height:200px;`)
         */
        StylingBuilder.prototype.registerStyleAttr = function (value) {
            this._initialStyleValues = style_parser_1.parse(value);
            this._hasInitialValues = true;
        };
        /**
         * Registers the element's static class string value to the builder.
         *
         * @param value the className string (e.g. `disabled gold zoom`)
         */
        StylingBuilder.prototype.registerClassAttr = function (value) {
            this._initialClassValues = value.trim().split(/\s+/g);
            this._hasInitialValues = true;
        };
        /**
         * Appends all styling-related expressions to the provided attrs array.
         *
         * @param attrs an existing array where each of the styling expressions
         * will be inserted into.
         */
        StylingBuilder.prototype.populateInitialStylingAttrs = function (attrs) {
            // [CLASS_MARKER, 'foo', 'bar', 'baz' ...]
            if (this._initialClassValues.length) {
                attrs.push(o.literal(1 /* Classes */));
                for (var i = 0; i < this._initialClassValues.length; i++) {
                    attrs.push(o.literal(this._initialClassValues[i]));
                }
            }
            // [STYLE_MARKER, 'width', '200px', 'height', '100px', ...]
            if (this._initialStyleValues.length) {
                attrs.push(o.literal(2 /* Styles */));
                for (var i = 0; i < this._initialStyleValues.length; i += 2) {
                    attrs.push(o.literal(this._initialStyleValues[i]), o.literal(this._initialStyleValues[i + 1]));
                }
            }
        };
        /**
         * Builds an instruction with all the expressions and parameters for `elementHostAttrs`.
         *
         * The instruction generation code below is used for producing the AOT statement code which is
         * responsible for registering initial styles (within a directive hostBindings' creation block),
         * as well as any of the provided attribute values, to the directive host element.
         */
        StylingBuilder.prototype.buildHostAttrsInstruction = function (sourceSpan, attrs, constantPool) {
            var _this = this;
            if (this._directiveExpr && (attrs.length || this._hasInitialValues)) {
                return {
                    sourceSpan: sourceSpan,
                    reference: r3_identifiers_1.Identifiers.elementHostAttrs,
                    allocateBindingSlots: 0,
                    params: function () {
                        // params => elementHostAttrs(attrs)
                        _this.populateInitialStylingAttrs(attrs);
                        var attrArray = !attrs.some(function (attr) { return attr instanceof o.WrappedNodeExpr; }) ?
                            getConstantLiteralFromArray(constantPool, attrs) :
                            o.literalArr(attrs);
                        return [attrArray];
                    }
                };
            }
            return null;
        };
        /**
         * Builds an instruction with all the expressions and parameters for `styling`.
         *
         * The instruction generation code below is used for producing the AOT statement code which is
         * responsible for registering style/class bindings to an element.
         */
        StylingBuilder.prototype.buildStylingInstruction = function (sourceSpan, constantPool) {
            if (this.hasBindings) {
                return {
                    sourceSpan: sourceSpan,
                    allocateBindingSlots: 0,
                    reference: r3_identifiers_1.Identifiers.styling,
                    params: function () { return []; },
                };
            }
            return null;
        };
        /**
         * Builds an instruction with all the expressions and parameters for `classMap`.
         *
         * The instruction data will contain all expressions for `classMap` to function
         * which includes the `[class]` expression params.
         */
        StylingBuilder.prototype.buildClassMapInstruction = function (valueConverter) {
            if (this._classMapInput) {
                return this._buildMapBasedInstruction(valueConverter, true, this._classMapInput);
            }
            return null;
        };
        /**
         * Builds an instruction with all the expressions and parameters for `styleMap`.
         *
         * The instruction data will contain all expressions for `styleMap` to function
         * which includes the `[style]` expression params.
         */
        StylingBuilder.prototype.buildStyleMapInstruction = function (valueConverter) {
            if (this._styleMapInput) {
                return this._buildMapBasedInstruction(valueConverter, false, this._styleMapInput);
            }
            return null;
        };
        StylingBuilder.prototype._buildMapBasedInstruction = function (valueConverter, isClassBased, stylingInput) {
            // each styling binding value is stored in the LView
            var totalBindingSlotsRequired = 1;
            // these values must be outside of the update block so that they can
            // be evaluated (the AST visit call) during creation time so that any
            // pipes can be picked up in time before the template is built
            var mapValue = stylingInput.value.visit(valueConverter);
            var reference;
            if (mapValue instanceof ast_1.Interpolation && isClassBased) {
                totalBindingSlotsRequired += mapValue.expressions.length;
                reference = getClassMapInterpolationExpression(mapValue);
            }
            else {
                reference = isClassBased ? r3_identifiers_1.Identifiers.classMap : r3_identifiers_1.Identifiers.styleMap;
            }
            return {
                sourceSpan: stylingInput.sourceSpan,
                reference: reference,
                allocateBindingSlots: totalBindingSlotsRequired,
                supportsInterpolation: isClassBased,
                params: function (convertFn) {
                    var convertResult = convertFn(mapValue);
                    return Array.isArray(convertResult) ? convertResult : [convertResult];
                }
            };
        };
        StylingBuilder.prototype._buildSingleInputs = function (reference, inputs, mapIndex, allowUnits, valueConverter, getInterpolationExpressionFn) {
            var totalBindingSlotsRequired = 0;
            return inputs.map(function (input) {
                var value = input.value.visit(valueConverter);
                // each styling binding value is stored in the LView
                var totalBindingSlotsRequired = 1;
                if (value instanceof ast_1.Interpolation) {
                    totalBindingSlotsRequired += value.expressions.length;
                    if (getInterpolationExpressionFn) {
                        reference = getInterpolationExpressionFn(value);
                    }
                }
                return {
                    sourceSpan: input.sourceSpan,
                    supportsInterpolation: !!getInterpolationExpressionFn,
                    allocateBindingSlots: totalBindingSlotsRequired, reference: reference,
                    params: function (convertFn) {
                        // params => stylingProp(propName, value)
                        var params = [];
                        params.push(o.literal(input.name));
                        var convertResult = convertFn(value);
                        if (Array.isArray(convertResult)) {
                            params.push.apply(params, tslib_1.__spread(convertResult));
                        }
                        else {
                            params.push(convertResult);
                        }
                        if (allowUnits && input.unit) {
                            params.push(o.literal(input.unit));
                        }
                        return params;
                    }
                };
            });
        };
        StylingBuilder.prototype._buildClassInputs = function (valueConverter) {
            if (this._singleClassInputs) {
                return this._buildSingleInputs(r3_identifiers_1.Identifiers.classProp, this._singleClassInputs, this._classesIndex, false, valueConverter);
            }
            return [];
        };
        StylingBuilder.prototype._buildStyleInputs = function (valueConverter) {
            if (this._singleStyleInputs) {
                return this._buildSingleInputs(r3_identifiers_1.Identifiers.styleProp, this._singleStyleInputs, this._stylesIndex, true, valueConverter, getStylePropInterpolationExpression);
            }
            return [];
        };
        StylingBuilder.prototype._buildApplyFn = function () {
            return {
                sourceSpan: this._lastStylingInput ? this._lastStylingInput.sourceSpan : null,
                reference: r3_identifiers_1.Identifiers.stylingApply,
                allocateBindingSlots: 0,
                params: function () { return []; }
            };
        };
        StylingBuilder.prototype._buildSanitizerFn = function () {
            return {
                sourceSpan: this._firstStylingInput ? this._firstStylingInput.sourceSpan : null,
                reference: r3_identifiers_1.Identifiers.styleSanitizer,
                allocateBindingSlots: 0,
                params: function () { return [o.importExpr(r3_identifiers_1.Identifiers.defaultStyleSanitizer)]; }
            };
        };
        /**
         * Constructs all instructions which contain the expressions that will be placed
         * into the update block of a template function or a directive hostBindings function.
         */
        StylingBuilder.prototype.buildUpdateLevelInstructions = function (valueConverter) {
            var instructions = [];
            if (this.hasBindings) {
                if (this._useDefaultSanitizer) {
                    instructions.push(this._buildSanitizerFn());
                }
                var styleMapInstruction = this.buildStyleMapInstruction(valueConverter);
                if (styleMapInstruction) {
                    instructions.push(styleMapInstruction);
                }
                var classMapInstruction = this.buildClassMapInstruction(valueConverter);
                if (classMapInstruction) {
                    instructions.push(classMapInstruction);
                }
                instructions.push.apply(instructions, tslib_1.__spread(this._buildStyleInputs(valueConverter)));
                instructions.push.apply(instructions, tslib_1.__spread(this._buildClassInputs(valueConverter)));
                instructions.push(this._buildApplyFn());
            }
            return instructions;
        };
        return StylingBuilder;
    }());
    exports.StylingBuilder = StylingBuilder;
    function registerIntoMap(map, key) {
        if (!map.has(key)) {
            map.set(key, map.size);
        }
    }
    function isStyleSanitizable(prop) {
        // Note that browsers support both the dash case and
        // camel case property names when setting through JS.
        return prop === 'background-image' || prop === 'backgroundImage' || prop === 'background' ||
            prop === 'border-image' || prop === 'borderImage' || prop === 'filter' ||
            prop === 'list-style' || prop === 'listStyle' || prop === 'list-style-image' ||
            prop === 'listStyleImage' || prop === 'clip-path' || prop === 'clipPath';
    }
    /**
     * Simple helper function to either provide the constant literal that will house the value
     * here or a null value if the provided values are empty.
     */
    function getConstantLiteralFromArray(constantPool, values) {
        return values.length ? constantPool.getConstLiteral(o.literalArr(values), true) : o.NULL_EXPR;
    }
    /**
     * Simple helper function that adds a parameter or does nothing at all depending on the provided
     * predicate and totalExpectedArgs values
     */
    function addParam(params, predicate, value, argNumber, totalExpectedArgs) {
        if (predicate && value) {
            params.push(value);
        }
        else if (argNumber < totalExpectedArgs) {
            params.push(o.NULL_EXPR);
        }
    }
    function parseProperty(name) {
        var hasOverrideFlag = false;
        var overrideIndex = name.indexOf(IMPORTANT_FLAG);
        if (overrideIndex !== -1) {
            name = overrideIndex > 0 ? name.substring(0, overrideIndex) : '';
            hasOverrideFlag = true;
        }
        var unit = '';
        var property = name;
        var unitIndex = name.lastIndexOf('.');
        if (unitIndex > 0) {
            unit = name.substr(unitIndex + 1);
            property = name.substring(0, unitIndex);
        }
        return { property: property, unit: unit, hasOverrideFlag: hasOverrideFlag };
    }
    exports.parseProperty = parseProperty;
    /**
     * Gets the instruction to generate for an interpolated class map.
     * @param interpolation An Interpolation AST
     */
    function getClassMapInterpolationExpression(interpolation) {
        switch (util_1.getInterpolationArgsLength(interpolation)) {
            case 1:
                return r3_identifiers_1.Identifiers.classMap;
            case 3:
                return r3_identifiers_1.Identifiers.classMapInterpolate1;
            case 5:
                return r3_identifiers_1.Identifiers.classMapInterpolate2;
            case 7:
                return r3_identifiers_1.Identifiers.classMapInterpolate3;
            case 9:
                return r3_identifiers_1.Identifiers.classMapInterpolate4;
            case 11:
                return r3_identifiers_1.Identifiers.classMapInterpolate5;
            case 13:
                return r3_identifiers_1.Identifiers.classMapInterpolate6;
            case 15:
                return r3_identifiers_1.Identifiers.classMapInterpolate7;
            case 17:
                return r3_identifiers_1.Identifiers.classMapInterpolate8;
            default:
                return r3_identifiers_1.Identifiers.classMapInterpolateV;
        }
    }
    /**
     * Gets the instruction to generate for an interpolated style prop.
     * @param interpolation An Interpolation AST
     */
    function getStylePropInterpolationExpression(interpolation) {
        switch (util_1.getInterpolationArgsLength(interpolation)) {
            case 1:
                return r3_identifiers_1.Identifiers.styleProp;
            case 3:
                return r3_identifiers_1.Identifiers.stylePropInterpolate1;
            case 5:
                return r3_identifiers_1.Identifiers.stylePropInterpolate2;
            case 7:
                return r3_identifiers_1.Identifiers.stylePropInterpolate3;
            case 9:
                return r3_identifiers_1.Identifiers.stylePropInterpolate4;
            case 11:
                return r3_identifiers_1.Identifiers.stylePropInterpolate5;
            case 13:
                return r3_identifiers_1.Identifiers.stylePropInterpolate6;
            case 15:
                return r3_identifiers_1.Identifiers.stylePropInterpolate7;
            case 17:
                return r3_identifiers_1.Identifiers.stylePropInterpolate8;
            default:
                return r3_identifiers_1.Identifiers.stylePropInterpolateV;
        }
    }
    function normalizePropName(prop) {
        return style_parser_1.hyphenate(prop);
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGluZ19idWlsZGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3JlbmRlcjMvdmlldy9zdHlsaW5nX2J1aWxkZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7O0lBU0EsbUVBQTRFO0lBQzVFLDJEQUE2QztJQUU3Qyx5RkFBd0U7SUFHeEUsK0VBQW9EO0lBRXBELGdGQUE4RDtJQUU5RCxnRUFBa0Q7SUFFbEQsSUFBTSxjQUFjLEdBQUcsWUFBWSxDQUFDO0lBd0JwQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7T0E2Qkc7SUFDSDtRQTJDRSx3QkFBb0IsaUJBQStCLEVBQVUsY0FBaUM7WUFBMUUsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFjO1lBQVUsbUJBQWMsR0FBZCxjQUFjLENBQW1CO1lBMUM5RixpRUFBaUU7WUFDekQsc0JBQWlCLEdBQUcsS0FBSyxDQUFDO1lBQ2xDOzs7ZUFHRztZQUNJLGdCQUFXLEdBQUcsS0FBSyxDQUFDO1lBRTNCLDJDQUEyQztZQUNuQyxtQkFBYyxHQUEyQixJQUFJLENBQUM7WUFDdEQsMkNBQTJDO1lBQ25DLG1CQUFjLEdBQTJCLElBQUksQ0FBQztZQUN0RCwwQ0FBMEM7WUFDbEMsdUJBQWtCLEdBQTZCLElBQUksQ0FBQztZQUM1RCwwQ0FBMEM7WUFDbEMsdUJBQWtCLEdBQTZCLElBQUksQ0FBQztZQUNwRCxzQkFBaUIsR0FBMkIsSUFBSSxDQUFDO1lBQ2pELHVCQUFrQixHQUEyQixJQUFJLENBQUM7WUFFMUQsd0RBQXdEO1lBQ3hELGtDQUFrQztZQUVsQzs7OztlQUlHO1lBQ0ssaUJBQVksR0FBRyxJQUFJLEdBQUcsRUFBa0IsQ0FBQztZQUVqRDs7OztlQUlHO1lBQ0ssa0JBQWEsR0FBRyxJQUFJLEdBQUcsRUFBa0IsQ0FBQztZQUMxQyx3QkFBbUIsR0FBYSxFQUFFLENBQUM7WUFDbkMsd0JBQW1C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Q0FBQyxxQkFBcUIsQ0FBQztZQUNsQyxLQUFLLEVBQUU7Z0JBQ0wsT0FBTyw0QkFBRSxDQUFDLHFCQUFxQixDQUFDO1lBQ2xDO2dCQUNFLE9BQU8sNEJBQUUsQ0FBQyxxQkFBcUIsQ0FBQztTQUNuQztJQUNILENBQUM7SUFFRCxTQUFTLGlCQUFpQixDQUFDLElBQVk7UUFDckMsT0FBTyx3QkFBUyxDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ3pCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5pbXBvcnQge0NvbnN0YW50UG9vbH0gZnJvbSAnLi4vLi4vY29uc3RhbnRfcG9vbCc7XG5pbXBvcnQge0F0dHJpYnV0ZU1hcmtlcn0gZnJvbSAnLi4vLi4vY29yZSc7XG5pbXBvcnQge0FTVCwgQmluZGluZ1R5cGUsIEludGVycG9sYXRpb259IGZyb20gJy4uLy4uL2V4cHJlc3Npb25fcGFyc2VyL2FzdCc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4uLy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7UGFyc2VTb3VyY2VTcGFufSBmcm9tICcuLi8uLi9wYXJzZV91dGlsJztcbmltcG9ydCB7aXNFbXB0eUV4cHJlc3Npb259IGZyb20gJy4uLy4uL3RlbXBsYXRlX3BhcnNlci90ZW1wbGF0ZV9wYXJzZXInO1xuaW1wb3J0IHtlcnJvcn0gZnJvbSAnLi4vLi4vdXRpbCc7XG5pbXBvcnQgKiBhcyB0IGZyb20gJy4uL3IzX2FzdCc7XG5pbXBvcnQge0lkZW50aWZpZXJzIGFzIFIzfSBmcm9tICcuLi9yM19pZGVudGlmaWVycyc7XG5cbmltcG9ydCB7aHlwaGVuYXRlLCBwYXJzZSBhcyBwYXJzZVN0eWxlfSBmcm9tICcuL3N0eWxlX3BhcnNlcic7XG5pbXBvcnQge1ZhbHVlQ29udmVydGVyfSBmcm9tICcuL3RlbXBsYXRlJztcbmltcG9ydCB7Z2V0SW50ZXJwb2xhdGlvbkFyZ3NMZW5ndGh9IGZyb20gJy4vdXRpbCc7XG5cbmNvbnN0IElNUE9SVEFOVF9GTEFHID0gJyFpbXBvcnRhbnQnO1xuXG4vKipcbiAqIEEgc3R5bGluZyBleHByZXNzaW9uIHN1bW1hcnkgdGhhdCBpcyB0byBiZSBwcm9jZXNzZWQgYnkgdGhlIGNvbXBpbGVyXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgU3R5bGluZ0luc3RydWN0aW9uIHtcbiAgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFufG51bGw7XG4gIHJlZmVyZW5jZTogby5FeHRlcm5hbFJlZmVyZW5jZTtcbiAgYWxsb2NhdGVCaW5kaW5nU2xvdHM6IG51bWJlcjtcbiAgc3VwcG9ydHNJbnRlcnBvbGF0aW9uPzogYm9vbGVhbjtcbiAgcGFyYW1zOiAoKGNvbnZlcnRGbjogKHZhbHVlOiBhbnkpID0+IG8uRXhwcmVzc2lvbiB8IG8uRXhwcmVzc2lvbltdKSA9PiBvLkV4cHJlc3Npb25bXSk7XG59XG5cbi8qKlxuICogQW4gaW50ZXJuYWwgcmVjb3JkIG9mIHRoZSBpbnB1dCBkYXRhIGZvciBhIHN0eWxpbmcgYmluZGluZ1xuICovXG5pbnRlcmZhY2UgQm91bmRTdHlsaW5nRW50cnkge1xuICBoYXNPdmVycmlkZUZsYWc6IGJvb2xlYW47XG4gIG5hbWU6IHN0cmluZ3xudWxsO1xuICB1bml0OiBzdHJpbmd8bnVsbDtcbiAgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuO1xuICB2YWx1ZTogQVNUO1xufVxuXG4vKipcbiAqIFByb2R1Y2VzIGNyZWF0aW9uL3VwZGF0ZSBpbnN0cnVjdGlvbnMgZm9yIGFsbCBzdHlsaW5nIGJpbmRpbmdzIChjbGFzcyBhbmQgc3R5bGUpXG4gKlxuICogSXQgYWxzbyBwcm9kdWNlcyB0aGUgY3JlYXRpb24gaW5zdHJ1Y3Rpb24gdG8gcmVnaXN0ZXIgYWxsIGluaXRpYWwgc3R5bGluZyB2YWx1ZXNcbiAqICh3aGljaCBhcmUgYWxsIHRoZSBzdGF0aWMgY2xhc3M9XCIuLi5cIiBhbmQgc3R5bGU9XCIuLi5cIiBhdHRyaWJ1dGUgdmFsdWVzIHRoYXQgZXhpc3RcbiAqIG9uIGFuIGVsZW1lbnQgd2l0aGluIGEgdGVtcGxhdGUpLlxuICpcbiAqIFRoZSBidWlsZGVyIGNsYXNzIGJlbG93IGhhbmRsZXMgcHJvZHVjaW5nIGluc3RydWN0aW9ucyBmb3IgdGhlIGZvbGxvd2luZyBjYXNlczpcbiAqXG4gKiAtIFN0YXRpYyBzdHlsZS9jbGFzcyBhdHRyaWJ1dGVzIChzdHlsZT1cIi4uLlwiIGFuZCBjbGFzcz1cIi4uLlwiKVxuICogLSBEeW5hbWljIHN0eWxlL2NsYXNzIG1hcCBiaW5kaW5ncyAoW3N0eWxlXT1cIm1hcFwiIGFuZCBbY2xhc3NdPVwibWFwfHN0cmluZ1wiKVxuICogLSBEeW5hbWljIHN0eWxlL2NsYXNzIHByb3BlcnR5IGJpbmRpbmdzIChbc3R5bGUucHJvcF09XCJleHBcIiBhbmQgW2NsYXNzLm5hbWVdPVwiZXhwXCIpXG4gKlxuICogRHVlIHRvIHRoZSBjb21wbGV4IHJlbGF0aW9uc2hpcCBvZiBhbGwgb2YgdGhlc2UgY2FzZXMsIHRoZSBpbnN0cnVjdGlvbnMgZ2VuZXJhdGVkXG4gKiBmb3IgdGhlc2UgYXR0cmlidXRlcy9wcm9wZXJ0aWVzL2JpbmRpbmdzIG11c3QgYmUgZG9uZSBzbyBpbiB0aGUgY29ycmVjdCBvcmRlci4gVGhlXG4gKiBvcmRlciB3aGljaCB0aGVzZSBtdXN0IGJlIGdlbmVyYXRlZCBpcyBhcyBmb2xsb3dzOlxuICpcbiAqIGlmIChjcmVhdGVNb2RlKSB7XG4gKiAgIHN0eWxpbmcoLi4uKVxuICogfVxuICogaWYgKHVwZGF0ZU1vZGUpIHtcbiAqICAgc3R5bGVNYXAoLi4uKVxuICogICBjbGFzc01hcCguLi4pXG4gKiAgIHN0eWxlUHJvcCguLi4pXG4gKiAgIGNsYXNzUHJvcCguLi4pXG4gKiAgIHN0eWxpbmdBcHBseSguLi4pXG4gKiB9XG4gKlxuICogVGhlIGNyZWF0aW9uL3VwZGF0ZSBtZXRob2RzIHdpdGhpbiB0aGUgYnVpbGRlciBjbGFzcyBwcm9kdWNlIHRoZXNlIGluc3RydWN0aW9ucy5cbiAqL1xuZXhwb3J0IGNsYXNzIFN0eWxpbmdCdWlsZGVyIHtcbiAgLyoqIFdoZXRoZXIgb3Igbm90IHRoZXJlIGFyZSBhbnkgc3RhdGljIHN0eWxpbmcgdmFsdWVzIHByZXNlbnQgKi9cbiAgcHJpdmF0ZSBfaGFzSW5pdGlhbFZhbHVlcyA9IGZhbHNlO1xuICAvKipcbiAgICogIFdoZXRoZXIgb3Igbm90IHRoZXJlIGFyZSBhbnkgc3R5bGluZyBiaW5kaW5ncyBwcmVzZW50XG4gICAqICAoaS5lLiBgW3N0eWxlXWAsIGBbY2xhc3NdYCwgYFtzdHlsZS5wcm9wXWAgb3IgYFtjbGFzcy5uYW1lXWApXG4gICAqL1xuICBwdWJsaWMgaGFzQmluZGluZ3MgPSBmYWxzZTtcblxuICAvKiogdGhlIGlucHV0IGZvciBbY2xhc3NdIChpZiBpdCBleGlzdHMpICovXG4gIHByaXZhdGUgX2NsYXNzTWFwSW5wdXQ6IEJvdW5kU3R5bGluZ0VudHJ5fG51bGwgPSBudWxsO1xuICAvKiogdGhlIGlucHV0IGZvciBbc3R5bGVdIChpZiBpdCBleGlzdHMpICovXG4gIHByaXZhdGUgX3N0eWxlTWFwSW5wdXQ6IEJvdW5kU3R5bGluZ0VudHJ5fG51bGwgPSBudWxsO1xuICAvKiogYW4gYXJyYXkgb2YgZWFjaCBbc3R5bGUucHJvcF0gaW5wdXQgKi9cbiAgcHJpdmF0ZSBfc2luZ2xlU3R5bGVJbnB1dHM6IEJvdW5kU3R5bGluZ0VudHJ5W118bnVsbCA9IG51bGw7XG4gIC8qKiBhbiBhcnJheSBvZiBlYWNoIFtjbGFzcy5uYW1lXSBpbnB1dCAqL1xuICBwcml2YXRlIF9zaW5nbGVDbGFzc0lucHV0czogQm91bmRTdHlsaW5nRW50cnlbXXxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfbGFzdFN0eWxpbmdJbnB1dDogQm91bmRTdHlsaW5nRW50cnl8bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX2ZpcnN0U3R5bGluZ0lucHV0OiBCb3VuZFN0eWxpbmdFbnRyeXxudWxsID0gbnVsbDtcblxuICAvLyBtYXBzIGFyZSB1c2VkIGluc3RlYWQgb2YgaGFzaCBtYXBzIGJlY2F1c2UgYSBNYXAgd2lsbFxuICAvLyByZXRhaW4gdGhlIG9yZGVyaW5nIG9mIHRoZSBrZXlzXG5cbiAgLyoqXG4gICAqIFJlcHJlc2VudHMgdGhlIGxvY2F0aW9uIG9mIGVhY2ggc3R5bGUgYmluZGluZyBpbiB0aGUgdGVtcGxhdGVcbiAgICogKGUuZy4gYDxkaXYgW3N0eWxlLndpZHRoXT1cIndcIiBbc3R5bGUuaGVpZ2h0XT1cImhcIj5gIGltcGxpZXNcbiAgICogdGhhdCBgd2lkdGg9MGAgYW5kIGBoZWlnaHQ9MWApXG4gICAqL1xuICBwcml2YXRlIF9zdHlsZXNJbmRleCA9IG5ldyBNYXA8c3RyaW5nLCBudW1iZXI+KCk7XG5cbiAgLyoqXG4gICAqIFJlcHJlc2VudHMgdGhlIGxvY2F0aW9uIG9mIGVhY2ggY2xhc3MgYmluZGluZyBpbiB0aGUgdGVtcGxhdGVcbiAgICogKGUuZy4gYDxkaXYgW2NsYXNzLmJpZ109XCJiXCIgW2NsYXNzLmhpZGRlbl09XCJoXCI+YCBpbXBsaWVzXG4gICAqIHRoYXQgYGJpZz0wYCBhbmQgYGhpZGRlbj0xYClcbiAgICovXG4gIHByaXZhdGUgX2NsYXNzZXNJbmRleCA9IG5ldyBNYXA8c3RyaW5nLCBudW1iZXI+KCk7XG4gIHByaXZhdGUgX2luaXRpYWxTdHlsZVZhbHVlczogc3RyaW5nW10gPSBbXTtcbiAgcHJpdmF0ZSBfaW5pdGlhbENsYXNzVmFsdWVzOiBzdHJpbmdbXSA9IFtdO1xuXG4gIC8vIGNlcnRhaW4gc3R5bGUgcHJvcGVydGllcyBBTFdBWVMgbmVlZCBzYW5pdGl6YXRpb25cbiAgLy8gdGhpcyBpcyBjaGVja2VkIGVhY2ggdGltZSBuZXcgc3R5bGVzIGFyZSBlbmNvdW50ZXJlZFxuICBwcml2YXRlIF91c2VEZWZhdWx0U2FuaXRpemVyID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfZWxlbWVudEluZGV4RXhwcjogby5FeHByZXNzaW9uLCBwcml2YXRlIF9kaXJlY3RpdmVFeHByOiBvLkV4cHJlc3Npb258bnVsbCkge31cblxuICAvKipcbiAgICogUmVnaXN0ZXJzIGEgZ2l2ZW4gaW5wdXQgdG8gdGhlIHN0eWxpbmcgYnVpbGRlciB0byBiZSBsYXRlciB1c2VkIHdoZW4gcHJvZHVjaW5nIEFPVCBjb2RlLlxuICAgKlxuICAgKiBUaGUgY29kZSBiZWxvdyB3aWxsIG9ubHkgYWNjZXB0IHRoZSBpbnB1dCBpZiBpdCBpcyBzb21laG93IHRpZWQgdG8gc3R5bGluZyAod2hldGhlciBpdCBiZVxuICAgKiBzdHlsZS9jbGFzcyBiaW5kaW5ncyBvciBzdGF0aWMgc3R5bGUvY2xhc3MgYXR0cmlidXRlcykuXG4gICAqL1xuICByZWdpc3RlckJvdW5kSW5wdXQoaW5wdXQ6IHQuQm91bmRBdHRyaWJ1dGUpOiBib29sZWFuIHtcbiAgICAvLyBbYXR0ci5zdHlsZV0gb3IgW2F0dHIuY2xhc3NdIGFyZSBza2lwcGVkIGluIHRoZSBjb2RlIGJlbG93LFxuICAgIC8vIHRoZXkgc2hvdWxkIG5vdCBiZSB0cmVhdGVkIGFzIHN0eWxpbmctYmFzZWQgYmluZGluZ3Mgc2luY2VcbiAgICAvLyB0aGV5IGFyZSBpbnRlbmRlZCB0byBiZSB3cml0dGVuIGRpcmVjdGx5IHRvIHRoZSBhdHRyIGFuZFxuICAgIC8vIHdpbGwgdGhlcmVmb3JlIHNraXAgYWxsIHN0eWxlL2NsYXNzIHJlc29sdXRpb24gdGhhdCBpcyBwcmVzZW50XG4gICAgLy8gd2l0aCBzdHlsZT1cIlwiLCBbc3R5bGVdPVwiXCIgYW5kIFtzdHlsZS5wcm9wXT1cIlwiLCBjbGFzcz1cIlwiLFxuICAgIC8vIFtjbGFzcy5wcm9wXT1cIlwiLiBbY2xhc3NdPVwiXCIgYXNzaWdubWVudHNcbiAgICBsZXQgYmluZGluZzogQm91bmRTdHlsaW5nRW50cnl8bnVsbCA9IG51bGw7XG4gICAgbGV0IG5hbWUgPSBpbnB1dC5uYW1lO1xuICAgIHN3aXRjaCAoaW5wdXQudHlwZSkge1xuICAgICAgY2FzZSBCaW5kaW5nVHlwZS5Qcm9wZXJ0eTpcbiAgICAgICAgYmluZGluZyA9IHRoaXMucmVnaXN0ZXJJbnB1dEJhc2VkT25OYW1lKG5hbWUsIGlucHV0LnZhbHVlLCBpbnB1dC5zb3VyY2VTcGFuKTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIEJpbmRpbmdUeXBlLlN0eWxlOlxuICAgICAgICBiaW5kaW5nID0gdGhpcy5yZWdpc3RlclN0eWxlSW5wdXQobmFtZSwgZmFsc2UsIGlucHV0LnZhbHVlLCBpbnB1dC5zb3VyY2VTcGFuLCBpbnB1dC51bml0KTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIEJpbmRpbmdUeXBlLkNsYXNzOlxuICAgICAgICBiaW5kaW5nID0gdGhpcy5yZWdpc3RlckNsYXNzSW5wdXQobmFtZSwgZmFsc2UsIGlucHV0LnZhbHVlLCBpbnB1dC5zb3VyY2VTcGFuKTtcbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIHJldHVybiBiaW5kaW5nID8gdHJ1ZSA6IGZhbHNlO1xuICB9XG5cbiAgcmVnaXN0ZXJJbnB1dEJhc2VkT25OYW1lKG5hbWU6IHN0cmluZywgZXhwcmVzc2lvbjogQVNULCBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHtcbiAgICBsZXQgYmluZGluZzogQm91bmRTdHlsaW5nRW50cnl8bnVsbCA9IG51bGw7XG4gICAgY29uc3QgbmFtZVRvTWF0Y2ggPSBuYW1lLnN1YnN0cmluZygwLCA1KTsgIC8vIGNsYXNzIHwgc3R5bGVcbiAgICBjb25zdCBpc1N0eWxlID0gbmFtZVRvTWF0Y2ggPT09ICdzdHlsZSc7XG4gICAgY29uc3QgaXNDbGFzcyA9IGlzU3R5bGUgPyBmYWxzZSA6IChuYW1lVG9NYXRjaCA9PT0gJ2NsYXNzJyk7XG4gICAgaWYgKGlzU3R5bGUgfHwgaXNDbGFzcykge1xuICAgICAgY29uc3QgaXNNYXBCYXNlZCA9IG5hbWUuY2hhckF0KDUpICE9PSAnLic7ICAgICAgICAgLy8gc3R5bGUucHJvcCBvciBjbGFzcy5wcm9wIG1ha2VzIHRoaXMgYSBub1xuICAgICAgY29uc3QgcHJvcGVydHkgPSBuYW1lLnN1YnN0cihpc01hcEJhc2VkID8gNSA6IDYpOyAgLy8gdGhlIGRvdCBleHBsYWlucyB3aHkgdGhlcmUncyBhICsxXG4gICAgICBpZiAoaXNTdHlsZSkge1xuICAgICAgICBiaW5kaW5nID0gdGhpcy5yZWdpc3RlclN0eWxlSW5wdXQocHJvcGVydHksIGlzTWFwQmFzZWQsIGV4cHJlc3Npb24sIHNvdXJjZVNwYW4pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgYmluZGluZyA9IHRoaXMucmVnaXN0ZXJDbGFzc0lucHV0KHByb3BlcnR5LCBpc01hcEJhc2VkLCBleHByZXNzaW9uLCBzb3VyY2VTcGFuKTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIGJpbmRpbmc7XG4gIH1cblxuICByZWdpc3RlclN0eWxlSW5wdXQoXG4gICAgICBuYW1lOiBzdHJpbmcsIGlzTWFwQmFzZWQ6IGJvb2xlYW4sIHZhbHVlOiBBU1QsIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbixcbiAgICAgIHVuaXQ/OiBzdHJpbmd8bnVsbCk6IEJvdW5kU3R5bGluZ0VudHJ5fG51bGwge1xuICAgIGlmIChpc0VtcHR5RXhwcmVzc2lvbih2YWx1ZSkpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICBuYW1lID0gbm9ybWFsaXplUHJvcE5hbWUobmFtZSk7XG4gICAgY29uc3Qge3Byb3BlcnR5LCBoYXNPdmVycmlkZUZsYWcsIHVuaXQ6IGJpbmRpbmdVbml0fSA9IHBhcnNlUHJvcGVydHkobmFtZSk7XG4gICAgY29uc3QgZW50cnk6IEJvdW5kU3R5bGluZ0VudHJ5ID0ge1xuICAgICAgbmFtZTogcHJvcGVydHksXG4gICAgICB1bml0OiB1bml0IHx8IGJpbmRpbmdVbml0LCB2YWx1ZSwgc291cmNlU3BhbiwgaGFzT3ZlcnJpZGVGbGFnXG4gICAgfTtcbiAgICBpZiAoaXNNYXBCYXNlZCkge1xuICAgICAgdGhpcy5fdXNlRGVmYXVsdFNhbml0aXplciA9IHRydWU7XG4gICAgICB0aGlzLl9zdHlsZU1hcElucHV0ID0gZW50cnk7XG4gICAgfSBlbHNlIHtcbiAgICAgICh0aGlzLl9zaW5nbGVTdHlsZUlucHV0cyA9IHRoaXMuX3NpbmdsZVN0eWxlSW5wdXRzIHx8IFtdKS5wdXNoKGVudHJ5KTtcbiAgICAgIHRoaXMuX3VzZURlZmF1bHRTYW5pdGl6ZXIgPSB0aGlzLl91c2VEZWZhdWx0U2FuaXRpemVyIHx8IGlzU3R5bGVTYW5pdGl6YWJsZShuYW1lKTtcbiAgICAgIHJlZ2lzdGVySW50b01hcCh0aGlzLl9zdHlsZXNJbmRleCwgcHJvcGVydHkpO1xuICAgIH1cbiAgICB0aGlzLl9sYXN0U3R5bGluZ0lucHV0ID0gZW50cnk7XG4gICAgdGhpcy5fZmlyc3RTdHlsaW5nSW5wdXQgPSB0aGlzLl9maXJzdFN0eWxpbmdJbnB1dCB8fCBlbnRyeTtcbiAgICB0aGlzLmhhc0JpbmRpbmdzID0gdHJ1ZTtcbiAgICByZXR1cm4gZW50cnk7XG4gIH1cblxuICByZWdpc3RlckNsYXNzSW5wdXQobmFtZTogc3RyaW5nLCBpc01hcEJhc2VkOiBib29sZWFuLCB2YWx1ZTogQVNULCBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pOlxuICAgICAgQm91bmRTdHlsaW5nRW50cnl8bnVsbCB7XG4gICAgaWYgKGlzRW1wdHlFeHByZXNzaW9uKHZhbHVlKSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGNvbnN0IHtwcm9wZXJ0eSwgaGFzT3ZlcnJpZGVGbGFnfSA9IHBhcnNlUHJvcGVydHkobmFtZSk7XG4gICAgY29uc3QgZW50cnk6XG4gICAgICAgIEJvdW5kU3R5bGluZ0VudHJ5ID0ge25hbWU6IHByb3BlcnR5LCB2YWx1ZSwgc291cmNlU3BhbiwgaGFzT3ZlcnJpZGVGbGFnLCB1bml0OiBudWxsfTtcbiAgICBpZiAoaXNNYXBCYXNlZCkge1xuICAgICAgdGhpcy5fY2xhc3NNYXBJbnB1dCA9IGVudHJ5O1xuICAgIH0gZWxzZSB7XG4gICAgICAodGhpcy5fc2luZ2xlQ2xhc3NJbnB1dHMgPSB0aGlzLl9zaW5nbGVDbGFzc0lucHV0cyB8fCBbXSkucHVzaChlbnRyeSk7XG4gICAgICByZWdpc3RlckludG9NYXAodGhpcy5fY2xhc3Nlc0luZGV4LCBwcm9wZXJ0eSk7XG4gICAgfVxuICAgIHRoaXMuX2xhc3RTdHlsaW5nSW5wdXQgPSBlbnRyeTtcbiAgICB0aGlzLl9maXJzdFN0eWxpbmdJbnB1dCA9IHRoaXMuX2ZpcnN0U3R5bGluZ0lucHV0IHx8IGVudHJ5O1xuICAgIHRoaXMuaGFzQmluZGluZ3MgPSB0cnVlO1xuICAgIHJldHVybiBlbnRyeTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgdGhlIGVsZW1lbnQncyBzdGF0aWMgc3R5bGUgc3RyaW5nIHZhbHVlIHRvIHRoZSBidWlsZGVyLlxuICAgKlxuICAgKiBAcGFyYW0gdmFsdWUgdGhlIHN0eWxlIHN0cmluZyAoZS5nLiBgd2lkdGg6MTAwcHg7IGhlaWdodDoyMDBweDtgKVxuICAgKi9cbiAgcmVnaXN0ZXJTdHlsZUF0dHIodmFsdWU6IHN0cmluZykge1xuICAgIHRoaXMuX2luaXRpYWxTdHlsZVZhbHVlcyA9IHBhcnNlU3R5bGUodmFsdWUpO1xuICAgIHRoaXMuX2hhc0luaXRpYWxWYWx1ZXMgPSB0cnVlO1xuICB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyB0aGUgZWxlbWVudCdzIHN0YXRpYyBjbGFzcyBzdHJpbmcgdmFsdWUgdG8gdGhlIGJ1aWxkZXIuXG4gICAqXG4gICAqIEBwYXJhbSB2YWx1ZSB0aGUgY2xhc3NOYW1lIHN0cmluZyAoZS5nLiBgZGlzYWJsZWQgZ29sZCB6b29tYClcbiAgICovXG4gIHJlZ2lzdGVyQ2xhc3NBdHRyKHZhbHVlOiBzdHJpbmcpIHtcbiAgICB0aGlzLl9pbml0aWFsQ2xhc3NWYWx1ZXMgPSB2YWx1ZS50cmltKCkuc3BsaXQoL1xccysvZyk7XG4gICAgdGhpcy5faGFzSW5pdGlhbFZhbHVlcyA9IHRydWU7XG4gIH1cblxuICAvKipcbiAgICogQXBwZW5kcyBhbGwgc3R5bGluZy1yZWxhdGVkIGV4cHJlc3Npb25zIHRvIHRoZSBwcm92aWRlZCBhdHRycyBhcnJheS5cbiAgICpcbiAgICogQHBhcmFtIGF0dHJzIGFuIGV4aXN0aW5nIGFycmF5IHdoZXJlIGVhY2ggb2YgdGhlIHN0eWxpbmcgZXhwcmVzc2lvbnNcbiAgICogd2lsbCBiZSBpbnNlcnRlZCBpbnRvLlxuICAgKi9cbiAgcG9wdWxhdGVJbml0aWFsU3R5bGluZ0F0dHJzKGF0dHJzOiBvLkV4cHJlc3Npb25bXSk6IHZvaWQge1xuICAgIC8vIFtDTEFTU19NQVJLRVIsICdmb28nLCAnYmFyJywgJ2JheicgLi4uXVxuICAgIGlmICh0aGlzLl9pbml0aWFsQ2xhc3NWYWx1ZXMubGVuZ3RoKSB7XG4gICAgICBhdHRycy5wdXNoKG8ubGl0ZXJhbChBdHRyaWJ1dGVNYXJrZXIuQ2xhc3NlcykpO1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLl9pbml0aWFsQ2xhc3NWYWx1ZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgYXR0cnMucHVzaChvLmxpdGVyYWwodGhpcy5faW5pdGlhbENsYXNzVmFsdWVzW2ldKSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gW1NUWUxFX01BUktFUiwgJ3dpZHRoJywgJzIwMHB4JywgJ2hlaWdodCcsICcxMDBweCcsIC4uLl1cbiAgICBpZiAodGhpcy5faW5pdGlhbFN0eWxlVmFsdWVzLmxlbmd0aCkge1xuICAgICAgYXR0cnMucHVzaChvLmxpdGVyYWwoQXR0cmlidXRlTWFya2VyLlN0eWxlcykpO1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLl9pbml0aWFsU3R5bGVWYWx1ZXMubGVuZ3RoOyBpICs9IDIpIHtcbiAgICAgICAgYXR0cnMucHVzaChcbiAgICAgICAgICAgIG8ubGl0ZXJhbCh0aGlzLl9pbml0aWFsU3R5bGVWYWx1ZXNbaV0pLCBvLmxpdGVyYWwodGhpcy5faW5pdGlhbFN0eWxlVmFsdWVzW2kgKyAxXSkpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBCdWlsZHMgYW4gaW5zdHJ1Y3Rpb24gd2l0aCBhbGwgdGhlIGV4cHJlc3Npb25zIGFuZCBwYXJhbWV0ZXJzIGZvciBgZWxlbWVudEhvc3RBdHRyc2AuXG4gICAqXG4gICAqIFRoZSBpbnN0cnVjdGlvbiBnZW5lcmF0aW9uIGNvZGUgYmVsb3cgaXMgdXNlZCBmb3IgcHJvZHVjaW5nIHRoZSBBT1Qgc3RhdGVtZW50IGNvZGUgd2hpY2ggaXNcbiAgICogcmVzcG9uc2libGUgZm9yIHJlZ2lzdGVyaW5nIGluaXRpYWwgc3R5bGVzICh3aXRoaW4gYSBkaXJlY3RpdmUgaG9zdEJpbmRpbmdzJyBjcmVhdGlvbiBibG9jayksXG4gICAqIGFzIHdlbGwgYXMgYW55IG9mIHRoZSBwcm92aWRlZCBhdHRyaWJ1dGUgdmFsdWVzLCB0byB0aGUgZGlyZWN0aXZlIGhvc3QgZWxlbWVudC5cbiAgICovXG4gIGJ1aWxkSG9zdEF0dHJzSW5zdHJ1Y3Rpb24oXG4gICAgICBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW58bnVsbCwgYXR0cnM6IG8uRXhwcmVzc2lvbltdLFxuICAgICAgY29uc3RhbnRQb29sOiBDb25zdGFudFBvb2wpOiBTdHlsaW5nSW5zdHJ1Y3Rpb258bnVsbCB7XG4gICAgaWYgKHRoaXMuX2RpcmVjdGl2ZUV4cHIgJiYgKGF0dHJzLmxlbmd0aCB8fCB0aGlzLl9oYXNJbml0aWFsVmFsdWVzKSkge1xuICAgICAgcmV0dXJuIHtcbiAgICAgICAgc291cmNlU3BhbixcbiAgICAgICAgcmVmZXJlbmNlOiBSMy5lbGVtZW50SG9zdEF0dHJzLFxuICAgICAgICBhbGxvY2F0ZUJpbmRpbmdTbG90czogMCxcbiAgICAgICAgcGFyYW1zOiAoKSA9PiB7XG4gICAgICAgICAgLy8gcGFyYW1zID0+IGVsZW1lbnRIb3N0QXR0cnMoYXR0cnMpXG4gICAgICAgICAgdGhpcy5wb3B1bGF0ZUluaXRpYWxTdHlsaW5nQXR0cnMoYXR0cnMpO1xuICAgICAgICAgIGNvbnN0IGF0dHJBcnJheSA9ICFhdHRycy5zb21lKGF0dHIgPT4gYXR0ciBpbnN0YW5jZW9mIG8uV3JhcHBlZE5vZGVFeHByKSA/XG4gICAgICAgICAgICAgIGdldENvbnN0YW50TGl0ZXJhbEZyb21BcnJheShjb25zdGFudFBvb2wsIGF0dHJzKSA6XG4gICAgICAgICAgICAgIG8ubGl0ZXJhbEFycihhdHRycyk7XG4gICAgICAgICAgcmV0dXJuIFthdHRyQXJyYXldO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIC8qKlxuICAgKiBCdWlsZHMgYW4gaW5zdHJ1Y3Rpb24gd2l0aCBhbGwgdGhlIGV4cHJlc3Npb25zIGFuZCBwYXJhbWV0ZXJzIGZvciBgc3R5bGluZ2AuXG4gICAqXG4gICAqIFRoZSBpbnN0cnVjdGlvbiBnZW5lcmF0aW9uIGNvZGUgYmVsb3cgaXMgdXNlZCBmb3IgcHJvZHVjaW5nIHRoZSBBT1Qgc3RhdGVtZW50IGNvZGUgd2hpY2ggaXNcbiAgICogcmVzcG9uc2libGUgZm9yIHJlZ2lzdGVyaW5nIHN0eWxlL2NsYXNzIGJpbmRpbmdzIHRvIGFuIGVsZW1lbnQuXG4gICAqL1xuICBidWlsZFN0eWxpbmdJbnN0cnVjdGlvbihzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW58bnVsbCwgY29uc3RhbnRQb29sOiBDb25zdGFudFBvb2wpOlxuICAgICAgU3R5bGluZ0luc3RydWN0aW9ufG51bGwge1xuICAgIGlmICh0aGlzLmhhc0JpbmRpbmdzKSB7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBzb3VyY2VTcGFuLFxuICAgICAgICBhbGxvY2F0ZUJpbmRpbmdTbG90czogMCxcbiAgICAgICAgcmVmZXJlbmNlOiBSMy5zdHlsaW5nLFxuICAgICAgICBwYXJhbXM6ICgpID0+IFtdLFxuICAgICAgfTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICAvKipcbiAgICogQnVpbGRzIGFuIGluc3RydWN0aW9uIHdpdGggYWxsIHRoZSBleHByZXNzaW9ucyBhbmQgcGFyYW1ldGVycyBmb3IgYGNsYXNzTWFwYC5cbiAgICpcbiAgICogVGhlIGluc3RydWN0aW9uIGRhdGEgd2lsbCBjb250YWluIGFsbCBleHByZXNzaW9ucyBmb3IgYGNsYXNzTWFwYCB0byBmdW5jdGlvblxuICAgKiB3aGljaCBpbmNsdWRlcyB0aGUgYFtjbGFzc11gIGV4cHJlc3Npb24gcGFyYW1zLlxuICAgKi9cbiAgYnVpbGRDbGFzc01hcEluc3RydWN0aW9uKHZhbHVlQ29udmVydGVyOiBWYWx1ZUNvbnZlcnRlcik6IFN0eWxpbmdJbnN0cnVjdGlvbnxudWxsIHtcbiAgICBpZiAodGhpcy5fY2xhc3NNYXBJbnB1dCkge1xuICAgICAgcmV0dXJuIHRoaXMuX2J1aWxkTWFwQmFzZWRJbnN0cnVjdGlvbih2YWx1ZUNvbnZlcnRlciwgdHJ1ZSwgdGhpcy5fY2xhc3NNYXBJbnB1dCk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAqIEJ1aWxkcyBhbiBpbnN0cnVjdGlvbiB3aXRoIGFsbCB0aGUgZXhwcmVzc2lvbnMgYW5kIHBhcmFtZXRlcnMgZm9yIGBzdHlsZU1hcGAuXG4gICAqXG4gICAqIFRoZSBpbnN0cnVjdGlvbiBkYXRhIHdpbGwgY29udGFpbiBhbGwgZXhwcmVzc2lvbnMgZm9yIGBzdHlsZU1hcGAgdG8gZnVuY3Rpb25cbiAgICogd2hpY2ggaW5jbHVkZXMgdGhlIGBbc3R5bGVdYCBleHByZXNzaW9uIHBhcmFtcy5cbiAgICovXG4gIGJ1aWxkU3R5bGVNYXBJbnN0cnVjdGlvbih2YWx1ZUNvbnZlcnRlcjogVmFsdWVDb252ZXJ0ZXIpOiBTdHlsaW5nSW5zdHJ1Y3Rpb258bnVsbCB7XG4gICAgaWYgKHRoaXMuX3N0eWxlTWFwSW5wdXQpIHtcbiAgICAgIHJldHVybiB0aGlzLl9idWlsZE1hcEJhc2VkSW5zdHJ1Y3Rpb24odmFsdWVDb252ZXJ0ZXIsIGZhbHNlLCB0aGlzLl9zdHlsZU1hcElucHV0KTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBwcml2YXRlIF9idWlsZE1hcEJhc2VkSW5zdHJ1Y3Rpb24oXG4gICAgICB2YWx1ZUNvbnZlcnRlcjogVmFsdWVDb252ZXJ0ZXIsIGlzQ2xhc3NCYXNlZDogYm9vbGVhbixcbiAgICAgIHN0eWxpbmdJbnB1dDogQm91bmRTdHlsaW5nRW50cnkpOiBTdHlsaW5nSW5zdHJ1Y3Rpb24ge1xuICAgIC8vIGVhY2ggc3R5bGluZyBiaW5kaW5nIHZhbHVlIGlzIHN0b3JlZCBpbiB0aGUgTFZpZXdcbiAgICBsZXQgdG90YWxCaW5kaW5nU2xvdHNSZXF1aXJlZCA9IDE7XG5cbiAgICAvLyB0aGVzZSB2YWx1ZXMgbXVzdCBiZSBvdXRzaWRlIG9mIHRoZSB1cGRhdGUgYmxvY2sgc28gdGhhdCB0aGV5IGNhblxuICAgIC8vIGJlIGV2YWx1YXRlZCAodGhlIEFTVCB2aXNpdCBjYWxsKSBkdXJpbmcgY3JlYXRpb24gdGltZSBzbyB0aGF0IGFueVxuICAgIC8vIHBpcGVzIGNhbiBiZSBwaWNrZWQgdXAgaW4gdGltZSBiZWZvcmUgdGhlIHRlbXBsYXRlIGlzIGJ1aWx0XG4gICAgY29uc3QgbWFwVmFsdWUgPSBzdHlsaW5nSW5wdXQudmFsdWUudmlzaXQodmFsdWVDb252ZXJ0ZXIpO1xuICAgIGxldCByZWZlcmVuY2U6IG8uRXh0ZXJuYWxSZWZlcmVuY2U7XG4gICAgaWYgKG1hcFZhbHVlIGluc3RhbmNlb2YgSW50ZXJwb2xhdGlvbiAmJiBpc0NsYXNzQmFzZWQpIHtcbiAgICAgIHRvdGFsQmluZGluZ1Nsb3RzUmVxdWlyZWQgKz0gbWFwVmFsdWUuZXhwcmVzc2lvbnMubGVuZ3RoO1xuICAgICAgcmVmZXJlbmNlID0gZ2V0Q2xhc3NNYXBJbnRlcnBvbGF0aW9uRXhwcmVzc2lvbihtYXBWYWx1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJlZmVyZW5jZSA9IGlzQ2xhc3NCYXNlZCA/IFIzLmNsYXNzTWFwIDogUjMuc3R5bGVNYXA7XG4gICAgfVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIHNvdXJjZVNwYW46IHN0eWxpbmdJbnB1dC5zb3VyY2VTcGFuLFxuICAgICAgcmVmZXJlbmNlLFxuICAgICAgYWxsb2NhdGVCaW5kaW5nU2xvdHM6IHRvdGFsQmluZGluZ1Nsb3RzUmVxdWlyZWQsXG4gICAgICBzdXBwb3J0c0ludGVycG9sYXRpb246IGlzQ2xhc3NCYXNlZCxcbiAgICAgIHBhcmFtczogKGNvbnZlcnRGbjogKHZhbHVlOiBhbnkpID0+IG8uRXhwcmVzc2lvbiB8IG8uRXhwcmVzc2lvbltdKSA9PiB7XG4gICAgICAgIGNvbnN0IGNvbnZlcnRSZXN1bHQgPSBjb252ZXJ0Rm4obWFwVmFsdWUpO1xuICAgICAgICByZXR1cm4gQXJyYXkuaXNBcnJheShjb252ZXJ0UmVzdWx0KSA/IGNvbnZlcnRSZXN1bHQgOiBbY29udmVydFJlc3VsdF07XG4gICAgICB9XG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX2J1aWxkU2luZ2xlSW5wdXRzKFxuICAgICAgcmVmZXJlbmNlOiBvLkV4dGVybmFsUmVmZXJlbmNlLCBpbnB1dHM6IEJvdW5kU3R5bGluZ0VudHJ5W10sIG1hcEluZGV4OiBNYXA8c3RyaW5nLCBudW1iZXI+LFxuICAgICAgYWxsb3dVbml0czogYm9vbGVhbiwgdmFsdWVDb252ZXJ0ZXI6IFZhbHVlQ29udmVydGVyLFxuICAgICAgZ2V0SW50ZXJwb2xhdGlvbkV4cHJlc3Npb25Gbj86ICh2YWx1ZTogSW50ZXJwb2xhdGlvbikgPT4gby5FeHRlcm5hbFJlZmVyZW5jZSk6XG4gICAgICBTdHlsaW5nSW5zdHJ1Y3Rpb25bXSB7XG4gICAgbGV0IHRvdGFsQmluZGluZ1Nsb3RzUmVxdWlyZWQgPSAwO1xuICAgIHJldHVybiBpbnB1dHMubWFwKGlucHV0ID0+IHtcbiAgICAgIGNvbnN0IHZhbHVlID0gaW5wdXQudmFsdWUudmlzaXQodmFsdWVDb252ZXJ0ZXIpO1xuXG4gICAgICAvLyBlYWNoIHN0eWxpbmcgYmluZGluZyB2YWx1ZSBpcyBzdG9yZWQgaW4gdGhlIExWaWV3XG4gICAgICBsZXQgdG90YWxCaW5kaW5nU2xvdHNSZXF1aXJlZCA9IDE7XG5cbiAgICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIEludGVycG9sYXRpb24pIHtcbiAgICAgICAgdG90YWxCaW5kaW5nU2xvdHNSZXF1aXJlZCArPSB2YWx1ZS5leHByZXNzaW9ucy5sZW5ndGg7XG5cbiAgICAgICAgaWYgKGdldEludGVycG9sYXRpb25FeHByZXNzaW9uRm4pIHtcbiAgICAgICAgICByZWZlcmVuY2UgPSBnZXRJbnRlcnBvbGF0aW9uRXhwcmVzc2lvbkZuKHZhbHVlKTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICByZXR1cm4ge1xuICAgICAgICBzb3VyY2VTcGFuOiBpbnB1dC5zb3VyY2VTcGFuLFxuICAgICAgICBzdXBwb3J0c0ludGVycG9sYXRpb246ICEhZ2V0SW50ZXJwb2xhdGlvbkV4cHJlc3Npb25GbixcbiAgICAgICAgYWxsb2NhdGVCaW5kaW5nU2xvdHM6IHRvdGFsQmluZGluZ1Nsb3RzUmVxdWlyZWQsIHJlZmVyZW5jZSxcbiAgICAgICAgcGFyYW1zOiAoY29udmVydEZuOiAodmFsdWU6IGFueSkgPT4gby5FeHByZXNzaW9uIHwgby5FeHByZXNzaW9uW10pID0+IHtcbiAgICAgICAgICAvLyBwYXJhbXMgPT4gc3R5bGluZ1Byb3AocHJvcE5hbWUsIHZhbHVlKVxuICAgICAgICAgIGNvbnN0IHBhcmFtczogby5FeHByZXNzaW9uW10gPSBbXTtcbiAgICAgICAgICBwYXJhbXMucHVzaChvLmxpdGVyYWwoaW5wdXQubmFtZSkpO1xuXG4gICAgICAgICAgY29uc3QgY29udmVydFJlc3VsdCA9IGNvbnZlcnRGbih2YWx1ZSk7XG4gICAgICAgICAgaWYgKEFycmF5LmlzQXJyYXkoY29udmVydFJlc3VsdCkpIHtcbiAgICAgICAgICAgIHBhcmFtcy5wdXNoKC4uLmNvbnZlcnRSZXN1bHQpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBwYXJhbXMucHVzaChjb252ZXJ0UmVzdWx0KTtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBpZiAoYWxsb3dVbml0cyAmJiBpbnB1dC51bml0KSB7XG4gICAgICAgICAgICBwYXJhbXMucHVzaChvLmxpdGVyYWwoaW5wdXQudW5pdCkpO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIHJldHVybiBwYXJhbXM7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIF9idWlsZENsYXNzSW5wdXRzKHZhbHVlQ29udmVydGVyOiBWYWx1ZUNvbnZlcnRlcik6IFN0eWxpbmdJbnN0cnVjdGlvbltdIHtcbiAgICBpZiAodGhpcy5fc2luZ2xlQ2xhc3NJbnB1dHMpIHtcbiAgICAgIHJldHVybiB0aGlzLl9idWlsZFNpbmdsZUlucHV0cyhcbiAgICAgICAgICBSMy5jbGFzc1Byb3AsIHRoaXMuX3NpbmdsZUNsYXNzSW5wdXRzLCB0aGlzLl9jbGFzc2VzSW5kZXgsIGZhbHNlLCB2YWx1ZUNvbnZlcnRlcik7XG4gICAgfVxuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIHByaXZhdGUgX2J1aWxkU3R5bGVJbnB1dHModmFsdWVDb252ZXJ0ZXI6IFZhbHVlQ29udmVydGVyKTogU3R5bGluZ0luc3RydWN0aW9uW10ge1xuICAgIGlmICh0aGlzLl9zaW5nbGVTdHlsZUlucHV0cykge1xuICAgICAgcmV0dXJuIHRoaXMuX2J1aWxkU2luZ2xlSW5wdXRzKFxuICAgICAgICAgIFIzLnN0eWxlUHJvcCwgdGhpcy5fc2luZ2xlU3R5bGVJbnB1dHMsIHRoaXMuX3N0eWxlc0luZGV4LCB0cnVlLCB2YWx1ZUNvbnZlcnRlcixcbiAgICAgICAgICBnZXRTdHlsZVByb3BJbnRlcnBvbGF0aW9uRXhwcmVzc2lvbik7XG4gICAgfVxuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIHByaXZhdGUgX2J1aWxkQXBwbHlGbigpOiBTdHlsaW5nSW5zdHJ1Y3Rpb24ge1xuICAgIHJldHVybiB7XG4gICAgICBzb3VyY2VTcGFuOiB0aGlzLl9sYXN0U3R5bGluZ0lucHV0ID8gdGhpcy5fbGFzdFN0eWxpbmdJbnB1dC5zb3VyY2VTcGFuIDogbnVsbCxcbiAgICAgIHJlZmVyZW5jZTogUjMuc3R5bGluZ0FwcGx5LFxuICAgICAgYWxsb2NhdGVCaW5kaW5nU2xvdHM6IDAsXG4gICAgICBwYXJhbXM6ICgpID0+IHsgcmV0dXJuIFtdOyB9XG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX2J1aWxkU2FuaXRpemVyRm4oKTogU3R5bGluZ0luc3RydWN0aW9uIHtcbiAgICByZXR1cm4ge1xuICAgICAgc291cmNlU3BhbjogdGhpcy5fZmlyc3RTdHlsaW5nSW5wdXQgPyB0aGlzLl9maXJzdFN0eWxpbmdJbnB1dC5zb3VyY2VTcGFuIDogbnVsbCxcbiAgICAgIHJlZmVyZW5jZTogUjMuc3R5bGVTYW5pdGl6ZXIsXG4gICAgICBhbGxvY2F0ZUJpbmRpbmdTbG90czogMCxcbiAgICAgIHBhcmFtczogKCkgPT4gW28uaW1wb3J0RXhwcihSMy5kZWZhdWx0U3R5bGVTYW5pdGl6ZXIpXVxuICAgIH07XG4gIH1cblxuICAvKipcbiAgICogQ29uc3RydWN0cyBhbGwgaW5zdHJ1Y3Rpb25zIHdoaWNoIGNvbnRhaW4gdGhlIGV4cHJlc3Npb25zIHRoYXQgd2lsbCBiZSBwbGFjZWRcbiAgICogaW50byB0aGUgdXBkYXRlIGJsb2NrIG9mIGEgdGVtcGxhdGUgZnVuY3Rpb24gb3IgYSBkaXJlY3RpdmUgaG9zdEJpbmRpbmdzIGZ1bmN0aW9uLlxuICAgKi9cbiAgYnVpbGRVcGRhdGVMZXZlbEluc3RydWN0aW9ucyh2YWx1ZUNvbnZlcnRlcjogVmFsdWVDb252ZXJ0ZXIpIHtcbiAgICBjb25zdCBpbnN0cnVjdGlvbnM6IFN0eWxpbmdJbnN0cnVjdGlvbltdID0gW107XG4gICAgaWYgKHRoaXMuaGFzQmluZGluZ3MpIHtcbiAgICAgIGlmICh0aGlzLl91c2VEZWZhdWx0U2FuaXRpemVyKSB7XG4gICAgICAgIGluc3RydWN0aW9ucy5wdXNoKHRoaXMuX2J1aWxkU2FuaXRpemVyRm4oKSk7XG4gICAgICB9XG4gICAgICBjb25zdCBzdHlsZU1hcEluc3RydWN0aW9uID0gdGhpcy5idWlsZFN0eWxlTWFwSW5zdHJ1Y3Rpb24odmFsdWVDb252ZXJ0ZXIpO1xuICAgICAgaWYgKHN0eWxlTWFwSW5zdHJ1Y3Rpb24pIHtcbiAgICAgICAgaW5zdHJ1Y3Rpb25zLnB1c2goc3R5bGVNYXBJbnN0cnVjdGlvbik7XG4gICAgICB9XG4gICAgICBjb25zdCBjbGFzc01hcEluc3RydWN0aW9uID0gdGhpcy5idWlsZENsYXNzTWFwSW5zdHJ1Y3Rpb24odmFsdWVDb252ZXJ0ZXIpO1xuICAgICAgaWYgKGNsYXNzTWFwSW5zdHJ1Y3Rpb24pIHtcbiAgICAgICAgaW5zdHJ1Y3Rpb25zLnB1c2goY2xhc3NNYXBJbnN0cnVjdGlvbik7XG4gICAgICB9XG4gICAgICBpbnN0cnVjdGlvbnMucHVzaCguLi50aGlzLl9idWlsZFN0eWxlSW5wdXRzKHZhbHVlQ29udmVydGVyKSk7XG4gICAgICBpbnN0cnVjdGlvbnMucHVzaCguLi50aGlzLl9idWlsZENsYXNzSW5wdXRzKHZhbHVlQ29udmVydGVyKSk7XG4gICAgICBpbnN0cnVjdGlvbnMucHVzaCh0aGlzLl9idWlsZEFwcGx5Rm4oKSk7XG4gICAgfVxuICAgIHJldHVybiBpbnN0cnVjdGlvbnM7XG4gIH1cbn1cblxuZnVuY3Rpb24gcmVnaXN0ZXJJbnRvTWFwKG1hcDogTWFwPHN0cmluZywgbnVtYmVyPiwga2V5OiBzdHJpbmcpIHtcbiAgaWYgKCFtYXAuaGFzKGtleSkpIHtcbiAgICBtYXAuc2V0KGtleSwgbWFwLnNpemUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIGlzU3R5bGVTYW5pdGl6YWJsZShwcm9wOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgLy8gTm90ZSB0aGF0IGJyb3dzZXJzIHN1cHBvcnQgYm90aCB0aGUgZGFzaCBjYXNlIGFuZFxuICAvLyBjYW1lbCBjYXNlIHByb3BlcnR5IG5hbWVzIHdoZW4gc2V0dGluZyB0aHJvdWdoIEpTLlxuICByZXR1cm4gcHJvcCA9PT0gJ2JhY2tncm91bmQtaW1hZ2UnIHx8IHByb3AgPT09ICdiYWNrZ3JvdW5kSW1hZ2UnIHx8IHByb3AgPT09ICdiYWNrZ3JvdW5kJyB8fFxuICAgICAgcHJvcCA9PT0gJ2JvcmRlci1pbWFnZScgfHwgcHJvcCA9PT0gJ2JvcmRlckltYWdlJyB8fCBwcm9wID09PSAnZmlsdGVyJyB8fFxuICAgICAgcHJvcCA9PT0gJ2xpc3Qtc3R5bGUnIHx8IHByb3AgPT09ICdsaXN0U3R5bGUnIHx8IHByb3AgPT09ICdsaXN0LXN0eWxlLWltYWdlJyB8fFxuICAgICAgcHJvcCA9PT0gJ2xpc3RTdHlsZUltYWdlJyB8fCBwcm9wID09PSAnY2xpcC1wYXRoJyB8fCBwcm9wID09PSAnY2xpcFBhdGgnO1xufVxuXG4vKipcbiAqIFNpbXBsZSBoZWxwZXIgZnVuY3Rpb24gdG8gZWl0aGVyIHByb3ZpZGUgdGhlIGNvbnN0YW50IGxpdGVyYWwgdGhhdCB3aWxsIGhvdXNlIHRoZSB2YWx1ZVxuICogaGVyZSBvciBhIG51bGwgdmFsdWUgaWYgdGhlIHByb3ZpZGVkIHZhbHVlcyBhcmUgZW1wdHkuXG4gKi9cbmZ1bmN0aW9uIGdldENvbnN0YW50TGl0ZXJhbEZyb21BcnJheShcbiAgICBjb25zdGFudFBvb2w6IENvbnN0YW50UG9vbCwgdmFsdWVzOiBvLkV4cHJlc3Npb25bXSk6IG8uRXhwcmVzc2lvbiB7XG4gIHJldHVybiB2YWx1ZXMubGVuZ3RoID8gY29uc3RhbnRQb29sLmdldENvbnN0TGl0ZXJhbChvLmxpdGVyYWxBcnIodmFsdWVzKSwgdHJ1ZSkgOiBvLk5VTExfRVhQUjtcbn1cblxuLyoqXG4gKiBTaW1wbGUgaGVscGVyIGZ1bmN0aW9uIHRoYXQgYWRkcyBhIHBhcmFtZXRlciBvciBkb2VzIG5vdGhpbmcgYXQgYWxsIGRlcGVuZGluZyBvbiB0aGUgcHJvdmlkZWRcbiAqIHByZWRpY2F0ZSBhbmQgdG90YWxFeHBlY3RlZEFyZ3MgdmFsdWVzXG4gKi9cbmZ1bmN0aW9uIGFkZFBhcmFtKFxuICAgIHBhcmFtczogby5FeHByZXNzaW9uW10sIHByZWRpY2F0ZTogYW55LCB2YWx1ZTogby5FeHByZXNzaW9uIHwgbnVsbCwgYXJnTnVtYmVyOiBudW1iZXIsXG4gICAgdG90YWxFeHBlY3RlZEFyZ3M6IG51bWJlcikge1xuICBpZiAocHJlZGljYXRlICYmIHZhbHVlKSB7XG4gICAgcGFyYW1zLnB1c2godmFsdWUpO1xuICB9IGVsc2UgaWYgKGFyZ051bWJlciA8IHRvdGFsRXhwZWN0ZWRBcmdzKSB7XG4gICAgcGFyYW1zLnB1c2goby5OVUxMX0VYUFIpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZVByb3BlcnR5KG5hbWU6IHN0cmluZyk6XG4gICAge3Byb3BlcnR5OiBzdHJpbmcsIHVuaXQ6IHN0cmluZywgaGFzT3ZlcnJpZGVGbGFnOiBib29sZWFufSB7XG4gIGxldCBoYXNPdmVycmlkZUZsYWcgPSBmYWxzZTtcbiAgY29uc3Qgb3ZlcnJpZGVJbmRleCA9IG5hbWUuaW5kZXhPZihJTVBPUlRBTlRfRkxBRyk7XG4gIGlmIChvdmVycmlkZUluZGV4ICE9PSAtMSkge1xuICAgIG5hbWUgPSBvdmVycmlkZUluZGV4ID4gMCA/IG5hbWUuc3Vic3RyaW5nKDAsIG92ZXJyaWRlSW5kZXgpIDogJyc7XG4gICAgaGFzT3ZlcnJpZGVGbGFnID0gdHJ1ZTtcbiAgfVxuXG4gIGxldCB1bml0ID0gJyc7XG4gIGxldCBwcm9wZXJ0eSA9IG5hbWU7XG4gIGNvbnN0IHVuaXRJbmRleCA9IG5hbWUubGFzdEluZGV4T2YoJy4nKTtcbiAgaWYgKHVuaXRJbmRleCA+IDApIHtcbiAgICB1bml0ID0gbmFtZS5zdWJzdHIodW5pdEluZGV4ICsgMSk7XG4gICAgcHJvcGVydHkgPSBuYW1lLnN1YnN0cmluZygwLCB1bml0SW5kZXgpO1xuICB9XG5cbiAgcmV0dXJuIHtwcm9wZXJ0eSwgdW5pdCwgaGFzT3ZlcnJpZGVGbGFnfTtcbn1cblxuLyoqXG4gKiBHZXRzIHRoZSBpbnN0cnVjdGlvbiB0byBnZW5lcmF0ZSBmb3IgYW4gaW50ZXJwb2xhdGVkIGNsYXNzIG1hcC5cbiAqIEBwYXJhbSBpbnRlcnBvbGF0aW9uIEFuIEludGVycG9sYXRpb24gQVNUXG4gKi9cbmZ1bmN0aW9uIGdldENsYXNzTWFwSW50ZXJwb2xhdGlvbkV4cHJlc3Npb24oaW50ZXJwb2xhdGlvbjogSW50ZXJwb2xhdGlvbik6IG8uRXh0ZXJuYWxSZWZlcmVuY2Uge1xuICBzd2l0Y2ggKGdldEludGVycG9sYXRpb25BcmdzTGVuZ3RoKGludGVycG9sYXRpb24pKSB7XG4gICAgY2FzZSAxOlxuICAgICAgcmV0dXJuIFIzLmNsYXNzTWFwO1xuICAgIGNhc2UgMzpcbiAgICAgIHJldHVybiBSMy5jbGFzc01hcEludGVycG9sYXRlMTtcbiAgICBjYXNlIDU6XG4gICAgICByZXR1cm4gUjMuY2xhc3NNYXBJbnRlcnBvbGF0ZTI7XG4gICAgY2FzZSA3OlxuICAgICAgcmV0dXJuIFIzLmNsYXNzTWFwSW50ZXJwb2xhdGUzO1xuICAgIGNhc2UgOTpcbiAgICAgIHJldHVybiBSMy5jbGFzc01hcEludGVycG9sYXRlNDtcbiAgICBjYXNlIDExOlxuICAgICAgcmV0dXJuIFIzLmNsYXNzTWFwSW50ZXJwb2xhdGU1O1xuICAgIGNhc2UgMTM6XG4gICAgICByZXR1cm4gUjMuY2xhc3NNYXBJbnRlcnBvbGF0ZTY7XG4gICAgY2FzZSAxNTpcbiAgICAgIHJldHVybiBSMy5jbGFzc01hcEludGVycG9sYXRlNztcbiAgICBjYXNlIDE3OlxuICAgICAgcmV0dXJuIFIzLmNsYXNzTWFwSW50ZXJwb2xhdGU4O1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gUjMuY2xhc3NNYXBJbnRlcnBvbGF0ZVY7XG4gIH1cbn1cblxuLyoqXG4gKiBHZXRzIHRoZSBpbnN0cnVjdGlvbiB0byBnZW5lcmF0ZSBmb3IgYW4gaW50ZXJwb2xhdGVkIHN0eWxlIHByb3AuXG4gKiBAcGFyYW0gaW50ZXJwb2xhdGlvbiBBbiBJbnRlcnBvbGF0aW9uIEFTVFxuICovXG5mdW5jdGlvbiBnZXRTdHlsZVByb3BJbnRlcnBvbGF0aW9uRXhwcmVzc2lvbihpbnRlcnBvbGF0aW9uOiBJbnRlcnBvbGF0aW9uKSB7XG4gIHN3aXRjaCAoZ2V0SW50ZXJwb2xhdGlvbkFyZ3NMZW5ndGgoaW50ZXJwb2xhdGlvbikpIHtcbiAgICBjYXNlIDE6XG4gICAgICByZXR1cm4gUjMuc3R5bGVQcm9wO1xuICAgIGNhc2UgMzpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZTE7XG4gICAgY2FzZSA1OlxuICAgICAgcmV0dXJuIFIzLnN0eWxlUHJvcEludGVycG9sYXRlMjtcbiAgICBjYXNlIDc6XG4gICAgICByZXR1cm4gUjMuc3R5bGVQcm9wSW50ZXJwb2xhdGUzO1xuICAgIGNhc2UgOTpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZTQ7XG4gICAgY2FzZSAxMTpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZTU7XG4gICAgY2FzZSAxMzpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZTY7XG4gICAgY2FzZSAxNTpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZTc7XG4gICAgY2FzZSAxNzpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZTg7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBSMy5zdHlsZVByb3BJbnRlcnBvbGF0ZVY7XG4gIH1cbn1cblxuZnVuY3Rpb24gbm9ybWFsaXplUHJvcE5hbWUocHJvcDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGh5cGhlbmF0ZShwcm9wKTtcbn1cbiJdfQ==