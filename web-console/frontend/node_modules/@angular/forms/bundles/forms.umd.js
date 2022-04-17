/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@angular/core'), require('@angular/platform-browser'), require('rxjs'), require('rxjs/operators')) :
    typeof define === 'function' && define.amd ? define('@angular/forms', ['exports', '@angular/core', '@angular/platform-browser', 'rxjs', 'rxjs/operators'], factory) :
    (global = global || self, factory((global.ng = global.ng || {}, global.ng.forms = {}), global.ng.core, global.ng.platformBrowser, global.rxjs, global.rxjs.operators));
}(this, function (exports, core, platformBrowser, rxjs, operators) { 'use strict';

    /*! *****************************************************************************
    Copyright (c) Microsoft Corporation. All rights reserved.
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use
    this file except in compliance with the License. You may obtain a copy of the
    License at http://www.apache.org/licenses/LICENSE-2.0

    THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
    WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
    MERCHANTABLITY OR NON-INFRINGEMENT.

    See the Apache Version 2.0 License for specific language governing permissions
    and limitations under the License.
    ***************************************************************************** */
    /* global Reflect, Promise */

    var extendStatics = function(d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };

    function __extends(d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    }

    var __assign = function() {
        __assign = Object.assign || function __assign(t) {
            for (var s, i = 1, n = arguments.length; i < n; i++) {
                s = arguments[i];
                for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p)) t[p] = s[p];
            }
            return t;
        };
        return __assign.apply(this, arguments);
    };

    function __rest(s, e) {
        var t = {};
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
            t[p] = s[p];
        if (s != null && typeof Object.getOwnPropertySymbols === "function")
            for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) if (e.indexOf(p[i]) < 0)
                t[p[i]] = s[p[i]];
        return t;
    }

    function __decorate(decorators, target, key, desc) {
        var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
        if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
        else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
        return c > 3 && r && Object.defineProperty(target, key, r), r;
    }

    function __param(paramIndex, decorator) {
        return function (target, key) { decorator(target, key, paramIndex); }
    }

    function __metadata(metadataKey, metadataValue) {
        if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(metadataKey, metadataValue);
    }

    function __awaiter(thisArg, _arguments, P, generator) {
        return new (P || (P = Promise))(function (resolve, reject) {
            function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
            function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
            function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
            step((generator = generator.apply(thisArg, _arguments || [])).next());
        });
    }

    function __generator(thisArg, body) {
        var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
        return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
        function verb(n) { return function (v) { return step([n, v]); }; }
        function step(op) {
            if (f) throw new TypeError("Generator is already executing.");
            while (_) try {
                if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
                if (y = 0, t) op = [op[0] & 2, t.value];
                switch (op[0]) {
                    case 0: case 1: t = op; break;
                    case 4: _.label++; return { value: op[1], done: false };
                    case 5: _.label++; y = op[1]; op = [0]; continue;
                    case 7: op = _.ops.pop(); _.trys.pop(); continue;
                    default:
                        if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                        if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                        if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                        if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                        if (t[2]) _.ops.pop();
                        _.trys.pop(); continue;
                }
                op = body.call(thisArg, _);
            } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
            if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
        }
    }

    function __exportStar(m, exports) {
        for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
    }

    function __values(o) {
        var m = typeof Symbol === "function" && o[Symbol.iterator], i = 0;
        if (m) return m.call(o);
        return {
            next: function () {
                if (o && i >= o.length) o = void 0;
                return { value: o && o[i++], done: !o };
            }
        };
    }

    function __read(o, n) {
        var m = typeof Symbol === "function" && o[Symbol.iterator];
        if (!m) return o;
        var i = m.call(o), r, ar = [], e;
        try {
            while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);
        }
        catch (error) { e = { error: error }; }
        finally {
            try {
                if (r && !r.done && (m = i["return"])) m.call(i);
            }
            finally { if (e) throw e.error; }
        }
        return ar;
    }

    function __spread() {
        for (var ar = [], i = 0; i < arguments.length; i++)
            ar = ar.concat(__read(arguments[i]));
        return ar;
    }

    function __await(v) {
        return this instanceof __await ? (this.v = v, this) : new __await(v);
    }

    function __asyncGenerator(thisArg, _arguments, generator) {
        if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
        var g = generator.apply(thisArg, _arguments || []), i, q = [];
        return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i;
        function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }
        function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
        function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
        function fulfill(value) { resume("next", value); }
        function reject(value) { resume("throw", value); }
        function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
    }

    function __asyncDelegator(o) {
        var i, p;
        return i = {}, verb("next"), verb("throw", function (e) { throw e; }), verb("return"), i[Symbol.iterator] = function () { return this; }, i;
        function verb(n, f) { i[n] = o[n] ? function (v) { return (p = !p) ? { value: __await(o[n](v)), done: n === "return" } : f ? f(v) : v; } : f; }
    }

    function __asyncValues(o) {
        if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
        var m = o[Symbol.asyncIterator], i;
        return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
        function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
        function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
    }

    function __makeTemplateObject(cooked, raw) {
        if (Object.defineProperty) { Object.defineProperty(cooked, "raw", { value: raw }); } else { cooked.raw = raw; }
        return cooked;
    };

    function __importStar(mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
        result.default = mod;
        return result;
    }

    function __importDefault(mod) {
        return (mod && mod.__esModule) ? mod : { default: mod };
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * Used to provide a `ControlValueAccessor` for form controls.
     *
     * See `DefaultValueAccessor` for how to implement one.
     *
     * @publicApi
     */
    var NG_VALUE_ACCESSOR = new core.InjectionToken('NgValueAccessor');

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var CHECKBOX_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return CheckboxControlValueAccessor; }),
        multi: true,
    };
    /**
     * @description
     * A `ControlValueAccessor` for writing a value and listening to changes on a checkbox input
     * element.
     *
     * @usageNotes
     *
     * ### Using a checkbox with a reactive form.
     *
     * The following example shows how to use a checkbox with a reactive form.
     *
     * ```ts
     * const rememberLoginControl = new FormControl();
     * ```
     *
     * ```
     * <input type="checkbox" [formControl]="rememberLoginControl">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var CheckboxControlValueAccessor = /** @class */ (function () {
        function CheckboxControlValueAccessor(_renderer, _elementRef) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            /**
             * @description
             * The registered callback function called when a change event occurs on the input element.
             */
            this.onChange = function (_) { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
        }
        /**
         * Sets the "checked" property on the input element.
         *
         * @param value The checked value
         */
        CheckboxControlValueAccessor.prototype.writeValue = function (value) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'checked', value);
        };
        /**
         * @description
         * Registers a function called when the control value changes.
         *
         * @param fn The callback function
         */
        CheckboxControlValueAccessor.prototype.registerOnChange = function (fn) { this.onChange = fn; };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        CheckboxControlValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the input element.
         *
         * @param isDisabled The disabled value
         */
        CheckboxControlValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        CheckboxControlValueAccessor = __decorate([
            core.Directive({
                selector: 'input[type=checkbox][formControlName],input[type=checkbox][formControl],input[type=checkbox][ngModel]',
                host: { '(change)': 'onChange($event.target.checked)', '(blur)': 'onTouched()' },
                providers: [CHECKBOX_VALUE_ACCESSOR]
            }),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef])
        ], CheckboxControlValueAccessor);
        return CheckboxControlValueAccessor;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var DEFAULT_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return DefaultValueAccessor; }),
        multi: true
    };
    /**
     * We must check whether the agent is Android because composition events
     * behave differently between iOS and Android.
     */
    function _isAndroid() {
        var userAgent = platformBrowser.ɵgetDOM() ? platformBrowser.ɵgetDOM().getUserAgent() : '';
        return /android (\d+)/.test(userAgent.toLowerCase());
    }
    /**
     * @description
     * Provide this token to control if form directives buffer IME input until
     * the "compositionend" event occurs.
     * @publicApi
     */
    var COMPOSITION_BUFFER_MODE = new core.InjectionToken('CompositionEventMode');
    /**
     * @description
     * The default `ControlValueAccessor` for writing a value and listening to changes on input
     * elements. The accessor is used by the `FormControlDirective`, `FormControlName`, and
     * `NgModel` directives.
     *
     * @usageNotes
     *
     * ### Using the default value accessor
     *
     * The following example shows how to use an input element that activates the default value accessor
     * (in this case, a text field).
     *
     * ```ts
     * const firstNameControl = new FormControl();
     * ```
     *
     * ```
     * <input type="text" [formControl]="firstNameControl">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var DefaultValueAccessor = /** @class */ (function () {
        function DefaultValueAccessor(_renderer, _elementRef, _compositionMode) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            this._compositionMode = _compositionMode;
            /**
             * @description
             * The registered callback function called when an input event occurs on the input element.
             */
            this.onChange = function (_) { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
            /** Whether the user is creating a composition string (IME events). */
            this._composing = false;
            if (this._compositionMode == null) {
                this._compositionMode = !_isAndroid();
            }
        }
        /**
         * Sets the "value" property on the input element.
         *
         * @param value The checked value
         */
        DefaultValueAccessor.prototype.writeValue = function (value) {
            var normalizedValue = value == null ? '' : value;
            this._renderer.setProperty(this._elementRef.nativeElement, 'value', normalizedValue);
        };
        /**
         * @description
         * Registers a function called when the control value changes.
         *
         * @param fn The callback function
         */
        DefaultValueAccessor.prototype.registerOnChange = function (fn) { this.onChange = fn; };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        DefaultValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the input element.
         *
         * @param isDisabled The disabled value
         */
        DefaultValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        /** @internal */
        DefaultValueAccessor.prototype._handleInput = function (value) {
            if (!this._compositionMode || (this._compositionMode && !this._composing)) {
                this.onChange(value);
            }
        };
        /** @internal */
        DefaultValueAccessor.prototype._compositionStart = function () { this._composing = true; };
        /** @internal */
        DefaultValueAccessor.prototype._compositionEnd = function (value) {
            this._composing = false;
            this._compositionMode && this.onChange(value);
        };
        DefaultValueAccessor = __decorate([
            core.Directive({
                selector: 'input:not([type=checkbox])[formControlName],textarea[formControlName],input:not([type=checkbox])[formControl],textarea[formControl],input:not([type=checkbox])[ngModel],textarea[ngModel],[ngDefaultControl]',
                // TODO: vsavkin replace the above selector with the one below it once
                // https://github.com/angular/angular/issues/3011 is implemented
                // selector: '[ngModel],[formControl],[formControlName]',
                host: {
                    '(input)': '$any(this)._handleInput($event.target.value)',
                    '(blur)': 'onTouched()',
                    '(compositionstart)': '$any(this)._compositionStart()',
                    '(compositionend)': '$any(this)._compositionEnd($event.target.value)'
                },
                providers: [DEFAULT_VALUE_ACCESSOR]
            }),
            __param(2, core.Optional()), __param(2, core.Inject(COMPOSITION_BUFFER_MODE)),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef, Boolean])
        ], DefaultValueAccessor);
        return DefaultValueAccessor;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @description
     * Base class for control directives.
     *
     * This class is only used internally in the `ReactiveFormsModule` and the `FormsModule`.
     *
     * @publicApi
     */
    var AbstractControlDirective = /** @class */ (function () {
        function AbstractControlDirective() {
        }
        Object.defineProperty(AbstractControlDirective.prototype, "value", {
            /**
             * @description
             * Reports the value of the control if it is present, otherwise null.
             */
            get: function () { return this.control ? this.control.value : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "valid", {
            /**
             * @description
             * Reports whether the control is valid. A control is considered valid if no
             * validation errors exist with the current value.
             * If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.valid : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "invalid", {
            /**
             * @description
             * Reports whether the control is invalid, meaning that an error exists in the input value.
             * If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.invalid : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "pending", {
            /**
             * @description
             * Reports whether a control is pending, meaning that that async validation is occurring and
             * errors are not yet available for the input value. If the control is not present, null is
             * returned.
             */
            get: function () { return this.control ? this.control.pending : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "disabled", {
            /**
             * @description
             * Reports whether the control is disabled, meaning that the control is disabled
             * in the UI and is exempt from validation checks and excluded from aggregate
             * values of ancestor controls. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.disabled : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "enabled", {
            /**
             * @description
             * Reports whether the control is enabled, meaning that the control is included in ancestor
             * calculations of validity or value. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.enabled : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "errors", {
            /**
             * @description
             * Reports the control's validation errors. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.errors : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "pristine", {
            /**
             * @description
             * Reports whether the control is pristine, meaning that the user has not yet changed
             * the value in the UI. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.pristine : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "dirty", {
            /**
             * @description
             * Reports whether the control is dirty, meaning that the user has changed
             * the value in the UI. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.dirty : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "touched", {
            /**
             * @description
             * Reports whether the control is touched, meaning that the user has triggered
             * a `blur` event on it. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.touched : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "status", {
            /**
             * @description
             * Reports the validation status of the control. Possible values include:
             * 'VALID', 'INVALID', 'DISABLED', and 'PENDING'.
             * If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.status : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "untouched", {
            /**
             * @description
             * Reports whether the control is untouched, meaning that the user has not yet triggered
             * a `blur` event on it. If the control is not present, null is returned.
             */
            get: function () { return this.control ? this.control.untouched : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "statusChanges", {
            /**
             * @description
             * Returns a multicasting observable that emits a validation status whenever it is
             * calculated for the control. If the control is not present, null is returned.
             */
            get: function () {
                return this.control ? this.control.statusChanges : null;
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "valueChanges", {
            /**
             * @description
             * Returns a multicasting observable of value changes for the control that emits every time the
             * value of the control changes in the UI or programmatically.
             * If the control is not present, null is returned.
             */
            get: function () {
                return this.control ? this.control.valueChanges : null;
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlDirective.prototype, "path", {
            /**
             * @description
             * Returns an array that represents the path from the top-level form to this control.
             * Each index is the string name of the control on that level.
             */
            get: function () { return null; },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Resets the control with the provided value if the control is present.
         */
        AbstractControlDirective.prototype.reset = function (value) {
            if (value === void 0) { value = undefined; }
            if (this.control)
                this.control.reset(value);
        };
        /**
         * @description
         * Reports whether the control with the given path has the error specified.
         *
         * @param errorCode The code of the error to check
         * @param path A list of control names that designates how to move from the current control
         * to the control that should be queried for errors.
         *
         * @usageNotes
         * For example, for the following `FormGroup`:
         *
         * ```
         * form = new FormGroup({
         *   address: new FormGroup({ street: new FormControl() })
         * });
         * ```
         *
         * The path to the 'street' control from the root form would be 'address' -> 'street'.
         *
         * It can be provided to this method in one of two formats:
         *
         * 1. An array of string control names, e.g. `['address', 'street']`
         * 1. A period-delimited list of control names in one string, e.g. `'address.street'`
         *
         * If no path is given, this method checks for the error on the current control.
         *
         * @returns whether the given error is present in the control at the given path.
         *
         * If the control is not present, false is returned.
         */
        AbstractControlDirective.prototype.hasError = function (errorCode, path) {
            return this.control ? this.control.hasError(errorCode, path) : false;
        };
        /**
         * @description
         * Reports error data for the control with the given path.
         *
         * @param errorCode The code of the error to check
         * @param path A list of control names that designates how to move from the current control
         * to the control that should be queried for errors.
         *
         * @usageNotes
         * For example, for the following `FormGroup`:
         *
         * ```
         * form = new FormGroup({
         *   address: new FormGroup({ street: new FormControl() })
         * });
         * ```
         *
         * The path to the 'street' control from the root form would be 'address' -> 'street'.
         *
         * It can be provided to this method in one of two formats:
         *
         * 1. An array of string control names, e.g. `['address', 'street']`
         * 1. A period-delimited list of control names in one string, e.g. `'address.street'`
         *
         * @returns error data for that particular error. If the control or error is not present,
         * null is returned.
         */
        AbstractControlDirective.prototype.getError = function (errorCode, path) {
            return this.control ? this.control.getError(errorCode, path) : null;
        };
        return AbstractControlDirective;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @description
     * A base class for directives that contain multiple registered instances of `NgControl`.
     * Only used by the forms module.
     *
     * @publicApi
     */
    var ControlContainer = /** @class */ (function (_super) {
        __extends(ControlContainer, _super);
        function ControlContainer() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        Object.defineProperty(ControlContainer.prototype, "formDirective", {
            /**
             * @description
             * The top-level form directive for the control.
             */
            get: function () { return null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(ControlContainer.prototype, "path", {
            /**
             * @description
             * The path to this group.
             */
            get: function () { return null; },
            enumerable: true,
            configurable: true
        });
        return ControlContainer;
    }(AbstractControlDirective));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    function unimplemented() {
        throw new Error('unimplemented');
    }
    /**
     * @description
     * A base class that all control `FormControl`-based directives extend. It binds a `FormControl`
     * object to a DOM element.
     *
     * @publicApi
     */
    var NgControl = /** @class */ (function (_super) {
        __extends(NgControl, _super);
        function NgControl() {
            var _this = _super !== null && _super.apply(this, arguments) || this;
            /**
             * @description
             * The parent form for the control.
             *
             * @internal
             */
            _this._parent = null;
            /**
             * @description
             * The name for the control
             */
            _this.name = null;
            /**
             * @description
             * The value accessor for the control
             */
            _this.valueAccessor = null;
            /**
             * @description
             * The uncomposed array of synchronous validators for the control
             *
             * @internal
             */
            _this._rawValidators = [];
            /**
             * @description
             * The uncomposed array of async validators for the control
             *
             * @internal
             */
            _this._rawAsyncValidators = [];
            return _this;
        }
        Object.defineProperty(NgControl.prototype, "validator", {
            /**
             * @description
             * The registered synchronous validator function for the control
             *
             * @throws An exception that this method is not implemented
             */
            get: function () { return unimplemented(); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgControl.prototype, "asyncValidator", {
            /**
             * @description
             * The registered async validator function for the control
             *
             * @throws An exception that this method is not implemented
             */
            get: function () { return unimplemented(); },
            enumerable: true,
            configurable: true
        });
        return NgControl;
    }(AbstractControlDirective));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var AbstractControlStatus = /** @class */ (function () {
        function AbstractControlStatus(cd) {
            this._cd = cd;
        }
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassUntouched", {
            get: function () { return this._cd.control ? this._cd.control.untouched : false; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassTouched", {
            get: function () { return this._cd.control ? this._cd.control.touched : false; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassPristine", {
            get: function () { return this._cd.control ? this._cd.control.pristine : false; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassDirty", {
            get: function () { return this._cd.control ? this._cd.control.dirty : false; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassValid", {
            get: function () { return this._cd.control ? this._cd.control.valid : false; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassInvalid", {
            get: function () { return this._cd.control ? this._cd.control.invalid : false; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControlStatus.prototype, "ngClassPending", {
            get: function () { return this._cd.control ? this._cd.control.pending : false; },
            enumerable: true,
            configurable: true
        });
        return AbstractControlStatus;
    }());
    var ngControlStatusHost = {
        '[class.ng-untouched]': 'ngClassUntouched',
        '[class.ng-touched]': 'ngClassTouched',
        '[class.ng-pristine]': 'ngClassPristine',
        '[class.ng-dirty]': 'ngClassDirty',
        '[class.ng-valid]': 'ngClassValid',
        '[class.ng-invalid]': 'ngClassInvalid',
        '[class.ng-pending]': 'ngClassPending',
    };
    /**
     * @description
     * Directive automatically applied to Angular form controls that sets CSS classes
     * based on control status.
     *
     * @usageNotes
     *
     * ### CSS classes applied
     *
     * The following classes are applied as the properties become true:
     *
     * * ng-valid
     * * ng-invalid
     * * ng-pending
     * * ng-pristine
     * * ng-dirty
     * * ng-untouched
     * * ng-touched
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var NgControlStatus = /** @class */ (function (_super) {
        __extends(NgControlStatus, _super);
        function NgControlStatus(cd) {
            return _super.call(this, cd) || this;
        }
        NgControlStatus = __decorate([
            core.Directive({ selector: '[formControlName],[ngModel],[formControl]', host: ngControlStatusHost }),
            __param(0, core.Self()),
            __metadata("design:paramtypes", [NgControl])
        ], NgControlStatus);
        return NgControlStatus;
    }(AbstractControlStatus));
    /**
     * @description
     * Directive automatically applied to Angular form groups that sets CSS classes
     * based on control status (valid/invalid/dirty/etc).
     *
     * @see `NgControlStatus`
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var NgControlStatusGroup = /** @class */ (function (_super) {
        __extends(NgControlStatusGroup, _super);
        function NgControlStatusGroup(cd) {
            return _super.call(this, cd) || this;
        }
        NgControlStatusGroup = __decorate([
            core.Directive({
                selector: '[formGroupName],[formArrayName],[ngModelGroup],[formGroup],form:not([ngNoForm]),[ngForm]',
                host: ngControlStatusHost
            }),
            __param(0, core.Self()),
            __metadata("design:paramtypes", [ControlContainer])
        ], NgControlStatusGroup);
        return NgControlStatusGroup;
    }(AbstractControlStatus));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    function isEmptyInputValue(value) {
        // we don't check for string here so it also works with arrays
        return value == null || value.length === 0;
    }
    /**
     * @description
     * An `InjectionToken` for registering additional synchronous validators used with `AbstractControl`s.
     *
     * @see `NG_ASYNC_VALIDATORS`
     *
     * @usageNotes
     *
     * ### Providing a custom validator
     *
     * The following example registers a custom validator directive. Adding the validator to the
     * existing collection of validators requires the `multi: true` option.
     *
     * ```typescript
     * @Directive({
     *   selector: '[customValidator]',
     *   providers: [{provide: NG_VALIDATORS, useExisting: CustomValidatorDirective, multi: true}]
     * })
     * class CustomValidatorDirective implements Validator {
     *   validate(control: AbstractControl): ValidationErrors | null {
     *     return { 'custom': true };
     *   }
     * }
     * ```
     *
     * @publicApi
     */
    var NG_VALIDATORS = new core.InjectionToken('NgValidators');
    /**
     * @description
     * An `InjectionToken` for registering additional asynchronous validators used with `AbstractControl`s.
     *
     * @see `NG_VALIDATORS`
     *
     * @publicApi
     */
    var NG_ASYNC_VALIDATORS = new core.InjectionToken('NgAsyncValidators');
    /**
     * A regular expression that matches valid e-mail addresses.
     *
     * At a high level, this regexp matches e-mail addresses of the format `local-part@tld`, where:
     * - `local-part` consists of one or more of the allowed characters (alphanumeric and some
     *   punctuation symbols).
     * - `local-part` cannot begin or end with a period (`.`).
     * - `local-part` cannot be longer than 64 characters.
     * - `tld` consists of one or more `labels` separated by periods (`.`). For example `localhost` or
     *   `foo.com`.
     * - A `label` consists of one or more of the allowed characters (alphanumeric, dashes (`-`) and
     *   periods (`.`)).
     * - A `label` cannot begin or end with a dash (`-`) or a period (`.`).
     * - A `label` cannot be longer than 63 characters.
     * - The whole address cannot be longer than 254 characters.
     *
     * ## Implementation background
     *
     * This regexp was ported over from AngularJS (see there for git history):
     * https://github.com/angular/angular.js/blob/c133ef836/src/ng/directive/input.js#L27
     * It is based on the
     * [WHATWG version](https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address) with
     * some enhancements to incorporate more RFC rules (such as rules related to domain names and the
     * lengths of different parts of the address). The main differences from the WHATWG version are:
     *   - Disallow `local-part` to begin or end with a period (`.`).
     *   - Disallow `local-part` length to exceed 64 characters.
     *   - Disallow total address length to exceed 254 characters.
     *
     * See [this commit](https://github.com/angular/angular.js/commit/f3f5cf72e) for more details.
     */
    var EMAIL_REGEXP = /^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
    /**
     * @description
     * Provides a set of built-in validators that can be used by form controls.
     *
     * A validator is a function that processes a `FormControl` or collection of
     * controls and returns an error map or null. A null map means that validation has passed.
     *
     * @see [Form Validation](/guide/form-validation)
     *
     * @publicApi
     */
    var Validators = /** @class */ (function () {
        function Validators() {
        }
        /**
         * @description
         * Validator that requires the control's value to be greater than or equal to the provided number.
         * The validator exists only as a function and not as a directive.
         *
         * @usageNotes
         *
         * ### Validate against a minimum of 3
         *
         * ```typescript
         * const control = new FormControl(2, Validators.min(3));
         *
         * console.log(control.errors); // {min: {min: 3, actual: 2}}
         * ```
         *
         * @returns A validator function that returns an error map with the
         * `min` property if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.min = function (min) {
            return function (control) {
                if (isEmptyInputValue(control.value) || isEmptyInputValue(min)) {
                    return null; // don't validate empty values to allow optional controls
                }
                var value = parseFloat(control.value);
                // Controls with NaN values after parsing should be treated as not having a
                // minimum, per the HTML forms spec: https://www.w3.org/TR/html5/forms.html#attr-input-min
                return !isNaN(value) && value < min ? { 'min': { 'min': min, 'actual': control.value } } : null;
            };
        };
        /**
         * @description
         * Validator that requires the control's value to be less than or equal to the provided number.
         * The validator exists only as a function and not as a directive.
         *
         * @usageNotes
         *
         * ### Validate against a maximum of 15
         *
         * ```typescript
         * const control = new FormControl(16, Validators.max(15));
         *
         * console.log(control.errors); // {max: {max: 15, actual: 16}}
         * ```
         *
         * @returns A validator function that returns an error map with the
         * `max` property if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.max = function (max) {
            return function (control) {
                if (isEmptyInputValue(control.value) || isEmptyInputValue(max)) {
                    return null; // don't validate empty values to allow optional controls
                }
                var value = parseFloat(control.value);
                // Controls with NaN values after parsing should be treated as not having a
                // maximum, per the HTML forms spec: https://www.w3.org/TR/html5/forms.html#attr-input-max
                return !isNaN(value) && value > max ? { 'max': { 'max': max, 'actual': control.value } } : null;
            };
        };
        /**
         * @description
         * Validator that requires the control have a non-empty value.
         *
         * @usageNotes
         *
         * ### Validate that the field is non-empty
         *
         * ```typescript
         * const control = new FormControl('', Validators.required);
         *
         * console.log(control.errors); // {required: true}
         * ```
         *
         * @returns An error map with the `required` property
         * if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.required = function (control) {
            return isEmptyInputValue(control.value) ? { 'required': true } : null;
        };
        /**
         * @description
         * Validator that requires the control's value be true. This validator is commonly
         * used for required checkboxes.
         *
         * @usageNotes
         *
         * ### Validate that the field value is true
         *
         * ```typescript
         * const control = new FormControl('', Validators.requiredTrue);
         *
         * console.log(control.errors); // {required: true}
         * ```
         *
         * @returns An error map that contains the `required` property
         * set to `true` if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.requiredTrue = function (control) {
            return control.value === true ? null : { 'required': true };
        };
        /**
         * @description
         * Validator that requires the control's value pass an email validation test.
         *
         * Tests the value using a [regular expression](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions)
         * pattern suitable for common usecases. The pattern is based on the definition of a valid email
         * address in the [WHATWG HTML specification](https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address)
         * with some enhancements to incorporate more RFC rules (such as rules related to domain names and
         * the lengths of different parts of the address).
         *
         * The differences from the WHATWG version include:
         * - Disallow `local-part` (the part before the `@` symbol) to begin or end with a period (`.`).
         * - Disallow `local-part` to be longer than 64 characters.
         * - Disallow the whole address to be longer than 254 characters.
         *
         * If this pattern does not satisfy your business needs, you can use `Validators.pattern()` to
         * validate the value against a different pattern.
         *
         * @usageNotes
         *
         * ### Validate that the field matches a valid email pattern
         *
         * ```typescript
         * const control = new FormControl('bad@', Validators.email);
         *
         * console.log(control.errors); // {email: true}
         * ```
         *
         * @returns An error map with the `email` property
         * if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.email = function (control) {
            if (isEmptyInputValue(control.value)) {
                return null; // don't validate empty values to allow optional controls
            }
            return EMAIL_REGEXP.test(control.value) ? null : { 'email': true };
        };
        /**
         * @description
         * Validator that requires the length of the control's value to be greater than or equal
         * to the provided minimum length. This validator is also provided by default if you use the
         * the HTML5 `minlength` attribute.
         *
         * @usageNotes
         *
         * ### Validate that the field has a minimum of 3 characters
         *
         * ```typescript
         * const control = new FormControl('ng', Validators.minLength(3));
         *
         * console.log(control.errors); // {minlength: {requiredLength: 3, actualLength: 2}}
         * ```
         *
         * ```html
         * <input minlength="5">
         * ```
         *
         * @returns A validator function that returns an error map with the
         * `minlength` if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.minLength = function (minLength) {
            return function (control) {
                if (isEmptyInputValue(control.value)) {
                    return null; // don't validate empty values to allow optional controls
                }
                var length = control.value ? control.value.length : 0;
                return length < minLength ?
                    { 'minlength': { 'requiredLength': minLength, 'actualLength': length } } :
                    null;
            };
        };
        /**
         * @description
         * Validator that requires the length of the control's value to be less than or equal
         * to the provided maximum length. This validator is also provided by default if you use the
         * the HTML5 `maxlength` attribute.
         *
         * @usageNotes
         *
         * ### Validate that the field has maximum of 5 characters
         *
         * ```typescript
         * const control = new FormControl('Angular', Validators.maxLength(5));
         *
         * console.log(control.errors); // {maxlength: {requiredLength: 5, actualLength: 7}}
         * ```
         *
         * ```html
         * <input maxlength="5">
         * ```
         *
         * @returns A validator function that returns an error map with the
         * `maxlength` property if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.maxLength = function (maxLength) {
            return function (control) {
                var length = control.value ? control.value.length : 0;
                return length > maxLength ?
                    { 'maxlength': { 'requiredLength': maxLength, 'actualLength': length } } :
                    null;
            };
        };
        /**
         * @description
         * Validator that requires the control's value to match a regex pattern. This validator is also
         * provided by default if you use the HTML5 `pattern` attribute.
         *
         * @usageNotes
         *
         * ### Validate that the field only contains letters or spaces
         *
         * ```typescript
         * const control = new FormControl('1', Validators.pattern('[a-zA-Z ]*'));
         *
         * console.log(control.errors); // {pattern: {requiredPattern: '^[a-zA-Z ]*$', actualValue: '1'}}
         * ```
         *
         * ```html
         * <input pattern="[a-zA-Z ]*">
         * ```
         *
         * @param pattern A regular expression to be used as is to test the values, or a string.
         * If a string is passed, the `^` character is prepended and the `$` character is
         * appended to the provided string (if not already present), and the resulting regular
         * expression is used to test the values.
         *
         * @returns A validator function that returns an error map with the
         * `pattern` property if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.pattern = function (pattern) {
            if (!pattern)
                return Validators.nullValidator;
            var regex;
            var regexStr;
            if (typeof pattern === 'string') {
                regexStr = '';
                if (pattern.charAt(0) !== '^')
                    regexStr += '^';
                regexStr += pattern;
                if (pattern.charAt(pattern.length - 1) !== '$')
                    regexStr += '$';
                regex = new RegExp(regexStr);
            }
            else {
                regexStr = pattern.toString();
                regex = pattern;
            }
            return function (control) {
                if (isEmptyInputValue(control.value)) {
                    return null; // don't validate empty values to allow optional controls
                }
                var value = control.value;
                return regex.test(value) ? null :
                    { 'pattern': { 'requiredPattern': regexStr, 'actualValue': value } };
            };
        };
        /**
         * @description
         * Validator that performs no operation.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.nullValidator = function (control) { return null; };
        Validators.compose = function (validators) {
            if (!validators)
                return null;
            var presentValidators = validators.filter(isPresent);
            if (presentValidators.length == 0)
                return null;
            return function (control) {
                return _mergeErrors(_executeValidators(control, presentValidators));
            };
        };
        /**
         * @description
         * Compose multiple async validators into a single function that returns the union
         * of the individual error objects for the provided control.
         *
         * @returns A validator function that returns an error map with the
         * merged error objects of the async validators if the validation check fails, otherwise `null`.
         *
         * @see `updateValueAndValidity()`
         *
         */
        Validators.composeAsync = function (validators) {
            if (!validators)
                return null;
            var presentValidators = validators.filter(isPresent);
            if (presentValidators.length == 0)
                return null;
            return function (control) {
                var observables = _executeAsyncValidators(control, presentValidators).map(toObservable);
                return rxjs.forkJoin(observables).pipe(operators.map(_mergeErrors));
            };
        };
        return Validators;
    }());
    function isPresent(o) {
        return o != null;
    }
    function toObservable(r) {
        var obs = core.ɵisPromise(r) ? rxjs.from(r) : r;
        if (!(core.ɵisObservable(obs))) {
            throw new Error("Expected validator to return Promise or Observable.");
        }
        return obs;
    }
    function _executeValidators(control, validators) {
        return validators.map(function (v) { return v(control); });
    }
    function _executeAsyncValidators(control, validators) {
        return validators.map(function (v) { return v(control); });
    }
    function _mergeErrors(arrayOfErrors) {
        var res = arrayOfErrors.reduce(function (res, errors) {
            return errors != null ? __assign({}, res, errors) : res;
        }, {});
        return Object.keys(res).length === 0 ? null : res;
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    function normalizeValidator(validator) {
        if (validator.validate) {
            return function (c) { return validator.validate(c); };
        }
        else {
            return validator;
        }
    }
    function normalizeAsyncValidator(validator) {
        if (validator.validate) {
            return function (c) { return validator.validate(c); };
        }
        else {
            return validator;
        }
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var NUMBER_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return NumberValueAccessor; }),
        multi: true
    };
    /**
     * @description
     * The `ControlValueAccessor` for writing a number value and listening to number input changes.
     * The value accessor is used by the `FormControlDirective`, `FormControlName`, and  `NgModel`
     * directives.
     *
     * @usageNotes
     *
     * ### Using a number input with a reactive form.
     *
     * The following example shows how to use a number input with a reactive form.
     *
     * ```ts
     * const totalCountControl = new FormControl();
     * ```
     *
     * ```
     * <input type="number" [formControl]="totalCountControl">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var NumberValueAccessor = /** @class */ (function () {
        function NumberValueAccessor(_renderer, _elementRef) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            /**
             * @description
             * The registered callback function called when a change or input event occurs on the input
             * element.
             */
            this.onChange = function (_) { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
        }
        /**
         * Sets the "value" property on the input element.
         *
         * @param value The checked value
         */
        NumberValueAccessor.prototype.writeValue = function (value) {
            // The value needs to be normalized for IE9, otherwise it is set to 'null' when null
            var normalizedValue = value == null ? '' : value;
            this._renderer.setProperty(this._elementRef.nativeElement, 'value', normalizedValue);
        };
        /**
         * @description
         * Registers a function called when the control value changes.
         *
         * @param fn The callback function
         */
        NumberValueAccessor.prototype.registerOnChange = function (fn) {
            this.onChange = function (value) { fn(value == '' ? null : parseFloat(value)); };
        };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        NumberValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the input element.
         *
         * @param isDisabled The disabled value
         */
        NumberValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        NumberValueAccessor = __decorate([
            core.Directive({
                selector: 'input[type=number][formControlName],input[type=number][formControl],input[type=number][ngModel]',
                host: {
                    '(change)': 'onChange($event.target.value)',
                    '(input)': 'onChange($event.target.value)',
                    '(blur)': 'onTouched()'
                },
                providers: [NUMBER_VALUE_ACCESSOR]
            }),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef])
        ], NumberValueAccessor);
        return NumberValueAccessor;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var RADIO_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return RadioControlValueAccessor; }),
        multi: true
    };
    /**
     * @description
     * Class used by Angular to track radio buttons. For internal use only.
     */
    var RadioControlRegistry = /** @class */ (function () {
        function RadioControlRegistry() {
            this._accessors = [];
        }
        /**
         * @description
         * Adds a control to the internal registry. For internal use only.
         */
        RadioControlRegistry.prototype.add = function (control, accessor) {
            this._accessors.push([control, accessor]);
        };
        /**
         * @description
         * Removes a control from the internal registry. For internal use only.
         */
        RadioControlRegistry.prototype.remove = function (accessor) {
            for (var i = this._accessors.length - 1; i >= 0; --i) {
                if (this._accessors[i][1] === accessor) {
                    this._accessors.splice(i, 1);
                    return;
                }
            }
        };
        /**
         * @description
         * Selects a radio button. For internal use only.
         */
        RadioControlRegistry.prototype.select = function (accessor) {
            var _this = this;
            this._accessors.forEach(function (c) {
                if (_this._isSameGroup(c, accessor) && c[1] !== accessor) {
                    c[1].fireUncheck(accessor.value);
                }
            });
        };
        RadioControlRegistry.prototype._isSameGroup = function (controlPair, accessor) {
            if (!controlPair[0].control)
                return false;
            return controlPair[0]._parent === accessor._control._parent &&
                controlPair[1].name === accessor.name;
        };
        RadioControlRegistry = __decorate([
            core.Injectable()
        ], RadioControlRegistry);
        return RadioControlRegistry;
    }());
    /**
     * @description
     * The `ControlValueAccessor` for writing radio control values and listening to radio control
     * changes. The value accessor is used by the `FormControlDirective`, `FormControlName`, and
     * `NgModel` directives.
     *
     * @usageNotes
     *
     * ### Using radio buttons with reactive form directives
     *
     * The follow example shows how to use radio buttons in a reactive form. When using radio buttons in
     * a reactive form, radio buttons in the same group should have the same `formControlName`.
     * Providing a `name` attribute is optional.
     *
     * {@example forms/ts/reactiveRadioButtons/reactive_radio_button_example.ts region='Reactive'}
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var RadioControlValueAccessor = /** @class */ (function () {
        function RadioControlValueAccessor(_renderer, _elementRef, _registry, _injector) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            this._registry = _registry;
            this._injector = _injector;
            /**
             * @description
             * The registered callback function called when a change event occurs on the input element.
             */
            this.onChange = function () { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
        }
        /**
         * @description
         * A lifecycle method called when the directive is initialized. For internal use only.
         */
        RadioControlValueAccessor.prototype.ngOnInit = function () {
            this._control = this._injector.get(NgControl);
            this._checkName();
            this._registry.add(this._control, this);
        };
        /**
         * @description
         * Lifecycle method called before the directive's instance is destroyed. For internal use only.
         */
        RadioControlValueAccessor.prototype.ngOnDestroy = function () { this._registry.remove(this); };
        /**
         * @description
         * Sets the "checked" property value on the radio input element.
         *
         * @param value The checked value
         */
        RadioControlValueAccessor.prototype.writeValue = function (value) {
            this._state = value === this.value;
            this._renderer.setProperty(this._elementRef.nativeElement, 'checked', this._state);
        };
        /**
         * @description
         * Registers a function called when the control value changes.
         *
         * @param fn The callback function
         */
        RadioControlValueAccessor.prototype.registerOnChange = function (fn) {
            var _this = this;
            this._fn = fn;
            this.onChange = function () {
                fn(_this.value);
                _this._registry.select(_this);
            };
        };
        /**
         * Sets the "value" on the radio input element and unchecks it.
         *
         * @param value
         */
        RadioControlValueAccessor.prototype.fireUncheck = function (value) { this.writeValue(value); };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        RadioControlValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the input element.
         *
         * @param isDisabled The disabled value
         */
        RadioControlValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        RadioControlValueAccessor.prototype._checkName = function () {
            if (this.name && this.formControlName && this.name !== this.formControlName) {
                this._throwNameError();
            }
            if (!this.name && this.formControlName)
                this.name = this.formControlName;
        };
        RadioControlValueAccessor.prototype._throwNameError = function () {
            throw new Error("\n      If you define both a name and a formControlName attribute on your radio button, their values\n      must match. Ex: <input type=\"radio\" formControlName=\"food\" name=\"food\">\n    ");
        };
        __decorate([
            core.Input(),
            __metadata("design:type", String)
        ], RadioControlValueAccessor.prototype, "name", void 0);
        __decorate([
            core.Input(),
            __metadata("design:type", String)
        ], RadioControlValueAccessor.prototype, "formControlName", void 0);
        __decorate([
            core.Input(),
            __metadata("design:type", Object)
        ], RadioControlValueAccessor.prototype, "value", void 0);
        RadioControlValueAccessor = __decorate([
            core.Directive({
                selector: 'input[type=radio][formControlName],input[type=radio][formControl],input[type=radio][ngModel]',
                host: { '(change)': 'onChange()', '(blur)': 'onTouched()' },
                providers: [RADIO_VALUE_ACCESSOR]
            }),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef,
                RadioControlRegistry, core.Injector])
        ], RadioControlValueAccessor);
        return RadioControlValueAccessor;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var RANGE_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return RangeValueAccessor; }),
        multi: true
    };
    /**
     * @description
     * The `ControlValueAccessor` for writing a range value and listening to range input changes.
     * The value accessor is used by the `FormControlDirective`, `FormControlName`, and  `NgModel`
     * directives.
     *
     * @usageNotes
     *
     * ### Using a range input with a reactive form
     *
     * The following example shows how to use a range input with a reactive form.
     *
     * ```ts
     * const ageControl = new FormControl();
     * ```
     *
     * ```
     * <input type="range" [formControl]="ageControl">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var RangeValueAccessor = /** @class */ (function () {
        function RangeValueAccessor(_renderer, _elementRef) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            /**
             * @description
             * The registered callback function called when a change or input event occurs on the input
             * element.
             */
            this.onChange = function (_) { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
        }
        /**
         * Sets the "value" property on the input element.
         *
         * @param value The checked value
         */
        RangeValueAccessor.prototype.writeValue = function (value) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'value', parseFloat(value));
        };
        /**
         * @description
         * Registers a function called when the control value changes.
         *
         * @param fn The callback function
         */
        RangeValueAccessor.prototype.registerOnChange = function (fn) {
            this.onChange = function (value) { fn(value == '' ? null : parseFloat(value)); };
        };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        RangeValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the range input element.
         *
         * @param isDisabled The disabled value
         */
        RangeValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        RangeValueAccessor = __decorate([
            core.Directive({
                selector: 'input[type=range][formControlName],input[type=range][formControl],input[type=range][ngModel]',
                host: {
                    '(change)': 'onChange($event.target.value)',
                    '(input)': 'onChange($event.target.value)',
                    '(blur)': 'onTouched()'
                },
                providers: [RANGE_VALUE_ACCESSOR]
            }),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef])
        ], RangeValueAccessor);
        return RangeValueAccessor;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var FormErrorExamples = {
        formControlName: "\n    <div [formGroup]=\"myGroup\">\n      <input formControlName=\"firstName\">\n    </div>\n\n    In your class:\n\n    this.myGroup = new FormGroup({\n       firstName: new FormControl()\n    });",
        formGroupName: "\n    <div [formGroup]=\"myGroup\">\n       <div formGroupName=\"person\">\n          <input formControlName=\"firstName\">\n       </div>\n    </div>\n\n    In your class:\n\n    this.myGroup = new FormGroup({\n       person: new FormGroup({ firstName: new FormControl() })\n    });",
        formArrayName: "\n    <div [formGroup]=\"myGroup\">\n      <div formArrayName=\"cities\">\n        <div *ngFor=\"let city of cityArray.controls; index as i\">\n          <input [formControlName]=\"i\">\n        </div>\n      </div>\n    </div>\n\n    In your class:\n\n    this.cityArray = new FormArray([new FormControl('SF')]);\n    this.myGroup = new FormGroup({\n      cities: this.cityArray\n    });",
        ngModelGroup: "\n    <form>\n       <div ngModelGroup=\"person\">\n          <input [(ngModel)]=\"person.name\" name=\"firstName\">\n       </div>\n    </form>",
        ngModelWithFormGroup: "\n    <div [formGroup]=\"myGroup\">\n       <input formControlName=\"firstName\">\n       <input [(ngModel)]=\"showMoreControls\" [ngModelOptions]=\"{standalone: true}\">\n    </div>\n  "
    };

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var ReactiveErrors = /** @class */ (function () {
        function ReactiveErrors() {
        }
        ReactiveErrors.controlParentException = function () {
            throw new Error("formControlName must be used with a parent formGroup directive.  You'll want to add a formGroup\n       directive and pass it an existing FormGroup instance (you can create one in your class).\n\n      Example:\n\n      " + FormErrorExamples.formControlName);
        };
        ReactiveErrors.ngModelGroupException = function () {
            throw new Error("formControlName cannot be used with an ngModelGroup parent. It is only compatible with parents\n       that also have a \"form\" prefix: formGroupName, formArrayName, or formGroup.\n\n       Option 1:  Update the parent to be formGroupName (reactive form strategy)\n\n        " + FormErrorExamples.formGroupName + "\n\n        Option 2: Use ngModel instead of formControlName (template-driven strategy)\n\n        " + FormErrorExamples.ngModelGroup);
        };
        ReactiveErrors.missingFormException = function () {
            throw new Error("formGroup expects a FormGroup instance. Please pass one in.\n\n       Example:\n\n       " + FormErrorExamples.formControlName);
        };
        ReactiveErrors.groupParentException = function () {
            throw new Error("formGroupName must be used with a parent formGroup directive.  You'll want to add a formGroup\n      directive and pass it an existing FormGroup instance (you can create one in your class).\n\n      Example:\n\n      " + FormErrorExamples.formGroupName);
        };
        ReactiveErrors.arrayParentException = function () {
            throw new Error("formArrayName must be used with a parent formGroup directive.  You'll want to add a formGroup\n       directive and pass it an existing FormGroup instance (you can create one in your class).\n\n        Example:\n\n        " + FormErrorExamples.formArrayName);
        };
        ReactiveErrors.disabledAttrWarning = function () {
            console.warn("\n      It looks like you're using the disabled attribute with a reactive form directive. If you set disabled to true\n      when you set up this control in your component class, the disabled attribute will actually be set in the DOM for\n      you. We recommend using this approach to avoid 'changed after checked' errors.\n       \n      Example: \n      form = new FormGroup({\n        first: new FormControl({value: 'Nancy', disabled: true}, Validators.required),\n        last: new FormControl('Drew', Validators.required)\n      });\n    ");
        };
        ReactiveErrors.ngModelWarning = function (directiveName) {
            console.warn("\n    It looks like you're using ngModel on the same form field as " + directiveName + ". \n    Support for using the ngModel input property and ngModelChange event with \n    reactive form directives has been deprecated in Angular v6 and will be removed \n    in Angular v7.\n    \n    For more information on this, see our API docs here:\n    https://angular.io/api/forms/" + (directiveName === 'formControl' ? 'FormControlDirective'
                : 'FormControlName') + "#use-with-ngmodel\n    ");
        };
        return ReactiveErrors;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var SELECT_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return SelectControlValueAccessor; }),
        multi: true
    };
    function _buildValueString(id, value) {
        if (id == null)
            return "" + value;
        if (value && typeof value === 'object')
            value = 'Object';
        return (id + ": " + value).slice(0, 50);
    }
    function _extractId(valueString) {
        return valueString.split(':')[0];
    }
    /**
     * @description
     * The `ControlValueAccessor` for writing select control values and listening to select control
     * changes. The value accessor is used by the `FormControlDirective`, `FormControlName`, and
     * `NgModel` directives.
     *
     * @usageNotes
     *
     * ### Using select controls in a reactive form
     *
     * The following examples show how to use a select control in a reactive form.
     *
     * {@example forms/ts/reactiveSelectControl/reactive_select_control_example.ts region='Component'}
     *
     * ### Using select controls in a template-driven form
     *
     * To use a select in a template-driven form, simply add an `ngModel` and a `name`
     * attribute to the main `<select>` tag.
     *
     * {@example forms/ts/selectControl/select_control_example.ts region='Component'}
     *
     * ### Customizing option selection
     *
     * Angular uses object identity to select option. It's possible for the identities of items
     * to change while the data does not. This can happen, for example, if the items are produced
     * from an RPC to the server, and that RPC is re-run. Even if the data hasn't changed, the
     * second response will produce objects with different identities.
     *
     * To customize the default option comparison algorithm, `<select>` supports `compareWith` input.
     * `compareWith` takes a **function** which has two arguments: `option1` and `option2`.
     * If `compareWith` is given, Angular selects option by the return value of the function.
     *
     * ```ts
     * const selectedCountriesControl = new FormControl();
     * ```
     *
     * ```
     * <select [compareWith]="compareFn"  [formControl]="selectedCountriesControl">
     *     <option *ngFor="let country of countries" [ngValue]="country">
     *         {{country.name}}
     *     </option>
     * </select>
     *
     * compareFn(c1: Country, c2: Country): boolean {
     *     return c1 && c2 ? c1.id === c2.id : c1 === c2;
     * }
     * ```
     *
     * **Note:** We listen to the 'change' event because 'input' events aren't fired
     * for selects in Firefox and IE:
     * https://bugzilla.mozilla.org/show_bug.cgi?id=1024350
     * https://developer.microsoft.com/en-us/microsoft-edge/platform/issues/4660045/
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var SelectControlValueAccessor = /** @class */ (function () {
        function SelectControlValueAccessor(_renderer, _elementRef) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            /** @internal */
            this._optionMap = new Map();
            /** @internal */
            this._idCounter = 0;
            /**
             * @description
             * The registered callback function called when a change event occurs on the input element.
             */
            this.onChange = function (_) { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
            this._compareWith = core.ɵlooseIdentical;
        }
        Object.defineProperty(SelectControlValueAccessor.prototype, "compareWith", {
            /**
             * @description
             * Tracks the option comparison algorithm for tracking identities when
             * checking for changes.
             */
            set: function (fn) {
                if (typeof fn !== 'function') {
                    throw new Error("compareWith must be a function, but received " + JSON.stringify(fn));
                }
                this._compareWith = fn;
            },
            enumerable: true,
            configurable: true
        });
        /**
         * Sets the "value" property on the input element. The "selectedIndex"
         * property is also set if an ID is provided on the option element.
         *
         * @param value The checked value
         */
        SelectControlValueAccessor.prototype.writeValue = function (value) {
            this.value = value;
            var id = this._getOptionId(value);
            if (id == null) {
                this._renderer.setProperty(this._elementRef.nativeElement, 'selectedIndex', -1);
            }
            var valueString = _buildValueString(id, value);
            this._renderer.setProperty(this._elementRef.nativeElement, 'value', valueString);
        };
        /**
         * @description
         * Registers a function called when the control value changes.
         *
         * @param fn The callback function
         */
        SelectControlValueAccessor.prototype.registerOnChange = function (fn) {
            var _this = this;
            this.onChange = function (valueString) {
                _this.value = _this._getOptionValue(valueString);
                fn(_this.value);
            };
        };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        SelectControlValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the select input element.
         *
         * @param isDisabled The disabled value
         */
        SelectControlValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        /** @internal */
        SelectControlValueAccessor.prototype._registerOption = function () { return (this._idCounter++).toString(); };
        /** @internal */
        SelectControlValueAccessor.prototype._getOptionId = function (value) {
            var e_1, _a;
            try {
                for (var _b = __values(Array.from(this._optionMap.keys())), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var id = _c.value;
                    if (this._compareWith(this._optionMap.get(id), value))
                        return id;
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
        };
        /** @internal */
        SelectControlValueAccessor.prototype._getOptionValue = function (valueString) {
            var id = _extractId(valueString);
            return this._optionMap.has(id) ? this._optionMap.get(id) : valueString;
        };
        __decorate([
            core.Input(),
            __metadata("design:type", Function),
            __metadata("design:paramtypes", [Function])
        ], SelectControlValueAccessor.prototype, "compareWith", null);
        SelectControlValueAccessor = __decorate([
            core.Directive({
                selector: 'select:not([multiple])[formControlName],select:not([multiple])[formControl],select:not([multiple])[ngModel]',
                host: { '(change)': 'onChange($event.target.value)', '(blur)': 'onTouched()' },
                providers: [SELECT_VALUE_ACCESSOR]
            }),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef])
        ], SelectControlValueAccessor);
        return SelectControlValueAccessor;
    }());
    /**
     * @description
     * Marks `<option>` as dynamic, so Angular can be notified when options change.
     *
     * @see `SelectControlValueAccessor`
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var NgSelectOption = /** @class */ (function () {
        function NgSelectOption(_element, _renderer, _select) {
            this._element = _element;
            this._renderer = _renderer;
            this._select = _select;
            if (this._select)
                this.id = this._select._registerOption();
        }
        Object.defineProperty(NgSelectOption.prototype, "ngValue", {
            /**
             * @description
             * Tracks the value bound to the option element. Unlike the value binding,
             * ngValue supports binding to objects.
             */
            set: function (value) {
                if (this._select == null)
                    return;
                this._select._optionMap.set(this.id, value);
                this._setElementValue(_buildValueString(this.id, value));
                this._select.writeValue(this._select.value);
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgSelectOption.prototype, "value", {
            /**
             * @description
             * Tracks simple string values bound to the option element.
             * For objects, use the `ngValue` input binding.
             */
            set: function (value) {
                this._setElementValue(value);
                if (this._select)
                    this._select.writeValue(this._select.value);
            },
            enumerable: true,
            configurable: true
        });
        /** @internal */
        NgSelectOption.prototype._setElementValue = function (value) {
            this._renderer.setProperty(this._element.nativeElement, 'value', value);
        };
        /**
         * @description
         * Lifecycle method called before the directive's instance is destroyed. For internal use only.
         */
        NgSelectOption.prototype.ngOnDestroy = function () {
            if (this._select) {
                this._select._optionMap.delete(this.id);
                this._select.writeValue(this._select.value);
            }
        };
        __decorate([
            core.Input('ngValue'),
            __metadata("design:type", Object),
            __metadata("design:paramtypes", [Object])
        ], NgSelectOption.prototype, "ngValue", null);
        __decorate([
            core.Input('value'),
            __metadata("design:type", Object),
            __metadata("design:paramtypes", [Object])
        ], NgSelectOption.prototype, "value", null);
        NgSelectOption = __decorate([
            core.Directive({ selector: 'option' }),
            __param(2, core.Optional()), __param(2, core.Host()),
            __metadata("design:paramtypes", [core.ElementRef, core.Renderer2,
                SelectControlValueAccessor])
        ], NgSelectOption);
        return NgSelectOption;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var SELECT_MULTIPLE_VALUE_ACCESSOR = {
        provide: NG_VALUE_ACCESSOR,
        useExisting: core.forwardRef(function () { return SelectMultipleControlValueAccessor; }),
        multi: true
    };
    function _buildValueString$1(id, value) {
        if (id == null)
            return "" + value;
        if (typeof value === 'string')
            value = "'" + value + "'";
        if (value && typeof value === 'object')
            value = 'Object';
        return (id + ": " + value).slice(0, 50);
    }
    function _extractId$1(valueString) {
        return valueString.split(':')[0];
    }
    /** Mock interface for HTMLCollection */
    var HTMLCollection = /** @class */ (function () {
        function HTMLCollection() {
        }
        return HTMLCollection;
    }());
    /**
     * @description
     * The `ControlValueAccessor` for writing multi-select control values and listening to multi-select control
     * changes. The value accessor is used by the `FormControlDirective`, `FormControlName`, and `NgModel`
     * directives.
     *
     * @see `SelectControlValueAccessor`
     *
     * @usageNotes
     *
     * ### Using a multi-select control
     *
     * The follow example shows you how to use a multi-select control with a reactive form.
     *
     * ```ts
     * const countryControl = new FormControl();
     * ```
     *
     * ```
     * <select multiple name="countries" [formControl]="countryControl">
     *   <option *ngFor="let country of countries" [ngValue]="country">
     *     {{ country.name }}
     *   </option>
     * </select>
     * ```
     *
     * ### Customizing option selection
     *
     * To customize the default option comparison algorithm, `<select>` supports `compareWith` input.
     * See the `SelectControlValueAccessor` for usage.
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var SelectMultipleControlValueAccessor = /** @class */ (function () {
        function SelectMultipleControlValueAccessor(_renderer, _elementRef) {
            this._renderer = _renderer;
            this._elementRef = _elementRef;
            /** @internal */
            this._optionMap = new Map();
            /** @internal */
            this._idCounter = 0;
            /**
             * @description
             * The registered callback function called when a change event occurs on the input element.
             */
            this.onChange = function (_) { };
            /**
             * @description
             * The registered callback function called when a blur event occurs on the input element.
             */
            this.onTouched = function () { };
            this._compareWith = core.ɵlooseIdentical;
        }
        Object.defineProperty(SelectMultipleControlValueAccessor.prototype, "compareWith", {
            /**
             * @description
             * Tracks the option comparison algorithm for tracking identities when
             * checking for changes.
             */
            set: function (fn) {
                if (typeof fn !== 'function') {
                    throw new Error("compareWith must be a function, but received " + JSON.stringify(fn));
                }
                this._compareWith = fn;
            },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Sets the "value" property on one or of more
         * of the select's options.
         *
         * @param value The value
         */
        SelectMultipleControlValueAccessor.prototype.writeValue = function (value) {
            var _this = this;
            this.value = value;
            var optionSelectedStateSetter;
            if (Array.isArray(value)) {
                // convert values to ids
                var ids_1 = value.map(function (v) { return _this._getOptionId(v); });
                optionSelectedStateSetter = function (opt, o) { opt._setSelected(ids_1.indexOf(o.toString()) > -1); };
            }
            else {
                optionSelectedStateSetter = function (opt, o) { opt._setSelected(false); };
            }
            this._optionMap.forEach(optionSelectedStateSetter);
        };
        /**
         * @description
         * Registers a function called when the control value changes
         * and writes an array of the selected options.
         *
         * @param fn The callback function
         */
        SelectMultipleControlValueAccessor.prototype.registerOnChange = function (fn) {
            var _this = this;
            this.onChange = function (_) {
                var selected = [];
                if (_.hasOwnProperty('selectedOptions')) {
                    var options = _.selectedOptions;
                    for (var i = 0; i < options.length; i++) {
                        var opt = options.item(i);
                        var val = _this._getOptionValue(opt.value);
                        selected.push(val);
                    }
                }
                // Degrade on IE
                else {
                    var options = _.options;
                    for (var i = 0; i < options.length; i++) {
                        var opt = options.item(i);
                        if (opt.selected) {
                            var val = _this._getOptionValue(opt.value);
                            selected.push(val);
                        }
                    }
                }
                _this.value = selected;
                fn(selected);
            };
        };
        /**
         * @description
         * Registers a function called when the control is touched.
         *
         * @param fn The callback function
         */
        SelectMultipleControlValueAccessor.prototype.registerOnTouched = function (fn) { this.onTouched = fn; };
        /**
         * Sets the "disabled" property on the select input element.
         *
         * @param isDisabled The disabled value
         */
        SelectMultipleControlValueAccessor.prototype.setDisabledState = function (isDisabled) {
            this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
        };
        /** @internal */
        SelectMultipleControlValueAccessor.prototype._registerOption = function (value) {
            var id = (this._idCounter++).toString();
            this._optionMap.set(id, value);
            return id;
        };
        /** @internal */
        SelectMultipleControlValueAccessor.prototype._getOptionId = function (value) {
            var e_1, _a;
            try {
                for (var _b = __values(Array.from(this._optionMap.keys())), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var id = _c.value;
                    if (this._compareWith(this._optionMap.get(id)._value, value))
                        return id;
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
        };
        /** @internal */
        SelectMultipleControlValueAccessor.prototype._getOptionValue = function (valueString) {
            var id = _extractId$1(valueString);
            return this._optionMap.has(id) ? this._optionMap.get(id)._value : valueString;
        };
        __decorate([
            core.Input(),
            __metadata("design:type", Function),
            __metadata("design:paramtypes", [Function])
        ], SelectMultipleControlValueAccessor.prototype, "compareWith", null);
        SelectMultipleControlValueAccessor = __decorate([
            core.Directive({
                selector: 'select[multiple][formControlName],select[multiple][formControl],select[multiple][ngModel]',
                host: { '(change)': 'onChange($event.target)', '(blur)': 'onTouched()' },
                providers: [SELECT_MULTIPLE_VALUE_ACCESSOR]
            }),
            __metadata("design:paramtypes", [core.Renderer2, core.ElementRef])
        ], SelectMultipleControlValueAccessor);
        return SelectMultipleControlValueAccessor;
    }());
    /**
     * @description
     * Marks `<option>` as dynamic, so Angular can be notified when options change.
     *
     * @see `SelectMultipleControlValueAccessor`
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var ɵNgSelectMultipleOption = /** @class */ (function () {
        function ɵNgSelectMultipleOption(_element, _renderer, _select) {
            this._element = _element;
            this._renderer = _renderer;
            this._select = _select;
            if (this._select) {
                this.id = this._select._registerOption(this);
            }
        }
        Object.defineProperty(ɵNgSelectMultipleOption.prototype, "ngValue", {
            /**
             * @description
             * Tracks the value bound to the option element. Unlike the value binding,
             * ngValue supports binding to objects.
             */
            set: function (value) {
                if (this._select == null)
                    return;
                this._value = value;
                this._setElementValue(_buildValueString$1(this.id, value));
                this._select.writeValue(this._select.value);
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(ɵNgSelectMultipleOption.prototype, "value", {
            /**
             * @description
             * Tracks simple string values bound to the option element.
             * For objects, use the `ngValue` input binding.
             */
            set: function (value) {
                if (this._select) {
                    this._value = value;
                    this._setElementValue(_buildValueString$1(this.id, value));
                    this._select.writeValue(this._select.value);
                }
                else {
                    this._setElementValue(value);
                }
            },
            enumerable: true,
            configurable: true
        });
        /** @internal */
        ɵNgSelectMultipleOption.prototype._setElementValue = function (value) {
            this._renderer.setProperty(this._element.nativeElement, 'value', value);
        };
        /** @internal */
        ɵNgSelectMultipleOption.prototype._setSelected = function (selected) {
            this._renderer.setProperty(this._element.nativeElement, 'selected', selected);
        };
        /**
         * @description
         * Lifecycle method called before the directive's instance is destroyed. For internal use only.
         */
        ɵNgSelectMultipleOption.prototype.ngOnDestroy = function () {
            if (this._select) {
                this._select._optionMap.delete(this.id);
                this._select.writeValue(this._select.value);
            }
        };
        __decorate([
            core.Input('ngValue'),
            __metadata("design:type", Object),
            __metadata("design:paramtypes", [Object])
        ], ɵNgSelectMultipleOption.prototype, "ngValue", null);
        __decorate([
            core.Input('value'),
            __metadata("design:type", Object),
            __metadata("design:paramtypes", [Object])
        ], ɵNgSelectMultipleOption.prototype, "value", null);
        ɵNgSelectMultipleOption = __decorate([
            core.Directive({ selector: 'option' }),
            __param(2, core.Optional()), __param(2, core.Host()),
            __metadata("design:paramtypes", [core.ElementRef, core.Renderer2,
                SelectMultipleControlValueAccessor])
        ], ɵNgSelectMultipleOption);
        return ɵNgSelectMultipleOption;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    function controlPath(name, parent) {
        return __spread(parent.path, [name]);
    }
    function setUpControl(control, dir) {
        if (!control)
            _throwError(dir, 'Cannot find control with');
        if (!dir.valueAccessor)
            _throwError(dir, 'No value accessor for form control with');
        control.validator = Validators.compose([control.validator, dir.validator]);
        control.asyncValidator = Validators.composeAsync([control.asyncValidator, dir.asyncValidator]);
        dir.valueAccessor.writeValue(control.value);
        setUpViewChangePipeline(control, dir);
        setUpModelChangePipeline(control, dir);
        setUpBlurPipeline(control, dir);
        if (dir.valueAccessor.setDisabledState) {
            control.registerOnDisabledChange(function (isDisabled) { dir.valueAccessor.setDisabledState(isDisabled); });
        }
        // re-run validation when validator binding changes, e.g. minlength=3 -> minlength=4
        dir._rawValidators.forEach(function (validator) {
            if (validator.registerOnValidatorChange)
                validator.registerOnValidatorChange(function () { return control.updateValueAndValidity(); });
        });
        dir._rawAsyncValidators.forEach(function (validator) {
            if (validator.registerOnValidatorChange)
                validator.registerOnValidatorChange(function () { return control.updateValueAndValidity(); });
        });
    }
    function cleanUpControl(control, dir) {
        dir.valueAccessor.registerOnChange(function () { return _noControlError(dir); });
        dir.valueAccessor.registerOnTouched(function () { return _noControlError(dir); });
        dir._rawValidators.forEach(function (validator) {
            if (validator.registerOnValidatorChange) {
                validator.registerOnValidatorChange(null);
            }
        });
        dir._rawAsyncValidators.forEach(function (validator) {
            if (validator.registerOnValidatorChange) {
                validator.registerOnValidatorChange(null);
            }
        });
        if (control)
            control._clearChangeFns();
    }
    function setUpViewChangePipeline(control, dir) {
        dir.valueAccessor.registerOnChange(function (newValue) {
            control._pendingValue = newValue;
            control._pendingChange = true;
            control._pendingDirty = true;
            if (control.updateOn === 'change')
                updateControl(control, dir);
        });
    }
    function setUpBlurPipeline(control, dir) {
        dir.valueAccessor.registerOnTouched(function () {
            control._pendingTouched = true;
            if (control.updateOn === 'blur' && control._pendingChange)
                updateControl(control, dir);
            if (control.updateOn !== 'submit')
                control.markAsTouched();
        });
    }
    function updateControl(control, dir) {
        if (control._pendingDirty)
            control.markAsDirty();
        control.setValue(control._pendingValue, { emitModelToViewChange: false });
        dir.viewToModelUpdate(control._pendingValue);
        control._pendingChange = false;
    }
    function setUpModelChangePipeline(control, dir) {
        control.registerOnChange(function (newValue, emitModelEvent) {
            // control -> view
            dir.valueAccessor.writeValue(newValue);
            // control -> ngModel
            if (emitModelEvent)
                dir.viewToModelUpdate(newValue);
        });
    }
    function setUpFormContainer(control, dir) {
        if (control == null)
            _throwError(dir, 'Cannot find control with');
        control.validator = Validators.compose([control.validator, dir.validator]);
        control.asyncValidator = Validators.composeAsync([control.asyncValidator, dir.asyncValidator]);
    }
    function _noControlError(dir) {
        return _throwError(dir, 'There is no FormControl instance attached to form control element with');
    }
    function _throwError(dir, message) {
        var messageEnd;
        if (dir.path.length > 1) {
            messageEnd = "path: '" + dir.path.join(' -> ') + "'";
        }
        else if (dir.path[0]) {
            messageEnd = "name: '" + dir.path + "'";
        }
        else {
            messageEnd = 'unspecified name attribute';
        }
        throw new Error(message + " " + messageEnd);
    }
    function composeValidators(validators) {
        return validators != null ? Validators.compose(validators.map(normalizeValidator)) : null;
    }
    function composeAsyncValidators(validators) {
        return validators != null ? Validators.composeAsync(validators.map(normalizeAsyncValidator)) :
            null;
    }
    function isPropertyUpdated(changes, viewModel) {
        if (!changes.hasOwnProperty('model'))
            return false;
        var change = changes['model'];
        if (change.isFirstChange())
            return true;
        return !core.ɵlooseIdentical(viewModel, change.currentValue);
    }
    var BUILTIN_ACCESSORS = [
        CheckboxControlValueAccessor,
        RangeValueAccessor,
        NumberValueAccessor,
        SelectControlValueAccessor,
        SelectMultipleControlValueAccessor,
        RadioControlValueAccessor,
    ];
    function isBuiltInAccessor(valueAccessor) {
        return BUILTIN_ACCESSORS.some(function (a) { return valueAccessor.constructor === a; });
    }
    function syncPendingControls(form, directives) {
        form._syncPendingControls();
        directives.forEach(function (dir) {
            var control = dir.control;
            if (control.updateOn === 'submit' && control._pendingChange) {
                dir.viewToModelUpdate(control._pendingValue);
                control._pendingChange = false;
            }
        });
    }
    // TODO: vsavkin remove it once https://github.com/angular/angular/issues/3011 is implemented
    function selectValueAccessor(dir, valueAccessors) {
        if (!valueAccessors)
            return null;
        if (!Array.isArray(valueAccessors))
            _throwError(dir, 'Value accessor was not provided as an array for form control with');
        var defaultAccessor = undefined;
        var builtinAccessor = undefined;
        var customAccessor = undefined;
        valueAccessors.forEach(function (v) {
            if (v.constructor === DefaultValueAccessor) {
                defaultAccessor = v;
            }
            else if (isBuiltInAccessor(v)) {
                if (builtinAccessor)
                    _throwError(dir, 'More than one built-in value accessor matches form control with');
                builtinAccessor = v;
            }
            else {
                if (customAccessor)
                    _throwError(dir, 'More than one custom value accessor matches form control with');
                customAccessor = v;
            }
        });
        if (customAccessor)
            return customAccessor;
        if (builtinAccessor)
            return builtinAccessor;
        if (defaultAccessor)
            return defaultAccessor;
        _throwError(dir, 'No valid value accessor for form control with');
        return null;
    }
    function removeDir(list, el) {
        var index = list.indexOf(el);
        if (index > -1)
            list.splice(index, 1);
    }
    // TODO(kara): remove after deprecation period
    function _ngModelWarning(name, type, instance, warningConfig) {
        if (!core.isDevMode() || warningConfig === 'never')
            return;
        if (((warningConfig === null || warningConfig === 'once') && !type._ngModelWarningSentOnce) ||
            (warningConfig === 'always' && !instance._ngModelWarningSent)) {
            ReactiveErrors.ngModelWarning(name);
            type._ngModelWarningSentOnce = true;
            instance._ngModelWarningSent = true;
        }
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * Reports that a FormControl is valid, meaning that no errors exist in the input value.
     *
     * @see `status`
     */
    var VALID = 'VALID';
    /**
     * Reports that a FormControl is invalid, meaning that an error exists in the input value.
     *
     * @see `status`
     */
    var INVALID = 'INVALID';
    /**
     * Reports that a FormControl is pending, meaning that that async validation is occurring and
     * errors are not yet available for the input value.
     *
     * @see `markAsPending`
     * @see `status`
     */
    var PENDING = 'PENDING';
    /**
     * Reports that a FormControl is disabled, meaning that the control is exempt from ancestor
     * calculations of validity or value.
     *
     * @see `markAsDisabled`
     * @see `status`
     */
    var DISABLED = 'DISABLED';
    function _find(control, path, delimiter) {
        if (path == null)
            return null;
        if (!(path instanceof Array)) {
            path = path.split(delimiter);
        }
        if (path instanceof Array && (path.length === 0))
            return null;
        return path.reduce(function (v, name) {
            if (v instanceof FormGroup) {
                return v.controls.hasOwnProperty(name) ? v.controls[name] : null;
            }
            if (v instanceof FormArray) {
                return v.at(name) || null;
            }
            return null;
        }, control);
    }
    function coerceToValidator(validatorOrOpts) {
        var validator = (isOptionsObj(validatorOrOpts) ? validatorOrOpts.validators :
            validatorOrOpts);
        return Array.isArray(validator) ? composeValidators(validator) : validator || null;
    }
    function coerceToAsyncValidator(asyncValidator, validatorOrOpts) {
        var origAsyncValidator = (isOptionsObj(validatorOrOpts) ? validatorOrOpts.asyncValidators :
            asyncValidator);
        return Array.isArray(origAsyncValidator) ? composeAsyncValidators(origAsyncValidator) :
            origAsyncValidator || null;
    }
    function isOptionsObj(validatorOrOpts) {
        return validatorOrOpts != null && !Array.isArray(validatorOrOpts) &&
            typeof validatorOrOpts === 'object';
    }
    /**
     * This is the base class for `FormControl`, `FormGroup`, and `FormArray`.
     *
     * It provides some of the shared behavior that all controls and groups of controls have, like
     * running validators, calculating status, and resetting state. It also defines the properties
     * that are shared between all sub-classes, like `value`, `valid`, and `dirty`. It shouldn't be
     * instantiated directly.
     *
     * @see [Forms Guide](/guide/forms)
     * @see [Reactive Forms Guide](/guide/reactive-forms)
     * @see [Dynamic Forms Guide](/guide/dynamic-form)
     *
     * @publicApi
     */
    var AbstractControl = /** @class */ (function () {
        /**
         * Initialize the AbstractControl instance.
         *
         * @param validator The function that determines the synchronous validity of this control.
         * @param asyncValidator The function that determines the asynchronous validity of this
         * control.
         */
        function AbstractControl(validator, asyncValidator) {
            this.validator = validator;
            this.asyncValidator = asyncValidator;
            /** @internal */
            this._onCollectionChange = function () { };
            /**
             * A control is `pristine` if the user has not yet changed
             * the value in the UI.
             *
             * @returns True if the user has not yet changed the value in the UI; compare `dirty`.
             * Programmatic changes to a control's value do not mark it dirty.
             */
            this.pristine = true;
            /**
             * True if the control is marked as `touched`.
             *
             * A control is marked `touched` once the user has triggered
             * a `blur` event on it.
             */
            this.touched = false;
            /** @internal */
            this._onDisabledChange = [];
        }
        Object.defineProperty(AbstractControl.prototype, "parent", {
            /**
             * The parent control.
             */
            get: function () { return this._parent; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "valid", {
            /**
             * A control is `valid` when its `status` is `VALID`.
             *
             * @see {@link AbstractControl.status}
             *
             * @returns True if the control has passed all of its validation tests,
             * false otherwise.
             */
            get: function () { return this.status === VALID; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "invalid", {
            /**
             * A control is `invalid` when its `status` is `INVALID`.
             *
             * @see {@link AbstractControl.status}
             *
             * @returns True if this control has failed one or more of its validation checks,
             * false otherwise.
             */
            get: function () { return this.status === INVALID; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "pending", {
            /**
             * A control is `pending` when its `status` is `PENDING`.
             *
             * @see {@link AbstractControl.status}
             *
             * @returns True if this control is in the process of conducting a validation check,
             * false otherwise.
             */
            get: function () { return this.status == PENDING; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "disabled", {
            /**
             * A control is `disabled` when its `status` is `DISABLED`.
             *
             * Disabled controls are exempt from validation checks and
             * are not included in the aggregate value of their ancestor
             * controls.
             *
             * @see {@link AbstractControl.status}
             *
             * @returns True if the control is disabled, false otherwise.
             */
            get: function () { return this.status === DISABLED; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "enabled", {
            /**
             * A control is `enabled` as long as its `status` is not `DISABLED`.
             *
             * @returns True if the control has any status other than 'DISABLED',
             * false if the status is 'DISABLED'.
             *
             * @see {@link AbstractControl.status}
             *
             */
            get: function () { return this.status !== DISABLED; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "dirty", {
            /**
             * A control is `dirty` if the user has changed the value
             * in the UI.
             *
             * @returns True if the user has changed the value of this control in the UI; compare `pristine`.
             * Programmatic changes to a control's value do not mark it dirty.
             */
            get: function () { return !this.pristine; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "untouched", {
            /**
             * True if the control has not been marked as touched
             *
             * A control is `untouched` if the user has not yet triggered
             * a `blur` event on it.
             */
            get: function () { return !this.touched; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractControl.prototype, "updateOn", {
            /**
             * Reports the update strategy of the `AbstractControl` (meaning
             * the event on which the control updates itself).
             * Possible values: `'change'` | `'blur'` | `'submit'`
             * Default value: `'change'`
             */
            get: function () {
                return this._updateOn ? this._updateOn : (this.parent ? this.parent.updateOn : 'change');
            },
            enumerable: true,
            configurable: true
        });
        /**
         * Sets the synchronous validators that are active on this control.  Calling
         * this overwrites any existing sync validators.
         *
         * When you add or remove a validator at run time, you must call
         * `updateValueAndValidity()` for the new validation to take effect.
         *
         */
        AbstractControl.prototype.setValidators = function (newValidator) {
            this.validator = coerceToValidator(newValidator);
        };
        /**
         * Sets the async validators that are active on this control. Calling this
         * overwrites any existing async validators.
         *
         * When you add or remove a validator at run time, you must call
         * `updateValueAndValidity()` for the new validation to take effect.
         *
         */
        AbstractControl.prototype.setAsyncValidators = function (newValidator) {
            this.asyncValidator = coerceToAsyncValidator(newValidator);
        };
        /**
         * Empties out the sync validator list.
         *
         * When you add or remove a validator at run time, you must call
         * `updateValueAndValidity()` for the new validation to take effect.
         *
         */
        AbstractControl.prototype.clearValidators = function () { this.validator = null; };
        /**
         * Empties out the async validator list.
         *
         * When you add or remove a validator at run time, you must call
         * `updateValueAndValidity()` for the new validation to take effect.
         *
         */
        AbstractControl.prototype.clearAsyncValidators = function () { this.asyncValidator = null; };
        /**
         * Marks the control as `touched`. A control is touched by focus and
         * blur events that do not change the value.
         *
         * @see `markAsUntouched()`
         * @see `markAsDirty()`
         * @see `markAsPristine()`
         *
         * @param opts Configuration options that determine how the control propagates changes
         * and emits events events after marking is applied.
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false.
         */
        AbstractControl.prototype.markAsTouched = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.touched = true;
            if (this._parent && !opts.onlySelf) {
                this._parent.markAsTouched(opts);
            }
        };
        /**
         * Marks the control and all its descendant controls as `touched`.
         * @see `markAsTouched()`
         */
        AbstractControl.prototype.markAllAsTouched = function () {
            this.markAsTouched({ onlySelf: true });
            this._forEachChild(function (control) { return control.markAllAsTouched(); });
        };
        /**
         * Marks the control as `untouched`.
         *
         * If the control has any children, also marks all children as `untouched`
         * and recalculates the `touched` status of all parent controls.
         *
         * @see `markAsTouched()`
         * @see `markAsDirty()`
         * @see `markAsPristine()`
         *
         * @param opts Configuration options that determine how the control propagates changes
         * and emits events after the marking is applied.
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false.
         */
        AbstractControl.prototype.markAsUntouched = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.touched = false;
            this._pendingTouched = false;
            this._forEachChild(function (control) { control.markAsUntouched({ onlySelf: true }); });
            if (this._parent && !opts.onlySelf) {
                this._parent._updateTouched(opts);
            }
        };
        /**
         * Marks the control as `dirty`. A control becomes dirty when
         * the control's value is changed through the UI; compare `markAsTouched`.
         *
         * @see `markAsTouched()`
         * @see `markAsUntouched()`
         * @see `markAsPristine()`
         *
         * @param opts Configuration options that determine how the control propagates changes
         * and emits events after marking is applied.
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false.
         */
        AbstractControl.prototype.markAsDirty = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.pristine = false;
            if (this._parent && !opts.onlySelf) {
                this._parent.markAsDirty(opts);
            }
        };
        /**
         * Marks the control as `pristine`.
         *
         * If the control has any children, marks all children as `pristine`,
         * and recalculates the `pristine` status of all parent
         * controls.
         *
         * @see `markAsTouched()`
         * @see `markAsUntouched()`
         * @see `markAsDirty()`
         *
         * @param opts Configuration options that determine how the control emits events after
         * marking is applied.
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false..
         */
        AbstractControl.prototype.markAsPristine = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.pristine = true;
            this._pendingDirty = false;
            this._forEachChild(function (control) { control.markAsPristine({ onlySelf: true }); });
            if (this._parent && !opts.onlySelf) {
                this._parent._updatePristine(opts);
            }
        };
        /**
         * Marks the control as `pending`.
         *
         * A control is pending while the control performs async validation.
         *
         * @see {@link AbstractControl.status}
         *
         * @param opts Configuration options that determine how the control propagates changes and
         * emits events after marking is applied.
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false..
         * * `emitEvent`: When true or not supplied (the default), the `statusChanges`
         * observable emits an event with the latest status the control is marked pending.
         * When false, no events are emitted.
         *
         */
        AbstractControl.prototype.markAsPending = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.status = PENDING;
            if (opts.emitEvent !== false) {
                this.statusChanges.emit(this.status);
            }
            if (this._parent && !opts.onlySelf) {
                this._parent.markAsPending(opts);
            }
        };
        /**
         * Disables the control. This means the control is exempt from validation checks and
         * excluded from the aggregate value of any parent. Its status is `DISABLED`.
         *
         * If the control has children, all children are also disabled.
         *
         * @see {@link AbstractControl.status}
         *
         * @param opts Configuration options that determine how the control propagates
         * changes and emits events after the control is disabled.
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false..
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control is disabled.
         * When false, no events are emitted.
         */
        AbstractControl.prototype.disable = function (opts) {
            if (opts === void 0) { opts = {}; }
            // If parent has been marked artificially dirty we don't want to re-calculate the
            // parent's dirtiness based on the children.
            var skipPristineCheck = this._parentMarkedDirty(opts.onlySelf);
            this.status = DISABLED;
            this.errors = null;
            this._forEachChild(function (control) { control.disable(__assign({}, opts, { onlySelf: true })); });
            this._updateValue();
            if (opts.emitEvent !== false) {
                this.valueChanges.emit(this.value);
                this.statusChanges.emit(this.status);
            }
            this._updateAncestors(__assign({}, opts, { skipPristineCheck: skipPristineCheck }));
            this._onDisabledChange.forEach(function (changeFn) { return changeFn(true); });
        };
        /**
         * Enables the control. This means the control is included in validation checks and
         * the aggregate value of its parent. Its status recalculates based on its value and
         * its validators.
         *
         * By default, if the control has children, all children are enabled.
         *
         * @see {@link AbstractControl.status}
         *
         * @param opts Configure options that control how the control propagates changes and
         * emits events when marked as untouched
         * * `onlySelf`: When true, mark only this control. When false or not supplied,
         * marks all direct ancestors. Default is false..
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control is enabled.
         * When false, no events are emitted.
         */
        AbstractControl.prototype.enable = function (opts) {
            if (opts === void 0) { opts = {}; }
            // If parent has been marked artificially dirty we don't want to re-calculate the
            // parent's dirtiness based on the children.
            var skipPristineCheck = this._parentMarkedDirty(opts.onlySelf);
            this.status = VALID;
            this._forEachChild(function (control) { control.enable(__assign({}, opts, { onlySelf: true })); });
            this.updateValueAndValidity({ onlySelf: true, emitEvent: opts.emitEvent });
            this._updateAncestors(__assign({}, opts, { skipPristineCheck: skipPristineCheck }));
            this._onDisabledChange.forEach(function (changeFn) { return changeFn(false); });
        };
        AbstractControl.prototype._updateAncestors = function (opts) {
            if (this._parent && !opts.onlySelf) {
                this._parent.updateValueAndValidity(opts);
                if (!opts.skipPristineCheck) {
                    this._parent._updatePristine();
                }
                this._parent._updateTouched();
            }
        };
        /**
         * @param parent Sets the parent of the control
         */
        AbstractControl.prototype.setParent = function (parent) { this._parent = parent; };
        /**
         * Recalculates the value and validation status of the control.
         *
         * By default, it also updates the value and validity of its ancestors.
         *
         * @param opts Configuration options determine how the control propagates changes and emits events
         * after updates and validity checks are applied.
         * * `onlySelf`: When true, only update this control. When false or not supplied,
         * update all direct ancestors. Default is false..
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control is updated.
         * When false, no events are emitted.
         */
        AbstractControl.prototype.updateValueAndValidity = function (opts) {
            if (opts === void 0) { opts = {}; }
            this._setInitialStatus();
            this._updateValue();
            if (this.enabled) {
                this._cancelExistingSubscription();
                this.errors = this._runValidator();
                this.status = this._calculateStatus();
                if (this.status === VALID || this.status === PENDING) {
                    this._runAsyncValidator(opts.emitEvent);
                }
            }
            if (opts.emitEvent !== false) {
                this.valueChanges.emit(this.value);
                this.statusChanges.emit(this.status);
            }
            if (this._parent && !opts.onlySelf) {
                this._parent.updateValueAndValidity(opts);
            }
        };
        /** @internal */
        AbstractControl.prototype._updateTreeValidity = function (opts) {
            if (opts === void 0) { opts = { emitEvent: true }; }
            this._forEachChild(function (ctrl) { return ctrl._updateTreeValidity(opts); });
            this.updateValueAndValidity({ onlySelf: true, emitEvent: opts.emitEvent });
        };
        AbstractControl.prototype._setInitialStatus = function () {
            this.status = this._allControlsDisabled() ? DISABLED : VALID;
        };
        AbstractControl.prototype._runValidator = function () {
            return this.validator ? this.validator(this) : null;
        };
        AbstractControl.prototype._runAsyncValidator = function (emitEvent) {
            var _this = this;
            if (this.asyncValidator) {
                this.status = PENDING;
                var obs = toObservable(this.asyncValidator(this));
                this._asyncValidationSubscription =
                    obs.subscribe(function (errors) { return _this.setErrors(errors, { emitEvent: emitEvent }); });
            }
        };
        AbstractControl.prototype._cancelExistingSubscription = function () {
            if (this._asyncValidationSubscription) {
                this._asyncValidationSubscription.unsubscribe();
            }
        };
        /**
         * Sets errors on a form control when running validations manually, rather than automatically.
         *
         * Calling `setErrors` also updates the validity of the parent control.
         *
         * @usageNotes
         * ### Manually set the errors for a control
         *
         * ```
         * const login = new FormControl('someLogin');
         * login.setErrors({
         *   notUnique: true
         * });
         *
         * expect(login.valid).toEqual(false);
         * expect(login.errors).toEqual({ notUnique: true });
         *
         * login.setValue('someOtherLogin');
         *
         * expect(login.valid).toEqual(true);
         * ```
         */
        AbstractControl.prototype.setErrors = function (errors, opts) {
            if (opts === void 0) { opts = {}; }
            this.errors = errors;
            this._updateControlsErrors(opts.emitEvent !== false);
        };
        /**
         * Retrieves a child control given the control's name or path.
         *
         * @param path A dot-delimited string or array of string/number values that define the path to the
         * control.
         *
         * @usageNotes
         * ### Retrieve a nested control
         *
         * For example, to get a `name` control nested within a `person` sub-group:
         *
         * * `this.form.get('person.name');`
         *
         * -OR-
         *
         * * `this.form.get(['person', 'name']);`
         */
        AbstractControl.prototype.get = function (path) { return _find(this, path, '.'); };
        /**
         * @description
         * Reports error data for the control with the given path.
         *
         * @param errorCode The code of the error to check
         * @param path A list of control names that designates how to move from the current control
         * to the control that should be queried for errors.
         *
         * @usageNotes
         * For example, for the following `FormGroup`:
         *
         * ```
         * form = new FormGroup({
         *   address: new FormGroup({ street: new FormControl() })
         * });
         * ```
         *
         * The path to the 'street' control from the root form would be 'address' -> 'street'.
         *
         * It can be provided to this method in one of two formats:
         *
         * 1. An array of string control names, e.g. `['address', 'street']`
         * 1. A period-delimited list of control names in one string, e.g. `'address.street'`
         *
         * @returns error data for that particular error. If the control or error is not present,
         * null is returned.
         */
        AbstractControl.prototype.getError = function (errorCode, path) {
            var control = path ? this.get(path) : this;
            return control && control.errors ? control.errors[errorCode] : null;
        };
        /**
         * @description
         * Reports whether the control with the given path has the error specified.
         *
         * @param errorCode The code of the error to check
         * @param path A list of control names that designates how to move from the current control
         * to the control that should be queried for errors.
         *
         * @usageNotes
         * For example, for the following `FormGroup`:
         *
         * ```
         * form = new FormGroup({
         *   address: new FormGroup({ street: new FormControl() })
         * });
         * ```
         *
         * The path to the 'street' control from the root form would be 'address' -> 'street'.
         *
         * It can be provided to this method in one of two formats:
         *
         * 1. An array of string control names, e.g. `['address', 'street']`
         * 1. A period-delimited list of control names in one string, e.g. `'address.street'`
         *
         * If no path is given, this method checks for the error on the current control.
         *
         * @returns whether the given error is present in the control at the given path.
         *
         * If the control is not present, false is returned.
         */
        AbstractControl.prototype.hasError = function (errorCode, path) {
            return !!this.getError(errorCode, path);
        };
        Object.defineProperty(AbstractControl.prototype, "root", {
            /**
             * Retrieves the top-level ancestor of this control.
             */
            get: function () {
                var x = this;
                while (x._parent) {
                    x = x._parent;
                }
                return x;
            },
            enumerable: true,
            configurable: true
        });
        /** @internal */
        AbstractControl.prototype._updateControlsErrors = function (emitEvent) {
            this.status = this._calculateStatus();
            if (emitEvent) {
                this.statusChanges.emit(this.status);
            }
            if (this._parent) {
                this._parent._updateControlsErrors(emitEvent);
            }
        };
        /** @internal */
        AbstractControl.prototype._initObservables = function () {
            this.valueChanges = new core.EventEmitter();
            this.statusChanges = new core.EventEmitter();
        };
        AbstractControl.prototype._calculateStatus = function () {
            if (this._allControlsDisabled())
                return DISABLED;
            if (this.errors)
                return INVALID;
            if (this._anyControlsHaveStatus(PENDING))
                return PENDING;
            if (this._anyControlsHaveStatus(INVALID))
                return INVALID;
            return VALID;
        };
        /** @internal */
        AbstractControl.prototype._anyControlsHaveStatus = function (status) {
            return this._anyControls(function (control) { return control.status === status; });
        };
        /** @internal */
        AbstractControl.prototype._anyControlsDirty = function () {
            return this._anyControls(function (control) { return control.dirty; });
        };
        /** @internal */
        AbstractControl.prototype._anyControlsTouched = function () {
            return this._anyControls(function (control) { return control.touched; });
        };
        /** @internal */
        AbstractControl.prototype._updatePristine = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.pristine = !this._anyControlsDirty();
            if (this._parent && !opts.onlySelf) {
                this._parent._updatePristine(opts);
            }
        };
        /** @internal */
        AbstractControl.prototype._updateTouched = function (opts) {
            if (opts === void 0) { opts = {}; }
            this.touched = this._anyControlsTouched();
            if (this._parent && !opts.onlySelf) {
                this._parent._updateTouched(opts);
            }
        };
        /** @internal */
        AbstractControl.prototype._isBoxedValue = function (formState) {
            return typeof formState === 'object' && formState !== null &&
                Object.keys(formState).length === 2 && 'value' in formState && 'disabled' in formState;
        };
        /** @internal */
        AbstractControl.prototype._registerOnCollectionChange = function (fn) { this._onCollectionChange = fn; };
        /** @internal */
        AbstractControl.prototype._setUpdateStrategy = function (opts) {
            if (isOptionsObj(opts) && opts.updateOn != null) {
                this._updateOn = opts.updateOn;
            }
        };
        /**
         * Check to see if parent has been marked artificially dirty.
         *
         * @internal
         */
        AbstractControl.prototype._parentMarkedDirty = function (onlySelf) {
            var parentDirty = this._parent && this._parent.dirty;
            return !onlySelf && parentDirty && !this._parent._anyControlsDirty();
        };
        return AbstractControl;
    }());
    /**
     * Tracks the value and validation status of an individual form control.
     *
     * This is one of the three fundamental building blocks of Angular forms, along with
     * `FormGroup` and `FormArray`. It extends the `AbstractControl` class that
     * implements most of the base functionality for accessing the value, validation status,
     * user interactions and events.
     *
     * @see `AbstractControl`
     * @see [Reactive Forms Guide](guide/reactive-forms)
     * @see [Usage Notes](#usage-notes)
     *
     * @usageNotes
     *
     * ### Initializing Form Controls
     *
     * Instantiate a `FormControl`, with an initial value.
     *
     * ```ts
     * const control = new FormControl('some value');
     * console.log(control.value);     // 'some value'
     *```
     *
     * The following example initializes the control with a form state object. The `value`
     * and `disabled` keys are required in this case.
     *
     * ```ts
     * const control = new FormControl({ value: 'n/a', disabled: true });
     * console.log(control.value);     // 'n/a'
     * console.log(control.status);    // 'DISABLED'
     * ```
     *
     * The following example initializes the control with a sync validator.
     *
     * ```ts
     * const control = new FormControl('', Validators.required);
     * console.log(control.value);      // ''
     * console.log(control.status);     // 'INVALID'
     * ```
     *
     * The following example initializes the control using an options object.
     *
     * ```ts
     * const control = new FormControl('', {
     *    validators: Validators.required,
     *    asyncValidators: myAsyncValidator
     * });
     * ```
     *
     * ### Configure the control to update on a blur event
     *
     * Set the `updateOn` option to `'blur'` to update on the blur `event`.
     *
     * ```ts
     * const control = new FormControl('', { updateOn: 'blur' });
     * ```
     *
     * ### Configure the control to update on a submit event
     *
     * Set the `updateOn` option to `'submit'` to update on a submit `event`.
     *
     * ```ts
     * const control = new FormControl('', { updateOn: 'submit' });
     * ```
     *
     * ### Reset the control back to an initial value
     *
     * You reset to a specific form state by passing through a standalone
     * value or a form state object that contains both a value and a disabled state
     * (these are the only two properties that cannot be calculated).
     *
     * ```ts
     * const control = new FormControl('Nancy');
     *
     * console.log(control.value); // 'Nancy'
     *
     * control.reset('Drew');
     *
     * console.log(control.value); // 'Drew'
     * ```
     *
     * ### Reset the control back to an initial value and disabled
     *
     * ```
     * const control = new FormControl('Nancy');
     *
     * console.log(control.value); // 'Nancy'
     * console.log(control.status); // 'VALID'
     *
     * control.reset({ value: 'Drew', disabled: true });
     *
     * console.log(control.value); // 'Drew'
     * console.log(control.status); // 'DISABLED'
     * ```
     *
     * @publicApi
     */
    var FormControl = /** @class */ (function (_super) {
        __extends(FormControl, _super);
        /**
        * Creates a new `FormControl` instance.
        *
        * @param formState Initializes the control with an initial value,
        * or an object that defines the initial value and disabled state.
        *
        * @param validatorOrOpts A synchronous validator function, or an array of
        * such functions, or an `AbstractControlOptions` object that contains validation functions
        * and a validation trigger.
        *
        * @param asyncValidator A single async validator or array of async validator functions
        *
        */
        function FormControl(formState, validatorOrOpts, asyncValidator) {
            if (formState === void 0) { formState = null; }
            var _this = _super.call(this, coerceToValidator(validatorOrOpts), coerceToAsyncValidator(asyncValidator, validatorOrOpts)) || this;
            /** @internal */
            _this._onChange = [];
            _this._applyFormState(formState);
            _this._setUpdateStrategy(validatorOrOpts);
            _this.updateValueAndValidity({ onlySelf: true, emitEvent: false });
            _this._initObservables();
            return _this;
        }
        /**
         * Sets a new value for the form control.
         *
         * @param value The new value for the control.
         * @param options Configuration options that determine how the control propagates changes
         * and emits events when the value changes.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         *
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default is
         * false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control value is updated.
         * When false, no events are emitted.
         * * `emitModelToViewChange`: When true or not supplied  (the default), each change triggers an
         * `onChange` event to
         * update the view.
         * * `emitViewToModelChange`: When true or not supplied (the default), each change triggers an
         * `ngModelChange`
         * event to update the model.
         *
         */
        FormControl.prototype.setValue = function (value, options) {
            var _this = this;
            if (options === void 0) { options = {}; }
            this.value = this._pendingValue = value;
            if (this._onChange.length && options.emitModelToViewChange !== false) {
                this._onChange.forEach(function (changeFn) { return changeFn(_this.value, options.emitViewToModelChange !== false); });
            }
            this.updateValueAndValidity(options);
        };
        /**
         * Patches the value of a control.
         *
         * This function is functionally the same as {@link FormControl#setValue setValue} at this level.
         * It exists for symmetry with {@link FormGroup#patchValue patchValue} on `FormGroups` and
         * `FormArrays`, where it does behave differently.
         *
         * @see `setValue` for options
         */
        FormControl.prototype.patchValue = function (value, options) {
            if (options === void 0) { options = {}; }
            this.setValue(value, options);
        };
        /**
         * Resets the form control, marking it `pristine` and `untouched`, and setting
         * the value to null.
         *
         * @param formState Resets the control with an initial value,
         * or an object that defines the initial value and disabled state.
         *
         * @param options Configuration options that determine how the control propagates changes
         * and emits events after the value changes.
         *
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default is
         * false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control is reset.
         * When false, no events are emitted.
         *
         */
        FormControl.prototype.reset = function (formState, options) {
            if (formState === void 0) { formState = null; }
            if (options === void 0) { options = {}; }
            this._applyFormState(formState);
            this.markAsPristine(options);
            this.markAsUntouched(options);
            this.setValue(this.value, options);
            this._pendingChange = false;
        };
        /**
         * @internal
         */
        FormControl.prototype._updateValue = function () { };
        /**
         * @internal
         */
        FormControl.prototype._anyControls = function (condition) { return false; };
        /**
         * @internal
         */
        FormControl.prototype._allControlsDisabled = function () { return this.disabled; };
        /**
         * Register a listener for change events.
         *
         * @param fn The method that is called when the value changes
         */
        FormControl.prototype.registerOnChange = function (fn) { this._onChange.push(fn); };
        /**
         * @internal
         */
        FormControl.prototype._clearChangeFns = function () {
            this._onChange = [];
            this._onDisabledChange = [];
            this._onCollectionChange = function () { };
        };
        /**
         * Register a listener for disabled events.
         *
         * @param fn The method that is called when the disabled status changes.
         */
        FormControl.prototype.registerOnDisabledChange = function (fn) {
            this._onDisabledChange.push(fn);
        };
        /**
         * @internal
         */
        FormControl.prototype._forEachChild = function (cb) { };
        /** @internal */
        FormControl.prototype._syncPendingControls = function () {
            if (this.updateOn === 'submit') {
                if (this._pendingDirty)
                    this.markAsDirty();
                if (this._pendingTouched)
                    this.markAsTouched();
                if (this._pendingChange) {
                    this.setValue(this._pendingValue, { onlySelf: true, emitModelToViewChange: false });
                    return true;
                }
            }
            return false;
        };
        FormControl.prototype._applyFormState = function (formState) {
            if (this._isBoxedValue(formState)) {
                this.value = this._pendingValue = formState.value;
                formState.disabled ? this.disable({ onlySelf: true, emitEvent: false }) :
                    this.enable({ onlySelf: true, emitEvent: false });
            }
            else {
                this.value = this._pendingValue = formState;
            }
        };
        return FormControl;
    }(AbstractControl));
    /**
     * Tracks the value and validity state of a group of `FormControl` instances.
     *
     * A `FormGroup` aggregates the values of each child `FormControl` into one object,
     * with each control name as the key.  It calculates its status by reducing the status values
     * of its children. For example, if one of the controls in a group is invalid, the entire
     * group becomes invalid.
     *
     * `FormGroup` is one of the three fundamental building blocks used to define forms in Angular,
     * along with `FormControl` and `FormArray`.
     *
     * When instantiating a `FormGroup`, pass in a collection of child controls as the first
     * argument. The key for each child registers the name for the control.
     *
     * @usageNotes
     *
     * ### Create a form group with 2 controls
     *
     * ```
     * const form = new FormGroup({
     *   first: new FormControl('Nancy', Validators.minLength(2)),
     *   last: new FormControl('Drew'),
     * });
     *
     * console.log(form.value);   // {first: 'Nancy', last; 'Drew'}
     * console.log(form.status);  // 'VALID'
     * ```
     *
     * ### Create a form group with a group-level validator
     *
     * You include group-level validators as the second arg, or group-level async
     * validators as the third arg. These come in handy when you want to perform validation
     * that considers the value of more than one child control.
     *
     * ```
     * const form = new FormGroup({
     *   password: new FormControl('', Validators.minLength(2)),
     *   passwordConfirm: new FormControl('', Validators.minLength(2)),
     * }, passwordMatchValidator);
     *
     *
     * function passwordMatchValidator(g: FormGroup) {
     *    return g.get('password').value === g.get('passwordConfirm').value
     *       ? null : {'mismatch': true};
     * }
     * ```
     *
     * Like `FormControl` instances, you choose to pass in
     * validators and async validators as part of an options object.
     *
     * ```
     * const form = new FormGroup({
     *   password: new FormControl('')
     *   passwordConfirm: new FormControl('')
     * }, { validators: passwordMatchValidator, asyncValidators: otherValidator });
     * ```
     *
     * ### Set the updateOn property for all controls in a form group
     *
     * The options object is used to set a default value for each child
     * control's `updateOn` property. If you set `updateOn` to `'blur'` at the
     * group level, all child controls default to 'blur', unless the child
     * has explicitly specified a different `updateOn` value.
     *
     * ```ts
     * const c = new FormGroup({
     *   one: new FormControl()
     * }, { updateOn: 'blur' });
     * ```
     *
     * @publicApi
     */
    var FormGroup = /** @class */ (function (_super) {
        __extends(FormGroup, _super);
        /**
        * Creates a new `FormGroup` instance.
        *
        * @param controls A collection of child controls. The key for each child is the name
        * under which it is registered.
        *
        * @param validatorOrOpts A synchronous validator function, or an array of
        * such functions, or an `AbstractControlOptions` object that contains validation functions
        * and a validation trigger.
        *
        * @param asyncValidator A single async validator or array of async validator functions
        *
        */
        function FormGroup(controls, validatorOrOpts, asyncValidator) {
            var _this = _super.call(this, coerceToValidator(validatorOrOpts), coerceToAsyncValidator(asyncValidator, validatorOrOpts)) || this;
            _this.controls = controls;
            _this._initObservables();
            _this._setUpdateStrategy(validatorOrOpts);
            _this._setUpControls();
            _this.updateValueAndValidity({ onlySelf: true, emitEvent: false });
            return _this;
        }
        /**
         * Registers a control with the group's list of controls.
         *
         * This method does not update the value or validity of the control.
         * Use {@link FormGroup#addControl addControl} instead.
         *
         * @param name The control name to register in the collection
         * @param control Provides the control for the given name
         */
        FormGroup.prototype.registerControl = function (name, control) {
            if (this.controls[name])
                return this.controls[name];
            this.controls[name] = control;
            control.setParent(this);
            control._registerOnCollectionChange(this._onCollectionChange);
            return control;
        };
        /**
         * Add a control to this group.
         *
         * This method also updates the value and validity of the control.
         *
         * @param name The control name to add to the collection
         * @param control Provides the control for the given name
         */
        FormGroup.prototype.addControl = function (name, control) {
            this.registerControl(name, control);
            this.updateValueAndValidity();
            this._onCollectionChange();
        };
        /**
         * Remove a control from this group.
         *
         * @param name The control name to remove from the collection
         */
        FormGroup.prototype.removeControl = function (name) {
            if (this.controls[name])
                this.controls[name]._registerOnCollectionChange(function () { });
            delete (this.controls[name]);
            this.updateValueAndValidity();
            this._onCollectionChange();
        };
        /**
         * Replace an existing control.
         *
         * @param name The control name to replace in the collection
         * @param control Provides the control for the given name
         */
        FormGroup.prototype.setControl = function (name, control) {
            if (this.controls[name])
                this.controls[name]._registerOnCollectionChange(function () { });
            delete (this.controls[name]);
            if (control)
                this.registerControl(name, control);
            this.updateValueAndValidity();
            this._onCollectionChange();
        };
        /**
         * Check whether there is an enabled control with the given name in the group.
         *
         * Reports false for disabled controls. If you'd like to check for existence in the group
         * only, use {@link AbstractControl#get get} instead.
         *
         * @param controlName The control name to check for existence in the collection
         *
         * @returns false for disabled controls, true otherwise.
         */
        FormGroup.prototype.contains = function (controlName) {
            return this.controls.hasOwnProperty(controlName) && this.controls[controlName].enabled;
        };
        /**
         * Sets the value of the `FormGroup`. It accepts an object that matches
         * the structure of the group, with control names as keys.
         *
         * @usageNotes
         * ### Set the complete value for the form group
         *
         * ```
         * const form = new FormGroup({
         *   first: new FormControl(),
         *   last: new FormControl()
         * });
         *
         * console.log(form.value);   // {first: null, last: null}
         *
         * form.setValue({first: 'Nancy', last: 'Drew'});
         * console.log(form.value);   // {first: 'Nancy', last: 'Drew'}
         * ```
         *
         * @throws When strict checks fail, such as setting the value of a control
         * that doesn't exist or if you exclude a value of a control that does exist.
         *
         * @param value The new value for the control that matches the structure of the group.
         * @param options Configuration options that determine how the control propagates changes
         * and emits events after the value changes.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         *
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default is
         * false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control value is updated.
         * When false, no events are emitted.
         */
        FormGroup.prototype.setValue = function (value, options) {
            var _this = this;
            if (options === void 0) { options = {}; }
            this._checkAllValuesPresent(value);
            Object.keys(value).forEach(function (name) {
                _this._throwIfControlMissing(name);
                _this.controls[name].setValue(value[name], { onlySelf: true, emitEvent: options.emitEvent });
            });
            this.updateValueAndValidity(options);
        };
        /**
         * Patches the value of the `FormGroup`. It accepts an object with control
         * names as keys, and does its best to match the values to the correct controls
         * in the group.
         *
         * It accepts both super-sets and sub-sets of the group without throwing an error.
         *
         * @usageNotes
         * ### Patch the value for a form group
         *
         * ```
         * const form = new FormGroup({
         *    first: new FormControl(),
         *    last: new FormControl()
         * });
         * console.log(form.value);   // {first: null, last: null}
         *
         * form.patchValue({first: 'Nancy'});
         * console.log(form.value);   // {first: 'Nancy', last: null}
         * ```
         *
         * @param value The object that matches the structure of the group.
         * @param options Configuration options that determine how the control propagates changes and
         * emits events after the value is patched.
         * * `onlySelf`: When true, each change only affects this control and not its parent. Default is
         * true.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control value is updated.
         * When false, no events are emitted.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         */
        FormGroup.prototype.patchValue = function (value, options) {
            var _this = this;
            if (options === void 0) { options = {}; }
            Object.keys(value).forEach(function (name) {
                if (_this.controls[name]) {
                    _this.controls[name].patchValue(value[name], { onlySelf: true, emitEvent: options.emitEvent });
                }
            });
            this.updateValueAndValidity(options);
        };
        /**
         * Resets the `FormGroup`, marks all descendants are marked `pristine` and `untouched`, and
         * the value of all descendants to null.
         *
         * You reset to a specific form state by passing in a map of states
         * that matches the structure of your form, with control names as keys. The state
         * is a standalone value or a form state object with both a value and a disabled
         * status.
         *
         * @param value Resets the control with an initial value,
         * or an object that defines the initial value and disabled state.
         *
         * @param options Configuration options that determine how the control propagates changes
         * and emits events when the group is reset.
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default is
         * false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control is reset.
         * When false, no events are emitted.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         *
         * @usageNotes
         *
         * ### Reset the form group values
         *
         * ```ts
         * const form = new FormGroup({
         *   first: new FormControl('first name'),
         *   last: new FormControl('last name')
         * });
         *
         * console.log(form.value);  // {first: 'first name', last: 'last name'}
         *
         * form.reset({ first: 'name', last: 'last name' });
         *
         * console.log(form.value);  // {first: 'name', last: 'last name'}
         * ```
         *
         * ### Reset the form group values and disabled status
         *
         * ```
         * const form = new FormGroup({
         *   first: new FormControl('first name'),
         *   last: new FormControl('last name')
         * });
         *
         * form.reset({
         *   first: {value: 'name', disabled: true},
         *   last: 'last'
         * });
         *
         * console.log(this.form.value);  // {first: 'name', last: 'last name'}
         * console.log(this.form.get('first').status);  // 'DISABLED'
         * ```
         */
        FormGroup.prototype.reset = function (value, options) {
            if (value === void 0) { value = {}; }
            if (options === void 0) { options = {}; }
            this._forEachChild(function (control, name) {
                control.reset(value[name], { onlySelf: true, emitEvent: options.emitEvent });
            });
            this._updatePristine(options);
            this._updateTouched(options);
            this.updateValueAndValidity(options);
        };
        /**
         * The aggregate value of the `FormGroup`, including any disabled controls.
         *
         * Retrieves all values regardless of disabled status.
         * The `value` property is the best way to get the value of the group, because
         * it excludes disabled controls in the `FormGroup`.
         */
        FormGroup.prototype.getRawValue = function () {
            return this._reduceChildren({}, function (acc, control, name) {
                acc[name] = control instanceof FormControl ? control.value : control.getRawValue();
                return acc;
            });
        };
        /** @internal */
        FormGroup.prototype._syncPendingControls = function () {
            var subtreeUpdated = this._reduceChildren(false, function (updated, child) {
                return child._syncPendingControls() ? true : updated;
            });
            if (subtreeUpdated)
                this.updateValueAndValidity({ onlySelf: true });
            return subtreeUpdated;
        };
        /** @internal */
        FormGroup.prototype._throwIfControlMissing = function (name) {
            if (!Object.keys(this.controls).length) {
                throw new Error("\n        There are no form controls registered with this group yet.  If you're using ngModel,\n        you may want to check next tick (e.g. use setTimeout).\n      ");
            }
            if (!this.controls[name]) {
                throw new Error("Cannot find form control with name: " + name + ".");
            }
        };
        /** @internal */
        FormGroup.prototype._forEachChild = function (cb) {
            var _this = this;
            Object.keys(this.controls).forEach(function (k) { return cb(_this.controls[k], k); });
        };
        /** @internal */
        FormGroup.prototype._setUpControls = function () {
            var _this = this;
            this._forEachChild(function (control) {
                control.setParent(_this);
                control._registerOnCollectionChange(_this._onCollectionChange);
            });
        };
        /** @internal */
        FormGroup.prototype._updateValue = function () { this.value = this._reduceValue(); };
        /** @internal */
        FormGroup.prototype._anyControls = function (condition) {
            var _this = this;
            var res = false;
            this._forEachChild(function (control, name) {
                res = res || (_this.contains(name) && condition(control));
            });
            return res;
        };
        /** @internal */
        FormGroup.prototype._reduceValue = function () {
            var _this = this;
            return this._reduceChildren({}, function (acc, control, name) {
                if (control.enabled || _this.disabled) {
                    acc[name] = control.value;
                }
                return acc;
            });
        };
        /** @internal */
        FormGroup.prototype._reduceChildren = function (initValue, fn) {
            var res = initValue;
            this._forEachChild(function (control, name) { res = fn(res, control, name); });
            return res;
        };
        /** @internal */
        FormGroup.prototype._allControlsDisabled = function () {
            var e_1, _a;
            try {
                for (var _b = __values(Object.keys(this.controls)), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var controlName = _c.value;
                    if (this.controls[controlName].enabled) {
                        return false;
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
            return Object.keys(this.controls).length > 0 || this.disabled;
        };
        /** @internal */
        FormGroup.prototype._checkAllValuesPresent = function (value) {
            this._forEachChild(function (control, name) {
                if (value[name] === undefined) {
                    throw new Error("Must supply a value for form control with name: '" + name + "'.");
                }
            });
        };
        return FormGroup;
    }(AbstractControl));
    /**
     * Tracks the value and validity state of an array of `FormControl`,
     * `FormGroup` or `FormArray` instances.
     *
     * A `FormArray` aggregates the values of each child `FormControl` into an array.
     * It calculates its status by reducing the status values of its children. For example, if one of
     * the controls in a `FormArray` is invalid, the entire array becomes invalid.
     *
     * `FormArray` is one of the three fundamental building blocks used to define forms in Angular,
     * along with `FormControl` and `FormGroup`.
     *
     * @usageNotes
     *
     * ### Create an array of form controls
     *
     * ```
     * const arr = new FormArray([
     *   new FormControl('Nancy', Validators.minLength(2)),
     *   new FormControl('Drew'),
     * ]);
     *
     * console.log(arr.value);   // ['Nancy', 'Drew']
     * console.log(arr.status);  // 'VALID'
     * ```
     *
     * ### Create a form array with array-level validators
     *
     * You include array-level validators and async validators. These come in handy
     * when you want to perform validation that considers the value of more than one child
     * control.
     *
     * The two types of validators are passed in separately as the second and third arg
     * respectively, or together as part of an options object.
     *
     * ```
     * const arr = new FormArray([
     *   new FormControl('Nancy'),
     *   new FormControl('Drew')
     * ], {validators: myValidator, asyncValidators: myAsyncValidator});
     * ```
     *
      * ### Set the updateOn property for all controls in a form array
     *
     * The options object is used to set a default value for each child
     * control's `updateOn` property. If you set `updateOn` to `'blur'` at the
     * array level, all child controls default to 'blur', unless the child
     * has explicitly specified a different `updateOn` value.
     *
     * ```ts
     * const arr = new FormArray([
     *    new FormControl()
     * ], {updateOn: 'blur'});
     * ```
     *
     * ### Adding or removing controls from a form array
     *
     * To change the controls in the array, use the `push`, `insert`, `removeAt` or `clear` methods
     * in `FormArray` itself. These methods ensure the controls are properly tracked in the
     * form's hierarchy. Do not modify the array of `AbstractControl`s used to instantiate
     * the `FormArray` directly, as that result in strange and unexpected behavior such
     * as broken change detection.
     *
     * @publicApi
     */
    var FormArray = /** @class */ (function (_super) {
        __extends(FormArray, _super);
        /**
        * Creates a new `FormArray` instance.
        *
        * @param controls An array of child controls. Each child control is given an index
        * where it is registered.
        *
        * @param validatorOrOpts A synchronous validator function, or an array of
        * such functions, or an `AbstractControlOptions` object that contains validation functions
        * and a validation trigger.
        *
        * @param asyncValidator A single async validator or array of async validator functions
        *
        */
        function FormArray(controls, validatorOrOpts, asyncValidator) {
            var _this = _super.call(this, coerceToValidator(validatorOrOpts), coerceToAsyncValidator(asyncValidator, validatorOrOpts)) || this;
            _this.controls = controls;
            _this._initObservables();
            _this._setUpdateStrategy(validatorOrOpts);
            _this._setUpControls();
            _this.updateValueAndValidity({ onlySelf: true, emitEvent: false });
            return _this;
        }
        /**
         * Get the `AbstractControl` at the given `index` in the array.
         *
         * @param index Index in the array to retrieve the control
         */
        FormArray.prototype.at = function (index) { return this.controls[index]; };
        /**
         * Insert a new `AbstractControl` at the end of the array.
         *
         * @param control Form control to be inserted
         */
        FormArray.prototype.push = function (control) {
            this.controls.push(control);
            this._registerControl(control);
            this.updateValueAndValidity();
            this._onCollectionChange();
        };
        /**
         * Insert a new `AbstractControl` at the given `index` in the array.
         *
         * @param index Index in the array to insert the control
         * @param control Form control to be inserted
         */
        FormArray.prototype.insert = function (index, control) {
            this.controls.splice(index, 0, control);
            this._registerControl(control);
            this.updateValueAndValidity();
        };
        /**
         * Remove the control at the given `index` in the array.
         *
         * @param index Index in the array to remove the control
         */
        FormArray.prototype.removeAt = function (index) {
            if (this.controls[index])
                this.controls[index]._registerOnCollectionChange(function () { });
            this.controls.splice(index, 1);
            this.updateValueAndValidity();
        };
        /**
         * Replace an existing control.
         *
         * @param index Index in the array to replace the control
         * @param control The `AbstractControl` control to replace the existing control
         */
        FormArray.prototype.setControl = function (index, control) {
            if (this.controls[index])
                this.controls[index]._registerOnCollectionChange(function () { });
            this.controls.splice(index, 1);
            if (control) {
                this.controls.splice(index, 0, control);
                this._registerControl(control);
            }
            this.updateValueAndValidity();
            this._onCollectionChange();
        };
        Object.defineProperty(FormArray.prototype, "length", {
            /**
             * Length of the control array.
             */
            get: function () { return this.controls.length; },
            enumerable: true,
            configurable: true
        });
        /**
         * Sets the value of the `FormArray`. It accepts an array that matches
         * the structure of the control.
         *
         * This method performs strict checks, and throws an error if you try
         * to set the value of a control that doesn't exist or if you exclude the
         * value of a control.
         *
         * @usageNotes
         * ### Set the values for the controls in the form array
         *
         * ```
         * const arr = new FormArray([
         *   new FormControl(),
         *   new FormControl()
         * ]);
         * console.log(arr.value);   // [null, null]
         *
         * arr.setValue(['Nancy', 'Drew']);
         * console.log(arr.value);   // ['Nancy', 'Drew']
         * ```
         *
         * @param value Array of values for the controls
         * @param options Configure options that determine how the control propagates changes and
         * emits events after the value changes
         *
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default
         * is false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control value is updated.
         * When false, no events are emitted.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         */
        FormArray.prototype.setValue = function (value, options) {
            var _this = this;
            if (options === void 0) { options = {}; }
            this._checkAllValuesPresent(value);
            value.forEach(function (newValue, index) {
                _this._throwIfControlMissing(index);
                _this.at(index).setValue(newValue, { onlySelf: true, emitEvent: options.emitEvent });
            });
            this.updateValueAndValidity(options);
        };
        /**
         * Patches the value of the `FormArray`. It accepts an array that matches the
         * structure of the control, and does its best to match the values to the correct
         * controls in the group.
         *
         * It accepts both super-sets and sub-sets of the array without throwing an error.
         *
         * @usageNotes
         * ### Patch the values for controls in a form array
         *
         * ```
         * const arr = new FormArray([
         *    new FormControl(),
         *    new FormControl()
         * ]);
         * console.log(arr.value);   // [null, null]
         *
         * arr.patchValue(['Nancy']);
         * console.log(arr.value);   // ['Nancy', null]
         * ```
         *
         * @param value Array of latest values for the controls
         * @param options Configure options that determine how the control propagates changes and
         * emits events after the value changes
         *
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default
         * is false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control value is updated.
         * When false, no events are emitted.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         */
        FormArray.prototype.patchValue = function (value, options) {
            var _this = this;
            if (options === void 0) { options = {}; }
            value.forEach(function (newValue, index) {
                if (_this.at(index)) {
                    _this.at(index).patchValue(newValue, { onlySelf: true, emitEvent: options.emitEvent });
                }
            });
            this.updateValueAndValidity(options);
        };
        /**
         * Resets the `FormArray` and all descendants are marked `pristine` and `untouched`, and the
         * value of all descendants to null or null maps.
         *
         * You reset to a specific form state by passing in an array of states
         * that matches the structure of the control. The state is a standalone value
         * or a form state object with both a value and a disabled status.
         *
         * @usageNotes
         * ### Reset the values in a form array
         *
         * ```ts
         * const arr = new FormArray([
         *    new FormControl(),
         *    new FormControl()
         * ]);
         * arr.reset(['name', 'last name']);
         *
         * console.log(this.arr.value);  // ['name', 'last name']
         * ```
         *
         * ### Reset the values in a form array and the disabled status for the first control
         *
         * ```
         * this.arr.reset([
         *   {value: 'name', disabled: true},
         *   'last'
         * ]);
         *
         * console.log(this.arr.value);  // ['name', 'last name']
         * console.log(this.arr.get(0).status);  // 'DISABLED'
         * ```
         *
         * @param value Array of values for the controls
         * @param options Configure options that determine how the control propagates changes and
         * emits events after the value changes
         *
         * * `onlySelf`: When true, each change only affects this control, and not its parent. Default
         * is false.
         * * `emitEvent`: When true or not supplied (the default), both the `statusChanges` and
         * `valueChanges`
         * observables emit events with the latest status and value when the control is reset.
         * When false, no events are emitted.
         * The configuration options are passed to the {@link AbstractControl#updateValueAndValidity
         * updateValueAndValidity} method.
         */
        FormArray.prototype.reset = function (value, options) {
            if (value === void 0) { value = []; }
            if (options === void 0) { options = {}; }
            this._forEachChild(function (control, index) {
                control.reset(value[index], { onlySelf: true, emitEvent: options.emitEvent });
            });
            this._updatePristine(options);
            this._updateTouched(options);
            this.updateValueAndValidity(options);
        };
        /**
         * The aggregate value of the array, including any disabled controls.
         *
         * Reports all values regardless of disabled status.
         * For enabled controls only, the `value` property is the best way to get the value of the array.
         */
        FormArray.prototype.getRawValue = function () {
            return this.controls.map(function (control) {
                return control instanceof FormControl ? control.value : control.getRawValue();
            });
        };
        /**
         * Remove all controls in the `FormArray`.
         *
         * @usageNotes
         * ### Remove all elements from a FormArray
         *
         * ```ts
         * const arr = new FormArray([
         *    new FormControl(),
         *    new FormControl()
         * ]);
         * console.log(arr.length);  // 2
         *
         * arr.clear();
         * console.log(arr.length);  // 0
         * ```
         *
         * It's a simpler and more efficient alternative to removing all elements one by one:
         *
         * ```ts
         * const arr = new FormArray([
         *    new FormControl(),
         *    new FormControl()
         * ]);
         *
         * while (arr.length) {
         *    arr.removeAt(0);
         * }
         * ```
         */
        FormArray.prototype.clear = function () {
            if (this.controls.length < 1)
                return;
            this._forEachChild(function (control) { return control._registerOnCollectionChange(function () { }); });
            this.controls.splice(0);
            this.updateValueAndValidity();
        };
        /** @internal */
        FormArray.prototype._syncPendingControls = function () {
            var subtreeUpdated = this.controls.reduce(function (updated, child) {
                return child._syncPendingControls() ? true : updated;
            }, false);
            if (subtreeUpdated)
                this.updateValueAndValidity({ onlySelf: true });
            return subtreeUpdated;
        };
        /** @internal */
        FormArray.prototype._throwIfControlMissing = function (index) {
            if (!this.controls.length) {
                throw new Error("\n        There are no form controls registered with this array yet.  If you're using ngModel,\n        you may want to check next tick (e.g. use setTimeout).\n      ");
            }
            if (!this.at(index)) {
                throw new Error("Cannot find form control at index " + index);
            }
        };
        /** @internal */
        FormArray.prototype._forEachChild = function (cb) {
            this.controls.forEach(function (control, index) { cb(control, index); });
        };
        /** @internal */
        FormArray.prototype._updateValue = function () {
            var _this = this;
            this.value =
                this.controls.filter(function (control) { return control.enabled || _this.disabled; })
                    .map(function (control) { return control.value; });
        };
        /** @internal */
        FormArray.prototype._anyControls = function (condition) {
            return this.controls.some(function (control) { return control.enabled && condition(control); });
        };
        /** @internal */
        FormArray.prototype._setUpControls = function () {
            var _this = this;
            this._forEachChild(function (control) { return _this._registerControl(control); });
        };
        /** @internal */
        FormArray.prototype._checkAllValuesPresent = function (value) {
            this._forEachChild(function (control, i) {
                if (value[i] === undefined) {
                    throw new Error("Must supply a value for form control at index: " + i + ".");
                }
            });
        };
        /** @internal */
        FormArray.prototype._allControlsDisabled = function () {
            var e_2, _a;
            try {
                for (var _b = __values(this.controls), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var control = _c.value;
                    if (control.enabled)
                        return false;
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                }
                finally { if (e_2) throw e_2.error; }
            }
            return this.controls.length > 0 || this.disabled;
        };
        FormArray.prototype._registerControl = function (control) {
            control.setParent(this);
            control._registerOnCollectionChange(this._onCollectionChange);
        };
        return FormArray;
    }(AbstractControl));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var formDirectiveProvider = {
        provide: ControlContainer,
        useExisting: core.forwardRef(function () { return NgForm; })
    };
    var ɵ0 = function () { return Promise.resolve(null); };
    var resolvedPromise = (ɵ0)();
    /**
     * @description
     * Creates a top-level `FormGroup` instance and binds it to a form
     * to track aggregate form value and validation status.
     *
     * As soon as you import the `FormsModule`, this directive becomes active by default on
     * all `<form>` tags.  You don't need to add a special selector.
     *
     * You optionally export the directive into a local template variable using `ngForm` as the key
     * (ex: `#myForm="ngForm"`). This is optional, but useful.  Many properties from the underlying
     * `FormGroup` instance are duplicated on the directive itself, so a reference to it
     * gives you access to the aggregate value and validity status of the form, as well as
     * user interaction properties like `dirty` and `touched`.
     *
     * To register child controls with the form, use `NgModel` with a `name`
     * attribute. You may use `NgModelGroup` to create sub-groups within the form.
     *
     * If necessary, listen to the directive's `ngSubmit` event to be notified when the user has
     * triggered a form submission. The `ngSubmit` event emits the original form
     * submission event.
     *
     * In template driven forms, all `<form>` tags are automatically tagged as `NgForm`.
     * To import the `FormsModule` but skip its usage in some forms,
     * for example, to use native HTML5 validation, add the `ngNoForm` and the `<form>`
     * tags won't create an `NgForm` directive. In reactive forms, using `ngNoForm` is
     * unnecessary because the `<form>` tags are inert. In that case, you would
     * refrain from using the `formGroup` directive.
     *
     * @usageNotes
     *
     * ### Migrating from deprecated ngForm selector
     *
     * Support for using `ngForm` element selector has been deprecated in Angular v6 and will be removed
     * in Angular v9.
     *
     * This has been deprecated to keep selectors consistent with other core Angular selectors,
     * as element selectors are typically written in kebab-case.
     *
     * Now deprecated:
     * ```html
     * <ngForm #myForm="ngForm">
     * ```
     *
     * After:
     * ```html
     * <ng-form #myForm="ngForm">
     * ```
     *
     * ### Listening for form submission
     *
     * The following example shows how to capture the form values from the "ngSubmit" event.
     *
     * {@example forms/ts/simpleForm/simple_form_example.ts region='Component'}
     *
     * ### Setting the update options
     *
     * The following example shows you how to change the "updateOn" option from its default using
     * ngFormOptions.
     *
     * ```html
     * <form [ngFormOptions]="{updateOn: 'blur'}">
     *    <input name="one" ngModel>  <!-- this ngModel will update on blur -->
     * </form>
     * ```
     *
     * @ngModule FormsModule
     * @publicApi
     */
    var NgForm = /** @class */ (function (_super) {
        __extends(NgForm, _super);
        function NgForm(validators, asyncValidators) {
            var _this = _super.call(this) || this;
            /**
             * @description
             * Returns whether the form submission has been triggered.
             */
            _this.submitted = false;
            _this._directives = [];
            /**
             * @description
             * Event emitter for the "ngSubmit" event
             */
            _this.ngSubmit = new core.EventEmitter();
            _this.form =
                new FormGroup({}, composeValidators(validators), composeAsyncValidators(asyncValidators));
            return _this;
        }
        /**
         * @description
         * Lifecycle method called after the view is initialized. For internal use only.
         */
        NgForm.prototype.ngAfterViewInit = function () { this._setUpdateStrategy(); };
        Object.defineProperty(NgForm.prototype, "formDirective", {
            /**
             * @description
             * The directive instance.
             */
            get: function () { return this; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgForm.prototype, "control", {
            /**
             * @description
             * The internal `FormGroup` instance.
             */
            get: function () { return this.form; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgForm.prototype, "path", {
            /**
             * @description
             * Returns an array representing the path to this group. Because this directive
             * always lives at the top level of a form, it is always an empty array.
             */
            get: function () { return []; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgForm.prototype, "controls", {
            /**
             * @description
             * Returns a map of the controls in this group.
             */
            get: function () { return this.form.controls; },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Method that sets up the control directive in this group, re-calculates its value
         * and validity, and adds the instance to the internal list of directives.
         *
         * @param dir The `NgModel` directive instance.
         */
        NgForm.prototype.addControl = function (dir) {
            var _this = this;
            resolvedPromise.then(function () {
                var container = _this._findContainer(dir.path);
                dir.control =
                    container.registerControl(dir.name, dir.control);
                setUpControl(dir.control, dir);
                dir.control.updateValueAndValidity({ emitEvent: false });
                _this._directives.push(dir);
            });
        };
        /**
         * @description
         * Retrieves the `FormControl` instance from the provided `NgModel` directive.
         *
         * @param dir The `NgModel` directive instance.
         */
        NgForm.prototype.getControl = function (dir) { return this.form.get(dir.path); };
        /**
         * @description
         * Removes the `NgModel` instance from the internal list of directives
         *
         * @param dir The `NgModel` directive instance.
         */
        NgForm.prototype.removeControl = function (dir) {
            var _this = this;
            resolvedPromise.then(function () {
                var container = _this._findContainer(dir.path);
                if (container) {
                    container.removeControl(dir.name);
                }
                removeDir(_this._directives, dir);
            });
        };
        /**
         * @description
         * Adds a new `NgModelGroup` directive instance to the form.
         *
         * @param dir The `NgModelGroup` directive instance.
         */
        NgForm.prototype.addFormGroup = function (dir) {
            var _this = this;
            resolvedPromise.then(function () {
                var container = _this._findContainer(dir.path);
                var group = new FormGroup({});
                setUpFormContainer(group, dir);
                container.registerControl(dir.name, group);
                group.updateValueAndValidity({ emitEvent: false });
            });
        };
        /**
         * @description
         * Removes the `NgModelGroup` directive instance from the form.
         *
         * @param dir The `NgModelGroup` directive instance.
         */
        NgForm.prototype.removeFormGroup = function (dir) {
            var _this = this;
            resolvedPromise.then(function () {
                var container = _this._findContainer(dir.path);
                if (container) {
                    container.removeControl(dir.name);
                }
            });
        };
        /**
         * @description
         * Retrieves the `FormGroup` for a provided `NgModelGroup` directive instance
         *
         * @param dir The `NgModelGroup` directive instance.
         */
        NgForm.prototype.getFormGroup = function (dir) { return this.form.get(dir.path); };
        /**
         * Sets the new value for the provided `NgControl` directive.
         *
         * @param dir The `NgControl` directive instance.
         * @param value The new value for the directive's control.
         */
        NgForm.prototype.updateModel = function (dir, value) {
            var _this = this;
            resolvedPromise.then(function () {
                var ctrl = _this.form.get(dir.path);
                ctrl.setValue(value);
            });
        };
        /**
         * @description
         * Sets the value for this `FormGroup`.
         *
         * @param value The new value
         */
        NgForm.prototype.setValue = function (value) { this.control.setValue(value); };
        /**
         * @description
         * Method called when the "submit" event is triggered on the form.
         * Triggers the `ngSubmit` emitter to emit the "submit" event as its payload.
         *
         * @param $event The "submit" event object
         */
        NgForm.prototype.onSubmit = function ($event) {
            this.submitted = true;
            syncPendingControls(this.form, this._directives);
            this.ngSubmit.emit($event);
            return false;
        };
        /**
         * @description
         * Method called when the "reset" event is triggered on the form.
         */
        NgForm.prototype.onReset = function () { this.resetForm(); };
        /**
         * @description
         * Resets the form to an initial value and resets its submitted status.
         *
         * @param value The new value for the form.
         */
        NgForm.prototype.resetForm = function (value) {
            if (value === void 0) { value = undefined; }
            this.form.reset(value);
            this.submitted = false;
        };
        NgForm.prototype._setUpdateStrategy = function () {
            if (this.options && this.options.updateOn != null) {
                this.form._updateOn = this.options.updateOn;
            }
        };
        /** @internal */
        NgForm.prototype._findContainer = function (path) {
            path.pop();
            return path.length ? this.form.get(path) : this.form;
        };
        __decorate([
            core.Input('ngFormOptions'),
            __metadata("design:type", Object)
        ], NgForm.prototype, "options", void 0);
        NgForm = __decorate([
            core.Directive({
                selector: 'form:not([ngNoForm]):not([formGroup]),ngForm,ng-form,[ngForm]',
                providers: [formDirectiveProvider],
                host: { '(submit)': 'onSubmit($event)', '(reset)': 'onReset()' },
                outputs: ['ngSubmit'],
                exportAs: 'ngForm'
            }),
            __param(0, core.Optional()), __param(0, core.Self()), __param(0, core.Inject(NG_VALIDATORS)),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_ASYNC_VALIDATORS)),
            __metadata("design:paramtypes", [Array, Array])
        ], NgForm);
        return NgForm;
    }(ControlContainer));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var TemplateDrivenErrors = /** @class */ (function () {
        function TemplateDrivenErrors() {
        }
        TemplateDrivenErrors.modelParentException = function () {
            throw new Error("\n      ngModel cannot be used to register form controls with a parent formGroup directive.  Try using\n      formGroup's partner directive \"formControlName\" instead.  Example:\n\n      " + FormErrorExamples.formControlName + "\n\n      Or, if you'd like to avoid registering this form control, indicate that it's standalone in ngModelOptions:\n\n      Example:\n\n      " + FormErrorExamples.ngModelWithFormGroup);
        };
        TemplateDrivenErrors.formGroupNameException = function () {
            throw new Error("\n      ngModel cannot be used to register form controls with a parent formGroupName or formArrayName directive.\n\n      Option 1: Use formControlName instead of ngModel (reactive strategy):\n\n      " + FormErrorExamples.formGroupName + "\n\n      Option 2:  Update ngModel's parent be ngModelGroup (template-driven strategy):\n\n      " + FormErrorExamples.ngModelGroup);
        };
        TemplateDrivenErrors.missingNameException = function () {
            throw new Error("If ngModel is used within a form tag, either the name attribute must be set or the form\n      control must be defined as 'standalone' in ngModelOptions.\n\n      Example 1: <input [(ngModel)]=\"person.firstName\" name=\"first\">\n      Example 2: <input [(ngModel)]=\"person.firstName\" [ngModelOptions]=\"{standalone: true}\">");
        };
        TemplateDrivenErrors.modelGroupParentException = function () {
            throw new Error("\n      ngModelGroup cannot be used with a parent formGroup directive.\n\n      Option 1: Use formGroupName instead of ngModelGroup (reactive strategy):\n\n      " + FormErrorExamples.formGroupName + "\n\n      Option 2:  Use a regular form tag instead of the formGroup directive (template-driven strategy):\n\n      " + FormErrorExamples.ngModelGroup);
        };
        TemplateDrivenErrors.ngFormWarning = function () {
            console.warn("\n    It looks like you're using 'ngForm'.\n\n    Support for using the 'ngForm' element selector has been deprecated in Angular v6 and will be removed\n    in Angular v9.\n\n    Use 'ng-form' instead.\n\n    Before:\n    <ngForm #myForm=\"ngForm\">\n\n    After:\n    <ng-form #myForm=\"ngForm\">\n    ");
        };
        return TemplateDrivenErrors;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @description
     * `InjectionToken` to provide to turn off the warning when using 'ngForm' deprecated selector.
     */
    var NG_FORM_SELECTOR_WARNING = new core.InjectionToken('NgFormSelectorWarning');
    /**
     * This directive is solely used to display warnings when the deprecated `ngForm` selector is used.
     *
     * @deprecated in Angular v6 and will be removed in Angular v9.
     * @ngModule FormsModule
     * @publicApi
     */
    var NgFormSelectorWarning = /** @class */ (function () {
        function NgFormSelectorWarning(ngFormWarning) {
            if (((!ngFormWarning || ngFormWarning === 'once') && !NgFormSelectorWarning_1._ngFormWarning) ||
                ngFormWarning === 'always') {
                TemplateDrivenErrors.ngFormWarning();
                NgFormSelectorWarning_1._ngFormWarning = true;
            }
        }
        NgFormSelectorWarning_1 = NgFormSelectorWarning;
        var NgFormSelectorWarning_1;
        /**
         * Static property used to track whether the deprecation warning for this selector has been sent.
         * Used to support warning config of "once".
         *
         * @internal
         */
        NgFormSelectorWarning._ngFormWarning = false;
        NgFormSelectorWarning = NgFormSelectorWarning_1 = __decorate([
            core.Directive({ selector: 'ngForm' }),
            __param(0, core.Optional()), __param(0, core.Inject(NG_FORM_SELECTOR_WARNING)),
            __metadata("design:paramtypes", [Object])
        ], NgFormSelectorWarning);
        return NgFormSelectorWarning;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @description
     * A base class for code shared between the `NgModelGroup` and `FormGroupName` directives.
     *
     * @publicApi
     */
    var AbstractFormGroupDirective = /** @class */ (function (_super) {
        __extends(AbstractFormGroupDirective, _super);
        function AbstractFormGroupDirective() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        /**
         * @description
         * An internal callback method triggered on the instance after the inputs are set.
         * Registers the group with its parent group.
         */
        AbstractFormGroupDirective.prototype.ngOnInit = function () {
            this._checkParentType();
            this.formDirective.addFormGroup(this);
        };
        /**
         * @description
         * An internal callback method triggered before the instance is destroyed.
         * Removes the group from its parent group.
         */
        AbstractFormGroupDirective.prototype.ngOnDestroy = function () {
            if (this.formDirective) {
                this.formDirective.removeFormGroup(this);
            }
        };
        Object.defineProperty(AbstractFormGroupDirective.prototype, "control", {
            /**
             * @description
             * The `FormGroup` bound to this directive.
             */
            get: function () { return this.formDirective.getFormGroup(this); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractFormGroupDirective.prototype, "path", {
            /**
             * @description
             * The path to this group from the top-level directive.
             */
            get: function () { return controlPath(this.name, this._parent); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractFormGroupDirective.prototype, "formDirective", {
            /**
             * @description
             * The top-level directive for this group if present, otherwise null.
             */
            get: function () { return this._parent ? this._parent.formDirective : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractFormGroupDirective.prototype, "validator", {
            /**
             * @description
             * The synchronous validators registered with this group.
             */
            get: function () { return composeValidators(this._validators); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(AbstractFormGroupDirective.prototype, "asyncValidator", {
            /**
             * @description
             * The async validators registered with this group.
             */
            get: function () {
                return composeAsyncValidators(this._asyncValidators);
            },
            enumerable: true,
            configurable: true
        });
        /** @internal */
        AbstractFormGroupDirective.prototype._checkParentType = function () { };
        return AbstractFormGroupDirective;
    }(ControlContainer));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var modelGroupProvider = {
        provide: ControlContainer,
        useExisting: core.forwardRef(function () { return NgModelGroup; })
    };
    /**
     * @description
     * Creates and binds a `FormGroup` instance to a DOM element.
     *
     * This directive can only be used as a child of `NgForm` (within `<form>` tags).
     *
     * Use this directive to validate a sub-group of your form separately from the
     * rest of your form, or if some values in your domain model make more sense
     * to consume together in a nested object.
     *
     * Provide a name for the sub-group and it will become the key
     * for the sub-group in the form's full value. If you need direct access, export the directive into
     * a local template variable using `ngModelGroup` (ex: `#myGroup="ngModelGroup"`).
     *
     * @usageNotes
     *
     * ### Consuming controls in a grouping
     *
     * The following example shows you how to combine controls together in a sub-group
     * of the form.
     *
     * {@example forms/ts/ngModelGroup/ng_model_group_example.ts region='Component'}
     *
     * @ngModule FormsModule
     * @publicApi
     */
    var NgModelGroup = /** @class */ (function (_super) {
        __extends(NgModelGroup, _super);
        function NgModelGroup(parent, validators, asyncValidators) {
            var _this = _super.call(this) || this;
            _this._parent = parent;
            _this._validators = validators;
            _this._asyncValidators = asyncValidators;
            return _this;
        }
        NgModelGroup_1 = NgModelGroup;
        /** @internal */
        NgModelGroup.prototype._checkParentType = function () {
            if (!(this._parent instanceof NgModelGroup_1) && !(this._parent instanceof NgForm)) {
                TemplateDrivenErrors.modelGroupParentException();
            }
        };
        var NgModelGroup_1;
        __decorate([
            core.Input('ngModelGroup'),
            __metadata("design:type", String)
        ], NgModelGroup.prototype, "name", void 0);
        NgModelGroup = NgModelGroup_1 = __decorate([
            core.Directive({ selector: '[ngModelGroup]', providers: [modelGroupProvider], exportAs: 'ngModelGroup' }),
            __param(0, core.Host()), __param(0, core.SkipSelf()),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_VALIDATORS)),
            __param(2, core.Optional()), __param(2, core.Self()), __param(2, core.Inject(NG_ASYNC_VALIDATORS)),
            __metadata("design:paramtypes", [ControlContainer, Array, Array])
        ], NgModelGroup);
        return NgModelGroup;
    }(AbstractFormGroupDirective));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var formControlBinding = {
        provide: NgControl,
        useExisting: core.forwardRef(function () { return NgModel; })
    };
    var ɵ0$1 = function () { return Promise.resolve(null); };
    /**
     * `ngModel` forces an additional change detection run when its inputs change:
     * E.g.:
     * ```
     * <div>{{myModel.valid}}</div>
     * <input [(ngModel)]="myValue" #myModel="ngModel">
     * ```
     * I.e. `ngModel` can export itself on the element and then be used in the template.
     * Normally, this would result in expressions before the `input` that use the exported directive
     * to have and old value as they have been
     * dirty checked before. As this is a very common case for `ngModel`, we added this second change
     * detection run.
     *
     * Notes:
     * - this is just one extra run no matter how many `ngModel` have been changed.
     * - this is a general problem when using `exportAs` for directives!
     */
    var resolvedPromise$1 = (ɵ0$1)();
    /**
     * @description
     * Creates a `FormControl` instance from a domain model and binds it
     * to a form control element.
     *
     * The `FormControl` instance tracks the value, user interaction, and
     * validation status of the control and keeps the view synced with the model. If used
     * within a parent form, the directive also registers itself with the form as a child
     * control.
     *
     * This directive is used by itself or as part of a larger form. Use the
     * `ngModel` selector to activate it.
     *
     * It accepts a domain model as an optional `Input`. If you have a one-way binding
     * to `ngModel` with `[]` syntax, changing the value of the domain model in the component
     * class sets the value in the view. If you have a two-way binding with `[()]` syntax
     * (also known as 'banana-box syntax'), the value in the UI always syncs back to
     * the domain model in your class.
     *
     * To inspect the properties of the associated `FormControl` (like validity state),
     * export the directive into a local template variable using `ngModel` as the key (ex: `#myVar="ngModel"`).
     * You then access the control using the directive's `control` property,
     * but most properties used (like `valid` and `dirty`) fall through to the control anyway for direct access.
     * See a full list of properties directly available in `AbstractControlDirective`.
     *
     * @see `RadioControlValueAccessor`
     * @see `SelectControlValueAccessor`
     *
     * @usageNotes
     *
     * ### Using ngModel on a standalone control
     *
     * The following examples show a simple standalone control using `ngModel`:
     *
     * {@example forms/ts/simpleNgModel/simple_ng_model_example.ts region='Component'}
     *
     * When using the `ngModel` within `<form>` tags, you'll also need to supply a `name` attribute
     * so that the control can be registered with the parent form under that name.
     *
     * In the context of a parent form, it's often unnecessary to include one-way or two-way binding,
     * as the parent form syncs the value for you. You access its properties by exporting it into a
     * local template variable using `ngForm` such as (`#f="ngForm"`). Use the variable where
     * needed on form submission.
     *
     * If you do need to populate initial values into your form, using a one-way binding for
     * `ngModel` tends to be sufficient as long as you use the exported form's value rather
     * than the domain model's value on submit.
     *
     * ### Using ngModel within a form
     *
     * The following example shows controls using `ngModel` within a form:
     *
     * {@example forms/ts/simpleForm/simple_form_example.ts region='Component'}
     *
     * ### Using a standalone ngModel within a group
     *
     * The following example shows you how to use a standalone ngModel control
     * within a form. This controls the display of the form, but doesn't contain form data.
     *
     * ```html
     * <form>
     *   <input name="login" ngModel placeholder="Login">
     *   <input type="checkbox" ngModel [ngModelOptions]="{standalone: true}"> Show more options?
     * </form>
     * <!-- form value: {login: ''} -->
     * ```
     *
     * ### Setting the ngModel name attribute through options
     *
     * The following example shows you an alternate way to set the name attribute. The name attribute is used
     * within a custom form component, and the name `@Input` property serves a different purpose.
     *
     * ```html
     * <form>
     *   <my-person-control name="Nancy" ngModel [ngModelOptions]="{name: 'user'}">
     *   </my-person-control>
     * </form>
     * <!-- form value: {user: ''} -->
     * ```
     *
     * @ngModule FormsModule
     * @publicApi
     */
    var NgModel = /** @class */ (function (_super) {
        __extends(NgModel, _super);
        function NgModel(parent, validators, asyncValidators, valueAccessors) {
            var _this = _super.call(this) || this;
            _this.control = new FormControl();
            /** @internal */
            _this._registered = false;
            /**
             * @description
             * Event emitter for producing the `ngModelChange` event after
             * the view model updates.
             */
            _this.update = new core.EventEmitter();
            _this._parent = parent;
            _this._rawValidators = validators || [];
            _this._rawAsyncValidators = asyncValidators || [];
            _this.valueAccessor = selectValueAccessor(_this, valueAccessors);
            return _this;
        }
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use
         * only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        NgModel.prototype.ngOnChanges = function (changes) {
            this._checkForErrors();
            if (!this._registered)
                this._setUpControl();
            if ('isDisabled' in changes) {
                this._updateDisabled(changes);
            }
            if (isPropertyUpdated(changes, this.viewModel)) {
                this._updateValue(this.model);
                this.viewModel = this.model;
            }
        };
        /**
         * @description
         * Lifecycle method called before the directive's instance is destroyed. For internal
         * use only.
         */
        NgModel.prototype.ngOnDestroy = function () { this.formDirective && this.formDirective.removeControl(this); };
        Object.defineProperty(NgModel.prototype, "path", {
            /**
             * @description
             * Returns an array that represents the path from the top-level form to this control.
             * Each index is the string name of the control on that level.
             */
            get: function () {
                return this._parent ? controlPath(this.name, this._parent) : [this.name];
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgModel.prototype, "formDirective", {
            /**
             * @description
             * The top-level directive for this control if present, otherwise null.
             */
            get: function () { return this._parent ? this._parent.formDirective : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgModel.prototype, "validator", {
            /**
             * @description
             * Synchronous validator function composed of all the synchronous validators
             * registered with this directive.
             */
            get: function () { return composeValidators(this._rawValidators); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(NgModel.prototype, "asyncValidator", {
            /**
             * @description
             * Async validator function composed of all the async validators registered with this
             * directive.
             */
            get: function () {
                return composeAsyncValidators(this._rawAsyncValidators);
            },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Sets the new value for the view model and emits an `ngModelChange` event.
         *
         * @param newValue The new value emitted by `ngModelChange`.
         */
        NgModel.prototype.viewToModelUpdate = function (newValue) {
            this.viewModel = newValue;
            this.update.emit(newValue);
        };
        NgModel.prototype._setUpControl = function () {
            this._setUpdateStrategy();
            this._isStandalone() ? this._setUpStandalone() :
                this.formDirective.addControl(this);
            this._registered = true;
        };
        NgModel.prototype._setUpdateStrategy = function () {
            if (this.options && this.options.updateOn != null) {
                this.control._updateOn = this.options.updateOn;
            }
        };
        NgModel.prototype._isStandalone = function () {
            return !this._parent || !!(this.options && this.options.standalone);
        };
        NgModel.prototype._setUpStandalone = function () {
            setUpControl(this.control, this);
            this.control.updateValueAndValidity({ emitEvent: false });
        };
        NgModel.prototype._checkForErrors = function () {
            if (!this._isStandalone()) {
                this._checkParentType();
            }
            this._checkName();
        };
        NgModel.prototype._checkParentType = function () {
            if (!(this._parent instanceof NgModelGroup) &&
                this._parent instanceof AbstractFormGroupDirective) {
                TemplateDrivenErrors.formGroupNameException();
            }
            else if (!(this._parent instanceof NgModelGroup) && !(this._parent instanceof NgForm)) {
                TemplateDrivenErrors.modelParentException();
            }
        };
        NgModel.prototype._checkName = function () {
            if (this.options && this.options.name)
                this.name = this.options.name;
            if (!this._isStandalone() && !this.name) {
                TemplateDrivenErrors.missingNameException();
            }
        };
        NgModel.prototype._updateValue = function (value) {
            var _this = this;
            resolvedPromise$1.then(function () { _this.control.setValue(value, { emitViewToModelChange: false }); });
        };
        NgModel.prototype._updateDisabled = function (changes) {
            var _this = this;
            var disabledValue = changes['isDisabled'].currentValue;
            var isDisabled = disabledValue === '' || (disabledValue && disabledValue !== 'false');
            resolvedPromise$1.then(function () {
                if (isDisabled && !_this.control.disabled) {
                    _this.control.disable();
                }
                else if (!isDisabled && _this.control.disabled) {
                    _this.control.enable();
                }
            });
        };
        __decorate([
            core.Input(),
            __metadata("design:type", String)
        ], NgModel.prototype, "name", void 0);
        __decorate([
            core.Input('disabled'),
            __metadata("design:type", Boolean)
        ], NgModel.prototype, "isDisabled", void 0);
        __decorate([
            core.Input('ngModel'),
            __metadata("design:type", Object)
        ], NgModel.prototype, "model", void 0);
        __decorate([
            core.Input('ngModelOptions'),
            __metadata("design:type", Object)
        ], NgModel.prototype, "options", void 0);
        __decorate([
            core.Output('ngModelChange'),
            __metadata("design:type", Object)
        ], NgModel.prototype, "update", void 0);
        NgModel = __decorate([
            core.Directive({
                selector: '[ngModel]:not([formControlName]):not([formControl])',
                providers: [formControlBinding],
                exportAs: 'ngModel'
            }),
            __param(0, core.Optional()), __param(0, core.Host()),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_VALIDATORS)),
            __param(2, core.Optional()), __param(2, core.Self()), __param(2, core.Inject(NG_ASYNC_VALIDATORS)),
            __param(3, core.Optional()), __param(3, core.Self()), __param(3, core.Inject(NG_VALUE_ACCESSOR)),
            __metadata("design:paramtypes", [ControlContainer,
                Array,
                Array, Array])
        ], NgModel);
        return NgModel;
    }(NgControl));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @description
     *
     * Adds `novalidate` attribute to all forms by default.
     *
     * `novalidate` is used to disable browser's native form validation.
     *
     * If you want to use native validation with Angular forms, just add `ngNativeValidate` attribute:
     *
     * ```
     * <form ngNativeValidate></form>
     * ```
     *
     * @publicApi
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     */
    var ɵNgNoValidate = /** @class */ (function () {
        function ɵNgNoValidate() {
        }
        ɵNgNoValidate = __decorate([
            core.Directive({
                selector: 'form:not([ngNoForm]):not([ngNativeValidate])',
                host: { 'novalidate': '' },
            })
        ], ɵNgNoValidate);
        return ɵNgNoValidate;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * Token to provide to turn off the ngModel warning on formControl and formControlName.
     */
    var NG_MODEL_WITH_FORM_CONTROL_WARNING = new core.InjectionToken('NgModelWithFormControlWarning');
    var formControlBinding$1 = {
        provide: NgControl,
        useExisting: core.forwardRef(function () { return FormControlDirective; })
    };
    /**
     * @description
     * * Syncs a standalone `FormControl` instance to a form control element.
     *
     * @see [Reactive Forms Guide](guide/reactive-forms)
     * @see `FormControl`
     * @see `AbstractControl`
     *
     * @usageNotes
     *
     * ### Registering a single form control
     *
     * The following examples shows how to register a standalone control and set its value.
     *
     * {@example forms/ts/simpleFormControl/simple_form_control_example.ts region='Component'}
     *
     * ### Use with ngModel
     *
     * Support for using the `ngModel` input property and `ngModelChange` event with reactive
     * form directives has been deprecated in Angular v6 and will be removed in Angular v7.
     *
     * Now deprecated:
     *
     * ```html
     * <input [formControl]="control" [(ngModel)]="value">
     * ```
     *
     * ```ts
     * this.value = 'some value';
     * ```
     *
     * This has been deprecated for a few reasons. First, developers have found this pattern
     * confusing. It seems like the actual `ngModel` directive is being used, but in fact it's
     * an input/output property named `ngModel` on the reactive form directive that simply
     * approximates (some of) its behavior. Specifically, it allows getting/setting the value
     * and intercepting value events. However, some of `ngModel`'s other features - like
     * delaying updates with`ngModelOptions` or exporting the directive - simply don't work,
     * which has understandably caused some confusion.
     *
     * In addition, this pattern mixes template-driven and reactive forms strategies, which
     * we generally don't recommend because it doesn't take advantage of the full benefits of
     * either strategy. Setting the value in the template violates the template-agnostic
     * principles behind reactive forms, whereas adding a `FormControl`/`FormGroup` layer in
     * the class removes the convenience of defining forms in the template.
     *
     * To update your code before v7, you'll want to decide whether to stick with reactive form
     * directives (and get/set values using reactive forms patterns) or switch over to
     * template-driven directives.
     *
     * After (choice 1 - use reactive forms):
     *
     * ```html
     * <input [formControl]="control">
     * ```
     *
     * ```ts
     * this.control.setValue('some value');
     * ```
     *
     * After (choice 2 - use template-driven forms):
     *
     * ```html
     * <input [(ngModel)]="value">
     * ```
     *
     * ```ts
     * this.value = 'some value';
     * ```
     *
     * By default, when you use this pattern, you will see a deprecation warning once in dev
     * mode. You can choose to silence this warning by providing a config for
     * `ReactiveFormsModule` at import time:
     *
     * ```ts
     * imports: [
     *   ReactiveFormsModule.withConfig({warnOnNgModelWithFormControl: 'never'});
     * ]
     * ```
     *
     * Alternatively, you can choose to surface a separate warning for each instance of this
     * pattern with a config value of `"always"`. This may help to track down where in the code
     * the pattern is being used as the code is being updated.
     *
     * @ngModule ReactiveFormsModule
     * @publicApi
     */
    var FormControlDirective = /** @class */ (function (_super) {
        __extends(FormControlDirective, _super);
        function FormControlDirective(validators, asyncValidators, valueAccessors, _ngModelWarningConfig) {
            var _this = _super.call(this) || this;
            _this._ngModelWarningConfig = _ngModelWarningConfig;
            /** @deprecated as of v6 */
            _this.update = new core.EventEmitter();
            /**
             * @description
             * Instance property used to track whether an ngModel warning has been sent out for this
             * particular `FormControlDirective` instance. Used to support warning config of "always".
             *
             * @internal
             */
            _this._ngModelWarningSent = false;
            _this._rawValidators = validators || [];
            _this._rawAsyncValidators = asyncValidators || [];
            _this.valueAccessor = selectValueAccessor(_this, valueAccessors);
            return _this;
        }
        FormControlDirective_1 = FormControlDirective;
        Object.defineProperty(FormControlDirective.prototype, "isDisabled", {
            /**
             * @description
             * Triggers a warning that this input should not be used with reactive forms.
             */
            set: function (isDisabled) { ReactiveErrors.disabledAttrWarning(); },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use
         * only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        FormControlDirective.prototype.ngOnChanges = function (changes) {
            if (this._isControlChanged(changes)) {
                setUpControl(this.form, this);
                if (this.control.disabled && this.valueAccessor.setDisabledState) {
                    this.valueAccessor.setDisabledState(true);
                }
                this.form.updateValueAndValidity({ emitEvent: false });
            }
            if (isPropertyUpdated(changes, this.viewModel)) {
                _ngModelWarning('formControl', FormControlDirective_1, this, this._ngModelWarningConfig);
                this.form.setValue(this.model);
                this.viewModel = this.model;
            }
        };
        Object.defineProperty(FormControlDirective.prototype, "path", {
            /**
             * @description
             * Returns an array that represents the path from the top-level form to this control.
             * Each index is the string name of the control on that level.
             */
            get: function () { return []; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormControlDirective.prototype, "validator", {
            /**
             * @description
             * Synchronous validator function composed of all the synchronous validators
             * registered with this directive.
             */
            get: function () { return composeValidators(this._rawValidators); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormControlDirective.prototype, "asyncValidator", {
            /**
             * @description
             * Async validator function composed of all the async validators registered with this
             * directive.
             */
            get: function () {
                return composeAsyncValidators(this._rawAsyncValidators);
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormControlDirective.prototype, "control", {
            /**
             * @description
             * The `FormControl` bound to this directive.
             */
            get: function () { return this.form; },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Sets the new value for the view model and emits an `ngModelChange` event.
         *
         * @param newValue The new value for the view model.
         */
        FormControlDirective.prototype.viewToModelUpdate = function (newValue) {
            this.viewModel = newValue;
            this.update.emit(newValue);
        };
        FormControlDirective.prototype._isControlChanged = function (changes) {
            return changes.hasOwnProperty('form');
        };
        var FormControlDirective_1;
        /**
         * @description
         * Static property used to track whether any ngModel warnings have been sent across
         * all instances of FormControlDirective. Used to support warning config of "once".
         *
         * @internal
         */
        FormControlDirective._ngModelWarningSentOnce = false;
        __decorate([
            core.Input('formControl'),
            __metadata("design:type", FormControl)
        ], FormControlDirective.prototype, "form", void 0);
        __decorate([
            core.Input('disabled'),
            __metadata("design:type", Boolean),
            __metadata("design:paramtypes", [Boolean])
        ], FormControlDirective.prototype, "isDisabled", null);
        __decorate([
            core.Input('ngModel'),
            __metadata("design:type", Object)
        ], FormControlDirective.prototype, "model", void 0);
        __decorate([
            core.Output('ngModelChange'),
            __metadata("design:type", Object)
        ], FormControlDirective.prototype, "update", void 0);
        FormControlDirective = FormControlDirective_1 = __decorate([
            core.Directive({ selector: '[formControl]', providers: [formControlBinding$1], exportAs: 'ngForm' }),
            __param(0, core.Optional()), __param(0, core.Self()), __param(0, core.Inject(NG_VALIDATORS)),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_ASYNC_VALIDATORS)),
            __param(2, core.Optional()), __param(2, core.Self()), __param(2, core.Inject(NG_VALUE_ACCESSOR)),
            __param(3, core.Optional()), __param(3, core.Inject(NG_MODEL_WITH_FORM_CONTROL_WARNING)),
            __metadata("design:paramtypes", [Array,
                Array, Array, Object])
        ], FormControlDirective);
        return FormControlDirective;
    }(NgControl));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var formDirectiveProvider$1 = {
        provide: ControlContainer,
        useExisting: core.forwardRef(function () { return FormGroupDirective; })
    };
    /**
     * @description
     *
     * Binds an existing `FormGroup` to a DOM element.
     *
     * This directive accepts an existing `FormGroup` instance. It will then use this
     * `FormGroup` instance to match any child `FormControl`, `FormGroup`,
     * and `FormArray` instances to child `FormControlName`, `FormGroupName`,
     * and `FormArrayName` directives.
     *
     * @see [Reactive Forms Guide](guide/reactive-forms)
     * @see `AbstractControl`
     *
     * ### Register Form Group
     *
     * The following example registers a `FormGroup` with first name and last name controls,
     * and listens for the *ngSubmit* event when the button is clicked.
     *
     * {@example forms/ts/simpleFormGroup/simple_form_group_example.ts region='Component'}
     *
     * @ngModule ReactiveFormsModule
     * @publicApi
     */
    var FormGroupDirective = /** @class */ (function (_super) {
        __extends(FormGroupDirective, _super);
        function FormGroupDirective(_validators, _asyncValidators) {
            var _this = _super.call(this) || this;
            _this._validators = _validators;
            _this._asyncValidators = _asyncValidators;
            /**
             * @description
             * Reports whether the form submission has been triggered.
             */
            _this.submitted = false;
            /**
             * @description
             * Tracks the list of added `FormControlName` instances
             */
            _this.directives = [];
            /**
             * @description
             * Tracks the `FormGroup` bound to this directive.
             */
            _this.form = null;
            /**
             * @description
             * Emits an event when the form submission has been triggered.
             */
            _this.ngSubmit = new core.EventEmitter();
            return _this;
        }
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        FormGroupDirective.prototype.ngOnChanges = function (changes) {
            this._checkFormPresent();
            if (changes.hasOwnProperty('form')) {
                this._updateValidators();
                this._updateDomValue();
                this._updateRegistrations();
            }
        };
        Object.defineProperty(FormGroupDirective.prototype, "formDirective", {
            /**
             * @description
             * Returns this directive's instance.
             */
            get: function () { return this; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormGroupDirective.prototype, "control", {
            /**
             * @description
             * Returns the `FormGroup` bound to this directive.
             */
            get: function () { return this.form; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormGroupDirective.prototype, "path", {
            /**
             * @description
             * Returns an array representing the path to this group. Because this directive
             * always lives at the top level of a form, it always an empty array.
             */
            get: function () { return []; },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Method that sets up the control directive in this group, re-calculates its value
         * and validity, and adds the instance to the internal list of directives.
         *
         * @param dir The `FormControlName` directive instance.
         */
        FormGroupDirective.prototype.addControl = function (dir) {
            var ctrl = this.form.get(dir.path);
            setUpControl(ctrl, dir);
            ctrl.updateValueAndValidity({ emitEvent: false });
            this.directives.push(dir);
            return ctrl;
        };
        /**
         * @description
         * Retrieves the `FormControl` instance from the provided `FormControlName` directive
         *
         * @param dir The `FormControlName` directive instance.
         */
        FormGroupDirective.prototype.getControl = function (dir) { return this.form.get(dir.path); };
        /**
         * @description
         * Removes the `FormControlName` instance from the internal list of directives
         *
         * @param dir The `FormControlName` directive instance.
         */
        FormGroupDirective.prototype.removeControl = function (dir) { removeDir(this.directives, dir); };
        /**
         * Adds a new `FormGroupName` directive instance to the form.
         *
         * @param dir The `FormGroupName` directive instance.
         */
        FormGroupDirective.prototype.addFormGroup = function (dir) {
            var ctrl = this.form.get(dir.path);
            setUpFormContainer(ctrl, dir);
            ctrl.updateValueAndValidity({ emitEvent: false });
        };
        /**
         * No-op method to remove the form group.
         *
         * @param dir The `FormGroupName` directive instance.
         */
        FormGroupDirective.prototype.removeFormGroup = function (dir) { };
        /**
         * @description
         * Retrieves the `FormGroup` for a provided `FormGroupName` directive instance
         *
         * @param dir The `FormGroupName` directive instance.
         */
        FormGroupDirective.prototype.getFormGroup = function (dir) { return this.form.get(dir.path); };
        /**
         * Adds a new `FormArrayName` directive instance to the form.
         *
         * @param dir The `FormArrayName` directive instance.
         */
        FormGroupDirective.prototype.addFormArray = function (dir) {
            var ctrl = this.form.get(dir.path);
            setUpFormContainer(ctrl, dir);
            ctrl.updateValueAndValidity({ emitEvent: false });
        };
        /**
         * No-op method to remove the form array.
         *
         * @param dir The `FormArrayName` directive instance.
         */
        FormGroupDirective.prototype.removeFormArray = function (dir) { };
        /**
         * @description
         * Retrieves the `FormArray` for a provided `FormArrayName` directive instance.
         *
         * @param dir The `FormArrayName` directive instance.
         */
        FormGroupDirective.prototype.getFormArray = function (dir) { return this.form.get(dir.path); };
        /**
         * Sets the new value for the provided `FormControlName` directive.
         *
         * @param dir The `FormControlName` directive instance.
         * @param value The new value for the directive's control.
         */
        FormGroupDirective.prototype.updateModel = function (dir, value) {
            var ctrl = this.form.get(dir.path);
            ctrl.setValue(value);
        };
        /**
         * @description
         * Method called with the "submit" event is triggered on the form.
         * Triggers the `ngSubmit` emitter to emit the "submit" event as its payload.
         *
         * @param $event The "submit" event object
         */
        FormGroupDirective.prototype.onSubmit = function ($event) {
            this.submitted = true;
            syncPendingControls(this.form, this.directives);
            this.ngSubmit.emit($event);
            return false;
        };
        /**
         * @description
         * Method called when the "reset" event is triggered on the form.
         */
        FormGroupDirective.prototype.onReset = function () { this.resetForm(); };
        /**
         * @description
         * Resets the form to an initial value and resets its submitted status.
         *
         * @param value The new value for the form.
         */
        FormGroupDirective.prototype.resetForm = function (value) {
            if (value === void 0) { value = undefined; }
            this.form.reset(value);
            this.submitted = false;
        };
        /** @internal */
        FormGroupDirective.prototype._updateDomValue = function () {
            var _this = this;
            this.directives.forEach(function (dir) {
                var newCtrl = _this.form.get(dir.path);
                if (dir.control !== newCtrl) {
                    cleanUpControl(dir.control, dir);
                    if (newCtrl)
                        setUpControl(newCtrl, dir);
                    dir.control = newCtrl;
                }
            });
            this.form._updateTreeValidity({ emitEvent: false });
        };
        FormGroupDirective.prototype._updateRegistrations = function () {
            var _this = this;
            this.form._registerOnCollectionChange(function () { return _this._updateDomValue(); });
            if (this._oldForm)
                this._oldForm._registerOnCollectionChange(function () { });
            this._oldForm = this.form;
        };
        FormGroupDirective.prototype._updateValidators = function () {
            var sync = composeValidators(this._validators);
            this.form.validator = Validators.compose([this.form.validator, sync]);
            var async = composeAsyncValidators(this._asyncValidators);
            this.form.asyncValidator = Validators.composeAsync([this.form.asyncValidator, async]);
        };
        FormGroupDirective.prototype._checkFormPresent = function () {
            if (!this.form) {
                ReactiveErrors.missingFormException();
            }
        };
        __decorate([
            core.Input('formGroup'),
            __metadata("design:type", FormGroup)
        ], FormGroupDirective.prototype, "form", void 0);
        __decorate([
            core.Output(),
            __metadata("design:type", Object)
        ], FormGroupDirective.prototype, "ngSubmit", void 0);
        FormGroupDirective = __decorate([
            core.Directive({
                selector: '[formGroup]',
                providers: [formDirectiveProvider$1],
                host: { '(submit)': 'onSubmit($event)', '(reset)': 'onReset()' },
                exportAs: 'ngForm'
            }),
            __param(0, core.Optional()), __param(0, core.Self()), __param(0, core.Inject(NG_VALIDATORS)),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_ASYNC_VALIDATORS)),
            __metadata("design:paramtypes", [Array, Array])
        ], FormGroupDirective);
        return FormGroupDirective;
    }(ControlContainer));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var formGroupNameProvider = {
        provide: ControlContainer,
        useExisting: core.forwardRef(function () { return FormGroupName; })
    };
    /**
     * @description
     *
     * Syncs a nested `FormGroup` to a DOM element.
     *
     * This directive can only be used with a parent `FormGroupDirective`.
     *
     * It accepts the string name of the nested `FormGroup` to link, and
     * looks for a `FormGroup` registered with that name in the parent
     * `FormGroup` instance you passed into `FormGroupDirective`.
     *
     * Use nested form groups to validate a sub-group of a
     * form separately from the rest or to group the values of certain
     * controls into their own nested object.
     *
     * @see [Reactive Forms Guide](guide/reactive-forms)
     *
     * @usageNotes
     *
     * ### Access the group by name
     *
     * The following example uses the {@link AbstractControl#get get} method to access the
     * associated `FormGroup`
     *
     * ```ts
     *   this.form.get('name');
     * ```
     *
     * ### Access individual controls in the group
     *
     * The following example uses the {@link AbstractControl#get get} method to access
     * individual controls within the group using dot syntax.
     *
     * ```ts
     *   this.form.get('name.first');
     * ```
     *
     * ### Register a nested `FormGroup`.
     *
     * The following example registers a nested *name* `FormGroup` within an existing `FormGroup`,
     * and provides methods to retrieve the nested `FormGroup` and individual controls.
     *
     * {@example forms/ts/nestedFormGroup/nested_form_group_example.ts region='Component'}
     *
     * @ngModule ReactiveFormsModule
     * @publicApi
     */
    var FormGroupName = /** @class */ (function (_super) {
        __extends(FormGroupName, _super);
        function FormGroupName(parent, validators, asyncValidators) {
            var _this = _super.call(this) || this;
            _this._parent = parent;
            _this._validators = validators;
            _this._asyncValidators = asyncValidators;
            return _this;
        }
        /** @internal */
        FormGroupName.prototype._checkParentType = function () {
            if (_hasInvalidParent(this._parent)) {
                ReactiveErrors.groupParentException();
            }
        };
        __decorate([
            core.Input('formGroupName'),
            __metadata("design:type", String)
        ], FormGroupName.prototype, "name", void 0);
        FormGroupName = __decorate([
            core.Directive({ selector: '[formGroupName]', providers: [formGroupNameProvider] }),
            __param(0, core.Optional()), __param(0, core.Host()), __param(0, core.SkipSelf()),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_VALIDATORS)),
            __param(2, core.Optional()), __param(2, core.Self()), __param(2, core.Inject(NG_ASYNC_VALIDATORS)),
            __metadata("design:paramtypes", [ControlContainer, Array, Array])
        ], FormGroupName);
        return FormGroupName;
    }(AbstractFormGroupDirective));
    var formArrayNameProvider = {
        provide: ControlContainer,
        useExisting: core.forwardRef(function () { return FormArrayName; })
    };
    /**
     * @description
     *
     * Syncs a nested `FormArray` to a DOM element.
     *
     * This directive is designed to be used with a parent `FormGroupDirective` (selector:
     * `[formGroup]`).
     *
     * It accepts the string name of the nested `FormArray` you want to link, and
     * will look for a `FormArray` registered with that name in the parent
     * `FormGroup` instance you passed into `FormGroupDirective`.
     *
     * @see [Reactive Forms Guide](guide/reactive-forms)
     * @see `AbstractControl`
     *
     * @usageNotes
     *
     * ### Example
     *
     * {@example forms/ts/nestedFormArray/nested_form_array_example.ts region='Component'}
     *
     * @ngModule ReactiveFormsModule
     * @publicApi
     */
    var FormArrayName = /** @class */ (function (_super) {
        __extends(FormArrayName, _super);
        function FormArrayName(parent, validators, asyncValidators) {
            var _this = _super.call(this) || this;
            _this._parent = parent;
            _this._validators = validators;
            _this._asyncValidators = asyncValidators;
            return _this;
        }
        /**
         * @description
         * A lifecycle method called when the directive's inputs are initialized. For internal use only.
         *
         * @throws If the directive does not have a valid parent.
         */
        FormArrayName.prototype.ngOnInit = function () {
            this._checkParentType();
            this.formDirective.addFormArray(this);
        };
        /**
         * @description
         * A lifecycle method called before the directive's instance is destroyed. For internal use only.
         */
        FormArrayName.prototype.ngOnDestroy = function () {
            if (this.formDirective) {
                this.formDirective.removeFormArray(this);
            }
        };
        Object.defineProperty(FormArrayName.prototype, "control", {
            /**
             * @description
             * The `FormArray` bound to this directive.
             */
            get: function () { return this.formDirective.getFormArray(this); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormArrayName.prototype, "formDirective", {
            /**
             * @description
             * The top-level directive for this group if present, otherwise null.
             */
            get: function () {
                return this._parent ? this._parent.formDirective : null;
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormArrayName.prototype, "path", {
            /**
             * @description
             * Returns an array that represents the path from the top-level form to this control.
             * Each index is the string name of the control on that level.
             */
            get: function () { return controlPath(this.name, this._parent); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormArrayName.prototype, "validator", {
            /**
             * @description
             * Synchronous validator function composed of all the synchronous validators registered with this
             * directive.
             */
            get: function () { return composeValidators(this._validators); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormArrayName.prototype, "asyncValidator", {
            /**
             * @description
             * Async validator function composed of all the async validators registered with this directive.
             */
            get: function () {
                return composeAsyncValidators(this._asyncValidators);
            },
            enumerable: true,
            configurable: true
        });
        FormArrayName.prototype._checkParentType = function () {
            if (_hasInvalidParent(this._parent)) {
                ReactiveErrors.arrayParentException();
            }
        };
        __decorate([
            core.Input('formArrayName'),
            __metadata("design:type", String)
        ], FormArrayName.prototype, "name", void 0);
        FormArrayName = __decorate([
            core.Directive({ selector: '[formArrayName]', providers: [formArrayNameProvider] }),
            __param(0, core.Optional()), __param(0, core.Host()), __param(0, core.SkipSelf()),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_VALIDATORS)),
            __param(2, core.Optional()), __param(2, core.Self()), __param(2, core.Inject(NG_ASYNC_VALIDATORS)),
            __metadata("design:paramtypes", [ControlContainer, Array, Array])
        ], FormArrayName);
        return FormArrayName;
    }(ControlContainer));
    function _hasInvalidParent(parent) {
        return !(parent instanceof FormGroupName) && !(parent instanceof FormGroupDirective) &&
            !(parent instanceof FormArrayName);
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var controlNameBinding = {
        provide: NgControl,
        useExisting: core.forwardRef(function () { return FormControlName; })
    };
    /**
     * @description
     * Syncs a `FormControl` in an existing `FormGroup` to a form control
     * element by name.
     *
     * @see [Reactive Forms Guide](guide/reactive-forms)
     * @see `FormControl`
     * @see `AbstractControl`
     *
     * @usageNotes
     *
     * ### Register `FormControl` within a group
     *
     * The following example shows how to register multiple form controls within a form group
     * and set their value.
     *
     * {@example forms/ts/simpleFormGroup/simple_form_group_example.ts region='Component'}
     *
     * To see `formControlName` examples with different form control types, see:
     *
     * * Radio buttons: `RadioControlValueAccessor`
     * * Selects: `SelectControlValueAccessor`
     *
     * ### Use with ngModel
     *
     * Support for using the `ngModel` input property and `ngModelChange` event with reactive
     * form directives has been deprecated in Angular v6 and will be removed in Angular v7.
     *
     * Now deprecated:
     *
     * ```html
     * <form [formGroup]="form">
     *   <input formControlName="first" [(ngModel)]="value">
     * </form>
     * ```
     *
     * ```ts
     * this.value = 'some value';
     * ```
     *
     * This has been deprecated for a few reasons. First, developers have found this pattern
     * confusing. It seems like the actual `ngModel` directive is being used, but in fact it's
     * an input/output property named `ngModel` on the reactive form directive that simply
     * approximates (some of) its behavior. Specifically, it allows getting/setting the value
     * and intercepting value events. However, some of `ngModel`'s other features - like
     * delaying updates with`ngModelOptions` or exporting the directive - simply don't work,
     * which has understandably caused some confusion.
     *
     * In addition, this pattern mixes template-driven and reactive forms strategies, which
     * we generally don't recommend because it doesn't take advantage of the full benefits of
     * either strategy. Setting the value in the template violates the template-agnostic
     * principles behind reactive forms, whereas adding a `FormControl`/`FormGroup` layer in
     * the class removes the convenience of defining forms in the template.
     *
     * To update your code before v7, you'll want to decide whether to stick with reactive form
     * directives (and get/set values using reactive forms patterns) or switch over to
     * template-driven directives.
     *
     * After (choice 1 - use reactive forms):
     *
     * ```html
     * <form [formGroup]="form">
     *   <input formControlName="first">
     * </form>
     * ```
     *
     * ```ts
     * this.form.get('first').setValue('some value');
     * ```
     *
     * After (choice 2 - use template-driven forms):
     *
     * ```html
     * <input [(ngModel)]="value">
     * ```
     *
     * ```ts
     * this.value = 'some value';
     * ```
     *
     * By default, when you use this pattern, you will see a deprecation warning once in dev
     * mode. You can choose to silence this warning by providing a config for
     * `ReactiveFormsModule` at import time:
     *
     * ```ts
     * imports: [
     *   ReactiveFormsModule.withConfig({warnOnNgModelWithFormControl: 'never'})
     * ]
     * ```
     *
     * Alternatively, you can choose to surface a separate warning for each instance of this
     * pattern with a config value of `"always"`. This may help to track down where in the code
     * the pattern is being used as the code is being updated.
     *
     * @ngModule ReactiveFormsModule
     * @publicApi
     */
    var FormControlName = /** @class */ (function (_super) {
        __extends(FormControlName, _super);
        function FormControlName(parent, validators, asyncValidators, valueAccessors, _ngModelWarningConfig) {
            var _this = _super.call(this) || this;
            _this._ngModelWarningConfig = _ngModelWarningConfig;
            _this._added = false;
            /** @deprecated as of v6 */
            _this.update = new core.EventEmitter();
            /**
             * @description
             * Instance property used to track whether an ngModel warning has been sent out for this
             * particular FormControlName instance. Used to support warning config of "always".
             *
             * @internal
             */
            _this._ngModelWarningSent = false;
            _this._parent = parent;
            _this._rawValidators = validators || [];
            _this._rawAsyncValidators = asyncValidators || [];
            _this.valueAccessor = selectValueAccessor(_this, valueAccessors);
            return _this;
        }
        FormControlName_1 = FormControlName;
        Object.defineProperty(FormControlName.prototype, "isDisabled", {
            /**
             * @description
             * Triggers a warning that this input should not be used with reactive forms.
             */
            set: function (isDisabled) { ReactiveErrors.disabledAttrWarning(); },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        FormControlName.prototype.ngOnChanges = function (changes) {
            if (!this._added)
                this._setUpControl();
            if (isPropertyUpdated(changes, this.viewModel)) {
                _ngModelWarning('formControlName', FormControlName_1, this, this._ngModelWarningConfig);
                this.viewModel = this.model;
                this.formDirective.updateModel(this, this.model);
            }
        };
        /**
         * @description
         * Lifecycle method called before the directive's instance is destroyed. For internal use only.
         */
        FormControlName.prototype.ngOnDestroy = function () {
            if (this.formDirective) {
                this.formDirective.removeControl(this);
            }
        };
        /**
         * @description
         * Sets the new value for the view model and emits an `ngModelChange` event.
         *
         * @param newValue The new value for the view model.
         */
        FormControlName.prototype.viewToModelUpdate = function (newValue) {
            this.viewModel = newValue;
            this.update.emit(newValue);
        };
        Object.defineProperty(FormControlName.prototype, "path", {
            /**
             * @description
             * Returns an array that represents the path from the top-level form to this control.
             * Each index is the string name of the control on that level.
             */
            get: function () { return controlPath(this.name, this._parent); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormControlName.prototype, "formDirective", {
            /**
             * @description
             * The top-level directive for this group if present, otherwise null.
             */
            get: function () { return this._parent ? this._parent.formDirective : null; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormControlName.prototype, "validator", {
            /**
             * @description
             * Synchronous validator function composed of all the synchronous validators
             * registered with this directive.
             */
            get: function () { return composeValidators(this._rawValidators); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(FormControlName.prototype, "asyncValidator", {
            /**
             * @description
             * Async validator function composed of all the async validators registered with this
             * directive.
             */
            get: function () {
                return composeAsyncValidators(this._rawAsyncValidators);
            },
            enumerable: true,
            configurable: true
        });
        FormControlName.prototype._checkParentType = function () {
            if (!(this._parent instanceof FormGroupName) &&
                this._parent instanceof AbstractFormGroupDirective) {
                ReactiveErrors.ngModelGroupException();
            }
            else if (!(this._parent instanceof FormGroupName) && !(this._parent instanceof FormGroupDirective) &&
                !(this._parent instanceof FormArrayName)) {
                ReactiveErrors.controlParentException();
            }
        };
        FormControlName.prototype._setUpControl = function () {
            this._checkParentType();
            this.control = this.formDirective.addControl(this);
            if (this.control.disabled && this.valueAccessor.setDisabledState) {
                this.valueAccessor.setDisabledState(true);
            }
            this._added = true;
        };
        var FormControlName_1;
        /**
         * @description
         * Static property used to track whether any ngModel warnings have been sent across
         * all instances of FormControlName. Used to support warning config of "once".
         *
         * @internal
         */
        FormControlName._ngModelWarningSentOnce = false;
        __decorate([
            core.Input('formControlName'),
            __metadata("design:type", String)
        ], FormControlName.prototype, "name", void 0);
        __decorate([
            core.Input('disabled'),
            __metadata("design:type", Boolean),
            __metadata("design:paramtypes", [Boolean])
        ], FormControlName.prototype, "isDisabled", null);
        __decorate([
            core.Input('ngModel'),
            __metadata("design:type", Object)
        ], FormControlName.prototype, "model", void 0);
        __decorate([
            core.Output('ngModelChange'),
            __metadata("design:type", Object)
        ], FormControlName.prototype, "update", void 0);
        FormControlName = FormControlName_1 = __decorate([
            core.Directive({ selector: '[formControlName]', providers: [controlNameBinding] }),
            __param(0, core.Optional()), __param(0, core.Host()), __param(0, core.SkipSelf()),
            __param(1, core.Optional()), __param(1, core.Self()), __param(1, core.Inject(NG_VALIDATORS)),
            __param(2, core.Optional()), __param(2, core.Self()), __param(2, core.Inject(NG_ASYNC_VALIDATORS)),
            __param(3, core.Optional()), __param(3, core.Self()), __param(3, core.Inject(NG_VALUE_ACCESSOR)),
            __param(4, core.Optional()), __param(4, core.Inject(NG_MODEL_WITH_FORM_CONTROL_WARNING)),
            __metadata("design:paramtypes", [ControlContainer,
                Array,
                Array, Array, Object])
        ], FormControlName);
        return FormControlName;
    }(NgControl));

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @description
     * Provider which adds `RequiredValidator` to the `NG_VALIDATORS` multi-provider list.
     */
    var REQUIRED_VALIDATOR = {
        provide: NG_VALIDATORS,
        useExisting: core.forwardRef(function () { return RequiredValidator; }),
        multi: true
    };
    /**
     * @description
     * Provider which adds `CheckboxRequiredValidator` to the `NG_VALIDATORS` multi-provider list.
     */
    var CHECKBOX_REQUIRED_VALIDATOR = {
        provide: NG_VALIDATORS,
        useExisting: core.forwardRef(function () { return CheckboxRequiredValidator; }),
        multi: true
    };
    /**
     * @description
     * A directive that adds the `required` validator to any controls marked with the
     * `required` attribute. The directive is provided with the `NG_VALIDATORS` multi-provider list.
     *
     * @see [Form Validation](guide/form-validation)
     *
     * @usageNotes
     *
     * ### Adding a required validator using template-driven forms
     *
     * ```
     * <input name="fullName" ngModel required>
     * ```
     *
     * @ngModule FormsModule
     * @ngModule ReactiveFormsModule
     * @publicApi
     */
    var RequiredValidator = /** @class */ (function () {
        function RequiredValidator() {
        }
        Object.defineProperty(RequiredValidator.prototype, "required", {
            /**
             * @description
             * Tracks changes to the required attribute bound to this directive.
             */
            get: function () { return this._required; },
            set: function (value) {
                this._required = value != null && value !== false && "" + value !== 'false';
                if (this._onChange)
                    this._onChange();
            },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Method that validates whether the control is empty.
         * Returns the validation result if enabled, otherwise null.
         */
        RequiredValidator.prototype.validate = function (control) {
            return this.required ? Validators.required(control) : null;
        };
        /**
         * @description
         * Registers a callback function to call when the validator inputs change.
         *
         * @param fn The callback function
         */
        RequiredValidator.prototype.registerOnValidatorChange = function (fn) { this._onChange = fn; };
        __decorate([
            core.Input(),
            __metadata("design:type", Object),
            __metadata("design:paramtypes", [Object])
        ], RequiredValidator.prototype, "required", null);
        RequiredValidator = __decorate([
            core.Directive({
                selector: ':not([type=checkbox])[required][formControlName],:not([type=checkbox])[required][formControl],:not([type=checkbox])[required][ngModel]',
                providers: [REQUIRED_VALIDATOR],
                host: { '[attr.required]': 'required ? "" : null' }
            })
        ], RequiredValidator);
        return RequiredValidator;
    }());
    /**
     * A Directive that adds the `required` validator to checkbox controls marked with the
     * `required` attribute. The directive is provided with the `NG_VALIDATORS` multi-provider list.
     *
     * @see [Form Validation](guide/form-validation)
     *
     * @usageNotes
     *
     * ### Adding a required checkbox validator using template-driven forms
     *
     * The following example shows how to add a checkbox required validator to an input attached to an ngModel binding.
     *
     * ```
     * <input type="checkbox" name="active" ngModel required>
     * ```
     *
     * @publicApi
     * @ngModule FormsModule
     * @ngModule ReactiveFormsModule
     */
    var CheckboxRequiredValidator = /** @class */ (function (_super) {
        __extends(CheckboxRequiredValidator, _super);
        function CheckboxRequiredValidator() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        /**
         * @description
         * Method that validates whether or not the checkbox has been checked.
         * Returns the validation result if enabled, otherwise null.
         */
        CheckboxRequiredValidator.prototype.validate = function (control) {
            return this.required ? Validators.requiredTrue(control) : null;
        };
        CheckboxRequiredValidator = __decorate([
            core.Directive({
                selector: 'input[type=checkbox][required][formControlName],input[type=checkbox][required][formControl],input[type=checkbox][required][ngModel]',
                providers: [CHECKBOX_REQUIRED_VALIDATOR],
                host: { '[attr.required]': 'required ? "" : null' }
            })
        ], CheckboxRequiredValidator);
        return CheckboxRequiredValidator;
    }(RequiredValidator));
    /**
     * @description
     * Provider which adds `EmailValidator` to the `NG_VALIDATORS` multi-provider list.
     */
    var EMAIL_VALIDATOR = {
        provide: NG_VALIDATORS,
        useExisting: core.forwardRef(function () { return EmailValidator; }),
        multi: true
    };
    /**
     * A directive that adds the `email` validator to controls marked with the
     * `email` attribute. The directive is provided with the `NG_VALIDATORS` multi-provider list.
     *
     * @see [Form Validation](guide/form-validation)
     *
     * @usageNotes
     *
     * ### Adding an email validator
     *
     * The following example shows how to add an email validator to an input attached to an ngModel binding.
     *
     * ```
     * <input type="email" name="email" ngModel email>
     * <input type="email" name="email" ngModel email="true">
     * <input type="email" name="email" ngModel [email]="true">
     * ```
     *
     * @publicApi
     * @ngModule FormsModule
     * @ngModule ReactiveFormsModule
     */
    var EmailValidator = /** @class */ (function () {
        function EmailValidator() {
        }
        Object.defineProperty(EmailValidator.prototype, "email", {
            /**
             * @description
             * Tracks changes to the email attribute bound to this directive.
             */
            set: function (value) {
                this._enabled = value === '' || value === true || value === 'true';
                if (this._onChange)
                    this._onChange();
            },
            enumerable: true,
            configurable: true
        });
        /**
         * @description
         * Method that validates whether an email address is valid.
         * Returns the validation result if enabled, otherwise null.
         */
        EmailValidator.prototype.validate = function (control) {
            return this._enabled ? Validators.email(control) : null;
        };
        /**
         * @description
         * Registers a callback function to call when the validator inputs change.
         *
         * @param fn The callback function
         */
        EmailValidator.prototype.registerOnValidatorChange = function (fn) { this._onChange = fn; };
        __decorate([
            core.Input(),
            __metadata("design:type", Object),
            __metadata("design:paramtypes", [Object])
        ], EmailValidator.prototype, "email", null);
        EmailValidator = __decorate([
            core.Directive({
                selector: '[email][formControlName],[email][formControl],[email][ngModel]',
                providers: [EMAIL_VALIDATOR]
            })
        ], EmailValidator);
        return EmailValidator;
    }());
    /**
     * @description
     * Provider which adds `MinLengthValidator` to the `NG_VALIDATORS` multi-provider list.
     */
    var MIN_LENGTH_VALIDATOR = {
        provide: NG_VALIDATORS,
        useExisting: core.forwardRef(function () { return MinLengthValidator; }),
        multi: true
    };
    /**
     * A directive that adds minimum length validation to controls marked with the
     * `minlength` attribute. The directive is provided with the `NG_VALIDATORS` mult-provider list.
     *
     * @see [Form Validation](guide/form-validation)
     *
     * @usageNotes
     *
     * ### Adding a minimum length validator
     *
     * The following example shows how to add a minimum length validator to an input attached to an
     * ngModel binding.
     *
     * ```html
     * <input name="firstName" ngModel minlength="4">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var MinLengthValidator = /** @class */ (function () {
        function MinLengthValidator() {
        }
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use
         * only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        MinLengthValidator.prototype.ngOnChanges = function (changes) {
            if ('minlength' in changes) {
                this._createValidator();
                if (this._onChange)
                    this._onChange();
            }
        };
        /**
         * @description
         * Method that validates whether the value meets a minimum length
         * requirement. Returns the validation result if enabled, otherwise null.
         */
        MinLengthValidator.prototype.validate = function (control) {
            return this.minlength == null ? null : this._validator(control);
        };
        /**
         * @description
         * Registers a callback function to call when the validator inputs change.
         *
         * @param fn The callback function
         */
        MinLengthValidator.prototype.registerOnValidatorChange = function (fn) { this._onChange = fn; };
        MinLengthValidator.prototype._createValidator = function () {
            this._validator = Validators.minLength(parseInt(this.minlength, 10));
        };
        __decorate([
            core.Input(),
            __metadata("design:type", String)
        ], MinLengthValidator.prototype, "minlength", void 0);
        MinLengthValidator = __decorate([
            core.Directive({
                selector: '[minlength][formControlName],[minlength][formControl],[minlength][ngModel]',
                providers: [MIN_LENGTH_VALIDATOR],
                host: { '[attr.minlength]': 'minlength ? minlength : null' }
            })
        ], MinLengthValidator);
        return MinLengthValidator;
    }());
    /**
     * @description
     * Provider which adds `MaxLengthValidator` to the `NG_VALIDATORS` multi-provider list.
     */
    var MAX_LENGTH_VALIDATOR = {
        provide: NG_VALIDATORS,
        useExisting: core.forwardRef(function () { return MaxLengthValidator; }),
        multi: true
    };
    /**
     * A directive that adds max length validation to controls marked with the
     * `maxlength` attribute. The directive is provided with the `NG_VALIDATORS` multi-provider list.
     *
     * @see [Form Validation](guide/form-validation)
     *
     * @usageNotes
     *
     * ### Adding a maximum length validator
     *
     * The following example shows how to add a maximum length validator to an input attached to an
     * ngModel binding.
     *
     * ```html
     * <input name="firstName" ngModel maxlength="25">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var MaxLengthValidator = /** @class */ (function () {
        function MaxLengthValidator() {
        }
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use
         * only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        MaxLengthValidator.prototype.ngOnChanges = function (changes) {
            if ('maxlength' in changes) {
                this._createValidator();
                if (this._onChange)
                    this._onChange();
            }
        };
        /**
         * @description
         * Method that validates whether the value exceeds
         * the maximum length requirement.
         */
        MaxLengthValidator.prototype.validate = function (control) {
            return this.maxlength != null ? this._validator(control) : null;
        };
        /**
         * @description
         * Registers a callback function to call when the validator inputs change.
         *
         * @param fn The callback function
         */
        MaxLengthValidator.prototype.registerOnValidatorChange = function (fn) { this._onChange = fn; };
        MaxLengthValidator.prototype._createValidator = function () {
            this._validator = Validators.maxLength(parseInt(this.maxlength, 10));
        };
        __decorate([
            core.Input(),
            __metadata("design:type", String)
        ], MaxLengthValidator.prototype, "maxlength", void 0);
        MaxLengthValidator = __decorate([
            core.Directive({
                selector: '[maxlength][formControlName],[maxlength][formControl],[maxlength][ngModel]',
                providers: [MAX_LENGTH_VALIDATOR],
                host: { '[attr.maxlength]': 'maxlength ? maxlength : null' }
            })
        ], MaxLengthValidator);
        return MaxLengthValidator;
    }());
    /**
     * @description
     * Provider which adds `PatternValidator` to the `NG_VALIDATORS` multi-provider list.
     */
    var PATTERN_VALIDATOR = {
        provide: NG_VALIDATORS,
        useExisting: core.forwardRef(function () { return PatternValidator; }),
        multi: true
    };
    /**
     * @description
     * A directive that adds regex pattern validation to controls marked with the
     * `pattern` attribute. The regex must match the entire control value.
     * The directive is provided with the `NG_VALIDATORS` multi-provider list.
     *
     * @see [Form Validation](guide/form-validation)
     *
     * @usageNotes
     *
     * ### Adding a pattern validator
     *
     * The following example shows how to add a pattern validator to an input attached to an
     * ngModel binding.
     *
     * ```html
     * <input name="firstName" ngModel pattern="[a-zA-Z ]*">
     * ```
     *
     * @ngModule ReactiveFormsModule
     * @ngModule FormsModule
     * @publicApi
     */
    var PatternValidator = /** @class */ (function () {
        function PatternValidator() {
        }
        /**
         * @description
         * A lifecycle method called when the directive's inputs change. For internal use
         * only.
         *
         * @param changes A object of key/value pairs for the set of changed inputs.
         */
        PatternValidator.prototype.ngOnChanges = function (changes) {
            if ('pattern' in changes) {
                this._createValidator();
                if (this._onChange)
                    this._onChange();
            }
        };
        /**
         * @description
         * Method that validates whether the value matches the
         * the pattern requirement.
         */
        PatternValidator.prototype.validate = function (control) { return this._validator(control); };
        /**
         * @description
         * Registers a callback function to call when the validator inputs change.
         *
         * @param fn The callback function
         */
        PatternValidator.prototype.registerOnValidatorChange = function (fn) { this._onChange = fn; };
        PatternValidator.prototype._createValidator = function () { this._validator = Validators.pattern(this.pattern); };
        __decorate([
            core.Input(),
            __metadata("design:type", Object)
        ], PatternValidator.prototype, "pattern", void 0);
        PatternValidator = __decorate([
            core.Directive({
                selector: '[pattern][formControlName],[pattern][formControl],[pattern][ngModel]',
                providers: [PATTERN_VALIDATOR],
                host: { '[attr.pattern]': 'pattern ? pattern : null' }
            })
        ], PatternValidator);
        return PatternValidator;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var SHARED_FORM_DIRECTIVES = [
        ɵNgNoValidate,
        NgSelectOption,
        ɵNgSelectMultipleOption,
        DefaultValueAccessor,
        NumberValueAccessor,
        RangeValueAccessor,
        CheckboxControlValueAccessor,
        SelectControlValueAccessor,
        SelectMultipleControlValueAccessor,
        RadioControlValueAccessor,
        NgControlStatus,
        NgControlStatusGroup,
        RequiredValidator,
        MinLengthValidator,
        MaxLengthValidator,
        PatternValidator,
        CheckboxRequiredValidator,
        EmailValidator,
    ];
    var TEMPLATE_DRIVEN_DIRECTIVES = [NgModel, NgModelGroup, NgForm, NgFormSelectorWarning];
    var REACTIVE_DRIVEN_DIRECTIVES = [FormControlDirective, FormGroupDirective, FormControlName, FormGroupName, FormArrayName];
    /**
     * Internal module used for sharing directives between FormsModule and ReactiveFormsModule
     */
    var ɵInternalFormsSharedModule = /** @class */ (function () {
        function ɵInternalFormsSharedModule() {
        }
        ɵInternalFormsSharedModule = __decorate([
            core.NgModule({
                declarations: SHARED_FORM_DIRECTIVES,
                exports: SHARED_FORM_DIRECTIVES,
            })
        ], ɵInternalFormsSharedModule);
        return ɵInternalFormsSharedModule;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    function isAbstractControlOptions(options) {
        return options.asyncValidators !== undefined ||
            options.validators !== undefined ||
            options.updateOn !== undefined;
    }
    /**
     * @description
     * Creates an `AbstractControl` from a user-specified configuration.
     *
     * The `FormBuilder` provides syntactic sugar that shortens creating instances of a `FormControl`,
     * `FormGroup`, or `FormArray`. It reduces the amount of boilerplate needed to build complex
     * forms.
     *
     * @see [Reactive Forms Guide](/guide/reactive-forms)
     *
     * @publicApi
     */
    var FormBuilder = /** @class */ (function () {
        function FormBuilder() {
        }
        /**
         * @description
         * Construct a new `FormGroup` instance.
         *
         * @param controlsConfig A collection of child controls. The key for each child is the name
         * under which it is registered.
         *
         * @param options Configuration options object for the `FormGroup`. The object can
         * have two shapes:
         *
         * 1) `AbstractControlOptions` object (preferred), which consists of:
         * * `validators`: A synchronous validator function, or an array of validator functions
         * * `asyncValidators`: A single async validator or array of async validator functions
         * * `updateOn`: The event upon which the control should be updated (options: 'change' | 'blur' |
         * submit')
         *
         * 2) Legacy configuration object, which consists of:
         * * `validator`: A synchronous validator function, or an array of validator functions
         * * `asyncValidator`: A single async validator or array of async validator functions
         *
         */
        FormBuilder.prototype.group = function (controlsConfig, options) {
            if (options === void 0) { options = null; }
            var controls = this._reduceControls(controlsConfig);
            var validators = null;
            var asyncValidators = null;
            var updateOn = undefined;
            if (options != null) {
                if (isAbstractControlOptions(options)) {
                    // `options` are `AbstractControlOptions`
                    validators = options.validators != null ? options.validators : null;
                    asyncValidators = options.asyncValidators != null ? options.asyncValidators : null;
                    updateOn = options.updateOn != null ? options.updateOn : undefined;
                }
                else {
                    // `options` are legacy form group options
                    validators = options['validator'] != null ? options['validator'] : null;
                    asyncValidators = options['asyncValidator'] != null ? options['asyncValidator'] : null;
                }
            }
            return new FormGroup(controls, { asyncValidators: asyncValidators, updateOn: updateOn, validators: validators });
        };
        /**
         * @description
         * Construct a new `FormControl` with the given state, validators and options.
         *
         * @param formState Initializes the control with an initial state value, or
         * with an object that contains both a value and a disabled status.
         *
         * @param validatorOrOpts A synchronous validator function, or an array of
         * such functions, or an `AbstractControlOptions` object that contains
         * validation functions and a validation trigger.
         *
         * @param asyncValidator A single async validator or array of async validator
         * functions.
         *
         * @usageNotes
         *
         * ### Initialize a control as disabled
         *
         * The following example returns a control with an initial value in a disabled state.
         *
         * <code-example path="forms/ts/formBuilder/form_builder_example.ts" region="disabled-control">
         * </code-example>
         */
        FormBuilder.prototype.control = function (formState, validatorOrOpts, asyncValidator) {
            return new FormControl(formState, validatorOrOpts, asyncValidator);
        };
        /**
         * Constructs a new `FormArray` from the given array of configurations,
         * validators and options.
         *
         * @param controlsConfig An array of child controls or control configs. Each
         * child control is given an index when it is registered.
         *
         * @param validatorOrOpts A synchronous validator function, or an array of
         * such functions, or an `AbstractControlOptions` object that contains
         * validation functions and a validation trigger.
         *
         * @param asyncValidator A single async validator or array of async validator
         * functions.
         */
        FormBuilder.prototype.array = function (controlsConfig, validatorOrOpts, asyncValidator) {
            var _this = this;
            var controls = controlsConfig.map(function (c) { return _this._createControl(c); });
            return new FormArray(controls, validatorOrOpts, asyncValidator);
        };
        /** @internal */
        FormBuilder.prototype._reduceControls = function (controlsConfig) {
            var _this = this;
            var controls = {};
            Object.keys(controlsConfig).forEach(function (controlName) {
                controls[controlName] = _this._createControl(controlsConfig[controlName]);
            });
            return controls;
        };
        /** @internal */
        FormBuilder.prototype._createControl = function (controlConfig) {
            if (controlConfig instanceof FormControl || controlConfig instanceof FormGroup ||
                controlConfig instanceof FormArray) {
                return controlConfig;
            }
            else if (Array.isArray(controlConfig)) {
                var value = controlConfig[0];
                var validator = controlConfig.length > 1 ? controlConfig[1] : null;
                var asyncValidator = controlConfig.length > 2 ? controlConfig[2] : null;
                return this.control(value, validator, asyncValidator);
            }
            else {
                return this.control(controlConfig);
            }
        };
        FormBuilder = __decorate([
            core.Injectable()
        ], FormBuilder);
        return FormBuilder;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * @publicApi
     */
    var VERSION = new core.Version('8.2.14');

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * Exports the required providers and directives for template-driven forms,
     * making them available for import by NgModules that import this module.
     *
     * @see [Forms Guide](/guide/forms)
     *
     * @publicApi
     */
    var FormsModule = /** @class */ (function () {
        function FormsModule() {
        }
        FormsModule_1 = FormsModule;
        /**
         * @description
         * Provides options for configuring the template-driven forms module.
         *
         * @param opts An object of configuration options
         * * `warnOnDeprecatedNgFormSelector` Configures when to emit a warning when the deprecated
         * `ngForm` selector is used.
         */
        FormsModule.withConfig = function (opts) {
            return {
                ngModule: FormsModule_1,
                providers: [{ provide: NG_FORM_SELECTOR_WARNING, useValue: opts.warnOnDeprecatedNgFormSelector }]
            };
        };
        var FormsModule_1;
        FormsModule = FormsModule_1 = __decorate([
            core.NgModule({
                declarations: TEMPLATE_DRIVEN_DIRECTIVES,
                providers: [RadioControlRegistry],
                exports: [ɵInternalFormsSharedModule, TEMPLATE_DRIVEN_DIRECTIVES]
            })
        ], FormsModule);
        return FormsModule;
    }());
    /**
     * Exports the required infrastructure and directives for reactive forms,
     * making them available for import by NgModules that import this module.
     * @see [Forms](guide/reactive-forms)
     *
     * @see [Reactive Forms Guide](/guide/reactive-forms)
     *
     * @publicApi
     */
    var ReactiveFormsModule = /** @class */ (function () {
        function ReactiveFormsModule() {
        }
        ReactiveFormsModule_1 = ReactiveFormsModule;
        /**
         * @description
         * Provides options for configuring the reactive forms module.
         *
         * @param opts An object of configuration options
         * * `warnOnNgModelWithFormControl` Configures when to emit a warning when an `ngModel`
         * binding is used with reactive form directives.
         */
        ReactiveFormsModule.withConfig = function (opts) {
            return {
                ngModule: ReactiveFormsModule_1,
                providers: [{
                        provide: NG_MODEL_WITH_FORM_CONTROL_WARNING,
                        useValue: opts.warnOnNgModelWithFormControl
                    }]
            };
        };
        var ReactiveFormsModule_1;
        ReactiveFormsModule = ReactiveFormsModule_1 = __decorate([
            core.NgModule({
                declarations: [REACTIVE_DRIVEN_DIRECTIVES],
                providers: [FormBuilder, RadioControlRegistry],
                exports: [ɵInternalFormsSharedModule, REACTIVE_DRIVEN_DIRECTIVES]
            })
        ], ReactiveFormsModule);
        return ReactiveFormsModule;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    // This file only reexports content of the `src` folder. Keep it that way.

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */

    /**
     * Generated bundle index. Do not edit.
     */

    exports.ɵangular_packages_forms_forms_d = ɵInternalFormsSharedModule;
    exports.ɵInternalFormsSharedModule = ɵInternalFormsSharedModule;
    exports.ɵangular_packages_forms_forms_c = REACTIVE_DRIVEN_DIRECTIVES;
    exports.ɵangular_packages_forms_forms_a = SHARED_FORM_DIRECTIVES;
    exports.ɵangular_packages_forms_forms_b = TEMPLATE_DRIVEN_DIRECTIVES;
    exports.ɵangular_packages_forms_forms_e = CHECKBOX_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_f = DEFAULT_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_g = AbstractControlStatus;
    exports.ɵangular_packages_forms_forms_h = ngControlStatusHost;
    exports.ɵangular_packages_forms_forms_i = formDirectiveProvider;
    exports.ɵangular_packages_forms_forms_j = NG_FORM_SELECTOR_WARNING;
    exports.ɵangular_packages_forms_forms_k = formControlBinding;
    exports.ɵangular_packages_forms_forms_l = modelGroupProvider;
    exports.ɵangular_packages_forms_forms_z = ɵNgNoValidate;
    exports.ɵNgNoValidate = ɵNgNoValidate;
    exports.ɵangular_packages_forms_forms_m = NUMBER_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_n = RADIO_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_o = RadioControlRegistry;
    exports.ɵangular_packages_forms_forms_p = RANGE_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_q = NG_MODEL_WITH_FORM_CONTROL_WARNING;
    exports.ɵangular_packages_forms_forms_r = formControlBinding$1;
    exports.ɵangular_packages_forms_forms_s = controlNameBinding;
    exports.ɵangular_packages_forms_forms_t = formDirectiveProvider$1;
    exports.ɵangular_packages_forms_forms_v = formArrayNameProvider;
    exports.ɵangular_packages_forms_forms_u = formGroupNameProvider;
    exports.ɵangular_packages_forms_forms_w = SELECT_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_y = ɵNgSelectMultipleOption;
    exports.ɵNgSelectMultipleOption = ɵNgSelectMultipleOption;
    exports.ɵangular_packages_forms_forms_x = SELECT_MULTIPLE_VALUE_ACCESSOR;
    exports.ɵangular_packages_forms_forms_bb = CHECKBOX_REQUIRED_VALIDATOR;
    exports.ɵangular_packages_forms_forms_bc = EMAIL_VALIDATOR;
    exports.ɵangular_packages_forms_forms_be = MAX_LENGTH_VALIDATOR;
    exports.ɵangular_packages_forms_forms_bd = MIN_LENGTH_VALIDATOR;
    exports.ɵangular_packages_forms_forms_bf = PATTERN_VALIDATOR;
    exports.ɵangular_packages_forms_forms_ba = REQUIRED_VALIDATOR;
    exports.AbstractControlDirective = AbstractControlDirective;
    exports.AbstractFormGroupDirective = AbstractFormGroupDirective;
    exports.CheckboxControlValueAccessor = CheckboxControlValueAccessor;
    exports.ControlContainer = ControlContainer;
    exports.NG_VALUE_ACCESSOR = NG_VALUE_ACCESSOR;
    exports.COMPOSITION_BUFFER_MODE = COMPOSITION_BUFFER_MODE;
    exports.DefaultValueAccessor = DefaultValueAccessor;
    exports.NgControl = NgControl;
    exports.NgControlStatus = NgControlStatus;
    exports.NgControlStatusGroup = NgControlStatusGroup;
    exports.NgForm = NgForm;
    exports.NgFormSelectorWarning = NgFormSelectorWarning;
    exports.NgModel = NgModel;
    exports.NgModelGroup = NgModelGroup;
    exports.NumberValueAccessor = NumberValueAccessor;
    exports.RadioControlValueAccessor = RadioControlValueAccessor;
    exports.RangeValueAccessor = RangeValueAccessor;
    exports.FormControlDirective = FormControlDirective;
    exports.FormControlName = FormControlName;
    exports.FormGroupDirective = FormGroupDirective;
    exports.FormArrayName = FormArrayName;
    exports.FormGroupName = FormGroupName;
    exports.NgSelectOption = NgSelectOption;
    exports.SelectControlValueAccessor = SelectControlValueAccessor;
    exports.SelectMultipleControlValueAccessor = SelectMultipleControlValueAccessor;
    exports.CheckboxRequiredValidator = CheckboxRequiredValidator;
    exports.EmailValidator = EmailValidator;
    exports.MaxLengthValidator = MaxLengthValidator;
    exports.MinLengthValidator = MinLengthValidator;
    exports.PatternValidator = PatternValidator;
    exports.RequiredValidator = RequiredValidator;
    exports.FormBuilder = FormBuilder;
    exports.AbstractControl = AbstractControl;
    exports.FormArray = FormArray;
    exports.FormControl = FormControl;
    exports.FormGroup = FormGroup;
    exports.NG_ASYNC_VALIDATORS = NG_ASYNC_VALIDATORS;
    exports.NG_VALIDATORS = NG_VALIDATORS;
    exports.Validators = Validators;
    exports.VERSION = VERSION;
    exports.FormsModule = FormsModule;
    exports.ReactiveFormsModule = ReactiveFormsModule;

    Object.defineProperty(exports, '__esModule', { value: true });

}));
//# sourceMappingURL=forms.umd.js.map
