/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { getCurrentStyleSanitizer } from '../state';
import { attachDebugObject } from '../util/debug_utils';
import { applyStyling } from './bindings';
import { activateStylingMapFeature } from './map_based_bindings';
import { getDefaultValue, getGuardMask, getProp, getValuesCount, isContextLocked, isSanitizationRequired } from './util';
/**
 * A debug/testing-oriented summary of a styling entry.
 *
 * A value such as this is generated as an artifact of the `DebugStyling`
 * summary.
 * @record
 */
export function LStylingSummary() { }
if (false) {
    /**
     * The style/class property that the summary is attached to
     * @type {?}
     */
    LStylingSummary.prototype.prop;
    /**
     * The last applied value for the style/class property
     * @type {?}
     */
    LStylingSummary.prototype.value;
    /**
     * The binding index of the last applied style/class property
     * @type {?}
     */
    LStylingSummary.prototype.bindingIndex;
}
/**
 * A debug/testing-oriented summary of all styling entries for a `DebugNode` instance.
 * @record
 */
export function DebugStyling() { }
if (false) {
    /**
     * The associated TStylingContext instance
     * @type {?}
     */
    DebugStyling.prototype.context;
    /**
     * A summarization of each style/class property
     * present in the context.
     * @type {?}
     */
    DebugStyling.prototype.summary;
    /**
     * A key/value map of all styling properties and their
     * runtime values.
     * @type {?}
     */
    DebugStyling.prototype.values;
    /**
     * Overrides the sanitizer used to process styles.
     * @param {?} sanitizer
     * @return {?}
     */
    DebugStyling.prototype.overrideSanitizer = function (sanitizer) { };
}
/**
 * A debug/testing-oriented summary of all styling entries within a `TStylingContext`.
 * @record
 */
export function TStylingTupleSummary() { }
if (false) {
    /**
     * The property (style or class property) that this tuple represents
     * @type {?}
     */
    TStylingTupleSummary.prototype.prop;
    /**
     * The total amount of styling entries a part of this tuple
     * @type {?}
     */
    TStylingTupleSummary.prototype.valuesCount;
    /**
     * The bit guard mask that is used to compare and protect against
     * styling changes when and styling bindings update
     * @type {?}
     */
    TStylingTupleSummary.prototype.guardMask;
    /**
     * Whether or not the entry requires sanitization
     * @type {?}
     */
    TStylingTupleSummary.prototype.sanitizationRequired;
    /**
     * The default value that will be applied if any bindings are falsy.
     * @type {?}
     */
    TStylingTupleSummary.prototype.defaultValue;
    /**
     * All bindingIndex sources that have been registered for this style.
     * @type {?}
     */
    TStylingTupleSummary.prototype.sources;
}
/**
 * Instantiates and attaches an instance of `TStylingContextDebug` to the provided context.
 * @param {?} context
 * @return {?}
 */
export function attachStylingDebugObject(context) {
    /** @type {?} */
    const debug = new TStylingContextDebug(context);
    attachDebugObject(context, debug);
    return debug;
}
/**
 * A human-readable debug summary of the styling data present within `TStylingContext`.
 *
 * This class is designed to be used within testing code or when an
 * application has `ngDevMode` activated.
 */
class TStylingContextDebug {
    /**
     * @param {?} context
     */
    constructor(context) {
        this.context = context;
    }
    /**
     * @return {?}
     */
    get isLocked() { return isContextLocked(this.context); }
    /**
     * Returns a detailed summary of each styling entry in the context.
     *
     * See `TStylingTupleSummary`.
     * @return {?}
     */
    get entries() {
        /** @type {?} */
        const context = this.context;
        /** @type {?} */
        const entries = {};
        /** @type {?} */
        const start = 3 /* MapBindingsPosition */;
        /** @type {?} */
        let i = start;
        while (i < context.length) {
            /** @type {?} */
            const valuesCount = getValuesCount(context, i);
            // the context may contain placeholder values which are populated ahead of time,
            // but contain no actual binding values. In this situation there is no point in
            // classifying this as an "entry" since no real data is stored here yet.
            if (valuesCount) {
                /** @type {?} */
                const prop = getProp(context, i);
                /** @type {?} */
                const guardMask = getGuardMask(context, i);
                /** @type {?} */
                const defaultValue = getDefaultValue(context, i);
                /** @type {?} */
                const sanitizationRequired = isSanitizationRequired(context, i);
                /** @type {?} */
                const bindingsStartPosition = i + 3 /* BindingsStartOffset */;
                /** @type {?} */
                const sources = [];
                for (let j = 0; j < valuesCount; j++) {
                    sources.push((/** @type {?} */ (context[bindingsStartPosition + j])));
                }
                entries[prop] = { prop, guardMask, sanitizationRequired, valuesCount, defaultValue, sources };
            }
            i += 3 /* BindingsStartOffset */ + valuesCount;
        }
        return entries;
    }
}
if (false) {
    /** @type {?} */
    TStylingContextDebug.prototype.context;
}
/**
 * A human-readable debug summary of the styling data present for a `DebugNode` instance.
 *
 * This class is designed to be used within testing code or when an
 * application has `ngDevMode` activated.
 */
export class NodeStylingDebug {
    /**
     * @param {?} context
     * @param {?} _data
     * @param {?=} _isClassBased
     */
    constructor(context, _data, _isClassBased) {
        this.context = context;
        this._data = _data;
        this._isClassBased = _isClassBased;
        this._sanitizer = null;
    }
    /**
     * Overrides the sanitizer used to process styles.
     * @param {?} sanitizer
     * @return {?}
     */
    overrideSanitizer(sanitizer) { this._sanitizer = sanitizer; }
    /**
     * Returns a detailed summary of each styling entry in the context and
     * what their runtime representation is.
     *
     * See `LStylingSummary`.
     * @return {?}
     */
    get summary() {
        /** @type {?} */
        const entries = {};
        this._mapValues((/**
         * @param {?} prop
         * @param {?} value
         * @param {?} bindingIndex
         * @return {?}
         */
        (prop, value, bindingIndex) => {
            entries[prop] = { prop, value, bindingIndex };
        }));
        return entries;
    }
    /**
     * Returns a key/value map of all the styles/classes that were last applied to the element.
     * @return {?}
     */
    get values() {
        /** @type {?} */
        const entries = {};
        this._mapValues((/**
         * @param {?} prop
         * @param {?} value
         * @return {?}
         */
        (prop, value) => { entries[prop] = value; }));
        return entries;
    }
    /**
     * @private
     * @param {?} fn
     * @return {?}
     */
    _mapValues(fn) {
        // there is no need to store/track an element instance. The
        // element is only used when the styling algorithm attempts to
        // style the value (and we mock out the stylingApplyFn anyway).
        /** @type {?} */
        const mockElement = (/** @type {?} */ ({}));
        /** @type {?} */
        const hasMaps = getValuesCount(this.context, 3 /* MapBindingsPosition */) > 0;
        if (hasMaps) {
            activateStylingMapFeature();
        }
        /** @type {?} */
        const mapFn = (/**
         * @param {?} renderer
         * @param {?} element
         * @param {?} prop
         * @param {?} value
         * @param {?=} bindingIndex
         * @return {?}
         */
        (renderer, element, prop, value, bindingIndex) => { fn(prop, value, bindingIndex || null); });
        /** @type {?} */
        const sanitizer = this._isClassBased ? null : (this._sanitizer || getCurrentStyleSanitizer());
        applyStyling(this.context, null, mockElement, this._data, true, mapFn, sanitizer);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    NodeStylingDebug.prototype._sanitizer;
    /** @type {?} */
    NodeStylingDebug.prototype.context;
    /**
     * @type {?}
     * @private
     */
    NodeStylingDebug.prototype._data;
    /**
     * @type {?}
     * @private
     */
    NodeStylingDebug.prototype._isClassBased;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGluZ19kZWJ1Zy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvc3R5bGluZ19uZXh0L3N0eWxpbmdfZGVidWcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQVVBLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUNsRCxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUV0RCxPQUFPLEVBQUMsWUFBWSxFQUFDLE1BQU0sWUFBWSxDQUFDO0FBRXhDLE9BQU8sRUFBQyx5QkFBeUIsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQy9ELE9BQU8sRUFBQyxlQUFlLEVBQUUsWUFBWSxFQUFFLE9BQU8sRUFBRSxjQUFjLEVBQUUsZUFBZSxFQUFjLHNCQUFzQixFQUFDLE1BQU0sUUFBUSxDQUFDOzs7Ozs7OztBQXFCbkkscUNBU0M7Ozs7OztJQVBDLCtCQUFhOzs7OztJQUdiLGdDQUEyQjs7Ozs7SUFHM0IsdUNBQTBCOzs7Ozs7QUFNNUIsa0NBb0JDOzs7Ozs7SUFsQkMsK0JBQXlCOzs7Ozs7SUFNekIsK0JBQTBDOzs7Ozs7SUFNMUMsOEJBQTBEOzs7Ozs7SUFLMUQsb0VBQXlEOzs7Ozs7QUFNM0QsMENBMkJDOzs7Ozs7SUF6QkMsb0NBQWE7Ozs7O0lBR2IsMkNBQW9COzs7Ozs7SUFNcEIseUNBQWtCOzs7OztJQUtsQixvREFBOEI7Ozs7O0lBSzlCLDRDQUFrQzs7Ozs7SUFLbEMsdUNBQWdDOzs7Ozs7O0FBTWxDLE1BQU0sVUFBVSx3QkFBd0IsQ0FBQyxPQUF3Qjs7VUFDekQsS0FBSyxHQUFHLElBQUksb0JBQW9CLENBQUMsT0FBTyxDQUFDO0lBQy9DLGlCQUFpQixDQUFDLE9BQU8sRUFBRSxLQUFLLENBQUMsQ0FBQztJQUNsQyxPQUFPLEtBQUssQ0FBQztBQUNmLENBQUM7Ozs7Ozs7QUFRRCxNQUFNLG9CQUFv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LENBQUMsT0FBTyw4QkFBMkMsR0FBRyxDQUFDO1FBQzFGLElBQUksT0FBTyxFQUFFO1lBQ1gseUJBQXlCLEVBQUUsQ0FBQztTQUM3Qjs7Y0FFSyxLQUFLOzs7Ozs7OztRQUNQLENBQUMsUUFBYSxFQUFFLE9BQWlCLEVBQUUsSUFBWSxFQUFFLEtBQW9CLEVBQ3BFLFlBQTRCLEVBQUUsRUFBRSxHQUFHLEVBQUUsQ0FBQyxJQUFJLEVBQUUsS0FBSyxFQUFFLFlBQVksSUFBSSxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQTs7Y0FFMUUsU0FBUyxHQUFHLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsVUFBVSxJQUFJLHdCQUF3QixFQUFFLENBQUM7UUFDN0YsWUFBWSxDQUFDLElBQUksQ0FBQyxPQUFPLEVBQUUsSUFBSSxFQUFFLFdBQVcsRUFBRSxJQUFJLENBQUMsS0FBSyxFQUFFLElBQUksRUFBRSxLQUFLLEVBQUUsU0FBUyxDQUFDLENBQUM7SUFDcEYsQ0FBQztDQUNGOzs7Ozs7SUFuREMsc0NBQWdEOztJQUc1QyxtQ0FBK0I7Ozs7O0lBQUUsaUNBQTJCOzs7OztJQUM1RCx5Q0FBK0IiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiogQGxpY2Vuc2VcbiogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4qXG4qIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4qIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiovXG5pbXBvcnQge1N0eWxlU2FuaXRpemVGbn0gZnJvbSAnLi4vLi4vc2FuaXRpemF0aW9uL3N0eWxlX3Nhbml0aXplcic7XG5pbXBvcnQge1JFbGVtZW50fSBmcm9tICcuLi9pbnRlcmZhY2VzL3JlbmRlcmVyJztcbmltcG9ydCB7TFZpZXd9IGZyb20gJy4uL2ludGVyZmFjZXMvdmlldyc7XG5pbXBvcnQge2dldEN1cnJlbnRTdHlsZVNhbml0aXplcn0gZnJvbSAnLi4vc3RhdGUnO1xuaW1wb3J0IHthdHRhY2hEZWJ1Z09iamVjdH0gZnJvbSAnLi4vdXRpbC9kZWJ1Z191dGlscyc7XG5cbmltcG9ydCB7YXBwbHlTdHlsaW5nfSBmcm9tICcuL2JpbmRpbmdzJztcbmltcG9ydCB7QXBwbHlTdHlsaW5nRm4sIExTdHlsaW5nRGF0YSwgVFN0eWxpbmdDb250ZXh0LCBUU3R5bGluZ0NvbnRleHRJbmRleH0gZnJvbSAnLi9pbnRlcmZhY2VzJztcbmltcG9ydCB7YWN0aXZhdGVTdHlsaW5nTWFwRmVhdHVyZX0gZnJvbSAnLi9tYXBfYmFzZWRfYmluZGluZ3MnO1xuaW1wb3J0IHtnZXREZWZhdWx0VmFsdWUsIGdldEd1YXJkTWFzaywgZ2V0UHJvcCwgZ2V0VmFsdWVzQ291bnQsIGlzQ29udGV4dExvY2tlZCwgaXNNYXBCYXNlZCwgaXNTYW5pdGl6YXRpb25SZXF1aXJlZH0gZnJvbSAnLi91dGlsJztcblxuXG5cbi8qKlxuICogLS0tLS0tLS1cbiAqXG4gKiBUaGlzIGZpbGUgY29udGFpbnMgdGhlIGNvcmUgZGVidWcgZnVuY3Rpb25hbGl0eSBmb3Igc3R5bGluZyBpbiBBbmd1bGFyLlxuICpcbiAqIFRvIGxlYXJuIG1vcmUgYWJvdXQgdGhlIGFsZ29yaXRobSBzZWUgYFRTdHlsaW5nQ29udGV4dGAuXG4gKlxuICogLS0tLS0tLS1cbiAqL1xuXG5cbi8qKlxuICogQSBkZWJ1Zy90ZXN0aW5nLW9yaWVudGVkIHN1bW1hcnkgb2YgYSBzdHlsaW5nIGVudHJ5LlxuICpcbiAqIEEgdmFsdWUgc3VjaCBhcyB0aGlzIGlzIGdlbmVyYXRlZCBhcyBhbiBhcnRpZmFjdCBvZiB0aGUgYERlYnVnU3R5bGluZ2BcbiAqIHN1bW1hcnkuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgTFN0eWxpbmdTdW1tYXJ5IHtcbiAgLyoqIFRoZSBzdHlsZS9jbGFzcyBwcm9wZXJ0eSB0aGF0IHRoZSBzdW1tYXJ5IGlzIGF0dGFjaGVkIHRvICovXG4gIHByb3A6IHN0cmluZztcblxuICAvKiogVGhlIGxhc3QgYXBwbGllZCB2YWx1ZSBmb3IgdGhlIHN0eWxlL2NsYXNzIHByb3BlcnR5ICovXG4gIHZhbHVlOiBzdHJpbmd8Ym9vbGVhbnxudWxsO1xuXG4gIC8qKiBUaGUgYmluZGluZyBpbmRleCBvZiB0aGUgbGFzdCBhcHBsaWVkIHN0eWxlL2NsYXNzIHByb3BlcnR5ICovXG4gIGJpbmRpbmdJbmRleDogbnVtYmVyfG51bGw7XG59XG5cbi8qKlxuICogQSBkZWJ1Zy90ZXN0aW5nLW9yaWVudGVkIHN1bW1hcnkgb2YgYWxsIHN0eWxpbmcgZW50cmllcyBmb3IgYSBgRGVidWdOb2RlYCBpbnN0YW5jZS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBEZWJ1Z1N0eWxpbmcge1xuICAvKiogVGhlIGFzc29jaWF0ZWQgVFN0eWxpbmdDb250ZXh0IGluc3RhbmNlICovXG4gIGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dDtcblxuICAvKipcbiAgICogQSBzdW1tYXJpemF0aW9uIG9mIGVhY2ggc3R5bGUvY2xhc3MgcHJvcGVydHlcbiAgICogcHJlc2VudCBpbiB0aGUgY29udGV4dC5cbiAgICovXG4gIHN1bW1hcnk6IHtba2V5OiBzdHJpbmddOiBMU3R5bGluZ1N1bW1hcnl9O1xuXG4gIC8qKlxuICAgKiBBIGtleS92YWx1ZSBtYXAgb2YgYWxsIHN0eWxpbmcgcHJvcGVydGllcyBhbmQgdGhlaXJcbiAgICogcnVudGltZSB2YWx1ZXMuXG4gICAqL1xuICB2YWx1ZXM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmcgfCBudW1iZXIgfCBudWxsIHwgYm9vbGVhbn07XG5cbiAgLyoqXG4gICAqIE92ZXJyaWRlcyB0aGUgc2FuaXRpemVyIHVzZWQgdG8gcHJvY2VzcyBzdHlsZXMuXG4gICAqL1xuICBvdmVycmlkZVNhbml0aXplcihzYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbnxudWxsKTogdm9pZDtcbn1cblxuLyoqXG4gKiBBIGRlYnVnL3Rlc3Rpbmctb3JpZW50ZWQgc3VtbWFyeSBvZiBhbGwgc3R5bGluZyBlbnRyaWVzIHdpdGhpbiBhIGBUU3R5bGluZ0NvbnRleHRgLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFRTdHlsaW5nVHVwbGVTdW1tYXJ5IHtcbiAgLyoqIFRoZSBwcm9wZXJ0eSAoc3R5bGUgb3IgY2xhc3MgcHJvcGVydHkpIHRoYXQgdGhpcyB0dXBsZSByZXByZXNlbnRzICovXG4gIHByb3A6IHN0cmluZztcblxuICAvKiogVGhlIHRvdGFsIGFtb3VudCBvZiBzdHlsaW5nIGVudHJpZXMgYSBwYXJ0IG9mIHRoaXMgdHVwbGUgKi9cbiAgdmFsdWVzQ291bnQ6IG51bWJlcjtcblxuICAvKipcbiAgICogVGhlIGJpdCBndWFyZCBtYXNrIHRoYXQgaXMgdXNlZCB0byBjb21wYXJlIGFuZCBwcm90ZWN0IGFnYWluc3RcbiAgICogc3R5bGluZyBjaGFuZ2VzIHdoZW4gYW5kIHN0eWxpbmcgYmluZGluZ3MgdXBkYXRlXG4gICAqL1xuICBndWFyZE1hc2s6IG51bWJlcjtcblxuICAvKipcbiAgICogV2hldGhlciBvciBub3QgdGhlIGVudHJ5IHJlcXVpcmVzIHNhbml0aXphdGlvblxuICAgKi9cbiAgc2FuaXRpemF0aW9uUmVxdWlyZWQ6IGJvb2xlYW47XG5cbiAgLyoqXG4gICAqIFRoZSBkZWZhdWx0IHZhbHVlIHRoYXQgd2lsbCBiZSBhcHBsaWVkIGlmIGFueSBiaW5kaW5ncyBhcmUgZmFsc3kuXG4gICAqL1xuICBkZWZhdWx0VmFsdWU6IHN0cmluZ3xib29sZWFufG51bGw7XG5cbiAgLyoqXG4gICAqIEFsbCBiaW5kaW5nSW5kZXggc291cmNlcyB0aGF0IGhhdmUgYmVlbiByZWdpc3RlcmVkIGZvciB0aGlzIHN0eWxlLlxuICAgKi9cbiAgc291cmNlczogKG51bWJlcnxudWxsfHN0cmluZylbXTtcbn1cblxuLyoqXG4gKiBJbnN0YW50aWF0ZXMgYW5kIGF0dGFjaGVzIGFuIGluc3RhbmNlIG9mIGBUU3R5bGluZ0NvbnRleHREZWJ1Z2AgdG8gdGhlIHByb3ZpZGVkIGNvbnRleHQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBhdHRhY2hTdHlsaW5nRGVidWdPYmplY3QoY29udGV4dDogVFN0eWxpbmdDb250ZXh0KSB7XG4gIGNvbnN0IGRlYnVnID0gbmV3IFRTdHlsaW5nQ29udGV4dERlYnVnKGNvbnRleHQpO1xuICBhdHRhY2hEZWJ1Z09iamVjdChjb250ZXh0LCBkZWJ1Zyk7XG4gIHJldHVybiBkZWJ1Zztcbn1cblxuLyoqXG4gKiBBIGh1bWFuLXJlYWRhYmxlIGRlYnVnIHN1bW1hcnkgb2YgdGhlIHN0eWxpbmcgZGF0YSBwcmVzZW50IHdpdGhpbiBgVFN0eWxpbmdDb250ZXh0YC5cbiAqXG4gKiBUaGlzIGNsYXNzIGlzIGRlc2lnbmVkIHRvIGJlIHVzZWQgd2l0aGluIHRlc3RpbmcgY29kZSBvciB3aGVuIGFuXG4gKiBhcHBsaWNhdGlvbiBoYXMgYG5nRGV2TW9kZWAgYWN0aXZhdGVkLlxuICovXG5jbGFzcyBUU3R5bGluZ0NvbnRleHREZWJ1ZyB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyByZWFkb25seSBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQpIHt9XG5cbiAgZ2V0IGlzTG9ja2VkKCkgeyByZXR1cm4gaXNDb250ZXh0TG9ja2VkKHRoaXMuY29udGV4dCk7IH1cblxuICAvKipcbiAgICogUmV0dXJucyBhIGRldGFpbGVkIHN1bW1hcnkgb2YgZWFjaCBzdHlsaW5nIGVudHJ5IGluIHRoZSBjb250ZXh0LlxuICAgKlxuICAgKiBTZWUgYFRTdHlsaW5nVHVwbGVTdW1tYXJ5YC5cbiAgICovXG4gIGdldCBlbnRyaWVzKCk6IHtbcHJvcDogc3RyaW5nXTogVFN0eWxpbmdUdXBsZVN1bW1hcnl9IHtcbiAgICBjb25zdCBjb250ZXh0ID0gdGhpcy5jb250ZXh0O1xuICAgIGNvbnN0IGVudHJpZXM6IHtbcHJvcDogc3RyaW5nXTogVFN0eWxpbmdUdXBsZVN1bW1hcnl9ID0ge307XG4gICAgY29uc3Qgc3RhcnQgPSBUU3R5bGluZ0NvbnRleHRJbmRleC5NYXBCaW5kaW5nc1Bvc2l0aW9uO1xuICAgIGxldCBpID0gc3RhcnQ7XG4gICAgd2hpbGUgKGkgPCBjb250ZXh0Lmxlbmd0aCkge1xuICAgICAgY29uc3QgdmFsdWVzQ291bnQgPSBnZXRWYWx1ZXNDb3VudChjb250ZXh0LCBpKTtcbiAgICAgIC8vIHRoZSBjb250ZXh0IG1heSBjb250YWluIHBsYWNlaG9sZGVyIHZhbHVlcyB3aGljaCBhcmUgcG9wdWxhdGVkIGFoZWFkIG9mIHRpbWUsXG4gICAgICAvLyBidXQgY29udGFpbiBubyBhY3R1YWwgYmluZGluZyB2YWx1ZXMuIEluIHRoaXMgc2l0dWF0aW9uIHRoZXJlIGlzIG5vIHBvaW50IGluXG4gICAgICAvLyBjbGFzc2lmeWluZyB0aGlzIGFzIGFuIFwiZW50cnlcIiBzaW5jZSBubyByZWFsIGRhdGEgaXMgc3RvcmVkIGhlcmUgeWV0LlxuICAgICAgaWYgKHZhbHVlc0NvdW50KSB7XG4gICAgICAgIGNvbnN0IHByb3AgPSBnZXRQcm9wKGNvbnRleHQsIGkpO1xuICAgICAgICBjb25zdCBndWFyZE1hc2sgPSBnZXRHdWFyZE1hc2soY29udGV4dCwgaSk7XG4gICAgICAgIGNvbnN0IGRlZmF1bHRWYWx1ZSA9IGdldERlZmF1bHRWYWx1ZShjb250ZXh0LCBpKTtcbiAgICAgICAgY29uc3Qgc2FuaXRpemF0aW9uUmVxdWlyZWQgPSBpc1Nhbml0aXphdGlvblJlcXVpcmVkKGNvbnRleHQsIGkpO1xuICAgICAgICBjb25zdCBiaW5kaW5nc1N0YXJ0UG9zaXRpb24gPSBpICsgVFN0eWxpbmdDb250ZXh0SW5kZXguQmluZGluZ3NTdGFydE9mZnNldDtcblxuICAgICAgICBjb25zdCBzb3VyY2VzOiAobnVtYmVyIHwgc3RyaW5nIHwgbnVsbClbXSA9IFtdO1xuICAgICAgICBmb3IgKGxldCBqID0gMDsgaiA8IHZhbHVlc0NvdW50OyBqKyspIHtcbiAgICAgICAgICBzb3VyY2VzLnB1c2goY29udGV4dFtiaW5kaW5nc1N0YXJ0UG9zaXRpb24gKyBqXSBhcyBudW1iZXIgfCBzdHJpbmcgfCBudWxsKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGVudHJpZXNbcHJvcF0gPSB7cHJvcCwgZ3VhcmRNYXNrLCBzYW5pdGl6YXRpb25SZXF1aXJlZCwgdmFsdWVzQ291bnQsIGRlZmF1bHRWYWx1ZSwgc291cmNlc307XG4gICAgICB9XG5cbiAgICAgIGkgKz0gVFN0eWxpbmdDb250ZXh0SW5kZXguQmluZGluZ3NTdGFydE9mZnNldCArIHZhbHVlc0NvdW50O1xuICAgIH1cbiAgICByZXR1cm4gZW50cmllcztcbiAgfVxufVxuXG4vKipcbiAqIEEgaHVtYW4tcmVhZGFibGUgZGVidWcgc3VtbWFyeSBvZiB0aGUgc3R5bGluZyBkYXRhIHByZXNlbnQgZm9yIGEgYERlYnVnTm9kZWAgaW5zdGFuY2UuXG4gKlxuICogVGhpcyBjbGFzcyBpcyBkZXNpZ25lZCB0byBiZSB1c2VkIHdpdGhpbiB0ZXN0aW5nIGNvZGUgb3Igd2hlbiBhblxuICogYXBwbGljYXRpb24gaGFzIGBuZ0Rldk1vZGVgIGFjdGl2YXRlZC5cbiAqL1xuZXhwb3J0IGNsYXNzIE5vZGVTdHlsaW5nRGVidWcgaW1wbGVtZW50cyBEZWJ1Z1N0eWxpbmcge1xuICBwcml2YXRlIF9zYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbnxudWxsID0gbnVsbDtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQsIHByaXZhdGUgX2RhdGE6IExTdHlsaW5nRGF0YSxcbiAgICAgIHByaXZhdGUgX2lzQ2xhc3NCYXNlZD86IGJvb2xlYW4pIHt9XG5cbiAgLyoqXG4gICAqIE92ZXJyaWRlcyB0aGUgc2FuaXRpemVyIHVzZWQgdG8gcHJvY2VzcyBzdHlsZXMuXG4gICAqL1xuICBvdmVycmlkZVNhbml0aXplcihzYW5pdGl6ZXI6IFN0eWxlU2FuaXRpemVGbnxudWxsKSB7IHRoaXMuX3Nhbml0aXplciA9IHNhbml0aXplcjsgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIGEgZGV0YWlsZWQgc3VtbWFyeSBvZiBlYWNoIHN0eWxpbmcgZW50cnkgaW4gdGhlIGNvbnRleHQgYW5kXG4gICAqIHdoYXQgdGhlaXIgcnVudGltZSByZXByZXNlbnRhdGlvbiBpcy5cbiAgICpcbiAgICogU2VlIGBMU3R5bGluZ1N1bW1hcnlgLlxuICAgKi9cbiAgZ2V0IHN1bW1hcnkoKToge1trZXk6IHN0cmluZ106IExTdHlsaW5nU3VtbWFyeX0ge1xuICAgIGNvbnN0IGVudHJpZXM6IHtba2V5OiBzdHJpbmddOiBMU3R5bGluZ1N1bW1hcnl9ID0ge307XG4gICAgdGhpcy5fbWFwVmFsdWVzKChwcm9wOiBzdHJpbmcsIHZhbHVlOiBhbnksIGJpbmRpbmdJbmRleDogbnVtYmVyIHwgbnVsbCkgPT4ge1xuICAgICAgZW50cmllc1twcm9wXSA9IHtwcm9wLCB2YWx1ZSwgYmluZGluZ0luZGV4fTtcbiAgICB9KTtcbiAgICByZXR1cm4gZW50cmllcztcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIGEga2V5L3ZhbHVlIG1hcCBvZiBhbGwgdGhlIHN0eWxlcy9jbGFzc2VzIHRoYXQgd2VyZSBsYXN0IGFwcGxpZWQgdG8gdGhlIGVsZW1lbnQuXG4gICAqL1xuICBnZXQgdmFsdWVzKCk6IHtba2V5OiBzdHJpbmddOiBhbnl9IHtcbiAgICBjb25zdCBlbnRyaWVzOiB7W2tleTogc3RyaW5nXTogYW55fSA9IHt9O1xuICAgIHRoaXMuX21hcFZhbHVlcygocHJvcDogc3RyaW5nLCB2YWx1ZTogYW55KSA9PiB7IGVudHJpZXNbcHJvcF0gPSB2YWx1ZTsgfSk7XG4gICAgcmV0dXJuIGVudHJpZXM7XG4gIH1cblxuICBwcml2YXRlIF9tYXBWYWx1ZXMoZm46IChwcm9wOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmd8bnVsbCwgYmluZGluZ0luZGV4OiBudW1iZXJ8bnVsbCkgPT4gYW55KSB7XG4gICAgLy8gdGhlcmUgaXMgbm8gbmVlZCB0byBzdG9yZS90cmFjayBhbiBlbGVtZW50IGluc3RhbmNlLiBUaGVcbiAgICAvLyBlbGVtZW50IGlzIG9ubHkgdXNlZCB3aGVuIHRoZSBzdHlsaW5nIGFsZ29yaXRobSBhdHRlbXB0cyB0b1xuICAgIC8vIHN0eWxlIHRoZSB2YWx1ZSAoYW5kIHdlIG1vY2sgb3V0IHRoZSBzdHlsaW5nQXBwbHlGbiBhbnl3YXkpLlxuICAgIGNvbnN0IG1vY2tFbGVtZW50ID0ge30gYXMgYW55O1xuICAgIGNvbnN0IGhhc01hcHMgPSBnZXRWYWx1ZXNDb3VudCh0aGlzLmNvbnRleHQsIFRTdHlsaW5nQ29udGV4dEluZGV4Lk1hcEJpbmRpbmdzUG9zaXRpb24pID4gMDtcbiAgICBpZiAoaGFzTWFwcykge1xuICAgICAgYWN0aXZhdGVTdHlsaW5nTWFwRmVhdHVyZSgpO1xuICAgIH1cblxuICAgIGNvbnN0IG1hcEZuOiBBcHBseVN0eWxpbmdGbiA9XG4gICAgICAgIChyZW5kZXJlcjogYW55LCBlbGVtZW50OiBSRWxlbWVudCwgcHJvcDogc3RyaW5nLCB2YWx1ZTogc3RyaW5nIHwgbnVsbCxcbiAgICAgICAgIGJpbmRpbmdJbmRleD86IG51bWJlciB8IG51bGwpID0+IHsgZm4ocHJvcCwgdmFsdWUsIGJpbmRpbmdJbmRleCB8fCBudWxsKTsgfTtcblxuICAgIGNvbnN0IHNhbml0aXplciA9IHRoaXMuX2lzQ2xhc3NCYXNlZCA/IG51bGwgOiAodGhpcy5fc2FuaXRpemVyIHx8IGdldEN1cnJlbnRTdHlsZVNhbml0aXplcigpKTtcbiAgICBhcHBseVN0eWxpbmcodGhpcy5jb250ZXh0LCBudWxsLCBtb2NrRWxlbWVudCwgdGhpcy5fZGF0YSwgdHJ1ZSwgbWFwRm4sIHNhbml0aXplcik7XG4gIH1cbn1cbiJdfQ==