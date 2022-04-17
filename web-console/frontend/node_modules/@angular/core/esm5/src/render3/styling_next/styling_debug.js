import { getCurrentStyleSanitizer } from '../state';
import { attachDebugObject } from '../util/debug_utils';
import { applyStyling } from './bindings';
import { activateStylingMapFeature } from './map_based_bindings';
import { getDefaultValue, getGuardMask, getProp, getValuesCount, isContextLocked, isSanitizationRequired } from './util';
/**
 * Instantiates and attaches an instance of `TStylingContextDebug` to the provided context.
 */
export function attachStylingDebugObject(context) {
    var debug = new TStylingContextDebug(context);
    attachDebugObject(context, debug);
    return debug;
}
/**
 * A human-readable debug summary of the styling data present within `TStylingContext`.
 *
 * This class is designed to be used within testing code or when an
 * application has `ngDevMode` activated.
 */
var TStylingContextDebug = /** @class */ (function () {
    function TStylingContextDebug(context) {
        this.context = context;
    }
    Object.defineProperty(TStylingContextDebug.prototype, "isLocked", {
        get: function () { return isContextLocked(this.context); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TStylingContextDebug.prototype, "entries", {
        /**
         * Returns a detailed summary of each styling entry in the context.
         *
         * See `TStylingTupleSummary`.
         */
        get: function () {
            var context = this.context;
            var entries = {};
            var start = 3 /* MapBindingsPosition */;
            var i = start;
            while (i < context.length) {
                var valuesCount = getValuesCount(context, i);
                // the context may contain placeholder values which are populated ahead of time,
                // but contain no actual binding values. In this situation there is no point in
                // classifying this as an "entry" since no real data is stored here yet.
                if (valuesCount) {
                    var prop = getProp(context, i);
                    var guardMask = getGuardMask(context, i);
                    var defaultValue = getDefaultValue(context, i);
                    var sanitizationRequired = isSanitizationRequired(context, i);
                    var bindingsStartPosition = i + 3 /* BindingsStartOffset */;
                    var sources = [];
                    for (var j = 0; j < valuesCount; j++) {
                        sources.push(context[bindingsStartPosition + j]);
                    }
                    entries[prop] = { prop: prop, guardMask: guardMask, sanitizationRequired: sanitizationRequired, valuesCount: valuesCount, defaultValue: defaultValue, sources: sources };
                }
                i += 3 /* BindingsStartOffset */ + valuesCount;
            }
            return entries;
        },
        enumerable: true,
        configurable: true
    });
    return TStylingContextDebug;
}());
/**
 * A human-readable debug summary of the styling data present for a `DebugNode` instance.
 *
 * This class is designed to be used within testing code or when an
 * application has `ngDevMode` activated.
 */
var NodeStylingDebug = /** @class */ (function () {
    function NodeStylingDebug(context, _data, _isClassBased) {
        this.context = context;
        this._data = _data;
        this._isClassBased = _isClassBased;
        this._sanitizer = null;
    }
    /**
     * Overrides the sanitizer used to process styles.
     */
    NodeStylingDebug.prototype.overrideSanitizer = function (sanitizer) { this._sanitizer = sanitizer; };
    Object.defineProperty(NodeStylingDebug.prototype, "summary", {
        /**
         * Returns a detailed summary of each styling entry in the context and
         * what their runtime representation is.
         *
         * See `LStylingSummary`.
         */
        get: function () {
            var entries = {};
            this._mapValues(function (prop, value, bindingIndex) {
                entries[prop] = { prop: prop, value: value, bindingIndex: bindingIndex };
            });
            return entries;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NodeStylingDebug.prototype, "values", {
        /**
         * Returns a key/value map of all the styles/classes that were last applied to the element.
         */
        get: function () {
            var entries = {};
            this._mapValues(function (prop, value) { entries[prop] = value; });
            return entries;
        },
        enumerable: true,
        configurable: true
    });
    NodeStylingDebug.prototype._mapValues = function (fn) {
        // there is no need to store/track an element instance. The
        // element is only used when the styling algorithm attempts to
        // style the value (and we mock out the stylingApplyFn anyway).
        var mockElement = {};
        var hasMaps = getValuesCount(this.context, 3 /* MapBindingsPosition */) > 0;
        if (hasMaps) {
            activateStylingMapFeature();
        }
        var mapFn = function (renderer, element, prop, value, bindingIndex) { fn(prop, value, bindingIndex || null); };
        var sanitizer = this._isClassBased ? null : (this._sanitizer || getCurrentStyleSanitizer());
        applyStyling(this.context, null, mockElement, this._data, true, mapFn, sanitizer);
    };
    return NodeStylingDebug;
}());
export { NodeStylingDebug };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGluZ19kZWJ1Zy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvc3R5bGluZ19uZXh0L3N0eWxpbmdfZGVidWcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBVUEsT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQ2xELE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBRXRELE9BQU8sRUFBQyxZQUFZLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFFeEMsT0FBTyxFQUFDLHlCQUF5QixFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFDL0QsT0FBTyxFQUFDLGVBQWUsRUFBRSxZQUFZLEVBQUUsT0FBTyxFQUFFLGNBQWMsRUFBRSxlQUFlLEVBQWMsc0JBQXNCLEVBQUMsTUFBTSxRQUFRLENBQUM7QUF5Rm5JOztHQUVHO0FBQ0gsTUFBTSxVQUFVLHdCQUF3QixDQUFDLE9BQXdCO0lBQy9ELElBQU0sS0FBSyxHQUFHLElBQUksb0JBQW9CLENBQUMsT0FBTyxDQUFDLENBQUM7SUFDaEQsaUJBQWlCLENBQUMsT0FBTyxFQUFFLEtBQUssQ0FBQyxDQUFDO0lBQ2xDLE9BQU8sS0FBSyxDQUFDO0FBQ2YsQ0FBQztBQUVEOzs7OztHQUtHO0FBQ0g7SUFDRSw4QkFBNEIsT0FBd0I7UUFBeEIsWUFBTyxHQUFQLE9BQU8sQ0FBaUI7SUFBRyxDQUFDO0lBRXhELHNCQUFJLDBDQUFRO2FBQVosY0FBaUIsT0FBTyxlQUFlLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQzs7O09BQUE7SUFPeEQsc0JBQUkseUNBQU87UUFMWDs7OztXQUlHO2FBQ0g7WUFDRSxJQUFNLE9BQU8sR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDO1lBQzdCLElBQU0sT0FBTyxHQUEyQyxFQUFFLENBQUM7WUFDM0QsSUFBTSxL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OEJBQTJDLEdBQUcsQ0FBQyxDQUFDO1FBQzNGLElBQUksT0FBTyxFQUFFO1lBQ1gseUJBQXlCLEVBQUUsQ0FBQztTQUM3QjtRQUVELElBQU0sS0FBSyxHQUNQLFVBQUMsUUFBYSxFQUFFLE9BQWlCLEVBQUUsSUFBWSxFQUFFLEtBQW9CLEVBQ3BFLFlBQTRCLElBQU8sRUFBRSxDQUFDLElBQUksRUFBRSxLQUFLLEVBQUUsWUFBWSxJQUFJLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBRWpGLElBQU0sU0FBUyxHQUFHLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsVUFBVSxJQUFJLHdCQUF3QixFQUFFLENBQUMsQ0FBQztRQUM5RixZQUFZLENBQUMsSUFBSSxDQUFDLE9BQU8sRUFBRSxJQUFJLEVBQUUsV0FBVyxFQUFFLElBQUksQ0FBQyxLQUFLLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBRSxTQUFTLENBQUMsQ0FBQztJQUNwRixDQUFDO0lBQ0gsdUJBQUM7QUFBRCxDQUFDLEFBcERELElBb0RDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4qIEBsaWNlbnNlXG4qIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuKlxuKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4qL1xuaW1wb3J0IHtTdHlsZVNhbml0aXplRm59IGZyb20gJy4uLy4uL3Nhbml0aXphdGlvbi9zdHlsZV9zYW5pdGl6ZXInO1xuaW1wb3J0IHtSRWxlbWVudH0gZnJvbSAnLi4vaW50ZXJmYWNlcy9yZW5kZXJlcic7XG5pbXBvcnQge0xWaWV3fSBmcm9tICcuLi9pbnRlcmZhY2VzL3ZpZXcnO1xuaW1wb3J0IHtnZXRDdXJyZW50U3R5bGVTYW5pdGl6ZXJ9IGZyb20gJy4uL3N0YXRlJztcbmltcG9ydCB7YXR0YWNoRGVidWdPYmplY3R9IGZyb20gJy4uL3V0aWwvZGVidWdfdXRpbHMnO1xuXG5pbXBvcnQge2FwcGx5U3R5bGluZ30gZnJvbSAnLi9iaW5kaW5ncyc7XG5pbXBvcnQge0FwcGx5U3R5bGluZ0ZuLCBMU3R5bGluZ0RhdGEsIFRTdHlsaW5nQ29udGV4dCwgVFN0eWxpbmdDb250ZXh0SW5kZXh9IGZyb20gJy4vaW50ZXJmYWNlcyc7XG5pbXBvcnQge2FjdGl2YXRlU3R5bGluZ01hcEZlYXR1cmV9IGZyb20gJy4vbWFwX2Jhc2VkX2JpbmRpbmdzJztcbmltcG9ydCB7Z2V0RGVmYXVsdFZhbHVlLCBnZXRHdWFyZE1hc2ssIGdldFByb3AsIGdldFZhbHVlc0NvdW50LCBpc0NvbnRleHRMb2NrZWQsIGlzTWFwQmFzZWQsIGlzU2FuaXRpemF0aW9uUmVxdWlyZWR9IGZyb20gJy4vdXRpbCc7XG5cblxuXG4vKipcbiAqIC0tLS0tLS0tXG4gKlxuICogVGhpcyBmaWxlIGNvbnRhaW5zIHRoZSBjb3JlIGRlYnVnIGZ1bmN0aW9uYWxpdHkgZm9yIHN0eWxpbmcgaW4gQW5ndWxhci5cbiAqXG4gKiBUbyBsZWFybiBtb3JlIGFib3V0IHRoZSBhbGdvcml0aG0gc2VlIGBUU3R5bGluZ0NvbnRleHRgLlxuICpcbiAqIC0tLS0tLS0tXG4gKi9cblxuXG4vKipcbiAqIEEgZGVidWcvdGVzdGluZy1vcmllbnRlZCBzdW1tYXJ5IG9mIGEgc3R5bGluZyBlbnRyeS5cbiAqXG4gKiBBIHZhbHVlIHN1Y2ggYXMgdGhpcyBpcyBnZW5lcmF0ZWQgYXMgYW4gYXJ0aWZhY3Qgb2YgdGhlIGBEZWJ1Z1N0eWxpbmdgXG4gKiBzdW1tYXJ5LlxuICovXG5leHBvcnQgaW50ZXJmYWNlIExTdHlsaW5nU3VtbWFyeSB7XG4gIC8qKiBUaGUgc3R5bGUvY2xhc3MgcHJvcGVydHkgdGhhdCB0aGUgc3VtbWFyeSBpcyBhdHRhY2hlZCB0byAqL1xuICBwcm9wOiBzdHJpbmc7XG5cbiAgLyoqIFRoZSBsYXN0IGFwcGxpZWQgdmFsdWUgZm9yIHRoZSBzdHlsZS9jbGFzcyBwcm9wZXJ0eSAqL1xuICB2YWx1ZTogc3RyaW5nfGJvb2xlYW58bnVsbDtcblxuICAvKiogVGhlIGJpbmRpbmcgaW5kZXggb2YgdGhlIGxhc3QgYXBwbGllZCBzdHlsZS9jbGFzcyBwcm9wZXJ0eSAqL1xuICBiaW5kaW5nSW5kZXg6IG51bWJlcnxudWxsO1xufVxuXG4vKipcbiAqIEEgZGVidWcvdGVzdGluZy1vcmllbnRlZCBzdW1tYXJ5IG9mIGFsbCBzdHlsaW5nIGVudHJpZXMgZm9yIGEgYERlYnVnTm9kZWAgaW5zdGFuY2UuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgRGVidWdTdHlsaW5nIHtcbiAgLyoqIFRoZSBhc3NvY2lhdGVkIFRTdHlsaW5nQ29udGV4dCBpbnN0YW5jZSAqL1xuICBjb250ZXh0OiBUU3R5bGluZ0NvbnRleHQ7XG5cbiAgLyoqXG4gICAqIEEgc3VtbWFyaXphdGlvbiBvZiBlYWNoIHN0eWxlL2NsYXNzIHByb3BlcnR5XG4gICAqIHByZXNlbnQgaW4gdGhlIGNvbnRleHQuXG4gICAqL1xuICBzdW1tYXJ5OiB7W2tleTogc3RyaW5nXTogTFN0eWxpbmdTdW1tYXJ5fTtcblxuICAvKipcbiAgICogQSBrZXkvdmFsdWUgbWFwIG9mIGFsbCBzdHlsaW5nIHByb3BlcnRpZXMgYW5kIHRoZWlyXG4gICAqIHJ1bnRpbWUgdmFsdWVzLlxuICAgKi9cbiAgdmFsdWVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nIHwgbnVtYmVyIHwgbnVsbCB8IGJvb2xlYW59O1xuXG4gIC8qKlxuICAgKiBPdmVycmlkZXMgdGhlIHNhbml0aXplciB1c2VkIHRvIHByb2Nlc3Mgc3R5bGVzLlxuICAgKi9cbiAgb3ZlcnJpZGVTYW5pdGl6ZXIoc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm58bnVsbCk6IHZvaWQ7XG59XG5cbi8qKlxuICogQSBkZWJ1Zy90ZXN0aW5nLW9yaWVudGVkIHN1bW1hcnkgb2YgYWxsIHN0eWxpbmcgZW50cmllcyB3aXRoaW4gYSBgVFN0eWxpbmdDb250ZXh0YC5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBUU3R5bGluZ1R1cGxlU3VtbWFyeSB7XG4gIC8qKiBUaGUgcHJvcGVydHkgKHN0eWxlIG9yIGNsYXNzIHByb3BlcnR5KSB0aGF0IHRoaXMgdHVwbGUgcmVwcmVzZW50cyAqL1xuICBwcm9wOiBzdHJpbmc7XG5cbiAgLyoqIFRoZSB0b3RhbCBhbW91bnQgb2Ygc3R5bGluZyBlbnRyaWVzIGEgcGFydCBvZiB0aGlzIHR1cGxlICovXG4gIHZhbHVlc0NvdW50OiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIFRoZSBiaXQgZ3VhcmQgbWFzayB0aGF0IGlzIHVzZWQgdG8gY29tcGFyZSBhbmQgcHJvdGVjdCBhZ2FpbnN0XG4gICAqIHN0eWxpbmcgY2hhbmdlcyB3aGVuIGFuZCBzdHlsaW5nIGJpbmRpbmdzIHVwZGF0ZVxuICAgKi9cbiAgZ3VhcmRNYXNrOiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgb3Igbm90IHRoZSBlbnRyeSByZXF1aXJlcyBzYW5pdGl6YXRpb25cbiAgICovXG4gIHNhbml0aXphdGlvblJlcXVpcmVkOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBUaGUgZGVmYXVsdCB2YWx1ZSB0aGF0IHdpbGwgYmUgYXBwbGllZCBpZiBhbnkgYmluZGluZ3MgYXJlIGZhbHN5LlxuICAgKi9cbiAgZGVmYXVsdFZhbHVlOiBzdHJpbmd8Ym9vbGVhbnxudWxsO1xuXG4gIC8qKlxuICAgKiBBbGwgYmluZGluZ0luZGV4IHNvdXJjZXMgdGhhdCBoYXZlIGJlZW4gcmVnaXN0ZXJlZCBmb3IgdGhpcyBzdHlsZS5cbiAgICovXG4gIHNvdXJjZXM6IChudW1iZXJ8bnVsbHxzdHJpbmcpW107XG59XG5cbi8qKlxuICogSW5zdGFudGlhdGVzIGFuZCBhdHRhY2hlcyBhbiBpbnN0YW5jZSBvZiBgVFN0eWxpbmdDb250ZXh0RGVidWdgIHRvIHRoZSBwcm92aWRlZCBjb250ZXh0LlxuICovXG5leHBvcnQgZnVuY3Rpb24gYXR0YWNoU3R5bGluZ0RlYnVnT2JqZWN0KGNvbnRleHQ6IFRTdHlsaW5nQ29udGV4dCkge1xuICBjb25zdCBkZWJ1ZyA9IG5ldyBUU3R5bGluZ0NvbnRleHREZWJ1Zyhjb250ZXh0KTtcbiAgYXR0YWNoRGVidWdPYmplY3QoY29udGV4dCwgZGVidWcpO1xuICByZXR1cm4gZGVidWc7XG59XG5cbi8qKlxuICogQSBodW1hbi1yZWFkYWJsZSBkZWJ1ZyBzdW1tYXJ5IG9mIHRoZSBzdHlsaW5nIGRhdGEgcHJlc2VudCB3aXRoaW4gYFRTdHlsaW5nQ29udGV4dGAuXG4gKlxuICogVGhpcyBjbGFzcyBpcyBkZXNpZ25lZCB0byBiZSB1c2VkIHdpdGhpbiB0ZXN0aW5nIGNvZGUgb3Igd2hlbiBhblxuICogYXBwbGljYXRpb24gaGFzIGBuZ0Rldk1vZGVgIGFjdGl2YXRlZC5cbiAqL1xuY2xhc3MgVFN0eWxpbmdDb250ZXh0RGVidWcge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgcmVhZG9ubHkgY29udGV4dDogVFN0eWxpbmdDb250ZXh0KSB7fVxuXG4gIGdldCBpc0xvY2tlZCgpIHsgcmV0dXJuIGlzQ29udGV4dExvY2tlZCh0aGlzLmNvbnRleHQpOyB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgYSBkZXRhaWxlZCBzdW1tYXJ5IG9mIGVhY2ggc3R5bGluZyBlbnRyeSBpbiB0aGUgY29udGV4dC5cbiAgICpcbiAgICogU2VlIGBUU3R5bGluZ1R1cGxlU3VtbWFyeWAuXG4gICAqL1xuICBnZXQgZW50cmllcygpOiB7W3Byb3A6IHN0cmluZ106IFRTdHlsaW5nVHVwbGVTdW1tYXJ5fSB7XG4gICAgY29uc3QgY29udGV4dCA9IHRoaXMuY29udGV4dDtcbiAgICBjb25zdCBlbnRyaWVzOiB7W3Byb3A6IHN0cmluZ106IFRTdHlsaW5nVHVwbGVTdW1tYXJ5fSA9IHt9O1xuICAgIGNvbnN0IHN0YXJ0ID0gVFN0eWxpbmdDb250ZXh0SW5kZXguTWFwQmluZGluZ3NQb3NpdGlvbjtcbiAgICBsZXQgaSA9IHN0YXJ0O1xuICAgIHdoaWxlIChpIDwgY29udGV4dC5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IHZhbHVlc0NvdW50ID0gZ2V0VmFsdWVzQ291bnQoY29udGV4dCwgaSk7XG4gICAgICAvLyB0aGUgY29udGV4dCBtYXkgY29udGFpbiBwbGFjZWhvbGRlciB2YWx1ZXMgd2hpY2ggYXJlIHBvcHVsYXRlZCBhaGVhZCBvZiB0aW1lLFxuICAgICAgLy8gYnV0IGNvbnRhaW4gbm8gYWN0dWFsIGJpbmRpbmcgdmFsdWVzLiBJbiB0aGlzIHNpdHVhdGlvbiB0aGVyZSBpcyBubyBwb2ludCBpblxuICAgICAgLy8gY2xhc3NpZnlpbmcgdGhpcyBhcyBhbiBcImVudHJ5XCIgc2luY2Ugbm8gcmVhbCBkYXRhIGlzIHN0b3JlZCBoZXJlIHlldC5cbiAgICAgIGlmICh2YWx1ZXNDb3VudCkge1xuICAgICAgICBjb25zdCBwcm9wID0gZ2V0UHJvcChjb250ZXh0LCBpKTtcbiAgICAgICAgY29uc3QgZ3VhcmRNYXNrID0gZ2V0R3VhcmRNYXNrKGNvbnRleHQsIGkpO1xuICAgICAgICBjb25zdCBkZWZhdWx0VmFsdWUgPSBnZXREZWZhdWx0VmFsdWUoY29udGV4dCwgaSk7XG4gICAgICAgIGNvbnN0IHNhbml0aXphdGlvblJlcXVpcmVkID0gaXNTYW5pdGl6YXRpb25SZXF1aXJlZChjb250ZXh0LCBpKTtcbiAgICAgICAgY29uc3QgYmluZGluZ3NTdGFydFBvc2l0aW9uID0gaSArIFRTdHlsaW5nQ29udGV4dEluZGV4LkJpbmRpbmdzU3RhcnRPZmZzZXQ7XG5cbiAgICAgICAgY29uc3Qgc291cmNlczogKG51bWJlciB8IHN0cmluZyB8IG51bGwpW10gPSBbXTtcbiAgICAgICAgZm9yIChsZXQgaiA9IDA7IGogPCB2YWx1ZXNDb3VudDsgaisrKSB7XG4gICAgICAgICAgc291cmNlcy5wdXNoKGNvbnRleHRbYmluZGluZ3NTdGFydFBvc2l0aW9uICsgal0gYXMgbnVtYmVyIHwgc3RyaW5nIHwgbnVsbCk7XG4gICAgICAgIH1cblxuICAgICAgICBlbnRyaWVzW3Byb3BdID0ge3Byb3AsIGd1YXJkTWFzaywgc2FuaXRpemF0aW9uUmVxdWlyZWQsIHZhbHVlc0NvdW50LCBkZWZhdWx0VmFsdWUsIHNvdXJjZXN9O1xuICAgICAgfVxuXG4gICAgICBpICs9IFRTdHlsaW5nQ29udGV4dEluZGV4LkJpbmRpbmdzU3RhcnRPZmZzZXQgKyB2YWx1ZXNDb3VudDtcbiAgICB9XG4gICAgcmV0dXJuIGVudHJpZXM7XG4gIH1cbn1cblxuLyoqXG4gKiBBIGh1bWFuLXJlYWRhYmxlIGRlYnVnIHN1bW1hcnkgb2YgdGhlIHN0eWxpbmcgZGF0YSBwcmVzZW50IGZvciBhIGBEZWJ1Z05vZGVgIGluc3RhbmNlLlxuICpcbiAqIFRoaXMgY2xhc3MgaXMgZGVzaWduZWQgdG8gYmUgdXNlZCB3aXRoaW4gdGVzdGluZyBjb2RlIG9yIHdoZW4gYW5cbiAqIGFwcGxpY2F0aW9uIGhhcyBgbmdEZXZNb2RlYCBhY3RpdmF0ZWQuXG4gKi9cbmV4cG9ydCBjbGFzcyBOb2RlU3R5bGluZ0RlYnVnIGltcGxlbWVudHMgRGVidWdTdHlsaW5nIHtcbiAgcHJpdmF0ZSBfc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm58bnVsbCA9IG51bGw7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgY29udGV4dDogVFN0eWxpbmdDb250ZXh0LCBwcml2YXRlIF9kYXRhOiBMU3R5bGluZ0RhdGEsXG4gICAgICBwcml2YXRlIF9pc0NsYXNzQmFzZWQ/OiBib29sZWFuKSB7fVxuXG4gIC8qKlxuICAgKiBPdmVycmlkZXMgdGhlIHNhbml0aXplciB1c2VkIHRvIHByb2Nlc3Mgc3R5bGVzLlxuICAgKi9cbiAgb3ZlcnJpZGVTYW5pdGl6ZXIoc2FuaXRpemVyOiBTdHlsZVNhbml0aXplRm58bnVsbCkgeyB0aGlzLl9zYW5pdGl6ZXIgPSBzYW5pdGl6ZXI7IH1cblxuICAvKipcbiAgICogUmV0dXJucyBhIGRldGFpbGVkIHN1bW1hcnkgb2YgZWFjaCBzdHlsaW5nIGVudHJ5IGluIHRoZSBjb250ZXh0IGFuZFxuICAgKiB3aGF0IHRoZWlyIHJ1bnRpbWUgcmVwcmVzZW50YXRpb24gaXMuXG4gICAqXG4gICAqIFNlZSBgTFN0eWxpbmdTdW1tYXJ5YC5cbiAgICovXG4gIGdldCBzdW1tYXJ5KCk6IHtba2V5OiBzdHJpbmddOiBMU3R5bGluZ1N1bW1hcnl9IHtcbiAgICBjb25zdCBlbnRyaWVzOiB7W2tleTogc3RyaW5nXTogTFN0eWxpbmdTdW1tYXJ5fSA9IHt9O1xuICAgIHRoaXMuX21hcFZhbHVlcygocHJvcDogc3RyaW5nLCB2YWx1ZTogYW55LCBiaW5kaW5nSW5kZXg6IG51bWJlciB8IG51bGwpID0+IHtcbiAgICAgIGVudHJpZXNbcHJvcF0gPSB7cHJvcCwgdmFsdWUsIGJpbmRpbmdJbmRleH07XG4gICAgfSk7XG4gICAgcmV0dXJuIGVudHJpZXM7XG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyBhIGtleS92YWx1ZSBtYXAgb2YgYWxsIHRoZSBzdHlsZXMvY2xhc3NlcyB0aGF0IHdlcmUgbGFzdCBhcHBsaWVkIHRvIHRoZSBlbGVtZW50LlxuICAgKi9cbiAgZ2V0IHZhbHVlcygpOiB7W2tleTogc3RyaW5nXTogYW55fSB7XG4gICAgY29uc3QgZW50cmllczoge1trZXk6IHN0cmluZ106IGFueX0gPSB7fTtcbiAgICB0aGlzLl9tYXBWYWx1ZXMoKHByb3A6IHN0cmluZywgdmFsdWU6IGFueSkgPT4geyBlbnRyaWVzW3Byb3BdID0gdmFsdWU7IH0pO1xuICAgIHJldHVybiBlbnRyaWVzO1xuICB9XG5cbiAgcHJpdmF0ZSBfbWFwVmFsdWVzKGZuOiAocHJvcDogc3RyaW5nLCB2YWx1ZTogc3RyaW5nfG51bGwsIGJpbmRpbmdJbmRleDogbnVtYmVyfG51bGwpID0+IGFueSkge1xuICAgIC8vIHRoZXJlIGlzIG5vIG5lZWQgdG8gc3RvcmUvdHJhY2sgYW4gZWxlbWVudCBpbnN0YW5jZS4gVGhlXG4gICAgLy8gZWxlbWVudCBpcyBvbmx5IHVzZWQgd2hlbiB0aGUgc3R5bGluZyBhbGdvcml0aG0gYXR0ZW1wdHMgdG9cbiAgICAvLyBzdHlsZSB0aGUgdmFsdWUgKGFuZCB3ZSBtb2NrIG91dCB0aGUgc3R5bGluZ0FwcGx5Rm4gYW55d2F5KS5cbiAgICBjb25zdCBtb2NrRWxlbWVudCA9IHt9IGFzIGFueTtcbiAgICBjb25zdCBoYXNNYXBzID0gZ2V0VmFsdWVzQ291bnQodGhpcy5jb250ZXh0LCBUU3R5bGluZ0NvbnRleHRJbmRleC5NYXBCaW5kaW5nc1Bvc2l0aW9uKSA+IDA7XG4gICAgaWYgKGhhc01hcHMpIHtcbiAgICAgIGFjdGl2YXRlU3R5bGluZ01hcEZlYXR1cmUoKTtcbiAgICB9XG5cbiAgICBjb25zdCBtYXBGbjogQXBwbHlTdHlsaW5nRm4gPVxuICAgICAgICAocmVuZGVyZXI6IGFueSwgZWxlbWVudDogUkVsZW1lbnQsIHByb3A6IHN0cmluZywgdmFsdWU6IHN0cmluZyB8IG51bGwsXG4gICAgICAgICBiaW5kaW5nSW5kZXg/OiBudW1iZXIgfCBudWxsKSA9PiB7IGZuKHByb3AsIHZhbHVlLCBiaW5kaW5nSW5kZXggfHwgbnVsbCk7IH07XG5cbiAgICBjb25zdCBzYW5pdGl6ZXIgPSB0aGlzLl9pc0NsYXNzQmFzZWQgPyBudWxsIDogKHRoaXMuX3Nhbml0aXplciB8fCBnZXRDdXJyZW50U3R5bGVTYW5pdGl6ZXIoKSk7XG4gICAgYXBwbHlTdHlsaW5nKHRoaXMuY29udGV4dCwgbnVsbCwgbW9ja0VsZW1lbnQsIHRoaXMuX2RhdGEsIHRydWUsIG1hcEZuLCBzYW5pdGl6ZXIpO1xuICB9XG59XG4iXX0=