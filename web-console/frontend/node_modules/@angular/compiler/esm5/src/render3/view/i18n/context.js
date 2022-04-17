/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { assembleBoundTextPlaceholders, findIndex, getSeqNumberGenerator, updatePlaceholderMap, wrapI18nPlaceholder } from './util';
var TagType;
(function (TagType) {
    TagType[TagType["ELEMENT"] = 0] = "ELEMENT";
    TagType[TagType["TEMPLATE"] = 1] = "TEMPLATE";
    TagType[TagType["PROJECTION"] = 2] = "PROJECTION";
})(TagType || (TagType = {}));
/**
 * Generates an object that is used as a shared state between parent and all child contexts.
 */
function setupRegistry() {
    return { getUniqueId: getSeqNumberGenerator(), icus: new Map() };
}
/**
 * I18nContext is a helper class which keeps track of all i18n-related aspects
 * (accumulates placeholders, bindings, etc) between i18nStart and i18nEnd instructions.
 *
 * When we enter a nested template, the top-level context is being passed down
 * to the nested component, which uses this context to generate a child instance
 * of I18nContext class (to handle nested template) and at the end, reconciles it back
 * with the parent context.
 *
 * @param index Instruction index of i18nStart, which initiates this context
 * @param ref Reference to a translation const that represents the content if thus context
 * @param level Nestng level defined for child contexts
 * @param templateIndex Instruction index of a template which this context belongs to
 * @param meta Meta information (id, meaning, description, etc) associated with this context
 */
var I18nContext = /** @class */ (function () {
    function I18nContext(index, ref, level, templateIndex, meta, registry) {
        if (level === void 0) { level = 0; }
        if (templateIndex === void 0) { templateIndex = null; }
        this.index = index;
        this.ref = ref;
        this.level = level;
        this.templateIndex = templateIndex;
        this.meta = meta;
        this.registry = registry;
        this.bindings = new Set();
        this.placeholders = new Map();
        this.isEmitted = false;
        this._unresolvedCtxCount = 0;
        this._registry = registry || setupRegistry();
        this.id = this._registry.getUniqueId();
    }
    I18nContext.prototype.appendTag = function (type, node, index, closed) {
        if (node.isVoid && closed) {
            return; // ignore "close" for void tags
        }
        var ph = node.isVoid || !closed ? node.startName : node.closeName;
        var content = { type: type, index: index, ctx: this.id, isVoid: node.isVoid, closed: closed };
        updatePlaceholderMap(this.placeholders, ph, content);
    };
    Object.defineProperty(I18nContext.prototype, "icus", {
        get: function () { return this._registry.icus; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(I18nContext.prototype, "isRoot", {
        get: function () { return this.level === 0; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(I18nContext.prototype, "isResolved", {
        get: function () { return this._unresolvedCtxCount === 0; },
        enumerable: true,
        configurable: true
    });
    I18nContext.prototype.getSerializedPlaceholders = function () {
        var result = new Map();
        this.placeholders.forEach(function (values, key) { return result.set(key, values.map(serializePlaceholderValue)); });
        return result;
    };
    // public API to accumulate i18n-related content
    I18nContext.prototype.appendBinding = function (binding) { this.bindings.add(binding); };
    I18nContext.prototype.appendIcu = function (name, ref) {
        updatePlaceholderMap(this._registry.icus, name, ref);
    };
    I18nContext.prototype.appendBoundText = function (node) {
        var _this = this;
        var phs = assembleBoundTextPlaceholders(node, this.bindings.size, this.id);
        phs.forEach(function (values, key) { return updatePlaceholderMap.apply(void 0, tslib_1.__spread([_this.placeholders, key], values)); });
    };
    I18nContext.prototype.appendTemplate = function (node, index) {
        // add open and close tags at the same time,
        // since we process nested templates separately
        this.appendTag(TagType.TEMPLATE, node, index, false);
        this.appendTag(TagType.TEMPLATE, node, index, true);
        this._unresolvedCtxCount++;
    };
    I18nContext.prototype.appendElement = function (node, index, closed) {
        this.appendTag(TagType.ELEMENT, node, index, closed);
    };
    I18nContext.prototype.appendProjection = function (node, index) {
        // add open and close tags at the same time,
        // since we process projected content separately
        this.appendTag(TagType.PROJECTION, node, index, false);
        this.appendTag(TagType.PROJECTION, node, index, true);
    };
    /**
     * Generates an instance of a child context based on the root one,
     * when we enter a nested template within I18n section.
     *
     * @param index Instruction index of corresponding i18nStart, which initiates this context
     * @param templateIndex Instruction index of a template which this context belongs to
     * @param meta Meta information (id, meaning, description, etc) associated with this context
     *
     * @returns I18nContext instance
     */
    I18nContext.prototype.forkChildContext = function (index, templateIndex, meta) {
        return new I18nContext(index, this.ref, this.level + 1, templateIndex, meta, this._registry);
    };
    /**
     * Reconciles child context into parent one once the end of the i18n block is reached (i18nEnd).
     *
     * @param context Child I18nContext instance to be reconciled with parent context.
     */
    I18nContext.prototype.reconcileChildContext = function (context) {
        var _this = this;
        // set the right context id for open and close
        // template tags, so we can use it as sub-block ids
        ['start', 'close'].forEach(function (op) {
            var key = context.meta[op + "Name"];
            var phs = _this.placeholders.get(key) || [];
            var tag = phs.find(findTemplateFn(_this.id, context.templateIndex));
            if (tag) {
                tag.ctx = context.id;
            }
        });
        // reconcile placeholders
        var childPhs = context.placeholders;
        childPhs.forEach(function (values, key) {
            var phs = _this.placeholders.get(key);
            if (!phs) {
                _this.placeholders.set(key, values);
                return;
            }
            // try to find matching template...
            var tmplIdx = findIndex(phs, findTemplateFn(context.id, context.templateIndex));
            if (tmplIdx >= 0) {
                // ... if found - replace it with nested template content
                var isCloseTag = key.startsWith('CLOSE');
                var isTemplateTag = key.endsWith('NG-TEMPLATE');
                if (isTemplateTag) {
                    // current template's content is placed before or after
                    // parent template tag, depending on the open/close atrribute
                    phs.splice.apply(phs, tslib_1.__spread([tmplIdx + (isCloseTag ? 0 : 1), 0], values));
                }
                else {
                    var idx = isCloseTag ? values.length - 1 : 0;
                    values[idx].tmpl = phs[tmplIdx];
                    phs.splice.apply(phs, tslib_1.__spread([tmplIdx, 1], values));
                }
            }
            else {
                // ... otherwise just append content to placeholder value
                phs.push.apply(phs, tslib_1.__spread(values));
            }
            _this.placeholders.set(key, phs);
        });
        this._unresolvedCtxCount--;
    };
    return I18nContext;
}());
export { I18nContext };
//
// Helper methods
//
function wrap(symbol, index, contextId, closed) {
    var state = closed ? '/' : '';
    return wrapI18nPlaceholder("" + state + symbol + index, contextId);
}
function wrapTag(symbol, _a, closed) {
    var index = _a.index, ctx = _a.ctx, isVoid = _a.isVoid;
    return isVoid ? wrap(symbol, index, ctx) + wrap(symbol, index, ctx, true) :
        wrap(symbol, index, ctx, closed);
}
function findTemplateFn(ctx, templateIndex) {
    return function (token) { return typeof token === 'object' && token.type === TagType.TEMPLATE &&
        token.index === templateIndex && token.ctx === ctx; };
}
function serializePlaceholderValue(value) {
    var element = function (data, closed) { return wrapTag('#', data, closed); };
    var template = function (data, closed) { return wrapTag('*', data, closed); };
    var projection = function (data, closed) { return wrapTag('!', data, closed); };
    switch (value.type) {
        case TagType.ELEMENT:
            // close element tag
            if (value.closed) {
                return element(value, true) + (value.tmpl ? template(value.tmpl, true) : '');
            }
            // open element tag that also initiates a template
            if (value.tmpl) {
                return template(value.tmpl) + element(value) +
                    (value.isVoid ? template(value.tmpl, true) : '');
            }
            return element(value);
        case TagType.TEMPLATE:
            return template(value, value.closed);
        case TagType.PROJECTION:
            return projection(value, value.closed);
        default:
            return value;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29udGV4dC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3ZpZXcvaTE4bi9jb250ZXh0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFNSCxPQUFPLEVBQUMsNkJBQTZCLEVBQUUsU0FBUyxFQUFFLHFCQUFxQixFQUFFLG9CQUFvQixFQUFFLG1CQUFtQixFQUFDLE1BQU0sUUFBUSxDQUFDO0FBRWxJLElBQUssT0FJSjtBQUpELFdBQUssT0FBTztJQUNWLDJDQUFPLENBQUE7SUFDUCw2Q0FBUSxDQUFBO0lBQ1IsaURBQVUsQ0FBQTtBQUNaLENBQUMsRUFKSSxPQUFPLEtBQVAsT0FBTyxRQUlYO0FBRUQ7O0dBRUc7QUFDSCxTQUFTLGFBQWE7SUFDcEIsT0FBTyxFQUFDLFdBQVcsRUFBRSxxQkFBcUIsRUFBRSxFQUFFLElBQUksRUFBRSxJQUFJLEdBQUcsRUFBaUIsRUFBQyxDQUFDO0FBQ2hGLENBQUM7QUFFRDs7Ozs7Ozs7Ozs7Ozs7R0FjRztBQUNIO0lBU0UscUJBQ2EsS0FBYSxFQUFXLEdBQWtCLEVBQVcsS0FBaUIsRUFDdEUsYUFBaUMsRUFBVyxJQUFjLEVBQVUsUUFBYztRQUQ3QixzQkFBQSxFQUFBLFNBQWlCO1FBQ3RFLDhCQUFBLEVBQUEsb0JBQWlDO1FBRGpDLFVBQUssR0FBTCxLQUFLLENBQVE7UUFBVyxRQUFHLEdBQUgsR0FBRyxDQUFlO1FBQVcsVUFBSyxHQUFMLEtBQUssQ0FBWTtRQUN0RSxrQkFBYSxHQUFiLGFBQWEsQ0FBb0I7UUFBVyxTQUFJLEdBQUosSUFBSSxDQUFVO1FBQVUsYUFBUSxHQUFSLFFBQVEsQ0FBTTtRQVR4RixhQUFRLEdBQUcsSUFBSSxHQUFHLEVBQU8sQ0FBQztRQUMxQixpQkFBWSxHQUFHLElBQUksR0FBRyxFQUFpQixDQUFDO1FBQ3hDLGNBQVMsR0FBWSxLQUFL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QyxDQUFDLENBQUMsUUFBUSxDQUFDLEtBQUssQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO2FBQzlFO1lBQ0Qsa0RBQWtEO1lBQ2xELElBQUksS0FBSyxDQUFDLElBQUksRUFBRTtnQkFDZCxPQUFPLFFBQVEsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLEdBQUcsT0FBTyxDQUFDLEtBQUssQ0FBQztvQkFDeEMsQ0FBQyxLQUFLLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLElBQUksRUFBRSxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUM7YUFDdEQ7WUFDRCxPQUFPLE9BQU8sQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUV4QixLQUFLLE9BQU8sQ0FBQyxRQUFRO1lBQ25CLE9BQU8sUUFBUSxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsTUFBTSxDQUFDLENBQUM7UUFFdkMsS0FBSyxPQUFPLENBQUMsVUFBVTtZQUNyQixPQUFPLFVBQVUsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDO1FBRXpDO1lBQ0UsT0FBTyxLQUFLLENBQUM7S0FDaEI7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0FTVH0gZnJvbSAnLi4vLi4vLi4vZXhwcmVzc2lvbl9wYXJzZXIvYXN0JztcbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi4vLi4vLi4vaTE4bi9pMThuX2FzdCc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4uLy4uLy4uL291dHB1dC9vdXRwdXRfYXN0JztcblxuaW1wb3J0IHthc3NlbWJsZUJvdW5kVGV4dFBsYWNlaG9sZGVycywgZmluZEluZGV4LCBnZXRTZXFOdW1iZXJHZW5lcmF0b3IsIHVwZGF0ZVBsYWNlaG9sZGVyTWFwLCB3cmFwSTE4blBsYWNlaG9sZGVyfSBmcm9tICcuL3V0aWwnO1xuXG5lbnVtIFRhZ1R5cGUge1xuICBFTEVNRU5ULFxuICBURU1QTEFURSxcbiAgUFJPSkVDVElPTlxufVxuXG4vKipcbiAqIEdlbmVyYXRlcyBhbiBvYmplY3QgdGhhdCBpcyB1c2VkIGFzIGEgc2hhcmVkIHN0YXRlIGJldHdlZW4gcGFyZW50IGFuZCBhbGwgY2hpbGQgY29udGV4dHMuXG4gKi9cbmZ1bmN0aW9uIHNldHVwUmVnaXN0cnkoKSB7XG4gIHJldHVybiB7Z2V0VW5pcXVlSWQ6IGdldFNlcU51bWJlckdlbmVyYXRvcigpLCBpY3VzOiBuZXcgTWFwPHN0cmluZywgYW55W10+KCl9O1xufVxuXG4vKipcbiAqIEkxOG5Db250ZXh0IGlzIGEgaGVscGVyIGNsYXNzIHdoaWNoIGtlZXBzIHRyYWNrIG9mIGFsbCBpMThuLXJlbGF0ZWQgYXNwZWN0c1xuICogKGFjY3VtdWxhdGVzIHBsYWNlaG9sZGVycywgYmluZGluZ3MsIGV0YykgYmV0d2VlbiBpMThuU3RhcnQgYW5kIGkxOG5FbmQgaW5zdHJ1Y3Rpb25zLlxuICpcbiAqIFdoZW4gd2UgZW50ZXIgYSBuZXN0ZWQgdGVtcGxhdGUsIHRoZSB0b3AtbGV2ZWwgY29udGV4dCBpcyBiZWluZyBwYXNzZWQgZG93blxuICogdG8gdGhlIG5lc3RlZCBjb21wb25lbnQsIHdoaWNoIHVzZXMgdGhpcyBjb250ZXh0IHRvIGdlbmVyYXRlIGEgY2hpbGQgaW5zdGFuY2VcbiAqIG9mIEkxOG5Db250ZXh0IGNsYXNzICh0byBoYW5kbGUgbmVzdGVkIHRlbXBsYXRlKSBhbmQgYXQgdGhlIGVuZCwgcmVjb25jaWxlcyBpdCBiYWNrXG4gKiB3aXRoIHRoZSBwYXJlbnQgY29udGV4dC5cbiAqXG4gKiBAcGFyYW0gaW5kZXggSW5zdHJ1Y3Rpb24gaW5kZXggb2YgaTE4blN0YXJ0LCB3aGljaCBpbml0aWF0ZXMgdGhpcyBjb250ZXh0XG4gKiBAcGFyYW0gcmVmIFJlZmVyZW5jZSB0byBhIHRyYW5zbGF0aW9uIGNvbnN0IHRoYXQgcmVwcmVzZW50cyB0aGUgY29udGVudCBpZiB0aHVzIGNvbnRleHRcbiAqIEBwYXJhbSBsZXZlbCBOZXN0bmcgbGV2ZWwgZGVmaW5lZCBmb3IgY2hpbGQgY29udGV4dHNcbiAqIEBwYXJhbSB0ZW1wbGF0ZUluZGV4IEluc3RydWN0aW9uIGluZGV4IG9mIGEgdGVtcGxhdGUgd2hpY2ggdGhpcyBjb250ZXh0IGJlbG9uZ3MgdG9cbiAqIEBwYXJhbSBtZXRhIE1ldGEgaW5mb3JtYXRpb24gKGlkLCBtZWFuaW5nLCBkZXNjcmlwdGlvbiwgZXRjKSBhc3NvY2lhdGVkIHdpdGggdGhpcyBjb250ZXh0XG4gKi9cbmV4cG9ydCBjbGFzcyBJMThuQ29udGV4dCB7XG4gIHB1YmxpYyByZWFkb25seSBpZDogbnVtYmVyO1xuICBwdWJsaWMgYmluZGluZ3MgPSBuZXcgU2V0PEFTVD4oKTtcbiAgcHVibGljIHBsYWNlaG9sZGVycyA9IG5ldyBNYXA8c3RyaW5nLCBhbnlbXT4oKTtcbiAgcHVibGljIGlzRW1pdHRlZDogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIHByaXZhdGUgX3JlZ2lzdHJ5ICE6IGFueTtcbiAgcHJpdmF0ZSBfdW5yZXNvbHZlZEN0eENvdW50OiBudW1iZXIgPSAwO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcmVhZG9ubHkgaW5kZXg6IG51bWJlciwgcmVhZG9ubHkgcmVmOiBvLlJlYWRWYXJFeHByLCByZWFkb25seSBsZXZlbDogbnVtYmVyID0gMCxcbiAgICAgIHJlYWRvbmx5IHRlbXBsYXRlSW5kZXg6IG51bWJlcnxudWxsID0gbnVsbCwgcmVhZG9ubHkgbWV0YTogaTE4bi5BU1QsIHByaXZhdGUgcmVnaXN0cnk/OiBhbnkpIHtcbiAgICB0aGlzLl9yZWdpc3RyeSA9IHJlZ2lzdHJ5IHx8IHNldHVwUmVnaXN0cnkoKTtcbiAgICB0aGlzLmlkID0gdGhpcy5fcmVnaXN0cnkuZ2V0VW5pcXVlSWQoKTtcbiAgfVxuXG4gIHByaXZhdGUgYXBwZW5kVGFnKHR5cGU6IFRhZ1R5cGUsIG5vZGU6IGkxOG4uVGFnUGxhY2Vob2xkZXIsIGluZGV4OiBudW1iZXIsIGNsb3NlZD86IGJvb2xlYW4pIHtcbiAgICBpZiAobm9kZS5pc1ZvaWQgJiYgY2xvc2VkKSB7XG4gICAgICByZXR1cm47ICAvLyBpZ25vcmUgXCJjbG9zZVwiIGZvciB2b2lkIHRhZ3NcbiAgICB9XG4gICAgY29uc3QgcGggPSBub2RlLmlzVm9pZCB8fCAhY2xvc2VkID8gbm9kZS5zdGFydE5hbWUgOiBub2RlLmNsb3NlTmFtZTtcbiAgICBjb25zdCBjb250ZW50ID0ge3R5cGUsIGluZGV4LCBjdHg6IHRoaXMuaWQsIGlzVm9pZDogbm9kZS5pc1ZvaWQsIGNsb3NlZH07XG4gICAgdXBkYXRlUGxhY2Vob2xkZXJNYXAodGhpcy5wbGFjZWhvbGRlcnMsIHBoLCBjb250ZW50KTtcbiAgfVxuXG4gIGdldCBpY3VzKCkgeyByZXR1cm4gdGhpcy5fcmVnaXN0cnkuaWN1czsgfVxuICBnZXQgaXNSb290KCkgeyByZXR1cm4gdGhpcy5sZXZlbCA9PT0gMDsgfVxuICBnZXQgaXNSZXNvbHZlZCgpIHsgcmV0dXJuIHRoaXMuX3VucmVzb2x2ZWRDdHhDb3VudCA9PT0gMDsgfVxuXG4gIGdldFNlcmlhbGl6ZWRQbGFjZWhvbGRlcnMoKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gbmV3IE1hcDxzdHJpbmcsIGFueVtdPigpO1xuICAgIHRoaXMucGxhY2Vob2xkZXJzLmZvckVhY2goXG4gICAgICAgICh2YWx1ZXMsIGtleSkgPT4gcmVzdWx0LnNldChrZXksIHZhbHVlcy5tYXAoc2VyaWFsaXplUGxhY2Vob2xkZXJWYWx1ZSkpKTtcbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgLy8gcHVibGljIEFQSSB0byBhY2N1bXVsYXRlIGkxOG4tcmVsYXRlZCBjb250ZW50XG4gIGFwcGVuZEJpbmRpbmcoYmluZGluZzogQVNUKSB7IHRoaXMuYmluZGluZ3MuYWRkKGJpbmRpbmcpOyB9XG4gIGFwcGVuZEljdShuYW1lOiBzdHJpbmcsIHJlZjogby5FeHByZXNzaW9uKSB7XG4gICAgdXBkYXRlUGxhY2Vob2xkZXJNYXAodGhpcy5fcmVnaXN0cnkuaWN1cywgbmFtZSwgcmVmKTtcbiAgfVxuICBhcHBlbmRCb3VuZFRleHQobm9kZTogaTE4bi5BU1QpIHtcbiAgICBjb25zdCBwaHMgPSBhc3NlbWJsZUJvdW5kVGV4dFBsYWNlaG9sZGVycyhub2RlLCB0aGlzLmJpbmRpbmdzLnNpemUsIHRoaXMuaWQpO1xuICAgIHBocy5mb3JFYWNoKCh2YWx1ZXMsIGtleSkgPT4gdXBkYXRlUGxhY2Vob2xkZXJNYXAodGhpcy5wbGFjZWhvbGRlcnMsIGtleSwgLi4udmFsdWVzKSk7XG4gIH1cbiAgYXBwZW5kVGVtcGxhdGUobm9kZTogaTE4bi5BU1QsIGluZGV4OiBudW1iZXIpIHtcbiAgICAvLyBhZGQgb3BlbiBhbmQgY2xvc2UgdGFncyBhdCB0aGUgc2FtZSB0aW1lLFxuICAgIC8vIHNpbmNlIHdlIHByb2Nlc3MgbmVzdGVkIHRlbXBsYXRlcyBzZXBhcmF0ZWx5XG4gICAgdGhpcy5hcHBlbmRUYWcoVGFnVHlwZS5URU1QTEFURSwgbm9kZSBhcyBpMThuLlRhZ1BsYWNlaG9sZGVyLCBpbmRleCwgZmFsc2UpO1xuICAgIHRoaXMuYXBwZW5kVGFnKFRhZ1R5cGUuVEVNUExBVEUsIG5vZGUgYXMgaTE4bi5UYWdQbGFjZWhvbGRlciwgaW5kZXgsIHRydWUpO1xuICAgIHRoaXMuX3VucmVzb2x2ZWRDdHhDb3VudCsrO1xuICB9XG4gIGFwcGVuZEVsZW1lbnQobm9kZTogaTE4bi5BU1QsIGluZGV4OiBudW1iZXIsIGNsb3NlZD86IGJvb2xlYW4pIHtcbiAgICB0aGlzLmFwcGVuZFRhZyhUYWdUeXBlLkVMRU1FTlQsIG5vZGUgYXMgaTE4bi5UYWdQbGFjZWhvbGRlciwgaW5kZXgsIGNsb3NlZCk7XG4gIH1cbiAgYXBwZW5kUHJvamVjdGlvbihub2RlOiBpMThuLkFTVCwgaW5kZXg6IG51bWJlcikge1xuICAgIC8vIGFkZCBvcGVuIGFuZCBjbG9zZSB0YWdzIGF0IHRoZSBzYW1lIHRpbWUsXG4gICAgLy8gc2luY2Ugd2UgcHJvY2VzcyBwcm9qZWN0ZWQgY29udGVudCBzZXBhcmF0ZWx5XG4gICAgdGhpcy5hcHBlbmRUYWcoVGFnVHlwZS5QUk9KRUNUSU9OLCBub2RlIGFzIGkxOG4uVGFnUGxhY2Vob2xkZXIsIGluZGV4LCBmYWxzZSk7XG4gICAgdGhpcy5hcHBlbmRUYWcoVGFnVHlwZS5QUk9KRUNUSU9OLCBub2RlIGFzIGkxOG4uVGFnUGxhY2Vob2xkZXIsIGluZGV4LCB0cnVlKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBHZW5lcmF0ZXMgYW4gaW5zdGFuY2Ugb2YgYSBjaGlsZCBjb250ZXh0IGJhc2VkIG9uIHRoZSByb290IG9uZSxcbiAgICogd2hlbiB3ZSBlbnRlciBhIG5lc3RlZCB0ZW1wbGF0ZSB3aXRoaW4gSTE4biBzZWN0aW9uLlxuICAgKlxuICAgKiBAcGFyYW0gaW5kZXggSW5zdHJ1Y3Rpb24gaW5kZXggb2YgY29ycmVzcG9uZGluZyBpMThuU3RhcnQsIHdoaWNoIGluaXRpYXRlcyB0aGlzIGNvbnRleHRcbiAgICogQHBhcmFtIHRlbXBsYXRlSW5kZXggSW5zdHJ1Y3Rpb24gaW5kZXggb2YgYSB0ZW1wbGF0ZSB3aGljaCB0aGlzIGNvbnRleHQgYmVsb25ncyB0b1xuICAgKiBAcGFyYW0gbWV0YSBNZXRhIGluZm9ybWF0aW9uIChpZCwgbWVhbmluZywgZGVzY3JpcHRpb24sIGV0YykgYXNzb2NpYXRlZCB3aXRoIHRoaXMgY29udGV4dFxuICAgKlxuICAgKiBAcmV0dXJucyBJMThuQ29udGV4dCBpbnN0YW5jZVxuICAgKi9cbiAgZm9ya0NoaWxkQ29udGV4dChpbmRleDogbnVtYmVyLCB0ZW1wbGF0ZUluZGV4OiBudW1iZXIsIG1ldGE6IGkxOG4uQVNUKSB7XG4gICAgcmV0dXJuIG5ldyBJMThuQ29udGV4dChpbmRleCwgdGhpcy5yZWYsIHRoaXMubGV2ZWwgKyAxLCB0ZW1wbGF0ZUluZGV4LCBtZXRhLCB0aGlzLl9yZWdpc3RyeSk7XG4gIH1cblxuICAvKipcbiAgICogUmVjb25jaWxlcyBjaGlsZCBjb250ZXh0IGludG8gcGFyZW50IG9uZSBvbmNlIHRoZSBlbmQgb2YgdGhlIGkxOG4gYmxvY2sgaXMgcmVhY2hlZCAoaTE4bkVuZCkuXG4gICAqXG4gICAqIEBwYXJhbSBjb250ZXh0IENoaWxkIEkxOG5Db250ZXh0IGluc3RhbmNlIHRvIGJlIHJlY29uY2lsZWQgd2l0aCBwYXJlbnQgY29udGV4dC5cbiAgICovXG4gIHJlY29uY2lsZUNoaWxkQ29udGV4dChjb250ZXh0OiBJMThuQ29udGV4dCkge1xuICAgIC8vIHNldCB0aGUgcmlnaHQgY29udGV4dCBpZCBmb3Igb3BlbiBhbmQgY2xvc2VcbiAgICAvLyB0ZW1wbGF0ZSB0YWdzLCBzbyB3ZSBjYW4gdXNlIGl0IGFzIHN1Yi1ibG9jayBpZHNcbiAgICBbJ3N0YXJ0JywgJ2Nsb3NlJ10uZm9yRWFjaCgob3A6IHN0cmluZykgPT4ge1xuICAgICAgY29uc3Qga2V5ID0gKGNvbnRleHQubWV0YSBhcyBhbnkpW2Ake29wfU5hbWVgXTtcbiAgICAgIGNvbnN0IHBocyA9IHRoaXMucGxhY2Vob2xkZXJzLmdldChrZXkpIHx8IFtdO1xuICAgICAgY29uc3QgdGFnID0gcGhzLmZpbmQoZmluZFRlbXBsYXRlRm4odGhpcy5pZCwgY29udGV4dC50ZW1wbGF0ZUluZGV4KSk7XG4gICAgICBpZiAodGFnKSB7XG4gICAgICAgIHRhZy5jdHggPSBjb250ZXh0LmlkO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgLy8gcmVjb25jaWxlIHBsYWNlaG9sZGVyc1xuICAgIGNvbnN0IGNoaWxkUGhzID0gY29udGV4dC5wbGFjZWhvbGRlcnM7XG4gICAgY2hpbGRQaHMuZm9yRWFjaCgodmFsdWVzOiBhbnlbXSwga2V5OiBzdHJpbmcpID0+IHtcbiAgICAgIGNvbnN0IHBocyA9IHRoaXMucGxhY2Vob2xkZXJzLmdldChrZXkpO1xuICAgICAgaWYgKCFwaHMpIHtcbiAgICAgICAgdGhpcy5wbGFjZWhvbGRlcnMuc2V0KGtleSwgdmFsdWVzKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgICAgLy8gdHJ5IHRvIGZpbmQgbWF0Y2hpbmcgdGVtcGxhdGUuLi5cbiAgICAgIGNvbnN0IHRtcGxJZHggPSBmaW5kSW5kZXgocGhzLCBmaW5kVGVtcGxhdGVGbihjb250ZXh0LmlkLCBjb250ZXh0LnRlbXBsYXRlSW5kZXgpKTtcbiAgICAgIGlmICh0bXBsSWR4ID49IDApIHtcbiAgICAgICAgLy8gLi4uIGlmIGZvdW5kIC0gcmVwbGFjZSBpdCB3aXRoIG5lc3RlZCB0ZW1wbGF0ZSBjb250ZW50XG4gICAgICAgIGNvbnN0IGlzQ2xvc2VUYWcgPSBrZXkuc3RhcnRzV2l0aCgnQ0xPU0UnKTtcbiAgICAgICAgY29uc3QgaXNUZW1wbGF0ZVRhZyA9IGtleS5lbmRzV2l0aCgnTkctVEVNUExBVEUnKTtcbiAgICAgICAgaWYgKGlzVGVtcGxhdGVUYWcpIHtcbiAgICAgICAgICAvLyBjdXJyZW50IHRlbXBsYXRlJ3MgY29udGVudCBpcyBwbGFjZWQgYmVmb3JlIG9yIGFmdGVyXG4gICAgICAgICAgLy8gcGFyZW50IHRlbXBsYXRlIHRhZywgZGVwZW5kaW5nIG9uIHRoZSBvcGVuL2Nsb3NlIGF0cnJpYnV0ZVxuICAgICAgICAgIHBocy5zcGxpY2UodG1wbElkeCArIChpc0Nsb3NlVGFnID8gMCA6IDEpLCAwLCAuLi52YWx1ZXMpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnN0IGlkeCA9IGlzQ2xvc2VUYWcgPyB2YWx1ZXMubGVuZ3RoIC0gMSA6IDA7XG4gICAgICAgICAgdmFsdWVzW2lkeF0udG1wbCA9IHBoc1t0bXBsSWR4XTtcbiAgICAgICAgICBwaHMuc3BsaWNlKHRtcGxJZHgsIDEsIC4uLnZhbHVlcyk7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIC4uLiBvdGhlcndpc2UganVzdCBhcHBlbmQgY29udGVudCB0byBwbGFjZWhvbGRlciB2YWx1ZVxuICAgICAgICBwaHMucHVzaCguLi52YWx1ZXMpO1xuICAgICAgfVxuICAgICAgdGhpcy5wbGFjZWhvbGRlcnMuc2V0KGtleSwgcGhzKTtcbiAgICB9KTtcbiAgICB0aGlzLl91bnJlc29sdmVkQ3R4Q291bnQtLTtcbiAgfVxufVxuXG4vL1xuLy8gSGVscGVyIG1ldGhvZHNcbi8vXG5cbmZ1bmN0aW9uIHdyYXAoc3ltYm9sOiBzdHJpbmcsIGluZGV4OiBudW1iZXIsIGNvbnRleHRJZDogbnVtYmVyLCBjbG9zZWQ/OiBib29sZWFuKTogc3RyaW5nIHtcbiAgY29uc3Qgc3RhdGUgPSBjbG9zZWQgPyAnLycgOiAnJztcbiAgcmV0dXJuIHdyYXBJMThuUGxhY2Vob2xkZXIoYCR7c3RhdGV9JHtzeW1ib2x9JHtpbmRleH1gLCBjb250ZXh0SWQpO1xufVxuXG5mdW5jdGlvbiB3cmFwVGFnKHN5bWJvbDogc3RyaW5nLCB7aW5kZXgsIGN0eCwgaXNWb2lkfTogYW55LCBjbG9zZWQ/OiBib29sZWFuKTogc3RyaW5nIHtcbiAgcmV0dXJuIGlzVm9pZCA/IHdyYXAoc3ltYm9sLCBpbmRleCwgY3R4KSArIHdyYXAoc3ltYm9sLCBpbmRleCwgY3R4LCB0cnVlKSA6XG4gICAgICAgICAgICAgICAgICB3cmFwKHN5bWJvbCwgaW5kZXgsIGN0eCwgY2xvc2VkKTtcbn1cblxuZnVuY3Rpb24gZmluZFRlbXBsYXRlRm4oY3R4OiBudW1iZXIsIHRlbXBsYXRlSW5kZXg6IG51bWJlciB8IG51bGwpIHtcbiAgcmV0dXJuICh0b2tlbjogYW55KSA9PiB0eXBlb2YgdG9rZW4gPT09ICdvYmplY3QnICYmIHRva2VuLnR5cGUgPT09IFRhZ1R5cGUuVEVNUExBVEUgJiZcbiAgICAgIHRva2VuLmluZGV4ID09PSB0ZW1wbGF0ZUluZGV4ICYmIHRva2VuLmN0eCA9PT0gY3R4O1xufVxuXG5mdW5jdGlvbiBzZXJpYWxpemVQbGFjZWhvbGRlclZhbHVlKHZhbHVlOiBhbnkpOiBzdHJpbmcge1xuICBjb25zdCBlbGVtZW50ID0gKGRhdGE6IGFueSwgY2xvc2VkPzogYm9vbGVhbikgPT4gd3JhcFRhZygnIycsIGRhdGEsIGNsb3NlZCk7XG4gIGNvbnN0IHRlbXBsYXRlID0gKGRhdGE6IGFueSwgY2xvc2VkPzogYm9vbGVhbikgPT4gd3JhcFRhZygnKicsIGRhdGEsIGNsb3NlZCk7XG4gIGNvbnN0IHByb2plY3Rpb24gPSAoZGF0YTogYW55LCBjbG9zZWQ/OiBib29sZWFuKSA9PiB3cmFwVGFnKCchJywgZGF0YSwgY2xvc2VkKTtcblxuICBzd2l0Y2ggKHZhbHVlLnR5cGUpIHtcbiAgICBjYXNlIFRhZ1R5cGUuRUxFTUVOVDpcbiAgICAgIC8vIGNsb3NlIGVsZW1lbnQgdGFnXG4gICAgICBpZiAodmFsdWUuY2xvc2VkKSB7XG4gICAgICAgIHJldHVybiBlbGVtZW50KHZhbHVlLCB0cnVlKSArICh2YWx1ZS50bXBsID8gdGVtcGxhdGUodmFsdWUudG1wbCwgdHJ1ZSkgOiAnJyk7XG4gICAgICB9XG4gICAgICAvLyBvcGVuIGVsZW1lbnQgdGFnIHRoYXQgYWxzbyBpbml0aWF0ZXMgYSB0ZW1wbGF0ZVxuICAgICAgaWYgKHZhbHVlLnRtcGwpIHtcbiAgICAgICAgcmV0dXJuIHRlbXBsYXRlKHZhbHVlLnRtcGwpICsgZWxlbWVudCh2YWx1ZSkgK1xuICAgICAgICAgICAgKHZhbHVlLmlzVm9pZCA/IHRlbXBsYXRlKHZhbHVlLnRtcGwsIHRydWUpIDogJycpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIGVsZW1lbnQodmFsdWUpO1xuXG4gICAgY2FzZSBUYWdUeXBlLlRFTVBMQVRFOlxuICAgICAgcmV0dXJuIHRlbXBsYXRlKHZhbHVlLCB2YWx1ZS5jbG9zZWQpO1xuXG4gICAgY2FzZSBUYWdUeXBlLlBST0pFQ1RJT046XG4gICAgICByZXR1cm4gcHJvamVjdGlvbih2YWx1ZSwgdmFsdWUuY2xvc2VkKTtcblxuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gdmFsdWU7XG4gIH1cbn1cbiJdfQ==