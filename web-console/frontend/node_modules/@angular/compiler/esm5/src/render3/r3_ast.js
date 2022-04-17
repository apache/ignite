/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { ParseSourceSpan } from '../parse_util';
var Text = /** @class */ (function () {
    function Text(value, sourceSpan) {
        this.value = value;
        this.sourceSpan = sourceSpan;
    }
    Text.prototype.visit = function (visitor) { return visitor.visitText(this); };
    return Text;
}());
export { Text };
var BoundText = /** @class */ (function () {
    function BoundText(value, sourceSpan, i18n) {
        this.value = value;
        this.sourceSpan = sourceSpan;
        this.i18n = i18n;
    }
    BoundText.prototype.visit = function (visitor) { return visitor.visitBoundText(this); };
    return BoundText;
}());
export { BoundText };
var TextAttribute = /** @class */ (function () {
    function TextAttribute(name, value, sourceSpan, valueSpan, i18n) {
        this.name = name;
        this.value = value;
        this.sourceSpan = sourceSpan;
        this.valueSpan = valueSpan;
        this.i18n = i18n;
    }
    TextAttribute.prototype.visit = function (visitor) { return visitor.visitTextAttribute(this); };
    return TextAttribute;
}());
export { TextAttribute };
var BoundAttribute = /** @class */ (function () {
    function BoundAttribute(name, type, securityContext, value, unit, sourceSpan, valueSpan, i18n) {
        this.name = name;
        this.type = type;
        this.securityContext = securityContext;
        this.value = value;
        this.unit = unit;
        this.sourceSpan = sourceSpan;
        this.valueSpan = valueSpan;
        this.i18n = i18n;
    }
    BoundAttribute.fromBoundElementProperty = function (prop, i18n) {
        return new BoundAttribute(prop.name, prop.type, prop.securityContext, prop.value, prop.unit, prop.sourceSpan, prop.valueSpan, i18n);
    };
    BoundAttribute.prototype.visit = function (visitor) { return visitor.visitBoundAttribute(this); };
    return BoundAttribute;
}());
export { BoundAttribute };
var BoundEvent = /** @class */ (function () {
    function BoundEvent(name, type, handler, target, phase, sourceSpan, handlerSpan) {
        this.name = name;
        this.type = type;
        this.handler = handler;
        this.target = target;
        this.phase = phase;
        this.sourceSpan = sourceSpan;
        this.handlerSpan = handlerSpan;
    }
    BoundEvent.fromParsedEvent = function (event) {
        var target = event.type === 0 /* Regular */ ? event.targetOrPhase : null;
        var phase = event.type === 1 /* Animation */ ? event.targetOrPhase : null;
        return new BoundEvent(event.name, event.type, event.handler, target, phase, event.sourceSpan, event.handlerSpan);
    };
    BoundEvent.prototype.visit = function (visitor) { return visitor.visitBoundEvent(this); };
    return BoundEvent;
}());
export { BoundEvent };
var Element = /** @class */ (function () {
    function Element(name, attributes, inputs, outputs, children, references, sourceSpan, startSourceSpan, endSourceSpan, i18n) {
        this.name = name;
        this.attributes = attributes;
        this.inputs = inputs;
        this.outputs = outputs;
        this.children = children;
        this.references = references;
        this.sourceSpan = sourceSpan;
        this.startSourceSpan = startSourceSpan;
        this.endSourceSpan = endSourceSpan;
        this.i18n = i18n;
        // If the element is empty then the source span should include any closing tag
        if (children.length === 0 && startSourceSpan && endSourceSpan) {
            this.sourceSpan = new ParseSourceSpan(sourceSpan.start, endSourceSpan.end);
        }
    }
    Element.prototype.visit = function (visitor) { return visitor.visitElement(this); };
    return Element;
}());
export { Element };
var Template = /** @class */ (function () {
    function Template(tagName, attributes, inputs, outputs, templateAttrs, children, references, variables, sourceSpan, startSourceSpan, endSourceSpan, i18n) {
        this.tagName = tagName;
        this.attributes = attributes;
        this.inputs = inputs;
        this.outputs = outputs;
        this.templateAttrs = templateAttrs;
        this.children = children;
        this.references = references;
        this.variables = variables;
        this.sourceSpan = sourceSpan;
        this.startSourceSpan = startSourceSpan;
        this.endSourceSpan = endSourceSpan;
        this.i18n = i18n;
    }
    Template.prototype.visit = function (visitor) { return visitor.visitTemplate(this); };
    return Template;
}());
export { Template };
var Content = /** @class */ (function () {
    function Content(selector, attributes, sourceSpan, i18n) {
        this.selector = selector;
        this.attributes = attributes;
        this.sourceSpan = sourceSpan;
        this.i18n = i18n;
    }
    Content.prototype.visit = function (visitor) { return visitor.visitContent(this); };
    return Content;
}());
export { Content };
var Variable = /** @class */ (function () {
    function Variable(name, value, sourceSpan, valueSpan) {
        this.name = name;
        this.value = value;
        this.sourceSpan = sourceSpan;
        this.valueSpan = valueSpan;
    }
    Variable.prototype.visit = function (visitor) { return visitor.visitVariable(this); };
    return Variable;
}());
export { Variable };
var Reference = /** @class */ (function () {
    function Reference(name, value, sourceSpan, valueSpan) {
        this.name = name;
        this.value = value;
        this.sourceSpan = sourceSpan;
        this.valueSpan = valueSpan;
    }
    Reference.prototype.visit = function (visitor) { return visitor.visitReference(this); };
    return Reference;
}());
export { Reference };
var Icu = /** @class */ (function () {
    function Icu(vars, placeholders, sourceSpan, i18n) {
        this.vars = vars;
        this.placeholders = placeholders;
        this.sourceSpan = sourceSpan;
        this.i18n = i18n;
    }
    Icu.prototype.visit = function (visitor) { return visitor.visitIcu(this); };
    return Icu;
}());
export { Icu };
var NullVisitor = /** @class */ (function () {
    function NullVisitor() {
    }
    NullVisitor.prototype.visitElement = function (element) { };
    NullVisitor.prototype.visitTemplate = function (template) { };
    NullVisitor.prototype.visitContent = function (content) { };
    NullVisitor.prototype.visitVariable = function (variable) { };
    NullVisitor.prototype.visitReference = function (reference) { };
    NullVisitor.prototype.visitTextAttribute = function (attribute) { };
    NullVisitor.prototype.visitBoundAttribute = function (attribute) { };
    NullVisitor.prototype.visitBoundEvent = function (attribute) { };
    NullVisitor.prototype.visitText = function (text) { };
    NullVisitor.prototype.visitBoundText = function (text) { };
    NullVisitor.prototype.visitIcu = function (icu) { };
    return NullVisitor;
}());
export { NullVisitor };
var RecursiveVisitor = /** @class */ (function () {
    function RecursiveVisitor() {
    }
    RecursiveVisitor.prototype.visitElement = function (element) {
        visitAll(this, element.attributes);
        visitAll(this, element.children);
        visitAll(this, element.references);
    };
    RecursiveVisitor.prototype.visitTemplate = function (template) {
        visitAll(this, template.attributes);
        visitAll(this, template.children);
        visitAll(this, template.references);
        visitAll(this, template.variables);
    };
    RecursiveVisitor.prototype.visitContent = function (content) { };
    RecursiveVisitor.prototype.visitVariable = function (variable) { };
    RecursiveVisitor.prototype.visitReference = function (reference) { };
    RecursiveVisitor.prototype.visitTextAttribute = function (attribute) { };
    RecursiveVisitor.prototype.visitBoundAttribute = function (attribute) { };
    RecursiveVisitor.prototype.visitBoundEvent = function (attribute) { };
    RecursiveVisitor.prototype.visitText = function (text) { };
    RecursiveVisitor.prototype.visitBoundText = function (text) { };
    RecursiveVisitor.prototype.visitIcu = function (icu) { };
    return RecursiveVisitor;
}());
export { RecursiveVisitor };
var TransformVisitor = /** @class */ (function () {
    function TransformVisitor() {
    }
    TransformVisitor.prototype.visitElement = function (element) {
        var newAttributes = transformAll(this, element.attributes);
        var newInputs = transformAll(this, element.inputs);
        var newOutputs = transformAll(this, element.outputs);
        var newChildren = transformAll(this, element.children);
        var newReferences = transformAll(this, element.references);
        if (newAttributes != element.attributes || newInputs != element.inputs ||
            newOutputs != element.outputs || newChildren != element.children ||
            newReferences != element.references) {
            return new Element(element.name, newAttributes, newInputs, newOutputs, newChildren, newReferences, element.sourceSpan, element.startSourceSpan, element.endSourceSpan);
        }
        return element;
    };
    TransformVisitor.prototype.visitTemplate = function (template) {
        var newAttributes = transformAll(this, template.attributes);
        var newInputs = transformAll(this, template.inputs);
        var newOutputs = transformAll(this, template.outputs);
        var newTemplateAttrs = transformAll(this, template.templateAttrs);
        var newChildren = transformAll(this, template.children);
        var newReferences = transformAll(this, template.references);
        var newVariables = transformAll(this, template.variables);
        if (newAttributes != template.attributes || newInputs != template.inputs ||
            newOutputs != template.outputs || newTemplateAttrs != template.templateAttrs ||
            newChildren != template.children || newReferences != template.references ||
            newVariables != template.variables) {
            return new Template(template.tagName, newAttributes, newInputs, newOutputs, newTemplateAttrs, newChildren, newReferences, newVariables, template.sourceSpan, template.startSourceSpan, template.endSourceSpan);
        }
        return template;
    };
    TransformVisitor.prototype.visitContent = function (content) { return content; };
    TransformVisitor.prototype.visitVariable = function (variable) { return variable; };
    TransformVisitor.prototype.visitReference = function (reference) { return reference; };
    TransformVisitor.prototype.visitTextAttribute = function (attribute) { return attribute; };
    TransformVisitor.prototype.visitBoundAttribute = function (attribute) { return attribute; };
    TransformVisitor.prototype.visitBoundEvent = function (attribute) { return attribute; };
    TransformVisitor.prototype.visitText = function (text) { return text; };
    TransformVisitor.prototype.visitBoundText = function (text) { return text; };
    TransformVisitor.prototype.visitIcu = function (icu) { return icu; };
    return TransformVisitor;
}());
export { TransformVisitor };
export function visitAll(visitor, nodes) {
    var e_1, _a, e_2, _b;
    var result = [];
    if (visitor.visit) {
        try {
            for (var nodes_1 = tslib_1.__values(nodes), nodes_1_1 = nodes_1.next(); !nodes_1_1.done; nodes_1_1 = nodes_1.next()) {
                var node = nodes_1_1.value;
                var newNode = visitor.visit(node) || node.visit(visitor);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (nodes_1_1 && !nodes_1_1.done && (_a = nodes_1.return)) _a.call(nodes_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
    }
    else {
        try {
            for (var nodes_2 = tslib_1.__values(nodes), nodes_2_1 = nodes_2.next(); !nodes_2_1.done; nodes_2_1 = nodes_2.next()) {
                var node = nodes_2_1.value;
                var newNode = node.visit(visitor);
                if (newNode) {
                    result.push(newNode);
                }
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (nodes_2_1 && !nodes_2_1.done && (_b = nodes_2.return)) _b.call(nodes_2);
            }
            finally { if (e_2) throw e_2.error; }
        }
    }
    return result;
}
export function transformAll(visitor, nodes) {
    var e_3, _a;
    var result = [];
    var changed = false;
    try {
        for (var nodes_3 = tslib_1.__values(nodes), nodes_3_1 = nodes_3.next(); !nodes_3_1.done; nodes_3_1 = nodes_3.next()) {
            var node = nodes_3_1.value;
            var newNode = node.visit(visitor);
            if (newNode) {
                result.push(newNode);
            }
            changed = changed || newNode != node;
        }
    }
    catch (e_3_1) { e_3 = { error: e_3_1 }; }
    finally {
        try {
            if (nodes_3_1 && !nodes_3_1.done && (_a = nodes_3.return)) _a.call(nodes_3);
        }
        finally { if (e_3) throw e_3.error; }
    }
    return changed ? result : nodes;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicjNfYXN0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3JlbmRlcjMvcjNfYXN0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFLSCxPQUFPLEVBQUMsZUFBZSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBTzlDO0lBQ0UsY0FBbUIsS0FBYSxFQUFTLFVBQTJCO1FBQWpELFVBQUssR0FBTCxLQUFLLENBQVE7UUFBUyxlQUFVLEdBQVYsVUFBVSxDQUFpQjtJQUFHLENBQUM7SUFDeEUsb0JBQUssR0FBTCxVQUFjLE9BQXdCLElBQVksT0FBTyxPQUFPLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUNyRixXQUFDO0FBQUQsQ0FBQyxBQUhELElBR0M7O0FBRUQ7SUFDRSxtQkFBbUIsS0FBVSxFQUFTLFVBQTJCLEVBQVMsSUFBYztRQUFyRSxVQUFLLEdBQUwsS0FBSyxDQUFLO1FBQVMsZUFBVSxHQUFWLFVBQVUsQ0FBaUI7UUFBUyxTQUFJLEdBQUosSUFBSSxDQUFVO0lBQUcsQ0FBQztJQUM1Rix5QkFBSyxHQUFMLFVBQWMsT0FBd0IsSUFBWSxPQUFPLE9BQU8sQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQzFGLGdCQUFDO0FBQUQsQ0FBQyxBQUhELElBR0M7O0FBRUQ7SUFDRSx1QkFDVyxJQUFZLEVBQVMsS0FBYSxFQUFTLFVBQTJCLEVBQ3RFLFNBQTJCLEVBQVMsSUFBYztRQURsRCxTQUFJLEdBQUosSUFBSSxDQUFRO1FBQVMsVUFBSyxHQUFMLEtBQUssQ0FBUTtRQUFTLGVBQVUsR0FBVixVQUFVLENBQWlCO1FBQ3RFLGNBQVMsR0FBVCxTQUFTLENBQWtCO1FBQVMsU0FBSSxHQUFKLElBQUksQ0FBVTtJQUFHLENBQUM7SUFDakUsNkJBQUssR0FBTCxVQUFjLE9BQXdCLElB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QUM1QixJQUFJLE9BQU8sR0FBRyxLQUFLLENBQUM7O1FBQ3BCLEtBQW1CLElBQUEsVUFBQSxpQkFBQSxLQUFLLENBQUEsNEJBQUEsK0NBQUU7WUFBckIsSUFBTSxJQUFJLGtCQUFBO1lBQ2IsSUFBTSxPQUFPLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztZQUNwQyxJQUFJLE9BQU8sRUFBRTtnQkFDWCxNQUFNLENBQUMsSUFBSSxDQUFDLE9BQWlCLENBQUMsQ0FBQzthQUNoQztZQUNELE9BQU8sR0FBRyxPQUFPLElBQUksT0FBTyxJQUFJLElBQUksQ0FBQztTQUN0Qzs7Ozs7Ozs7O0lBQ0QsT0FBTyxPQUFPLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDO0FBQ2xDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U2VjdXJpdHlDb250ZXh0fSBmcm9tICcuLi9jb3JlJztcbmltcG9ydCB7QVNULCBCaW5kaW5nVHlwZSwgQm91bmRFbGVtZW50UHJvcGVydHksIFBhcnNlZEV2ZW50LCBQYXJzZWRFdmVudFR5cGV9IGZyb20gJy4uL2V4cHJlc3Npb25fcGFyc2VyL2FzdCc7XG5pbXBvcnQge0FTVCBhcyBJMThuQVNUfSBmcm9tICcuLi9pMThuL2kxOG5fYXN0JztcbmltcG9ydCB7UGFyc2VTb3VyY2VTcGFufSBmcm9tICcuLi9wYXJzZV91dGlsJztcblxuZXhwb3J0IGludGVyZmFjZSBOb2RlIHtcbiAgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuO1xuICB2aXNpdDxSZXN1bHQ+KHZpc2l0b3I6IFZpc2l0b3I8UmVzdWx0Pik6IFJlc3VsdDtcbn1cblxuZXhwb3J0IGNsYXNzIFRleHQgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IocHVibGljIHZhbHVlOiBzdHJpbmcsIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHt9XG4gIHZpc2l0PFJlc3VsdD4odmlzaXRvcjogVmlzaXRvcjxSZXN1bHQ+KTogUmVzdWx0IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRUZXh0KHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBCb3VuZFRleHQgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IocHVibGljIHZhbHVlOiBBU1QsIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBpMThuPzogSTE4bkFTVCkge31cbiAgdmlzaXQ8UmVzdWx0Pih2aXNpdG9yOiBWaXNpdG9yPFJlc3VsdD4pOiBSZXN1bHQgeyByZXR1cm4gdmlzaXRvci52aXNpdEJvdW5kVGV4dCh0aGlzKTsgfVxufVxuXG5leHBvcnQgY2xhc3MgVGV4dEF0dHJpYnV0ZSBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBuYW1lOiBzdHJpbmcsIHB1YmxpYyB2YWx1ZTogc3RyaW5nLCBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLFxuICAgICAgcHVibGljIHZhbHVlU3Bhbj86IFBhcnNlU291cmNlU3BhbiwgcHVibGljIGkxOG4/OiBJMThuQVNUKSB7fVxuICB2aXNpdDxSZXN1bHQ+KHZpc2l0b3I6IFZpc2l0b3I8UmVzdWx0Pik6IFJlc3VsdCB7IHJldHVybiB2aXNpdG9yLnZpc2l0VGV4dEF0dHJpYnV0ZSh0aGlzKTsgfVxufVxuXG5leHBvcnQgY2xhc3MgQm91bmRBdHRyaWJ1dGUgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgbmFtZTogc3RyaW5nLCBwdWJsaWMgdHlwZTogQmluZGluZ1R5cGUsIHB1YmxpYyBzZWN1cml0eUNvbnRleHQ6IFNlY3VyaXR5Q29udGV4dCxcbiAgICAgIHB1YmxpYyB2YWx1ZTogQVNULCBwdWJsaWMgdW5pdDogc3RyaW5nfG51bGwsIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4sXG4gICAgICBwdWJsaWMgdmFsdWVTcGFuPzogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgaTE4bj86IEkxOG5BU1QpIHt9XG5cbiAgc3RhdGljIGZyb21Cb3VuZEVsZW1lbnRQcm9wZXJ0eShwcm9wOiBCb3VuZEVsZW1lbnRQcm9wZXJ0eSwgaTE4bj86IEkxOG5BU1QpIHtcbiAgICByZXR1cm4gbmV3IEJvdW5kQXR0cmlidXRlKFxuICAgICAgICBwcm9wLm5hbWUsIHByb3AudHlwZSwgcHJvcC5zZWN1cml0eUNvbnRleHQsIHByb3AudmFsdWUsIHByb3AudW5pdCwgcHJvcC5zb3VyY2VTcGFuLFxuICAgICAgICBwcm9wLnZhbHVlU3BhbiwgaTE4bik7XG4gIH1cblxuICB2aXNpdDxSZXN1bHQ+KHZpc2l0b3I6IFZpc2l0b3I8UmVzdWx0Pik6IFJlc3VsdCB7IHJldHVybiB2aXNpdG9yLnZpc2l0Qm91bmRBdHRyaWJ1dGUodGhpcyk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIEJvdW5kRXZlbnQgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgbmFtZTogc3RyaW5nLCBwdWJsaWMgdHlwZTogUGFyc2VkRXZlbnRUeXBlLCBwdWJsaWMgaGFuZGxlcjogQVNULFxuICAgICAgcHVibGljIHRhcmdldDogc3RyaW5nfG51bGwsIHB1YmxpYyBwaGFzZTogc3RyaW5nfG51bGwsIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4sXG4gICAgICBwdWJsaWMgaGFuZGxlclNwYW46IFBhcnNlU291cmNlU3Bhbikge31cblxuICBzdGF0aWMgZnJvbVBhcnNlZEV2ZW50KGV2ZW50OiBQYXJzZWRFdmVudCkge1xuICAgIGNvbnN0IHRhcmdldDogc3RyaW5nfG51bGwgPSBldmVudC50eXBlID09PSBQYXJzZWRFdmVudFR5cGUuUmVndWxhciA/IGV2ZW50LnRhcmdldE9yUGhhc2UgOiBudWxsO1xuICAgIGNvbnN0IHBoYXNlOiBzdHJpbmd8bnVsbCA9XG4gICAgICAgIGV2ZW50LnR5cGUgPT09IFBhcnNlZEV2ZW50VHlwZS5BbmltYXRpb24gPyBldmVudC50YXJnZXRPclBoYXNlIDogbnVsbDtcbiAgICByZXR1cm4gbmV3IEJvdW5kRXZlbnQoXG4gICAgICAgIGV2ZW50Lm5hbWUsIGV2ZW50LnR5cGUsIGV2ZW50LmhhbmRsZXIsIHRhcmdldCwgcGhhc2UsIGV2ZW50LnNvdXJjZVNwYW4sIGV2ZW50LmhhbmRsZXJTcGFuKTtcbiAgfVxuXG4gIHZpc2l0PFJlc3VsdD4odmlzaXRvcjogVmlzaXRvcjxSZXN1bHQ+KTogUmVzdWx0IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRCb3VuZEV2ZW50KHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBFbGVtZW50IGltcGxlbWVudHMgTm9kZSB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIG5hbWU6IHN0cmluZywgcHVibGljIGF0dHJpYnV0ZXM6IFRleHRBdHRyaWJ1dGVbXSwgcHVibGljIGlucHV0czogQm91bmRBdHRyaWJ1dGVbXSxcbiAgICAgIHB1YmxpYyBvdXRwdXRzOiBCb3VuZEV2ZW50W10sIHB1YmxpYyBjaGlsZHJlbjogTm9kZVtdLCBwdWJsaWMgcmVmZXJlbmNlczogUmVmZXJlbmNlW10sXG4gICAgICBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgc3RhcnRTb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW58bnVsbCxcbiAgICAgIHB1YmxpYyBlbmRTb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW58bnVsbCwgcHVibGljIGkxOG4/OiBJMThuQVNUKSB7XG4gICAgLy8gSWYgdGhlIGVsZW1lbnQgaXMgZW1wdHkgdGhlbiB0aGUgc291cmNlIHNwYW4gc2hvdWxkIGluY2x1ZGUgYW55IGNsb3NpbmcgdGFnXG4gICAgaWYgKGNoaWxkcmVuLmxlbmd0aCA9PT0gMCAmJiBzdGFydFNvdXJjZVNwYW4gJiYgZW5kU291cmNlU3Bhbikge1xuICAgICAgdGhpcy5zb3VyY2VTcGFuID0gbmV3IFBhcnNlU291cmNlU3Bhbihzb3VyY2VTcGFuLnN0YXJ0LCBlbmRTb3VyY2VTcGFuLmVuZCk7XG4gICAgfVxuICB9XG4gIHZpc2l0PFJlc3VsdD4odmlzaXRvcjogVmlzaXRvcjxSZXN1bHQ+KTogUmVzdWx0IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRFbGVtZW50KHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBUZW1wbGF0ZSBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyB0YWdOYW1lOiBzdHJpbmcsIHB1YmxpYyBhdHRyaWJ1dGVzOiBUZXh0QXR0cmlidXRlW10sIHB1YmxpYyBpbnB1dHM6IEJvdW5kQXR0cmlidXRlW10sXG4gICAgICBwdWJsaWMgb3V0cHV0czogQm91bmRFdmVudFtdLCBwdWJsaWMgdGVtcGxhdGVBdHRyczogKEJvdW5kQXR0cmlidXRlfFRleHRBdHRyaWJ1dGUpW10sXG4gICAgICBwdWJsaWMgY2hpbGRyZW46IE5vZGVbXSwgcHVibGljIHJlZmVyZW5jZXM6IFJlZmVyZW5jZVtdLCBwdWJsaWMgdmFyaWFibGVzOiBWYXJpYWJsZVtdLFxuICAgICAgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbiwgcHVibGljIHN0YXJ0U291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFufG51bGwsXG4gICAgICBwdWJsaWMgZW5kU291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFufG51bGwsIHB1YmxpYyBpMThuPzogSTE4bkFTVCkge31cbiAgdmlzaXQ8UmVzdWx0Pih2aXNpdG9yOiBWaXNpdG9yPFJlc3VsdD4pOiBSZXN1bHQgeyByZXR1cm4gdmlzaXRvci52aXNpdFRlbXBsYXRlKHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb250ZW50IGltcGxlbWVudHMgTm9kZSB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIHNlbGVjdG9yOiBzdHJpbmcsIHB1YmxpYyBhdHRyaWJ1dGVzOiBUZXh0QXR0cmlidXRlW10sXG4gICAgICBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgaTE4bj86IEkxOG5BU1QpIHt9XG4gIHZpc2l0PFJlc3VsdD4odmlzaXRvcjogVmlzaXRvcjxSZXN1bHQ+KTogUmVzdWx0IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRDb250ZW50KHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYXJpYWJsZSBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBuYW1lOiBzdHJpbmcsIHB1YmxpYyB2YWx1ZTogc3RyaW5nLCBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLFxuICAgICAgcHVibGljIHZhbHVlU3Bhbj86IFBhcnNlU291cmNlU3Bhbikge31cbiAgdmlzaXQ8UmVzdWx0Pih2aXNpdG9yOiBWaXNpdG9yPFJlc3VsdD4pOiBSZXN1bHQgeyByZXR1cm4gdmlzaXRvci52aXNpdFZhcmlhYmxlKHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZWZlcmVuY2UgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgbmFtZTogc3RyaW5nLCBwdWJsaWMgdmFsdWU6IHN0cmluZywgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbixcbiAgICAgIHB1YmxpYyB2YWx1ZVNwYW4/OiBQYXJzZVNvdXJjZVNwYW4pIHt9XG4gIHZpc2l0PFJlc3VsdD4odmlzaXRvcjogVmlzaXRvcjxSZXN1bHQ+KTogUmVzdWx0IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRSZWZlcmVuY2UodGhpcyk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIEljdSBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyB2YXJzOiB7W25hbWU6IHN0cmluZ106IEJvdW5kVGV4dH0sXG4gICAgICBwdWJsaWMgcGxhY2Vob2xkZXJzOiB7W25hbWU6IHN0cmluZ106IFRleHQgfCBCb3VuZFRleHR9LCBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLFxuICAgICAgcHVibGljIGkxOG4/OiBJMThuQVNUKSB7fVxuICB2aXNpdDxSZXN1bHQ+KHZpc2l0b3I6IFZpc2l0b3I8UmVzdWx0Pik6IFJlc3VsdCB7IHJldHVybiB2aXNpdG9yLnZpc2l0SWN1KHRoaXMpOyB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgVmlzaXRvcjxSZXN1bHQgPSBhbnk+IHtcbiAgLy8gUmV0dXJuaW5nIGEgdHJ1dGh5IHZhbHVlIGZyb20gYHZpc2l0KClgIHdpbGwgcHJldmVudCBgdmlzaXRBbGwoKWAgZnJvbSB0aGUgY2FsbCB0byB0aGUgdHlwZWRcbiAgLy8gbWV0aG9kIGFuZCByZXN1bHQgcmV0dXJuZWQgd2lsbCBiZWNvbWUgdGhlIHJlc3VsdCBpbmNsdWRlZCBpbiBgdmlzaXRBbGwoKWBzIHJlc3VsdCBhcnJheS5cbiAgdmlzaXQ/KG5vZGU6IE5vZGUpOiBSZXN1bHQ7XG5cbiAgdmlzaXRFbGVtZW50KGVsZW1lbnQ6IEVsZW1lbnQpOiBSZXN1bHQ7XG4gIHZpc2l0VGVtcGxhdGUodGVtcGxhdGU6IFRlbXBsYXRlKTogUmVzdWx0O1xuICB2aXNpdENvbnRlbnQoY29udGVudDogQ29udGVudCk6IFJlc3VsdDtcbiAgdmlzaXRWYXJpYWJsZSh2YXJpYWJsZTogVmFyaWFibGUpOiBSZXN1bHQ7XG4gIHZpc2l0UmVmZXJlbmNlKHJlZmVyZW5jZTogUmVmZXJlbmNlKTogUmVzdWx0O1xuICB2aXNpdFRleHRBdHRyaWJ1dGUoYXR0cmlidXRlOiBUZXh0QXR0cmlidXRlKTogUmVzdWx0O1xuICB2aXNpdEJvdW5kQXR0cmlidXRlKGF0dHJpYnV0ZTogQm91bmRBdHRyaWJ1dGUpOiBSZXN1bHQ7XG4gIHZpc2l0Qm91bmRFdmVudChhdHRyaWJ1dGU6IEJvdW5kRXZlbnQpOiBSZXN1bHQ7XG4gIHZpc2l0VGV4dCh0ZXh0OiBUZXh0KTogUmVzdWx0O1xuICB2aXNpdEJvdW5kVGV4dCh0ZXh0OiBCb3VuZFRleHQpOiBSZXN1bHQ7XG4gIHZpc2l0SWN1KGljdTogSWN1KTogUmVzdWx0O1xufVxuXG5leHBvcnQgY2xhc3MgTnVsbFZpc2l0b3IgaW1wbGVtZW50cyBWaXNpdG9yPHZvaWQ+IHtcbiAgdmlzaXRFbGVtZW50KGVsZW1lbnQ6IEVsZW1lbnQpOiB2b2lkIHt9XG4gIHZpc2l0VGVtcGxhdGUodGVtcGxhdGU6IFRlbXBsYXRlKTogdm9pZCB7fVxuICB2aXNpdENvbnRlbnQoY29udGVudDogQ29udGVudCk6IHZvaWQge31cbiAgdmlzaXRWYXJpYWJsZSh2YXJpYWJsZTogVmFyaWFibGUpOiB2b2lkIHt9XG4gIHZpc2l0UmVmZXJlbmNlKHJlZmVyZW5jZTogUmVmZXJlbmNlKTogdm9pZCB7fVxuICB2aXNpdFRleHRBdHRyaWJ1dGUoYXR0cmlidXRlOiBUZXh0QXR0cmlidXRlKTogdm9pZCB7fVxuICB2aXNpdEJvdW5kQXR0cmlidXRlKGF0dHJpYnV0ZTogQm91bmRBdHRyaWJ1dGUpOiB2b2lkIHt9XG4gIHZpc2l0Qm91bmRFdmVudChhdHRyaWJ1dGU6IEJvdW5kRXZlbnQpOiB2b2lkIHt9XG4gIHZpc2l0VGV4dCh0ZXh0OiBUZXh0KTogdm9pZCB7fVxuICB2aXNpdEJvdW5kVGV4dCh0ZXh0OiBCb3VuZFRleHQpOiB2b2lkIHt9XG4gIHZpc2l0SWN1KGljdTogSWN1KTogdm9pZCB7fVxufVxuXG5leHBvcnQgY2xhc3MgUmVjdXJzaXZlVmlzaXRvciBpbXBsZW1lbnRzIFZpc2l0b3I8dm9pZD4ge1xuICB2aXNpdEVsZW1lbnQoZWxlbWVudDogRWxlbWVudCk6IHZvaWQge1xuICAgIHZpc2l0QWxsKHRoaXMsIGVsZW1lbnQuYXR0cmlidXRlcyk7XG4gICAgdmlzaXRBbGwodGhpcywgZWxlbWVudC5jaGlsZHJlbik7XG4gICAgdmlzaXRBbGwodGhpcywgZWxlbWVudC5yZWZlcmVuY2VzKTtcbiAgfVxuICB2aXNpdFRlbXBsYXRlKHRlbXBsYXRlOiBUZW1wbGF0ZSk6IHZvaWQge1xuICAgIHZpc2l0QWxsKHRoaXMsIHRlbXBsYXRlLmF0dHJpYnV0ZXMpO1xuICAgIHZpc2l0QWxsKHRoaXMsIHRlbXBsYXRlLmNoaWxkcmVuKTtcbiAgICB2aXNpdEFsbCh0aGlzLCB0ZW1wbGF0ZS5yZWZlcmVuY2VzKTtcbiAgICB2aXNpdEFsbCh0aGlzLCB0ZW1wbGF0ZS52YXJpYWJsZXMpO1xuICB9XG4gIHZpc2l0Q29udGVudChjb250ZW50OiBDb250ZW50KTogdm9pZCB7fVxuICB2aXNpdFZhcmlhYmxlKHZhcmlhYmxlOiBWYXJpYWJsZSk6IHZvaWQge31cbiAgdmlzaXRSZWZlcmVuY2UocmVmZXJlbmNlOiBSZWZlcmVuY2UpOiB2b2lkIHt9XG4gIHZpc2l0VGV4dEF0dHJpYnV0ZShhdHRyaWJ1dGU6IFRleHRBdHRyaWJ1dGUpOiB2b2lkIHt9XG4gIHZpc2l0Qm91bmRBdHRyaWJ1dGUoYXR0cmlidXRlOiBCb3VuZEF0dHJpYnV0ZSk6IHZvaWQge31cbiAgdmlzaXRCb3VuZEV2ZW50KGF0dHJpYnV0ZTogQm91bmRFdmVudCk6IHZvaWQge31cbiAgdmlzaXRUZXh0KHRleHQ6IFRleHQpOiB2b2lkIHt9XG4gIHZpc2l0Qm91bmRUZXh0KHRleHQ6IEJvdW5kVGV4dCk6IHZvaWQge31cbiAgdmlzaXRJY3UoaWN1OiBJY3UpOiB2b2lkIHt9XG59XG5cbmV4cG9ydCBjbGFzcyBUcmFuc2Zvcm1WaXNpdG9yIGltcGxlbWVudHMgVmlzaXRvcjxOb2RlPiB7XG4gIHZpc2l0RWxlbWVudChlbGVtZW50OiBFbGVtZW50KTogTm9kZSB7XG4gICAgY29uc3QgbmV3QXR0cmlidXRlcyA9IHRyYW5zZm9ybUFsbCh0aGlzLCBlbGVtZW50LmF0dHJpYnV0ZXMpO1xuICAgIGNvbnN0IG5ld0lucHV0cyA9IHRyYW5zZm9ybUFsbCh0aGlzLCBlbGVtZW50LmlucHV0cyk7XG4gICAgY29uc3QgbmV3T3V0cHV0cyA9IHRyYW5zZm9ybUFsbCh0aGlzLCBlbGVtZW50Lm91dHB1dHMpO1xuICAgIGNvbnN0IG5ld0NoaWxkcmVuID0gdHJhbnNmb3JtQWxsKHRoaXMsIGVsZW1lbnQuY2hpbGRyZW4pO1xuICAgIGNvbnN0IG5ld1JlZmVyZW5jZXMgPSB0cmFuc2Zvcm1BbGwodGhpcywgZWxlbWVudC5yZWZlcmVuY2VzKTtcbiAgICBpZiAobmV3QXR0cmlidXRlcyAhPSBlbGVtZW50LmF0dHJpYnV0ZXMgfHwgbmV3SW5wdXRzICE9IGVsZW1lbnQuaW5wdXRzIHx8XG4gICAgICAgIG5ld091dHB1dHMgIT0gZWxlbWVudC5vdXRwdXRzIHx8IG5ld0NoaWxkcmVuICE9IGVsZW1lbnQuY2hpbGRyZW4gfHxcbiAgICAgICAgbmV3UmVmZXJlbmNlcyAhPSBlbGVtZW50LnJlZmVyZW5jZXMpIHtcbiAgICAgIHJldHVybiBuZXcgRWxlbWVudChcbiAgICAgICAgICBlbGVtZW50Lm5hbWUsIG5ld0F0dHJpYnV0ZXMsIG5ld0lucHV0cywgbmV3T3V0cHV0cywgbmV3Q2hpbGRyZW4sIG5ld1JlZmVyZW5jZXMsXG4gICAgICAgICAgZWxlbWVudC5zb3VyY2VTcGFuLCBlbGVtZW50LnN0YXJ0U291cmNlU3BhbiwgZWxlbWVudC5lbmRTb3VyY2VTcGFuKTtcbiAgICB9XG4gICAgcmV0dXJuIGVsZW1lbnQ7XG4gIH1cblxuICB2aXNpdFRlbXBsYXRlKHRlbXBsYXRlOiBUZW1wbGF0ZSk6IE5vZGUge1xuICAgIGNvbnN0IG5ld0F0dHJpYnV0ZXMgPSB0cmFuc2Zvcm1BbGwodGhpcywgdGVtcGxhdGUuYXR0cmlidXRlcyk7XG4gICAgY29uc3QgbmV3SW5wdXRzID0gdHJhbnNmb3JtQWxsKHRoaXMsIHRlbXBsYXRlLmlucHV0cyk7XG4gICAgY29uc3QgbmV3T3V0cHV0cyA9IHRyYW5zZm9ybUFsbCh0aGlzLCB0ZW1wbGF0ZS5vdXRwdXRzKTtcbiAgICBjb25zdCBuZXdUZW1wbGF0ZUF0dHJzID0gdHJhbnNmb3JtQWxsKHRoaXMsIHRlbXBsYXRlLnRlbXBsYXRlQXR0cnMpO1xuICAgIGNvbnN0IG5ld0NoaWxkcmVuID0gdHJhbnNmb3JtQWxsKHRoaXMsIHRlbXBsYXRlLmNoaWxkcmVuKTtcbiAgICBjb25zdCBuZXdSZWZlcmVuY2VzID0gdHJhbnNmb3JtQWxsKHRoaXMsIHRlbXBsYXRlLnJlZmVyZW5jZXMpO1xuICAgIGNvbnN0IG5ld1ZhcmlhYmxlcyA9IHRyYW5zZm9ybUFsbCh0aGlzLCB0ZW1wbGF0ZS52YXJpYWJsZXMpO1xuICAgIGlmIChuZXdBdHRyaWJ1dGVzICE9IHRlbXBsYXRlLmF0dHJpYnV0ZXMgfHwgbmV3SW5wdXRzICE9IHRlbXBsYXRlLmlucHV0cyB8fFxuICAgICAgICBuZXdPdXRwdXRzICE9IHRlbXBsYXRlLm91dHB1dHMgfHwgbmV3VGVtcGxhdGVBdHRycyAhPSB0ZW1wbGF0ZS50ZW1wbGF0ZUF0dHJzIHx8XG4gICAgICAgIG5ld0NoaWxkcmVuICE9IHRlbXBsYXRlLmNoaWxkcmVuIHx8IG5ld1JlZmVyZW5jZXMgIT0gdGVtcGxhdGUucmVmZXJlbmNlcyB8fFxuICAgICAgICBuZXdWYXJpYWJsZXMgIT0gdGVtcGxhdGUudmFyaWFibGVzKSB7XG4gICAgICByZXR1cm4gbmV3IFRlbXBsYXRlKFxuICAgICAgICAgIHRlbXBsYXRlLnRhZ05hbWUsIG5ld0F0dHJpYnV0ZXMsIG5ld0lucHV0cywgbmV3T3V0cHV0cywgbmV3VGVtcGxhdGVBdHRycywgbmV3Q2hpbGRyZW4sXG4gICAgICAgICAgbmV3UmVmZXJlbmNlcywgbmV3VmFyaWFibGVzLCB0ZW1wbGF0ZS5zb3VyY2VTcGFuLCB0ZW1wbGF0ZS5zdGFydFNvdXJjZVNwYW4sXG4gICAgICAgICAgdGVtcGxhdGUuZW5kU291cmNlU3Bhbik7XG4gICAgfVxuICAgIHJldHVybiB0ZW1wbGF0ZTtcbiAgfVxuXG4gIHZpc2l0Q29udGVudChjb250ZW50OiBDb250ZW50KTogTm9kZSB7IHJldHVybiBjb250ZW50OyB9XG5cbiAgdmlzaXRWYXJpYWJsZSh2YXJpYWJsZTogVmFyaWFibGUpOiBOb2RlIHsgcmV0dXJuIHZhcmlhYmxlOyB9XG4gIHZpc2l0UmVmZXJlbmNlKHJlZmVyZW5jZTogUmVmZXJlbmNlKTogTm9kZSB7IHJldHVybiByZWZlcmVuY2U7IH1cbiAgdmlzaXRUZXh0QXR0cmlidXRlKGF0dHJpYnV0ZTogVGV4dEF0dHJpYnV0ZSk6IE5vZGUgeyByZXR1cm4gYXR0cmlidXRlOyB9XG4gIHZpc2l0Qm91bmRBdHRyaWJ1dGUoYXR0cmlidXRlOiBCb3VuZEF0dHJpYnV0ZSk6IE5vZGUgeyByZXR1cm4gYXR0cmlidXRlOyB9XG4gIHZpc2l0Qm91bmRFdmVudChhdHRyaWJ1dGU6IEJvdW5kRXZlbnQpOiBOb2RlIHsgcmV0dXJuIGF0dHJpYnV0ZTsgfVxuICB2aXNpdFRleHQodGV4dDogVGV4dCk6IE5vZGUgeyByZXR1cm4gdGV4dDsgfVxuICB2aXNpdEJvdW5kVGV4dCh0ZXh0OiBCb3VuZFRleHQpOiBOb2RlIHsgcmV0dXJuIHRleHQ7IH1cbiAgdmlzaXRJY3UoaWN1OiBJY3UpOiBOb2RlIHsgcmV0dXJuIGljdTsgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gdmlzaXRBbGw8UmVzdWx0Pih2aXNpdG9yOiBWaXNpdG9yPFJlc3VsdD4sIG5vZGVzOiBOb2RlW10pOiBSZXN1bHRbXSB7XG4gIGNvbnN0IHJlc3VsdDogUmVzdWx0W10gPSBbXTtcbiAgaWYgKHZpc2l0b3IudmlzaXQpIHtcbiAgICBmb3IgKGNvbnN0IG5vZGUgb2Ygbm9kZXMpIHtcbiAgICAgIGNvbnN0IG5ld05vZGUgPSB2aXNpdG9yLnZpc2l0KG5vZGUpIHx8IG5vZGUudmlzaXQodmlzaXRvcik7XG4gICAgfVxuICB9IGVsc2Uge1xuICAgIGZvciAoY29uc3Qgbm9kZSBvZiBub2Rlcykge1xuICAgICAgY29uc3QgbmV3Tm9kZSA9IG5vZGUudmlzaXQodmlzaXRvcik7XG4gICAgICBpZiAobmV3Tm9kZSkge1xuICAgICAgICByZXN1bHQucHVzaChuZXdOb2RlKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbiAgcmV0dXJuIHJlc3VsdDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHRyYW5zZm9ybUFsbDxSZXN1bHQgZXh0ZW5kcyBOb2RlPihcbiAgICB2aXNpdG9yOiBWaXNpdG9yPE5vZGU+LCBub2RlczogUmVzdWx0W10pOiBSZXN1bHRbXSB7XG4gIGNvbnN0IHJlc3VsdDogUmVzdWx0W10gPSBbXTtcbiAgbGV0IGNoYW5nZWQgPSBmYWxzZTtcbiAgZm9yIChjb25zdCBub2RlIG9mIG5vZGVzKSB7XG4gICAgY29uc3QgbmV3Tm9kZSA9IG5vZGUudmlzaXQodmlzaXRvcik7XG4gICAgaWYgKG5ld05vZGUpIHtcbiAgICAgIHJlc3VsdC5wdXNoKG5ld05vZGUgYXMgUmVzdWx0KTtcbiAgICB9XG4gICAgY2hhbmdlZCA9IGNoYW5nZWQgfHwgbmV3Tm9kZSAhPSBub2RlO1xuICB9XG4gIHJldHVybiBjaGFuZ2VkID8gcmVzdWx0IDogbm9kZXM7XG59XG4iXX0=