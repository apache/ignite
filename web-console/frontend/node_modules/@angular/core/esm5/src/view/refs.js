/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Injector } from '../di/injector';
import { InjectFlags } from '../di/interface/injector';
import { ComponentFactory, ComponentRef } from '../linker/component_factory';
import { ComponentFactoryBoundToModule, ComponentFactoryResolver } from '../linker/component_factory_resolver';
import { ElementRef } from '../linker/element_ref';
import { NgModuleRef } from '../linker/ng_module_factory';
import { TemplateRef } from '../linker/template_ref';
import { stringify } from '../util/stringify';
import { VERSION } from '../version';
import { callNgModuleLifecycle, initNgModule, resolveNgModuleDep } from './ng_module';
import { Services, asElementData, asProviderData, asTextData } from './types';
import { markParentViewsForCheck, resolveDefinition, rootRenderNodes, splitNamespace, tokenKey, viewParentEl } from './util';
import { attachEmbeddedView, detachEmbeddedView, moveEmbeddedView, renderDetachView } from './view_attach';
var EMPTY_CONTEXT = new Object();
// Attention: this function is called as top level function.
// Putting any logic in here will destroy closure tree shaking!
export function createComponentFactory(selector, componentType, viewDefFactory, inputs, outputs, ngContentSelectors) {
    return new ComponentFactory_(selector, componentType, viewDefFactory, inputs, outputs, ngContentSelectors);
}
export function getComponentViewDefinitionFactory(componentFactory) {
    return componentFactory.viewDefFactory;
}
var ComponentFactory_ = /** @class */ (function (_super) {
    tslib_1.__extends(ComponentFactory_, _super);
    function ComponentFactory_(selector, componentType, viewDefFactory, _inputs, _outputs, ngContentSelectors) {
        var _this = 
        // Attention: this ctor is called as top level function.
        // Putting any logic in here will destroy closure tree shaking!
        _super.call(this) || this;
        _this.selector = selector;
        _this.componentType = componentType;
        _this._inputs = _inputs;
        _this._outputs = _outputs;
        _this.ngContentSelectors = ngContentSelectors;
        _this.viewDefFactory = viewDefFactory;
        return _this;
    }
    Object.defineProperty(ComponentFactory_.prototype, "inputs", {
        get: function () {
            var inputsArr = [];
            var inputs = this._inputs;
            for (var propName in inputs) {
                var templateName = inputs[propName];
                inputsArr.push({ propName: propName, templateName: templateName });
            }
            return inputsArr;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ComponentFactory_.prototype, "outputs", {
        get: function () {
            var outputsArr = [];
            for (var propName in this._outputs) {
                var templateName = this._outputs[propName];
                outputsArr.push({ propName: propName, templateName: templateName });
            }
            return outputsArr;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * Creates a new component.
     */
    ComponentFactory_.prototype.create = function (injector, projectableNodes, rootSelectorOrNode, ngModule) {
        if (!ngModule) {
            throw new Error('ngModule should be provided');
        }
        var viewDef = resolveDefinition(this.viewDefFactory);
        var componentNodeIndex = viewDef.nodes[0].element.componentProvider.nodeIndex;
        var view = Services.createRootView(injector, projectableNodes || [], rootSelectorOrNode, viewDef, ngModule, EMPTY_CONTEXT);
        var component = asProviderData(view, componentNodeIndex).instance;
        if (rootSelectorOrNode) {
            view.renderer.setAttribute(asElementData(view, 0).renderElement, 'ng-version', VERSION.full);
        }
        return new ComponentRef_(view, new ViewRef_(view), component);
    };
    return ComponentFactory_;
}(ComponentFactory));
var ComponentRef_ = /** @class */ (function (_super) {
    tslib_1.__extends(ComponentRef_, _super);
    function ComponentRef_(_view, _viewRef, _component) {
        var _this = _super.call(this) || this;
        _this._view = _view;
        _this._viewRef = _viewRef;
        _this._component = _component;
        _this._elDef = _this._view.def.nodes[0];
        _this.hostView = _viewRef;
        _this.changeDetectorRef = _viewRef;
        _this.instance = _component;
        return _this;
    }
    Object.defineProperty(ComponentRef_.prototype, "location", {
        get: function () {
            return new ElementRef(asElementData(this._view, this._elDef.nodeIndex).renderElement);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ComponentRef_.prototype, "injector", {
        get: function () { return new Injector_(this._view, this._elDef); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ComponentRef_.prototype, "componentType", {
        get: function () { return this._component.constructor; },
        enumerable: true,
        configurable: true
    });
    ComponentRef_.prototype.destroy = function () { this._viewRef.destroy(); };
    ComponentRef_.prototype.onDestroy = function (callback) { this._viewRef.onDestroy(callback); };
    return ComponentRef_;
}(ComponentRef));
export function createViewContainerData(view, elDef, elData) {
    return new ViewContainerRef_(view, elDef, elData);
}
var ViewContainerRef_ = /** @class */ (function () {
    function ViewContainerRef_(_view, _elDef, _data) {
        this._view = _view;
        this._elDef = _elDef;
        this._data = _data;
        /**
         * @internal
         */
        this._embeddedViews = [];
    }
    Object.defineProperty(ViewContainerRef_.prototype, "element", {
        get: function () { return new ElementRef(this._data.renderElement); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ViewContainerRef_.prototype, "injector", {
        get: function () { return new Injector_(this._view, this._elDef); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ViewContainerRef_.prototype, "parentInjector", {
        /** @deprecated No replacement */
        get: function () {
            var view = this._view;
            var elDef = this._elDef.parent;
            while (!elDef && view) {
                elDef = viewParentEl(view);
                view = view.parent;
            }
            return view ? new Injector_(view, elDef) : new Injector_(this._view, null);
        },
        enumerable: true,
        configurable: true
    });
    ViewContainerRef_.prototype.clear = function () {
        var len = this._embeddedViews.length;
        for (var i = len - 1; i >= 0; i--) {
            var view = detachEmbeddedView(this._data, i);
            Services.destroyView(view);
        }
    };
    ViewContainerRef_.prototype.get = function (index) {
        var view = this._embeddedViews[index];
        if (view) {
            var ref = new ViewRef_(view);
            ref.attachToViewContainerRef(this);
            return ref;
        }
        return null;
    };
    Object.defineProperty(ViewContainerRef_.prototype, "length", {
        get: function () { return this._embeddedViews.length; },
        enumerable: true,
        configurable: true
    });
    ViewContainerRef_.prototype.createEmbeddedView = function (templateRef, context, index) {
        var viewRef = templateRef.createEmbeddedView(context || {});
        this.insert(viewRef, index);
        return viewRef;
    };
    ViewContainerRef_.prototype.createComponent = function (componentFactory, index, injector, projectableNodes, ngModuleRef) {
        var contextInjector = injector || this.parentInjector;
        if (!ngModuleRef && !(componentFactory instanceof ComponentFactoryBoundToModule)) {
            ngModuleRef = contextInjector.get(NgModuleRef);
        }
        var componentRef = componentFactory.create(contextInjector, projectableNodes, undefined, ngModuleRef);
        this.insert(componentRef.hostView, index);
        return componentRef;
    };
    ViewContainerRef_.prototype.insert = function (viewRef, index) {
        if (viewRef.destroyed) {
            throw new Error('Cannot insert a destroyed View in a ViewContainer!');
        }
        var viewRef_ = viewRef;
        var viewData = viewRef_._view;
        attachEmbeddedView(this._view, this._data, index, viewData);
        viewRef_.attachToViewContainerRef(this);
        return viewRef;
    };
    ViewContainerRef_.prototype.move = function (viewRef, currentIndex) {
        if (viewRef.destroyed) {
            throw new Error('Cannot move a destroyed View in a ViewContainer!');
        }
        var previousIndex = this._embeddedViews.indexOf(viewRef._view);
        moveEmbeddedView(this._data, previousIndex, currentIndex);
        return viewRef;
    };
    ViewContainerRef_.prototype.indexOf = function (viewRef) {
        return this._embeddedViews.indexOf(viewRef._view);
    };
    ViewContainerRef_.prototype.remove = function (index) {
        var viewData = detachEmbeddedView(this._data, index);
        if (viewData) {
            Services.destroyView(viewData);
        }
    };
    ViewContainerRef_.prototype.detach = function (index) {
        var view = detachEmbeddedView(this._data, index);
        return view ? new ViewRef_(view) : null;
    };
    return ViewContainerRef_;
}());
export function createChangeDetectorRef(view) {
    return new ViewRef_(view);
}
var ViewRef_ = /** @class */ (function () {
    function ViewRef_(_view) {
        this._view = _view;
        this._viewContainerRef = null;
        this._appRef = null;
    }
    Object.defineProperty(ViewRef_.prototype, "rootNodes", {
        get: function () { return rootRenderNodes(this._view); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ViewRef_.prototype, "context", {
        get: function () { return this._view.context; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(ViewRef_.prototype, "destroyed", {
        get: function () { return (this._view.state & 128 /* Destroyed */) !== 0; },
        enumerable: true,
        configurable: true
    });
    ViewRef_.prototype.markForCheck = function () { markParentViewsForCheck(this._view); };
    ViewRef_.prototype.detach = function () { this._view.state &= ~4 /* Attached */; };
    ViewRef_.prototype.detectChanges = function () {
        var fs = this._view.root.rendererFactory;
        if (fs.begin) {
            fs.begin();
        }
        try {
            Services.checkAndUpdateView(this._view);
        }
        finally {
            if (fs.end) {
                fs.end();
            }
        }
    };
    ViewRef_.prototype.checkNoChanges = function () { Services.checkNoChangesView(this._view); };
    ViewRef_.prototype.reattach = function () { this._view.state |= 4 /* Attached */; };
    ViewRef_.prototype.onDestroy = function (callback) {
        if (!this._view.disposables) {
            this._view.disposables = [];
        }
        this._view.disposables.push(callback);
    };
    ViewRef_.prototype.destroy = function () {
        if (this._appRef) {
            this._appRef.detachView(this);
        }
        else if (this._viewContainerRef) {
            this._viewContainerRef.detach(this._viewContainerRef.indexOf(this));
        }
        Services.destroyView(this._view);
    };
    ViewRef_.prototype.detachFromAppRef = function () {
        this._appRef = null;
        renderDetachView(this._view);
        Services.dirtyParentQueries(this._view);
    };
    ViewRef_.prototype.attachToAppRef = function (appRef) {
        if (this._viewContainerRef) {
            throw new Error('This view is already attached to a ViewContainer!');
        }
        this._appRef = appRef;
    };
    ViewRef_.prototype.attachToViewContainerRef = function (vcRef) {
        if (this._appRef) {
            throw new Error('This view is already attached directly to the ApplicationRef!');
        }
        this._viewContainerRef = vcRef;
    };
    return ViewRef_;
}());
export { ViewRef_ };
export function createTemplateData(view, def) {
    return new TemplateRef_(view, def);
}
var TemplateRef_ = /** @class */ (function (_super) {
    tslib_1.__extends(TemplateRef_, _super);
    function TemplateRef_(_parentView, _def) {
        var _this = _super.call(this) || this;
        _this._parentView = _parentView;
        _this._def = _def;
        return _this;
    }
    TemplateRef_.prototype.createEmbeddedView = function (context) {
        return new ViewRef_(Services.createEmbeddedView(this._parentView, this._def, this._def.element.template, context));
    };
    Object.defineProperty(TemplateRef_.prototype, "elementRef", {
        get: function () {
            return new ElementRef(asElementData(this._parentView, this._def.nodeIndex).renderElement);
        },
        enumerable: true,
        configurable: true
    });
    return TemplateRef_;
}(TemplateRef));
export function createInjector(view, elDef) {
    return new Injector_(view, elDef);
}
var Injector_ = /** @class */ (function () {
    function Injector_(view, elDef) {
        this.view = view;
        this.elDef = elDef;
    }
    Injector_.prototype.get = function (token, notFoundValue) {
        if (notFoundValue === void 0) { notFoundValue = Injector.THROW_IF_NOT_FOUND; }
        var allowPrivateServices = this.elDef ? (this.elDef.flags & 33554432 /* ComponentView */) !== 0 : false;
        return Services.resolveDep(this.view, this.elDef, allowPrivateServices, { flags: 0 /* None */, token: token, tokenKey: tokenKey(token) }, notFoundValue);
    };
    return Injector_;
}());
export function nodeValue(view, index) {
    var def = view.def.nodes[index];
    if (def.flags & 1 /* TypeElement */) {
        var elData = asElementData(view, def.nodeIndex);
        return def.element.template ? elData.template : elData.renderElement;
    }
    else if (def.flags & 2 /* TypeText */) {
        return asTextData(view, def.nodeIndex).renderText;
    }
    else if (def.flags & (20224 /* CatProvider */ | 16 /* TypePipe */)) {
        return asProviderData(view, def.nodeIndex).instance;
    }
    throw new Error("Illegal state: read nodeValue for node index " + index);
}
export function createRendererV1(view) {
    return new RendererAdapter(view.renderer);
}
var RendererAdapter = /** @class */ (function () {
    function RendererAdapter(delegate) {
        this.delegate = delegate;
    }
    RendererAdapter.prototype.selectRootElement = function (selectorOrNode) {
        return this.delegate.selectRootElement(selectorOrNode);
    };
    RendererAdapter.prototype.createElement = function (parent, namespaceAndName) {
        var _a = tslib_1.__read(splitNamespace(namespaceAndName), 2), ns = _a[0], name = _a[1];
        var el = this.delegate.createElement(name, ns);
        if (parent) {
            this.delegate.appendChild(parent, el);
        }
        return el;
    };
    RendererAdapter.prototype.createViewRoot = function (hostElement) { return hostElement; };
    RendererAdapter.prototype.createTemplateAnchor = function (parentElement) {
        var comment = this.delegate.createComment('');
        if (parentElement) {
            this.delegate.appendChild(parentElement, comment);
        }
        return comment;
    };
    RendererAdapter.prototype.createText = function (parentElement, value) {
        var node = this.delegate.createText(value);
        if (parentElement) {
            this.delegate.appendChild(parentElement, node);
        }
        return node;
    };
    RendererAdapter.prototype.projectNodes = function (parentElement, nodes) {
        for (var i = 0; i < nodes.length; i++) {
            this.delegate.appendChild(parentElement, nodes[i]);
        }
    };
    RendererAdapter.prototype.attachViewAfter = function (node, viewRootNodes) {
        var parentElement = this.delegate.parentNode(node);
        var nextSibling = this.delegate.nextSibling(node);
        for (var i = 0; i < viewRootNodes.length; i++) {
            this.delegate.insertBefore(parentElement, viewRootNodes[i], nextSibling);
        }
    };
    RendererAdapter.prototype.detachView = function (viewRootNodes) {
        for (var i = 0; i < viewRootNodes.length; i++) {
            var node = viewRootNodes[i];
            var parentElement = this.delegate.parentNode(node);
            this.delegate.removeChild(parentElement, node);
        }
    };
    RendererAdapter.prototype.destroyView = function (hostElement, viewAllNodes) {
        for (var i = 0; i < viewAllNodes.length; i++) {
            this.delegate.destroyNode(viewAllNodes[i]);
        }
    };
    RendererAdapter.prototype.listen = function (renderElement, name, callback) {
        return this.delegate.listen(renderElement, name, callback);
    };
    RendererAdapter.prototype.listenGlobal = function (target, name, callback) {
        return this.delegate.listen(target, name, callback);
    };
    RendererAdapter.prototype.setElementProperty = function (renderElement, propertyName, propertyValue) {
        this.delegate.setProperty(renderElement, propertyName, propertyValue);
    };
    RendererAdapter.prototype.setElementAttribute = function (renderElement, namespaceAndName, attributeValue) {
        var _a = tslib_1.__read(splitNamespace(namespaceAndName), 2), ns = _a[0], name = _a[1];
        if (attributeValue != null) {
            this.delegate.setAttribute(renderElement, name, attributeValue, ns);
        }
        else {
            this.delegate.removeAttribute(renderElement, name, ns);
        }
    };
    RendererAdapter.prototype.setBindingDebugInfo = function (renderElement, propertyName, propertyValue) { };
    RendererAdapter.prototype.setElementClass = function (renderElement, className, isAdd) {
        if (isAdd) {
            this.delegate.addClass(renderElement, className);
        }
        else {
            this.delegate.removeClass(renderElement, className);
        }
    };
    RendererAdapter.prototype.setElementStyle = function (renderElement, styleName, styleValue) {
        if (styleValue != null) {
            this.delegate.setStyle(renderElement, styleName, styleValue);
        }
        else {
            this.delegate.removeStyle(renderElement, styleName);
        }
    };
    RendererAdapter.prototype.invokeElementMethod = function (renderElement, methodName, args) {
        renderElement[methodName].apply(renderElement, args);
    };
    RendererAdapter.prototype.setText = function (renderNode, text) { this.delegate.setValue(renderNode, text); };
    RendererAdapter.prototype.animate = function () { throw new Error('Renderer.animate is no longer supported!'); };
    return RendererAdapter;
}());
export function createNgModuleRef(moduleType, parent, bootstrapComponents, def) {
    return new NgModuleRef_(moduleType, parent, bootstrapComponents, def);
}
var NgModuleRef_ = /** @class */ (function () {
    function NgModuleRef_(_moduleType, _parent, _bootstrapComponents, _def) {
        this._moduleType = _moduleType;
        this._parent = _parent;
        this._bootstrapComponents = _bootstrapComponents;
        this._def = _def;
        this._destroyListeners = [];
        this._destroyed = false;
        this.injector = this;
        initNgModule(this);
    }
    NgModuleRef_.prototype.get = function (token, notFoundValue, injectFlags) {
        if (notFoundValue === void 0) { notFoundValue = Injector.THROW_IF_NOT_FOUND; }
        if (injectFlags === void 0) { injectFlags = InjectFlags.Default; }
        var flags = 0 /* None */;
        if (injectFlags & InjectFlags.SkipSelf) {
            flags |= 1 /* SkipSelf */;
        }
        else if (injectFlags & InjectFlags.Self) {
            flags |= 4 /* Self */;
        }
        return resolveNgModuleDep(this, { token: token, tokenKey: tokenKey(token), flags: flags }, notFoundValue);
    };
    Object.defineProperty(NgModuleRef_.prototype, "instance", {
        get: function () { return this.get(this._moduleType); },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgModuleRef_.prototype, "componentFactoryResolver", {
        get: function () { return this.get(ComponentFactoryResolver); },
        enumerable: true,
        configurable: true
    });
    NgModuleRef_.prototype.destroy = function () {
        if (this._destroyed) {
            throw new Error("The ng module " + stringify(this.instance.constructor) + " has already been destroyed.");
        }
        this._destroyed = true;
        callNgModuleLifecycle(this, 131072 /* OnDestroy */);
        this._destroyListeners.forEach(function (listener) { return listener(); });
    };
    NgModuleRef_.prototype.onDestroy = function (callback) { this._destroyListeners.push(callback); };
    return NgModuleRef_;
}());
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmcy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvcmVmcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBSUgsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBQ3hDLE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSwwQkFBMEIsQ0FBQztBQUVyRCxPQUFPLEVBQUMsZ0JBQWdCLEVBQUUsWUFBWSxFQUFDLE1BQU0sNkJBQTZCLENBQUM7QUFDM0UsT0FBTyxFQUFDLDZCQUE2QixFQUFFLHdCQUF3QixFQUFDLE1BQU0sc0NBQXNDLENBQUM7QUFDN0csT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQ2pELE9BQU8sRUFBc0IsV0FBVyxFQUFDLE1BQU0sNkJBQTZCLENBQUM7QUFDN0UsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLHdCQUF3QixDQUFDO0FBSW5ELE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUM1QyxPQUFPLEVBQUMsT0FBTyxFQUFDLE1BQU0sWUFBWSxDQUFDO0FBRW5DLE9BQU8sRUFBQyxxQkFBcUIsRUFBRSxZQUFZLEVBQUUsa0JBQWtCLEVBQUMsTUFBTSxhQUFhLENBQUM7QUFDcEYsT0FBTyxFQUE4RSxRQUFRLEVBQStFLGFBQWEsRUFBRSxjQUFjLEVBQUUsVUFBVSxFQUFDLE1BQU0sU0FBUyxDQUFDO0FBQ3RPLE9BQU8sRUFBQyx1QkFBdUIsRUFBRSxpQkFBaUIsRUFBRSxlQUFlLEVBQUUsY0FBYyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUMsTUFBTSxRQUFRLENBQUM7QUFDM0gsT0FBTyxFQUFDLGtCQUFrQixFQUFFLGtCQUFrQixFQUFFLGdCQUFnQixFQUFFLGdCQUFnQixFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRXpHLElBQU0sYUFBYSxH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QWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QXBwbGljYXRpb25SZWZ9IGZyb20gJy4uL2FwcGxpY2F0aW9uX3JlZic7XG5pbXBvcnQge0NoYW5nZURldGVjdG9yUmVmfSBmcm9tICcuLi9jaGFuZ2VfZGV0ZWN0aW9uL2NoYW5nZV9kZXRlY3Rpb24nO1xuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnLi4vZGkvaW5qZWN0b3InO1xuaW1wb3J0IHtJbmplY3RGbGFnc30gZnJvbSAnLi4vZGkvaW50ZXJmYWNlL2luamVjdG9yJztcbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtDb21wb25lbnRGYWN0b3J5LCBDb21wb25lbnRSZWZ9IGZyb20gJy4uL2xpbmtlci9jb21wb25lbnRfZmFjdG9yeSc7XG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlCb3VuZFRvTW9kdWxlLCBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJ9IGZyb20gJy4uL2xpbmtlci9jb21wb25lbnRfZmFjdG9yeV9yZXNvbHZlcic7XG5pbXBvcnQge0VsZW1lbnRSZWZ9IGZyb20gJy4uL2xpbmtlci9lbGVtZW50X3JlZic7XG5pbXBvcnQge0ludGVybmFsTmdNb2R1bGVSZWYsIE5nTW9kdWxlUmVmfSBmcm9tICcuLi9saW5rZXIvbmdfbW9kdWxlX2ZhY3RvcnknO1xuaW1wb3J0IHtUZW1wbGF0ZVJlZn0gZnJvbSAnLi4vbGlua2VyL3RlbXBsYXRlX3JlZic7XG5pbXBvcnQge1ZpZXdDb250YWluZXJSZWZ9IGZyb20gJy4uL2xpbmtlci92aWV3X2NvbnRhaW5lcl9yZWYnO1xuaW1wb3J0IHtFbWJlZGRlZFZpZXdSZWYsIEludGVybmFsVmlld1JlZiwgVmlld1JlZn0gZnJvbSAnLi4vbGlua2VyL3ZpZXdfcmVmJztcbmltcG9ydCB7UmVuZGVyZXIgYXMgUmVuZGVyZXJWMSwgUmVuZGVyZXIyfSBmcm9tICcuLi9yZW5kZXIvYXBpJztcbmltcG9ydCB7c3RyaW5naWZ5fSBmcm9tICcuLi91dGlsL3N0cmluZ2lmeSc7XG5pbXBvcnQge1ZFUlNJT059IGZyb20gJy4uL3ZlcnNpb24nO1xuXG5pbXBvcnQge2NhbGxOZ01vZHVsZUxpZmVjeWNsZSwgaW5pdE5nTW9kdWxlLCByZXNvbHZlTmdNb2R1bGVEZXB9IGZyb20gJy4vbmdfbW9kdWxlJztcbmltcG9ydCB7RGVwRmxhZ3MsIEVsZW1lbnREYXRhLCBOZ01vZHVsZURhdGEsIE5nTW9kdWxlRGVmaW5pdGlvbiwgTm9kZURlZiwgTm9kZUZsYWdzLCBTZXJ2aWNlcywgVGVtcGxhdGVEYXRhLCBWaWV3Q29udGFpbmVyRGF0YSwgVmlld0RhdGEsIFZpZXdEZWZpbml0aW9uRmFjdG9yeSwgVmlld1N0YXRlLCBhc0VsZW1lbnREYXRhLCBhc1Byb3ZpZGVyRGF0YSwgYXNUZXh0RGF0YX0gZnJvbSAnLi90eXBlcyc7XG5pbXBvcnQge21hcmtQYXJlbnRWaWV3c0ZvckNoZWNrLCByZXNvbHZlRGVmaW5pdGlvbiwgcm9vdFJlbmRlck5vZGVzLCBzcGxpdE5hbWVzcGFjZSwgdG9rZW5LZXksIHZpZXdQYXJlbnRFbH0gZnJvbSAnLi91dGlsJztcbmltcG9ydCB7YXR0YWNoRW1iZWRkZWRWaWV3LCBkZXRhY2hFbWJlZGRlZFZpZXcsIG1vdmVFbWJlZGRlZFZpZXcsIHJlbmRlckRldGFjaFZpZXd9IGZyb20gJy4vdmlld19hdHRhY2gnO1xuXG5jb25zdCBFTVBUWV9DT05URVhUID0gbmV3IE9iamVjdCgpO1xuXG4vLyBBdHRlbnRpb246IHRoaXMgZnVuY3Rpb24gaXMgY2FsbGVkIGFzIHRvcCBsZXZlbCBmdW5jdGlvbi5cbi8vIFB1dHRpbmcgYW55IGxvZ2ljIGluIGhlcmUgd2lsbCBkZXN0cm95IGNsb3N1cmUgdHJlZSBzaGFraW5nIVxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZUNvbXBvbmVudEZhY3RvcnkoXG4gICAgc2VsZWN0b3I6IHN0cmluZywgY29tcG9uZW50VHlwZTogVHlwZTxhbnk+LCB2aWV3RGVmRmFjdG9yeTogVmlld0RlZmluaXRpb25GYWN0b3J5LFxuICAgIGlucHV0czoge1twcm9wTmFtZTogc3RyaW5nXTogc3RyaW5nfSB8IG51bGwsIG91dHB1dHM6IHtbcHJvcE5hbWU6IHN0cmluZ106IHN0cmluZ30sXG4gICAgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXSk6IENvbXBvbmVudEZhY3Rvcnk8YW55PiB7XG4gIHJldHVybiBuZXcgQ29tcG9uZW50RmFjdG9yeV8oXG4gICAgICBzZWxlY3RvciwgY29tcG9uZW50VHlwZSwgdmlld0RlZkZhY3RvcnksIGlucHV0cywgb3V0cHV0cywgbmdDb250ZW50U2VsZWN0b3JzKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbXBvbmVudFZpZXdEZWZpbml0aW9uRmFjdG9yeShjb21wb25lbnRGYWN0b3J5OiBDb21wb25lbnRGYWN0b3J5PGFueT4pOlxuICAgIFZpZXdEZWZpbml0aW9uRmFjdG9yeSB7XG4gIHJldHVybiAoY29tcG9uZW50RmFjdG9yeSBhcyBDb21wb25lbnRGYWN0b3J5Xykudmlld0RlZkZhY3Rvcnk7XG59XG5cbmNsYXNzIENvbXBvbmVudEZhY3RvcnlfIGV4dGVuZHMgQ29tcG9uZW50RmFjdG9yeTxhbnk+IHtcbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgdmlld0RlZkZhY3Rvcnk6IFZpZXdEZWZpbml0aW9uRmFjdG9yeTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBzZWxlY3Rvcjogc3RyaW5nLCBwdWJsaWMgY29tcG9uZW50VHlwZTogVHlwZTxhbnk+LFxuICAgICAgdmlld0RlZkZhY3Rvcnk6IFZpZXdEZWZpbml0aW9uRmFjdG9yeSwgcHJpdmF0ZSBfaW5wdXRzOiB7W3Byb3BOYW1lOiBzdHJpbmddOiBzdHJpbmd9fG51bGwsXG4gICAgICBwcml2YXRlIF9vdXRwdXRzOiB7W3Byb3BOYW1lOiBzdHJpbmddOiBzdHJpbmd9LCBwdWJsaWMgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXSkge1xuICAgIC8vIEF0dGVudGlvbjogdGhpcyBjdG9yIGlzIGNhbGxlZCBhcyB0b3AgbGV2ZWwgZnVuY3Rpb24uXG4gICAgLy8gUHV0dGluZyBhbnkgbG9naWMgaW4gaGVyZSB3aWxsIGRlc3Ryb3kgY2xvc3VyZSB0cmVlIHNoYWtpbmchXG4gICAgc3VwZXIoKTtcbiAgICB0aGlzLnZpZXdEZWZGYWN0b3J5ID0gdmlld0RlZkZhY3Rvcnk7XG4gIH1cblxuICBnZXQgaW5wdXRzKCkge1xuICAgIGNvbnN0IGlucHV0c0Fycjoge3Byb3BOYW1lOiBzdHJpbmcsIHRlbXBsYXRlTmFtZTogc3RyaW5nfVtdID0gW107XG4gICAgY29uc3QgaW5wdXRzID0gdGhpcy5faW5wdXRzICE7XG4gICAgZm9yIChsZXQgcHJvcE5hbWUgaW4gaW5wdXRzKSB7XG4gICAgICBjb25zdCB0ZW1wbGF0ZU5hbWUgPSBpbnB1dHNbcHJvcE5hbWVdO1xuICAgICAgaW5wdXRzQXJyLnB1c2goe3Byb3BOYW1lLCB0ZW1wbGF0ZU5hbWV9KTtcbiAgICB9XG4gICAgcmV0dXJuIGlucHV0c0FycjtcbiAgfVxuXG4gIGdldCBvdXRwdXRzKCkge1xuICAgIGNvbnN0IG91dHB1dHNBcnI6IHtwcm9wTmFtZTogc3RyaW5nLCB0ZW1wbGF0ZU5hbWU6IHN0cmluZ31bXSA9IFtdO1xuICAgIGZvciAobGV0IHByb3BOYW1lIGluIHRoaXMuX291dHB1dHMpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlTmFtZSA9IHRoaXMuX291dHB1dHNbcHJvcE5hbWVdO1xuICAgICAgb3V0cHV0c0Fyci5wdXNoKHtwcm9wTmFtZSwgdGVtcGxhdGVOYW1lfSk7XG4gICAgfVxuICAgIHJldHVybiBvdXRwdXRzQXJyO1xuICB9XG5cbiAgLyoqXG4gICAqIENyZWF0ZXMgYSBuZXcgY29tcG9uZW50LlxuICAgKi9cbiAgY3JlYXRlKFxuICAgICAgaW5qZWN0b3I6IEluamVjdG9yLCBwcm9qZWN0YWJsZU5vZGVzPzogYW55W11bXSwgcm9vdFNlbGVjdG9yT3JOb2RlPzogc3RyaW5nfGFueSxcbiAgICAgIG5nTW9kdWxlPzogTmdNb2R1bGVSZWY8YW55Pik6IENvbXBvbmVudFJlZjxhbnk+IHtcbiAgICBpZiAoIW5nTW9kdWxlKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ25nTW9kdWxlIHNob3VsZCBiZSBwcm92aWRlZCcpO1xuICAgIH1cbiAgICBjb25zdCB2aWV3RGVmID0gcmVzb2x2ZURlZmluaXRpb24odGhpcy52aWV3RGVmRmFjdG9yeSk7XG4gICAgY29uc3QgY29tcG9uZW50Tm9kZUluZGV4ID0gdmlld0RlZi5ub2Rlc1swXS5lbGVtZW50ICEuY29tcG9uZW50UHJvdmlkZXIgIS5ub2RlSW5kZXg7XG4gICAgY29uc3QgdmlldyA9IFNlcnZpY2VzLmNyZWF0ZVJvb3RWaWV3KFxuICAgICAgICBpbmplY3RvciwgcHJvamVjdGFibGVOb2RlcyB8fCBbXSwgcm9vdFNlbGVjdG9yT3JOb2RlLCB2aWV3RGVmLCBuZ01vZHVsZSwgRU1QVFlfQ09OVEVYVCk7XG4gICAgY29uc3QgY29tcG9uZW50ID0gYXNQcm92aWRlckRhdGEodmlldywgY29tcG9uZW50Tm9kZUluZGV4KS5pbnN0YW5jZTtcbiAgICBpZiAocm9vdFNlbGVjdG9yT3JOb2RlKSB7XG4gICAgICB2aWV3LnJlbmRlcmVyLnNldEF0dHJpYnV0ZShhc0VsZW1lbnREYXRhKHZpZXcsIDApLnJlbmRlckVsZW1lbnQsICduZy12ZXJzaW9uJywgVkVSU0lPTi5mdWxsKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbmV3IENvbXBvbmVudFJlZl8odmlldywgbmV3IFZpZXdSZWZfKHZpZXcpLCBjb21wb25lbnQpO1xuICB9XG59XG5cbmNsYXNzIENvbXBvbmVudFJlZl8gZXh0ZW5kcyBDb21wb25lbnRSZWY8YW55PiB7XG4gIHB1YmxpYyByZWFkb25seSBob3N0VmlldzogVmlld1JlZjtcbiAgcHVibGljIHJlYWRvbmx5IGluc3RhbmNlOiBhbnk7XG4gIHB1YmxpYyByZWFkb25seSBjaGFuZ2VEZXRlY3RvclJlZjogQ2hhbmdlRGV0ZWN0b3JSZWY7XG4gIHByaXZhdGUgX2VsRGVmOiBOb2RlRGVmO1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF92aWV3OiBWaWV3RGF0YSwgcHJpdmF0ZSBfdmlld1JlZjogVmlld1JlZiwgcHJpdmF0ZSBfY29tcG9uZW50OiBhbnkpIHtcbiAgICBzdXBlcigpO1xuICAgIHRoaXMuX2VsRGVmID0gdGhpcy5fdmlldy5kZWYubm9kZXNbMF07XG4gICAgdGhpcy5ob3N0VmlldyA9IF92aWV3UmVmO1xuICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYgPSBfdmlld1JlZjtcbiAgICB0aGlzLmluc3RhbmNlID0gX2NvbXBvbmVudDtcbiAgfVxuICBnZXQgbG9jYXRpb24oKTogRWxlbWVudFJlZiB7XG4gICAgcmV0dXJuIG5ldyBFbGVtZW50UmVmKGFzRWxlbWVudERhdGEodGhpcy5fdmlldywgdGhpcy5fZWxEZWYubm9kZUluZGV4KS5yZW5kZXJFbGVtZW50KTtcbiAgfVxuICBnZXQgaW5qZWN0b3IoKTogSW5qZWN0b3IgeyByZXR1cm4gbmV3IEluamVjdG9yXyh0aGlzLl92aWV3LCB0aGlzLl9lbERlZik7IH1cbiAgZ2V0IGNvbXBvbmVudFR5cGUoKTogVHlwZTxhbnk+IHsgcmV0dXJuIDxhbnk+dGhpcy5fY29tcG9uZW50LmNvbnN0cnVjdG9yOyB9XG5cbiAgZGVzdHJveSgpOiB2b2lkIHsgdGhpcy5fdmlld1JlZi5kZXN0cm95KCk7IH1cbiAgb25EZXN0cm95KGNhbGxiYWNrOiBGdW5jdGlvbik6IHZvaWQgeyB0aGlzLl92aWV3UmVmLm9uRGVzdHJveShjYWxsYmFjayk7IH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVZpZXdDb250YWluZXJEYXRhKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBlbERlZjogTm9kZURlZiwgZWxEYXRhOiBFbGVtZW50RGF0YSk6IFZpZXdDb250YWluZXJEYXRhIHtcbiAgcmV0dXJuIG5ldyBWaWV3Q29udGFpbmVyUmVmXyh2aWV3LCBlbERlZiwgZWxEYXRhKTtcbn1cblxuY2xhc3MgVmlld0NvbnRhaW5lclJlZl8gaW1wbGVtZW50cyBWaWV3Q29udGFpbmVyRGF0YSB7XG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIF9lbWJlZGRlZFZpZXdzOiBWaWV3RGF0YVtdID0gW107XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3ZpZXc6IFZpZXdEYXRhLCBwcml2YXRlIF9lbERlZjogTm9kZURlZiwgcHJpdmF0ZSBfZGF0YTogRWxlbWVudERhdGEpIHt9XG5cbiAgZ2V0IGVsZW1lbnQoKTogRWxlbWVudFJlZiB7IHJldHVybiBuZXcgRWxlbWVudFJlZih0aGlzLl9kYXRhLnJlbmRlckVsZW1lbnQpOyB9XG5cbiAgZ2V0IGluamVjdG9yKCk6IEluamVjdG9yIHsgcmV0dXJuIG5ldyBJbmplY3Rvcl8odGhpcy5fdmlldywgdGhpcy5fZWxEZWYpOyB9XG5cbiAgLyoqIEBkZXByZWNhdGVkIE5vIHJlcGxhY2VtZW50ICovXG4gIGdldCBwYXJlbnRJbmplY3RvcigpOiBJbmplY3RvciB7XG4gICAgbGV0IHZpZXcgPSB0aGlzLl92aWV3O1xuICAgIGxldCBlbERlZiA9IHRoaXMuX2VsRGVmLnBhcmVudDtcbiAgICB3aGlsZSAoIWVsRGVmICYmIHZpZXcpIHtcbiAgICAgIGVsRGVmID0gdmlld1BhcmVudEVsKHZpZXcpO1xuICAgICAgdmlldyA9IHZpZXcucGFyZW50ICE7XG4gICAgfVxuXG4gICAgcmV0dXJuIHZpZXcgPyBuZXcgSW5qZWN0b3JfKHZpZXcsIGVsRGVmKSA6IG5ldyBJbmplY3Rvcl8odGhpcy5fdmlldywgbnVsbCk7XG4gIH1cblxuICBjbGVhcigpOiB2b2lkIHtcbiAgICBjb25zdCBsZW4gPSB0aGlzLl9lbWJlZGRlZFZpZXdzLmxlbmd0aDtcbiAgICBmb3IgKGxldCBpID0gbGVuIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICAgIGNvbnN0IHZpZXcgPSBkZXRhY2hFbWJlZGRlZFZpZXcodGhpcy5fZGF0YSwgaSkgITtcbiAgICAgIFNlcnZpY2VzLmRlc3Ryb3lWaWV3KHZpZXcpO1xuICAgIH1cbiAgfVxuXG4gIGdldChpbmRleDogbnVtYmVyKTogVmlld1JlZnxudWxsIHtcbiAgICBjb25zdCB2aWV3ID0gdGhpcy5fZW1iZWRkZWRWaWV3c1tpbmRleF07XG4gICAgaWYgKHZpZXcpIHtcbiAgICAgIGNvbnN0IHJlZiA9IG5ldyBWaWV3UmVmXyh2aWV3KTtcbiAgICAgIHJlZi5hdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodGhpcyk7XG4gICAgICByZXR1cm4gcmVmO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGdldCBsZW5ndGgoKTogbnVtYmVyIHsgcmV0dXJuIHRoaXMuX2VtYmVkZGVkVmlld3MubGVuZ3RoOyB9XG5cbiAgY3JlYXRlRW1iZWRkZWRWaWV3PEM+KHRlbXBsYXRlUmVmOiBUZW1wbGF0ZVJlZjxDPiwgY29udGV4dD86IEMsIGluZGV4PzogbnVtYmVyKTpcbiAgICAgIEVtYmVkZGVkVmlld1JlZjxDPiB7XG4gICAgY29uc3Qgdmlld1JlZiA9IHRlbXBsYXRlUmVmLmNyZWF0ZUVtYmVkZGVkVmlldyhjb250ZXh0IHx8IDxhbnk+e30pO1xuICAgIHRoaXMuaW5zZXJ0KHZpZXdSZWYsIGluZGV4KTtcbiAgICByZXR1cm4gdmlld1JlZjtcbiAgfVxuXG4gIGNyZWF0ZUNvbXBvbmVudDxDPihcbiAgICAgIGNvbXBvbmVudEZhY3Rvcnk6IENvbXBvbmVudEZhY3Rvcnk8Qz4sIGluZGV4PzogbnVtYmVyLCBpbmplY3Rvcj86IEluamVjdG9yLFxuICAgICAgcHJvamVjdGFibGVOb2Rlcz86IGFueVtdW10sIG5nTW9kdWxlUmVmPzogTmdNb2R1bGVSZWY8YW55Pik6IENvbXBvbmVudFJlZjxDPiB7XG4gICAgY29uc3QgY29udGV4dEluamVjdG9yID0gaW5qZWN0b3IgfHwgdGhpcy5wYXJlbnRJbmplY3RvcjtcbiAgICBpZiAoIW5nTW9kdWxlUmVmICYmICEoY29tcG9uZW50RmFjdG9yeSBpbnN0YW5jZW9mIENvbXBvbmVudEZhY3RvcnlCb3VuZFRvTW9kdWxlKSkge1xuICAgICAgbmdNb2R1bGVSZWYgPSBjb250ZXh0SW5qZWN0b3IuZ2V0KE5nTW9kdWxlUmVmKTtcbiAgICB9XG4gICAgY29uc3QgY29tcG9uZW50UmVmID1cbiAgICAgICAgY29tcG9uZW50RmFjdG9yeS5jcmVhdGUoY29udGV4dEluamVjdG9yLCBwcm9qZWN0YWJsZU5vZGVzLCB1bmRlZmluZWQsIG5nTW9kdWxlUmVmKTtcbiAgICB0aGlzLmluc2VydChjb21wb25lbnRSZWYuaG9zdFZpZXcsIGluZGV4KTtcbiAgICByZXR1cm4gY29tcG9uZW50UmVmO1xuICB9XG5cbiAgaW5zZXJ0KHZpZXdSZWY6IFZpZXdSZWYsIGluZGV4PzogbnVtYmVyKTogVmlld1JlZiB7XG4gICAgaWYgKHZpZXdSZWYuZGVzdHJveWVkKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0Nhbm5vdCBpbnNlcnQgYSBkZXN0cm95ZWQgVmlldyBpbiBhIFZpZXdDb250YWluZXIhJyk7XG4gICAgfVxuICAgIGNvbnN0IHZpZXdSZWZfID0gPFZpZXdSZWZfPnZpZXdSZWY7XG4gICAgY29uc3Qgdmlld0RhdGEgPSB2aWV3UmVmXy5fdmlldztcbiAgICBhdHRhY2hFbWJlZGRlZFZpZXcodGhpcy5fdmlldywgdGhpcy5fZGF0YSwgaW5kZXgsIHZpZXdEYXRhKTtcbiAgICB2aWV3UmVmXy5hdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodGhpcyk7XG4gICAgcmV0dXJuIHZpZXdSZWY7XG4gIH1cblxuICBtb3ZlKHZpZXdSZWY6IFZpZXdSZWZfLCBjdXJyZW50SW5kZXg6IG51bWJlcik6IFZpZXdSZWYge1xuICAgIGlmICh2aWV3UmVmLmRlc3Ryb3llZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdDYW5ub3QgbW92ZSBhIGRlc3Ryb3llZCBWaWV3IGluIGEgVmlld0NvbnRhaW5lciEnKTtcbiAgICB9XG4gICAgY29uc3QgcHJldmlvdXNJbmRleCA9IHRoaXMuX2VtYmVkZGVkVmlld3MuaW5kZXhPZih2aWV3UmVmLl92aWV3KTtcbiAgICBtb3ZlRW1iZWRkZWRWaWV3KHRoaXMuX2RhdGEsIHByZXZpb3VzSW5kZXgsIGN1cnJlbnRJbmRleCk7XG4gICAgcmV0dXJuIHZpZXdSZWY7XG4gIH1cblxuICBpbmRleE9mKHZpZXdSZWY6IFZpZXdSZWYpOiBudW1iZXIge1xuICAgIHJldHVybiB0aGlzLl9lbWJlZGRlZFZpZXdzLmluZGV4T2YoKDxWaWV3UmVmXz52aWV3UmVmKS5fdmlldyk7XG4gIH1cblxuICByZW1vdmUoaW5kZXg/OiBudW1iZXIpOiB2b2lkIHtcbiAgICBjb25zdCB2aWV3RGF0YSA9IGRldGFjaEVtYmVkZGVkVmlldyh0aGlzLl9kYXRhLCBpbmRleCk7XG4gICAgaWYgKHZpZXdEYXRhKSB7XG4gICAgICBTZXJ2aWNlcy5kZXN0cm95Vmlldyh2aWV3RGF0YSk7XG4gICAgfVxuICB9XG5cbiAgZGV0YWNoKGluZGV4PzogbnVtYmVyKTogVmlld1JlZnxudWxsIHtcbiAgICBjb25zdCB2aWV3ID0gZGV0YWNoRW1iZWRkZWRWaWV3KHRoaXMuX2RhdGEsIGluZGV4KTtcbiAgICByZXR1cm4gdmlldyA/IG5ldyBWaWV3UmVmXyh2aWV3KSA6IG51bGw7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZUNoYW5nZURldGVjdG9yUmVmKHZpZXc6IFZpZXdEYXRhKTogQ2hhbmdlRGV0ZWN0b3JSZWYge1xuICByZXR1cm4gbmV3IFZpZXdSZWZfKHZpZXcpO1xufVxuXG5leHBvcnQgY2xhc3MgVmlld1JlZl8gaW1wbGVtZW50cyBFbWJlZGRlZFZpZXdSZWY8YW55PiwgSW50ZXJuYWxWaWV3UmVmIHtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfdmlldzogVmlld0RhdGE7XG4gIHByaXZhdGUgX3ZpZXdDb250YWluZXJSZWY6IFZpZXdDb250YWluZXJSZWZ8bnVsbDtcbiAgcHJpdmF0ZSBfYXBwUmVmOiBBcHBsaWNhdGlvblJlZnxudWxsO1xuXG4gIGNvbnN0cnVjdG9yKF92aWV3OiBWaWV3RGF0YSkge1xuICAgIHRoaXMuX3ZpZXcgPSBfdmlldztcbiAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmID0gbnVsbDtcbiAgICB0aGlzLl9hcHBSZWYgPSBudWxsO1xuICB9XG5cbiAgZ2V0IHJvb3ROb2RlcygpOiBhbnlbXSB7IHJldHVybiByb290UmVuZGVyTm9kZXModGhpcy5fdmlldyk7IH1cblxuICBnZXQgY29udGV4dCgpIHsgcmV0dXJuIHRoaXMuX3ZpZXcuY29udGV4dDsgfVxuXG4gIGdldCBkZXN0cm95ZWQoKTogYm9vbGVhbiB7IHJldHVybiAodGhpcy5fdmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5EZXN0cm95ZWQpICE9PSAwOyB9XG5cbiAgbWFya0ZvckNoZWNrKCk6IHZvaWQgeyBtYXJrUGFyZW50Vmlld3NGb3JDaGVjayh0aGlzLl92aWV3KTsgfVxuICBkZXRhY2goKTogdm9pZCB7IHRoaXMuX3ZpZXcuc3RhdGUgJj0gflZpZXdTdGF0ZS5BdHRhY2hlZDsgfVxuICBkZXRlY3RDaGFuZ2VzKCk6IHZvaWQge1xuICAgIGNvbnN0IGZzID0gdGhpcy5fdmlldy5yb290LnJlbmRlcmVyRmFjdG9yeTtcbiAgICBpZiAoZnMuYmVnaW4pIHtcbiAgICAgIGZzLmJlZ2luKCk7XG4gICAgfVxuICAgIHRyeSB7XG4gICAgICBTZXJ2aWNlcy5jaGVja0FuZFVwZGF0ZVZpZXcodGhpcy5fdmlldyk7XG4gICAgfSBmaW5hbGx5IHtcbiAgICAgIGlmIChmcy5lbmQpIHtcbiAgICAgICAgZnMuZW5kKCk7XG4gICAgICB9XG4gICAgfVxuICB9XG4gIGNoZWNrTm9DaGFuZ2VzKCk6IHZvaWQgeyBTZXJ2aWNlcy5jaGVja05vQ2hhbmdlc1ZpZXcodGhpcy5fdmlldyk7IH1cblxuICByZWF0dGFjaCgpOiB2b2lkIHsgdGhpcy5fdmlldy5zdGF0ZSB8PSBWaWV3U3RhdGUuQXR0YWNoZWQ7IH1cbiAgb25EZXN0cm95KGNhbGxiYWNrOiBGdW5jdGlvbikge1xuICAgIGlmICghdGhpcy5fdmlldy5kaXNwb3NhYmxlcykge1xuICAgICAgdGhpcy5fdmlldy5kaXNwb3NhYmxlcyA9IFtdO1xuICAgIH1cbiAgICB0aGlzLl92aWV3LmRpc3Bvc2FibGVzLnB1c2goPGFueT5jYWxsYmFjayk7XG4gIH1cblxuICBkZXN0cm95KCkge1xuICAgIGlmICh0aGlzLl9hcHBSZWYpIHtcbiAgICAgIHRoaXMuX2FwcFJlZi5kZXRhY2hWaWV3KHRoaXMpO1xuICAgIH0gZWxzZSBpZiAodGhpcy5fdmlld0NvbnRhaW5lclJlZikge1xuICAgICAgdGhpcy5fdmlld0NvbnRhaW5lclJlZi5kZXRhY2godGhpcy5fdmlld0NvbnRhaW5lclJlZi5pbmRleE9mKHRoaXMpKTtcbiAgICB9XG4gICAgU2VydmljZXMuZGVzdHJveVZpZXcodGhpcy5fdmlldyk7XG4gIH1cblxuICBkZXRhY2hGcm9tQXBwUmVmKCkge1xuICAgIHRoaXMuX2FwcFJlZiA9IG51bGw7XG4gICAgcmVuZGVyRGV0YWNoVmlldyh0aGlzLl92aWV3KTtcbiAgICBTZXJ2aWNlcy5kaXJ0eVBhcmVudFF1ZXJpZXModGhpcy5fdmlldyk7XG4gIH1cblxuICBhdHRhY2hUb0FwcFJlZihhcHBSZWY6IEFwcGxpY2F0aW9uUmVmKSB7XG4gICAgaWYgKHRoaXMuX3ZpZXdDb250YWluZXJSZWYpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignVGhpcyB2aWV3IGlzIGFscmVhZHkgYXR0YWNoZWQgdG8gYSBWaWV3Q29udGFpbmVyIScpO1xuICAgIH1cbiAgICB0aGlzLl9hcHBSZWYgPSBhcHBSZWY7XG4gIH1cblxuICBhdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodmNSZWY6IFZpZXdDb250YWluZXJSZWYpIHtcbiAgICBpZiAodGhpcy5fYXBwUmVmKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1RoaXMgdmlldyBpcyBhbHJlYWR5IGF0dGFjaGVkIGRpcmVjdGx5IHRvIHRoZSBBcHBsaWNhdGlvblJlZiEnKTtcbiAgICB9XG4gICAgdGhpcy5fdmlld0NvbnRhaW5lclJlZiA9IHZjUmVmO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVUZW1wbGF0ZURhdGEodmlldzogVmlld0RhdGEsIGRlZjogTm9kZURlZik6IFRlbXBsYXRlRGF0YSB7XG4gIHJldHVybiBuZXcgVGVtcGxhdGVSZWZfKHZpZXcsIGRlZik7XG59XG5cbmNsYXNzIFRlbXBsYXRlUmVmXyBleHRlbmRzIFRlbXBsYXRlUmVmPGFueT4gaW1wbGVtZW50cyBUZW1wbGF0ZURhdGEge1xuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgX3Byb2plY3RlZFZpZXdzICE6IFZpZXdEYXRhW107XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfcGFyZW50VmlldzogVmlld0RhdGEsIHByaXZhdGUgX2RlZjogTm9kZURlZikgeyBzdXBlcigpOyB9XG5cbiAgY3JlYXRlRW1iZWRkZWRWaWV3KGNvbnRleHQ6IGFueSk6IEVtYmVkZGVkVmlld1JlZjxhbnk+IHtcbiAgICByZXR1cm4gbmV3IFZpZXdSZWZfKFNlcnZpY2VzLmNyZWF0ZUVtYmVkZGVkVmlldyhcbiAgICAgICAgdGhpcy5fcGFyZW50VmlldywgdGhpcy5fZGVmLCB0aGlzLl9kZWYuZWxlbWVudCAhLnRlbXBsYXRlICEsIGNvbnRleHQpKTtcbiAgfVxuXG4gIGdldCBlbGVtZW50UmVmKCk6IEVsZW1lbnRSZWYge1xuICAgIHJldHVybiBuZXcgRWxlbWVudFJlZihhc0VsZW1lbnREYXRhKHRoaXMuX3BhcmVudFZpZXcsIHRoaXMuX2RlZi5ub2RlSW5kZXgpLnJlbmRlckVsZW1lbnQpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVJbmplY3Rvcih2aWV3OiBWaWV3RGF0YSwgZWxEZWY6IE5vZGVEZWYpOiBJbmplY3RvciB7XG4gIHJldHVybiBuZXcgSW5qZWN0b3JfKHZpZXcsIGVsRGVmKTtcbn1cblxuY2xhc3MgSW5qZWN0b3JfIGltcGxlbWVudHMgSW5qZWN0b3Ige1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHZpZXc6IFZpZXdEYXRhLCBwcml2YXRlIGVsRGVmOiBOb2RlRGVmfG51bGwpIHt9XG4gIGdldCh0b2tlbjogYW55LCBub3RGb3VuZFZhbHVlOiBhbnkgPSBJbmplY3Rvci5USFJPV19JRl9OT1RfRk9VTkQpOiBhbnkge1xuICAgIGNvbnN0IGFsbG93UHJpdmF0ZVNlcnZpY2VzID1cbiAgICAgICAgdGhpcy5lbERlZiA/ICh0aGlzLmVsRGVmLmZsYWdzICYgTm9kZUZsYWdzLkNvbXBvbmVudFZpZXcpICE9PSAwIDogZmFsc2U7XG4gICAgcmV0dXJuIFNlcnZpY2VzLnJlc29sdmVEZXAoXG4gICAgICAgIHRoaXMudmlldywgdGhpcy5lbERlZiwgYWxsb3dQcml2YXRlU2VydmljZXMsXG4gICAgICAgIHtmbGFnczogRGVwRmxhZ3MuTm9uZSwgdG9rZW4sIHRva2VuS2V5OiB0b2tlbktleSh0b2tlbil9LCBub3RGb3VuZFZhbHVlKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gbm9kZVZhbHVlKHZpZXc6IFZpZXdEYXRhLCBpbmRleDogbnVtYmVyKTogYW55IHtcbiAgY29uc3QgZGVmID0gdmlldy5kZWYubm9kZXNbaW5kZXhdO1xuICBpZiAoZGVmLmZsYWdzICYgTm9kZUZsYWdzLlR5cGVFbGVtZW50KSB7XG4gICAgY29uc3QgZWxEYXRhID0gYXNFbGVtZW50RGF0YSh2aWV3LCBkZWYubm9kZUluZGV4KTtcbiAgICByZXR1cm4gZGVmLmVsZW1lbnQgIS50ZW1wbGF0ZSA/IGVsRGF0YS50ZW1wbGF0ZSA6IGVsRGF0YS5yZW5kZXJFbGVtZW50O1xuICB9IGVsc2UgaWYgKGRlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlVGV4dCkge1xuICAgIHJldHVybiBhc1RleHREYXRhKHZpZXcsIGRlZi5ub2RlSW5kZXgpLnJlbmRlclRleHQ7XG4gIH0gZWxzZSBpZiAoZGVmLmZsYWdzICYgKE5vZGVGbGFncy5DYXRQcm92aWRlciB8IE5vZGVGbGFncy5UeXBlUGlwZSkpIHtcbiAgICByZXR1cm4gYXNQcm92aWRlckRhdGEodmlldywgZGVmLm5vZGVJbmRleCkuaW5zdGFuY2U7XG4gIH1cbiAgdGhyb3cgbmV3IEVycm9yKGBJbGxlZ2FsIHN0YXRlOiByZWFkIG5vZGVWYWx1ZSBmb3Igbm9kZSBpbmRleCAke2luZGV4fWApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlUmVuZGVyZXJWMSh2aWV3OiBWaWV3RGF0YSk6IFJlbmRlcmVyVjEge1xuICByZXR1cm4gbmV3IFJlbmRlcmVyQWRhcHRlcih2aWV3LnJlbmRlcmVyKTtcbn1cblxuY2xhc3MgUmVuZGVyZXJBZGFwdGVyIGltcGxlbWVudHMgUmVuZGVyZXJWMSB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZGVsZWdhdGU6IFJlbmRlcmVyMikge31cbiAgc2VsZWN0Um9vdEVsZW1lbnQoc2VsZWN0b3JPck5vZGU6IHN0cmluZ3xFbGVtZW50KTogRWxlbWVudCB7XG4gICAgcmV0dXJuIHRoaXMuZGVsZWdhdGUuc2VsZWN0Um9vdEVsZW1lbnQoc2VsZWN0b3JPck5vZGUpO1xuICB9XG5cbiAgY3JlYXRlRWxlbWVudChwYXJlbnQ6IEVsZW1lbnR8RG9jdW1lbnRGcmFnbWVudCwgbmFtZXNwYWNlQW5kTmFtZTogc3RyaW5nKTogRWxlbWVudCB7XG4gICAgY29uc3QgW25zLCBuYW1lXSA9IHNwbGl0TmFtZXNwYWNlKG5hbWVzcGFjZUFuZE5hbWUpO1xuICAgIGNvbnN0IGVsID0gdGhpcy5kZWxlZ2F0ZS5jcmVhdGVFbGVtZW50KG5hbWUsIG5zKTtcbiAgICBpZiAocGFyZW50KSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLmFwcGVuZENoaWxkKHBhcmVudCwgZWwpO1xuICAgIH1cbiAgICByZXR1cm4gZWw7XG4gIH1cblxuICBjcmVhdGVWaWV3Um9vdChob3N0RWxlbWVudDogRWxlbWVudCk6IEVsZW1lbnR8RG9jdW1lbnRGcmFnbWVudCB7IHJldHVybiBob3N0RWxlbWVudDsgfVxuXG4gIGNyZWF0ZVRlbXBsYXRlQW5jaG9yKHBhcmVudEVsZW1lbnQ6IEVsZW1lbnR8RG9jdW1lbnRGcmFnbWVudCk6IENvbW1lbnQge1xuICAgIGNvbnN0IGNvbW1lbnQgPSB0aGlzLmRlbGVnYXRlLmNyZWF0ZUNvbW1lbnQoJycpO1xuICAgIGlmIChwYXJlbnRFbGVtZW50KSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLmFwcGVuZENoaWxkKHBhcmVudEVsZW1lbnQsIGNvbW1lbnQpO1xuICAgIH1cbiAgICByZXR1cm4gY29tbWVudDtcbiAgfVxuXG4gIGNyZWF0ZVRleHQocGFyZW50RWxlbWVudDogRWxlbWVudHxEb2N1bWVudEZyYWdtZW50LCB2YWx1ZTogc3RyaW5nKTogYW55IHtcbiAgICBjb25zdCBub2RlID0gdGhpcy5kZWxlZ2F0ZS5jcmVhdGVUZXh0KHZhbHVlKTtcbiAgICBpZiAocGFyZW50RWxlbWVudCkge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5hcHBlbmRDaGlsZChwYXJlbnRFbGVtZW50LCBub2RlKTtcbiAgICB9XG4gICAgcmV0dXJuIG5vZGU7XG4gIH1cblxuICBwcm9qZWN0Tm9kZXMocGFyZW50RWxlbWVudDogRWxlbWVudHxEb2N1bWVudEZyYWdtZW50LCBub2RlczogTm9kZVtdKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBub2Rlcy5sZW5ndGg7IGkrKykge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5hcHBlbmRDaGlsZChwYXJlbnRFbGVtZW50LCBub2Rlc1tpXSk7XG4gICAgfVxuICB9XG5cbiAgYXR0YWNoVmlld0FmdGVyKG5vZGU6IE5vZGUsIHZpZXdSb290Tm9kZXM6IE5vZGVbXSkge1xuICAgIGNvbnN0IHBhcmVudEVsZW1lbnQgPSB0aGlzLmRlbGVnYXRlLnBhcmVudE5vZGUobm9kZSk7XG4gICAgY29uc3QgbmV4dFNpYmxpbmcgPSB0aGlzLmRlbGVnYXRlLm5leHRTaWJsaW5nKG5vZGUpO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdmlld1Jvb3ROb2Rlcy5sZW5ndGg7IGkrKykge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5pbnNlcnRCZWZvcmUocGFyZW50RWxlbWVudCwgdmlld1Jvb3ROb2Rlc1tpXSwgbmV4dFNpYmxpbmcpO1xuICAgIH1cbiAgfVxuXG4gIGRldGFjaFZpZXcodmlld1Jvb3ROb2RlczogKEVsZW1lbnR8VGV4dHxDb21tZW50KVtdKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB2aWV3Um9vdE5vZGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBub2RlID0gdmlld1Jvb3ROb2Rlc1tpXTtcbiAgICAgIGNvbnN0IHBhcmVudEVsZW1lbnQgPSB0aGlzLmRlbGVnYXRlLnBhcmVudE5vZGUobm9kZSk7XG4gICAgICB0aGlzLmRlbGVnYXRlLnJlbW92ZUNoaWxkKHBhcmVudEVsZW1lbnQsIG5vZGUpO1xuICAgIH1cbiAgfVxuXG4gIGRlc3Ryb3lWaWV3KGhvc3RFbGVtZW50OiBFbGVtZW50fERvY3VtZW50RnJhZ21lbnQsIHZpZXdBbGxOb2RlczogTm9kZVtdKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB2aWV3QWxsTm9kZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHRoaXMuZGVsZWdhdGUuZGVzdHJveU5vZGUgISh2aWV3QWxsTm9kZXNbaV0pO1xuICAgIH1cbiAgfVxuXG4gIGxpc3RlbihyZW5kZXJFbGVtZW50OiBhbnksIG5hbWU6IHN0cmluZywgY2FsbGJhY2s6IEZ1bmN0aW9uKTogRnVuY3Rpb24ge1xuICAgIHJldHVybiB0aGlzLmRlbGVnYXRlLmxpc3RlbihyZW5kZXJFbGVtZW50LCBuYW1lLCA8YW55PmNhbGxiYWNrKTtcbiAgfVxuXG4gIGxpc3Rlbkdsb2JhbCh0YXJnZXQ6IHN0cmluZywgbmFtZTogc3RyaW5nLCBjYWxsYmFjazogRnVuY3Rpb24pOiBGdW5jdGlvbiB7XG4gICAgcmV0dXJuIHRoaXMuZGVsZWdhdGUubGlzdGVuKHRhcmdldCwgbmFtZSwgPGFueT5jYWxsYmFjayk7XG4gIH1cblxuICBzZXRFbGVtZW50UHJvcGVydHkoXG4gICAgICByZW5kZXJFbGVtZW50OiBFbGVtZW50fERvY3VtZW50RnJhZ21lbnQsIHByb3BlcnR5TmFtZTogc3RyaW5nLCBwcm9wZXJ0eVZhbHVlOiBhbnkpOiB2b2lkIHtcbiAgICB0aGlzLmRlbGVnYXRlLnNldFByb3BlcnR5KHJlbmRlckVsZW1lbnQsIHByb3BlcnR5TmFtZSwgcHJvcGVydHlWYWx1ZSk7XG4gIH1cblxuICBzZXRFbGVtZW50QXR0cmlidXRlKHJlbmRlckVsZW1lbnQ6IEVsZW1lbnQsIG5hbWVzcGFjZUFuZE5hbWU6IHN0cmluZywgYXR0cmlidXRlVmFsdWU/OiBzdHJpbmcpOlxuICAgICAgdm9pZCB7XG4gICAgY29uc3QgW25zLCBuYW1lXSA9IHNwbGl0TmFtZXNwYWNlKG5hbWVzcGFjZUFuZE5hbWUpO1xuICAgIGlmIChhdHRyaWJ1dGVWYWx1ZSAhPSBudWxsKSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLnNldEF0dHJpYnV0ZShyZW5kZXJFbGVtZW50LCBuYW1lLCBhdHRyaWJ1dGVWYWx1ZSwgbnMpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLnJlbW92ZUF0dHJpYnV0ZShyZW5kZXJFbGVtZW50LCBuYW1lLCBucyk7XG4gICAgfVxuICB9XG5cbiAgc2V0QmluZGluZ0RlYnVnSW5mbyhyZW5kZXJFbGVtZW50OiBFbGVtZW50LCBwcm9wZXJ0eU5hbWU6IHN0cmluZywgcHJvcGVydHlWYWx1ZTogc3RyaW5nKTogdm9pZCB7fVxuXG4gIHNldEVsZW1lbnRDbGFzcyhyZW5kZXJFbGVtZW50OiBFbGVtZW50LCBjbGFzc05hbWU6IHN0cmluZywgaXNBZGQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAoaXNBZGQpIHtcbiAgICAgIHRoaXMuZGVsZWdhdGUuYWRkQ2xhc3MocmVuZGVyRWxlbWVudCwgY2xhc3NOYW1lKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5yZW1vdmVDbGFzcyhyZW5kZXJFbGVtZW50LCBjbGFzc05hbWUpO1xuICAgIH1cbiAgfVxuXG4gIHNldEVsZW1lbnRTdHlsZShyZW5kZXJFbGVtZW50OiBIVE1MRWxlbWVudCwgc3R5bGVOYW1lOiBzdHJpbmcsIHN0eWxlVmFsdWU/OiBzdHJpbmcpOiB2b2lkIHtcbiAgICBpZiAoc3R5bGVWYWx1ZSAhPSBudWxsKSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLnNldFN0eWxlKHJlbmRlckVsZW1lbnQsIHN0eWxlTmFtZSwgc3R5bGVWYWx1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuZGVsZWdhdGUucmVtb3ZlU3R5bGUocmVuZGVyRWxlbWVudCwgc3R5bGVOYW1lKTtcbiAgICB9XG4gIH1cblxuICBpbnZva2VFbGVtZW50TWV0aG9kKHJlbmRlckVsZW1lbnQ6IEVsZW1lbnQsIG1ldGhvZE5hbWU6IHN0cmluZywgYXJnczogYW55W10pOiB2b2lkIHtcbiAgICAocmVuZGVyRWxlbWVudCBhcyBhbnkpW21ldGhvZE5hbWVdLmFwcGx5KHJlbmRlckVsZW1lbnQsIGFyZ3MpO1xuICB9XG5cbiAgc2V0VGV4dChyZW5kZXJOb2RlOiBUZXh0LCB0ZXh0OiBzdHJpbmcpOiB2b2lkIHsgdGhpcy5kZWxlZ2F0ZS5zZXRWYWx1ZShyZW5kZXJOb2RlLCB0ZXh0KTsgfVxuXG4gIGFuaW1hdGUoKTogYW55IHsgdGhyb3cgbmV3IEVycm9yKCdSZW5kZXJlci5hbmltYXRlIGlzIG5vIGxvbmdlciBzdXBwb3J0ZWQhJyk7IH1cbn1cblxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlTmdNb2R1bGVSZWYoXG4gICAgbW9kdWxlVHlwZTogVHlwZTxhbnk+LCBwYXJlbnQ6IEluamVjdG9yLCBib290c3RyYXBDb21wb25lbnRzOiBUeXBlPGFueT5bXSxcbiAgICBkZWY6IE5nTW9kdWxlRGVmaW5pdGlvbik6IE5nTW9kdWxlUmVmPGFueT4ge1xuICByZXR1cm4gbmV3IE5nTW9kdWxlUmVmXyhtb2R1bGVUeXBlLCBwYXJlbnQsIGJvb3RzdHJhcENvbXBvbmVudHMsIGRlZik7XG59XG5cbmNsYXNzIE5nTW9kdWxlUmVmXyBpbXBsZW1lbnRzIE5nTW9kdWxlRGF0YSwgSW50ZXJuYWxOZ01vZHVsZVJlZjxhbnk+IHtcbiAgcHJpdmF0ZSBfZGVzdHJveUxpc3RlbmVyczogKCgpID0+IHZvaWQpW10gPSBbXTtcbiAgcHJpdmF0ZSBfZGVzdHJveWVkOiBib29sZWFuID0gZmFsc2U7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9wcm92aWRlcnMgITogYW55W107XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9tb2R1bGVzICE6IGFueVtdO1xuXG4gIHJlYWRvbmx5IGluamVjdG9yOiBJbmplY3RvciA9IHRoaXM7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9tb2R1bGVUeXBlOiBUeXBlPGFueT4sIHB1YmxpYyBfcGFyZW50OiBJbmplY3RvcixcbiAgICAgIHB1YmxpYyBfYm9vdHN0cmFwQ29tcG9uZW50czogVHlwZTxhbnk+W10sIHB1YmxpYyBfZGVmOiBOZ01vZHVsZURlZmluaXRpb24pIHtcbiAgICBpbml0TmdNb2R1bGUodGhpcyk7XG4gIH1cblxuICBnZXQodG9rZW46IGFueSwgbm90Rm91bmRWYWx1ZTogYW55ID0gSW5qZWN0b3IuVEhST1dfSUZfTk9UX0ZPVU5ELFxuICAgICAgaW5qZWN0RmxhZ3M6IEluamVjdEZsYWdzID0gSW5qZWN0RmxhZ3MuRGVmYXVsdCk6IGFueSB7XG4gICAgbGV0IGZsYWdzID0gRGVwRmxhZ3MuTm9uZTtcbiAgICBpZiAoaW5qZWN0RmxhZ3MgJiBJbmplY3RGbGFncy5Ta2lwU2VsZikge1xuICAgICAgZmxhZ3MgfD0gRGVwRmxhZ3MuU2tpcFNlbGY7XG4gICAgfSBlbHNlIGlmIChpbmplY3RGbGFncyAmIEluamVjdEZsYWdzLlNlbGYpIHtcbiAgICAgIGZsYWdzIHw9IERlcEZsYWdzLlNlbGY7XG4gICAgfVxuICAgIHJldHVybiByZXNvbHZlTmdNb2R1bGVEZXAoXG4gICAgICAgIHRoaXMsIHt0b2tlbjogdG9rZW4sIHRva2VuS2V5OiB0b2tlbktleSh0b2tlbiksIGZsYWdzOiBmbGFnc30sIG5vdEZvdW5kVmFsdWUpO1xuICB9XG5cbiAgZ2V0IGluc3RhbmNlKCkgeyByZXR1cm4gdGhpcy5nZXQodGhpcy5fbW9kdWxlVHlwZSk7IH1cblxuICBnZXQgY29tcG9uZW50RmFjdG9yeVJlc29sdmVyKCkgeyByZXR1cm4gdGhpcy5nZXQoQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyKTsgfVxuXG4gIGRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2Rlc3Ryb3llZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBUaGUgbmcgbW9kdWxlICR7c3RyaW5naWZ5KHRoaXMuaW5zdGFuY2UuY29uc3RydWN0b3IpfSBoYXMgYWxyZWFkeSBiZWVuIGRlc3Ryb3llZC5gKTtcbiAgICB9XG4gICAgdGhpcy5fZGVzdHJveWVkID0gdHJ1ZTtcbiAgICBjYWxsTmdNb2R1bGVMaWZlY3ljbGUodGhpcywgTm9kZUZsYWdzLk9uRGVzdHJveSk7XG4gICAgdGhpcy5fZGVzdHJveUxpc3RlbmVycy5mb3JFYWNoKChsaXN0ZW5lcikgPT4gbGlzdGVuZXIoKSk7XG4gIH1cblxuICBvbkRlc3Ryb3koY2FsbGJhY2s6ICgpID0+IHZvaWQpOiB2b2lkIHsgdGhpcy5fZGVzdHJveUxpc3RlbmVycy5wdXNoKGNhbGxiYWNrKTsgfVxufVxuIl19