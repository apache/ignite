/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
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
/** @type {?} */
const EMPTY_CONTEXT = new Object();
// Attention: this function is called as top level function.
// Putting any logic in here will destroy closure tree shaking!
/**
 * @param {?} selector
 * @param {?} componentType
 * @param {?} viewDefFactory
 * @param {?} inputs
 * @param {?} outputs
 * @param {?} ngContentSelectors
 * @return {?}
 */
export function createComponentFactory(selector, componentType, viewDefFactory, inputs, outputs, ngContentSelectors) {
    return new ComponentFactory_(selector, componentType, viewDefFactory, inputs, outputs, ngContentSelectors);
}
/**
 * @param {?} componentFactory
 * @return {?}
 */
export function getComponentViewDefinitionFactory(componentFactory) {
    return ((/** @type {?} */ (componentFactory))).viewDefFactory;
}
class ComponentFactory_ extends ComponentFactory {
    /**
     * @param {?} selector
     * @param {?} componentType
     * @param {?} viewDefFactory
     * @param {?} _inputs
     * @param {?} _outputs
     * @param {?} ngContentSelectors
     */
    constructor(selector, componentType, viewDefFactory, _inputs, _outputs, ngContentSelectors) {
        // Attention: this ctor is called as top level function.
        // Putting any logic in here will destroy closure tree shaking!
        super();
        this.selector = selector;
        this.componentType = componentType;
        this._inputs = _inputs;
        this._outputs = _outputs;
        this.ngContentSelectors = ngContentSelectors;
        this.viewDefFactory = viewDefFactory;
    }
    /**
     * @return {?}
     */
    get inputs() {
        /** @type {?} */
        const inputsArr = [];
        /** @type {?} */
        const inputs = (/** @type {?} */ (this._inputs));
        for (let propName in inputs) {
            /** @type {?} */
            const templateName = inputs[propName];
            inputsArr.push({ propName, templateName });
        }
        return inputsArr;
    }
    /**
     * @return {?}
     */
    get outputs() {
        /** @type {?} */
        const outputsArr = [];
        for (let propName in this._outputs) {
            /** @type {?} */
            const templateName = this._outputs[propName];
            outputsArr.push({ propName, templateName });
        }
        return outputsArr;
    }
    /**
     * Creates a new component.
     * @param {?} injector
     * @param {?=} projectableNodes
     * @param {?=} rootSelectorOrNode
     * @param {?=} ngModule
     * @return {?}
     */
    create(injector, projectableNodes, rootSelectorOrNode, ngModule) {
        if (!ngModule) {
            throw new Error('ngModule should be provided');
        }
        /** @type {?} */
        const viewDef = resolveDefinition(this.viewDefFactory);
        /** @type {?} */
        const componentNodeIndex = (/** @type {?} */ ((/** @type {?} */ (viewDef.nodes[0].element)).componentProvider)).nodeIndex;
        /** @type {?} */
        const view = Services.createRootView(injector, projectableNodes || [], rootSelectorOrNode, viewDef, ngModule, EMPTY_CONTEXT);
        /** @type {?} */
        const component = asProviderData(view, componentNodeIndex).instance;
        if (rootSelectorOrNode) {
            view.renderer.setAttribute(asElementData(view, 0).renderElement, 'ng-version', VERSION.full);
        }
        return new ComponentRef_(view, new ViewRef_(view), component);
    }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    ComponentFactory_.prototype.viewDefFactory;
    /** @type {?} */
    ComponentFactory_.prototype.selector;
    /** @type {?} */
    ComponentFactory_.prototype.componentType;
    /**
     * @type {?}
     * @private
     */
    ComponentFactory_.prototype._inputs;
    /**
     * @type {?}
     * @private
     */
    ComponentFactory_.prototype._outputs;
    /** @type {?} */
    ComponentFactory_.prototype.ngContentSelectors;
}
class ComponentRef_ extends ComponentRef {
    /**
     * @param {?} _view
     * @param {?} _viewRef
     * @param {?} _component
     */
    constructor(_view, _viewRef, _component) {
        super();
        this._view = _view;
        this._viewRef = _viewRef;
        this._component = _component;
        this._elDef = this._view.def.nodes[0];
        this.hostView = _viewRef;
        this.changeDetectorRef = _viewRef;
        this.instance = _component;
    }
    /**
     * @return {?}
     */
    get location() {
        return new ElementRef(asElementData(this._view, this._elDef.nodeIndex).renderElement);
    }
    /**
     * @return {?}
     */
    get injector() { return new Injector_(this._view, this._elDef); }
    /**
     * @return {?}
     */
    get componentType() { return (/** @type {?} */ (this._component.constructor)); }
    /**
     * @return {?}
     */
    destroy() { this._viewRef.destroy(); }
    /**
     * @param {?} callback
     * @return {?}
     */
    onDestroy(callback) { this._viewRef.onDestroy(callback); }
}
if (false) {
    /** @type {?} */
    ComponentRef_.prototype.hostView;
    /** @type {?} */
    ComponentRef_.prototype.instance;
    /** @type {?} */
    ComponentRef_.prototype.changeDetectorRef;
    /**
     * @type {?}
     * @private
     */
    ComponentRef_.prototype._elDef;
    /**
     * @type {?}
     * @private
     */
    ComponentRef_.prototype._view;
    /**
     * @type {?}
     * @private
     */
    ComponentRef_.prototype._viewRef;
    /**
     * @type {?}
     * @private
     */
    ComponentRef_.prototype._component;
}
/**
 * @param {?} view
 * @param {?} elDef
 * @param {?} elData
 * @return {?}
 */
export function createViewContainerData(view, elDef, elData) {
    return new ViewContainerRef_(view, elDef, elData);
}
class ViewContainerRef_ {
    /**
     * @param {?} _view
     * @param {?} _elDef
     * @param {?} _data
     */
    constructor(_view, _elDef, _data) {
        this._view = _view;
        this._elDef = _elDef;
        this._data = _data;
        /**
         * \@internal
         */
        this._embeddedViews = [];
    }
    /**
     * @return {?}
     */
    get element() { return new ElementRef(this._data.renderElement); }
    /**
     * @return {?}
     */
    get injector() { return new Injector_(this._view, this._elDef); }
    /**
     * @deprecated No replacement
     * @return {?}
     */
    get parentInjector() {
        /** @type {?} */
        let view = this._view;
        /** @type {?} */
        let elDef = this._elDef.parent;
        while (!elDef && view) {
            elDef = viewParentEl(view);
            view = (/** @type {?} */ (view.parent));
        }
        return view ? new Injector_(view, elDef) : new Injector_(this._view, null);
    }
    /**
     * @return {?}
     */
    clear() {
        /** @type {?} */
        const len = this._embeddedViews.length;
        for (let i = len - 1; i >= 0; i--) {
            /** @type {?} */
            const view = (/** @type {?} */ (detachEmbeddedView(this._data, i)));
            Services.destroyView(view);
        }
    }
    /**
     * @param {?} index
     * @return {?}
     */
    get(index) {
        /** @type {?} */
        const view = this._embeddedViews[index];
        if (view) {
            /** @type {?} */
            const ref = new ViewRef_(view);
            ref.attachToViewContainerRef(this);
            return ref;
        }
        return null;
    }
    /**
     * @return {?}
     */
    get length() { return this._embeddedViews.length; }
    /**
     * @template C
     * @param {?} templateRef
     * @param {?=} context
     * @param {?=} index
     * @return {?}
     */
    createEmbeddedView(templateRef, context, index) {
        /** @type {?} */
        const viewRef = templateRef.createEmbeddedView(context || (/** @type {?} */ ({})));
        this.insert(viewRef, index);
        return viewRef;
    }
    /**
     * @template C
     * @param {?} componentFactory
     * @param {?=} index
     * @param {?=} injector
     * @param {?=} projectableNodes
     * @param {?=} ngModuleRef
     * @return {?}
     */
    createComponent(componentFactory, index, injector, projectableNodes, ngModuleRef) {
        /** @type {?} */
        const contextInjector = injector || this.parentInjector;
        if (!ngModuleRef && !(componentFactory instanceof ComponentFactoryBoundToModule)) {
            ngModuleRef = contextInjector.get(NgModuleRef);
        }
        /** @type {?} */
        const componentRef = componentFactory.create(contextInjector, projectableNodes, undefined, ngModuleRef);
        this.insert(componentRef.hostView, index);
        return componentRef;
    }
    /**
     * @param {?} viewRef
     * @param {?=} index
     * @return {?}
     */
    insert(viewRef, index) {
        if (viewRef.destroyed) {
            throw new Error('Cannot insert a destroyed View in a ViewContainer!');
        }
        /** @type {?} */
        const viewRef_ = (/** @type {?} */ (viewRef));
        /** @type {?} */
        const viewData = viewRef_._view;
        attachEmbeddedView(this._view, this._data, index, viewData);
        viewRef_.attachToViewContainerRef(this);
        return viewRef;
    }
    /**
     * @param {?} viewRef
     * @param {?} currentIndex
     * @return {?}
     */
    move(viewRef, currentIndex) {
        if (viewRef.destroyed) {
            throw new Error('Cannot move a destroyed View in a ViewContainer!');
        }
        /** @type {?} */
        const previousIndex = this._embeddedViews.indexOf(viewRef._view);
        moveEmbeddedView(this._data, previousIndex, currentIndex);
        return viewRef;
    }
    /**
     * @param {?} viewRef
     * @return {?}
     */
    indexOf(viewRef) {
        return this._embeddedViews.indexOf(((/** @type {?} */ (viewRef)))._view);
    }
    /**
     * @param {?=} index
     * @return {?}
     */
    remove(index) {
        /** @type {?} */
        const viewData = detachEmbeddedView(this._data, index);
        if (viewData) {
            Services.destroyView(viewData);
        }
    }
    /**
     * @param {?=} index
     * @return {?}
     */
    detach(index) {
        /** @type {?} */
        const view = detachEmbeddedView(this._data, index);
        return view ? new ViewRef_(view) : null;
    }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    ViewContainerRef_.prototype._embeddedViews;
    /**
     * @type {?}
     * @private
     */
    ViewContainerRef_.prototype._view;
    /**
     * @type {?}
     * @private
     */
    ViewContainerRef_.prototype._elDef;
    /**
     * @type {?}
     * @private
     */
    ViewContainerRef_.prototype._data;
}
/**
 * @param {?} view
 * @return {?}
 */
export function createChangeDetectorRef(view) {
    return new ViewRef_(view);
}
export class ViewRef_ {
    /**
     * @param {?} _view
     */
    constructor(_view) {
        this._view = _view;
        this._viewContainerRef = null;
        this._appRef = null;
    }
    /**
     * @return {?}
     */
    get rootNodes() { return rootRenderNodes(this._view); }
    /**
     * @return {?}
     */
    get context() { return this._view.context; }
    /**
     * @return {?}
     */
    get destroyed() { return (this._view.state & 128 /* Destroyed */) !== 0; }
    /**
     * @return {?}
     */
    markForCheck() { markParentViewsForCheck(this._view); }
    /**
     * @return {?}
     */
    detach() { this._view.state &= ~4 /* Attached */; }
    /**
     * @return {?}
     */
    detectChanges() {
        /** @type {?} */
        const fs = this._view.root.rendererFactory;
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
    }
    /**
     * @return {?}
     */
    checkNoChanges() { Services.checkNoChangesView(this._view); }
    /**
     * @return {?}
     */
    reattach() { this._view.state |= 4 /* Attached */; }
    /**
     * @param {?} callback
     * @return {?}
     */
    onDestroy(callback) {
        if (!this._view.disposables) {
            this._view.disposables = [];
        }
        this._view.disposables.push((/** @type {?} */ (callback)));
    }
    /**
     * @return {?}
     */
    destroy() {
        if (this._appRef) {
            this._appRef.detachView(this);
        }
        else if (this._viewContainerRef) {
            this._viewContainerRef.detach(this._viewContainerRef.indexOf(this));
        }
        Services.destroyView(this._view);
    }
    /**
     * @return {?}
     */
    detachFromAppRef() {
        this._appRef = null;
        renderDetachView(this._view);
        Services.dirtyParentQueries(this._view);
    }
    /**
     * @param {?} appRef
     * @return {?}
     */
    attachToAppRef(appRef) {
        if (this._viewContainerRef) {
            throw new Error('This view is already attached to a ViewContainer!');
        }
        this._appRef = appRef;
    }
    /**
     * @param {?} vcRef
     * @return {?}
     */
    attachToViewContainerRef(vcRef) {
        if (this._appRef) {
            throw new Error('This view is already attached directly to the ApplicationRef!');
        }
        this._viewContainerRef = vcRef;
    }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    ViewRef_.prototype._view;
    /**
     * @type {?}
     * @private
     */
    ViewRef_.prototype._viewContainerRef;
    /**
     * @type {?}
     * @private
     */
    ViewRef_.prototype._appRef;
}
/**
 * @param {?} view
 * @param {?} def
 * @return {?}
 */
export function createTemplateData(view, def) {
    return new TemplateRef_(view, def);
}
class TemplateRef_ extends TemplateRef {
    /**
     * @param {?} _parentView
     * @param {?} _def
     */
    constructor(_parentView, _def) {
        super();
        this._parentView = _parentView;
        this._def = _def;
    }
    /**
     * @param {?} context
     * @return {?}
     */
    createEmbeddedView(context) {
        return new ViewRef_(Services.createEmbeddedView(this._parentView, this._def, (/** @type {?} */ ((/** @type {?} */ (this._def.element)).template)), context));
    }
    /**
     * @return {?}
     */
    get elementRef() {
        return new ElementRef(asElementData(this._parentView, this._def.nodeIndex).renderElement);
    }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    TemplateRef_.prototype._projectedViews;
    /**
     * @type {?}
     * @private
     */
    TemplateRef_.prototype._parentView;
    /**
     * @type {?}
     * @private
     */
    TemplateRef_.prototype._def;
}
/**
 * @param {?} view
 * @param {?} elDef
 * @return {?}
 */
export function createInjector(view, elDef) {
    return new Injector_(view, elDef);
}
class Injector_ {
    /**
     * @param {?} view
     * @param {?} elDef
     */
    constructor(view, elDef) {
        this.view = view;
        this.elDef = elDef;
    }
    /**
     * @param {?} token
     * @param {?=} notFoundValue
     * @return {?}
     */
    get(token, notFoundValue = Injector.THROW_IF_NOT_FOUND) {
        /** @type {?} */
        const allowPrivateServices = this.elDef ? (this.elDef.flags & 33554432 /* ComponentView */) !== 0 : false;
        return Services.resolveDep(this.view, this.elDef, allowPrivateServices, { flags: 0 /* None */, token, tokenKey: tokenKey(token) }, notFoundValue);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    Injector_.prototype.view;
    /**
     * @type {?}
     * @private
     */
    Injector_.prototype.elDef;
}
/**
 * @param {?} view
 * @param {?} index
 * @return {?}
 */
export function nodeValue(view, index) {
    /** @type {?} */
    const def = view.def.nodes[index];
    if (def.flags & 1 /* TypeElement */) {
        /** @type {?} */
        const elData = asElementData(view, def.nodeIndex);
        return (/** @type {?} */ (def.element)).template ? elData.template : elData.renderElement;
    }
    else if (def.flags & 2 /* TypeText */) {
        return asTextData(view, def.nodeIndex).renderText;
    }
    else if (def.flags & (20224 /* CatProvider */ | 16 /* TypePipe */)) {
        return asProviderData(view, def.nodeIndex).instance;
    }
    throw new Error(`Illegal state: read nodeValue for node index ${index}`);
}
/**
 * @param {?} view
 * @return {?}
 */
export function createRendererV1(view) {
    return new RendererAdapter(view.renderer);
}
class RendererAdapter {
    /**
     * @param {?} delegate
     */
    constructor(delegate) {
        this.delegate = delegate;
    }
    /**
     * @param {?} selectorOrNode
     * @return {?}
     */
    selectRootElement(selectorOrNode) {
        return this.delegate.selectRootElement(selectorOrNode);
    }
    /**
     * @param {?} parent
     * @param {?} namespaceAndName
     * @return {?}
     */
    createElement(parent, namespaceAndName) {
        const [ns, name] = splitNamespace(namespaceAndName);
        /** @type {?} */
        const el = this.delegate.createElement(name, ns);
        if (parent) {
            this.delegate.appendChild(parent, el);
        }
        return el;
    }
    /**
     * @param {?} hostElement
     * @return {?}
     */
    createViewRoot(hostElement) { return hostElement; }
    /**
     * @param {?} parentElement
     * @return {?}
     */
    createTemplateAnchor(parentElement) {
        /** @type {?} */
        const comment = this.delegate.createComment('');
        if (parentElement) {
            this.delegate.appendChild(parentElement, comment);
        }
        return comment;
    }
    /**
     * @param {?} parentElement
     * @param {?} value
     * @return {?}
     */
    createText(parentElement, value) {
        /** @type {?} */
        const node = this.delegate.createText(value);
        if (parentElement) {
            this.delegate.appendChild(parentElement, node);
        }
        return node;
    }
    /**
     * @param {?} parentElement
     * @param {?} nodes
     * @return {?}
     */
    projectNodes(parentElement, nodes) {
        for (let i = 0; i < nodes.length; i++) {
            this.delegate.appendChild(parentElement, nodes[i]);
        }
    }
    /**
     * @param {?} node
     * @param {?} viewRootNodes
     * @return {?}
     */
    attachViewAfter(node, viewRootNodes) {
        /** @type {?} */
        const parentElement = this.delegate.parentNode(node);
        /** @type {?} */
        const nextSibling = this.delegate.nextSibling(node);
        for (let i = 0; i < viewRootNodes.length; i++) {
            this.delegate.insertBefore(parentElement, viewRootNodes[i], nextSibling);
        }
    }
    /**
     * @param {?} viewRootNodes
     * @return {?}
     */
    detachView(viewRootNodes) {
        for (let i = 0; i < viewRootNodes.length; i++) {
            /** @type {?} */
            const node = viewRootNodes[i];
            /** @type {?} */
            const parentElement = this.delegate.parentNode(node);
            this.delegate.removeChild(parentElement, node);
        }
    }
    /**
     * @param {?} hostElement
     * @param {?} viewAllNodes
     * @return {?}
     */
    destroyView(hostElement, viewAllNodes) {
        for (let i = 0; i < viewAllNodes.length; i++) {
            (/** @type {?} */ (this.delegate.destroyNode))(viewAllNodes[i]);
        }
    }
    /**
     * @param {?} renderElement
     * @param {?} name
     * @param {?} callback
     * @return {?}
     */
    listen(renderElement, name, callback) {
        return this.delegate.listen(renderElement, name, (/** @type {?} */ (callback)));
    }
    /**
     * @param {?} target
     * @param {?} name
     * @param {?} callback
     * @return {?}
     */
    listenGlobal(target, name, callback) {
        return this.delegate.listen(target, name, (/** @type {?} */ (callback)));
    }
    /**
     * @param {?} renderElement
     * @param {?} propertyName
     * @param {?} propertyValue
     * @return {?}
     */
    setElementProperty(renderElement, propertyName, propertyValue) {
        this.delegate.setProperty(renderElement, propertyName, propertyValue);
    }
    /**
     * @param {?} renderElement
     * @param {?} namespaceAndName
     * @param {?=} attributeValue
     * @return {?}
     */
    setElementAttribute(renderElement, namespaceAndName, attributeValue) {
        const [ns, name] = splitNamespace(namespaceAndName);
        if (attributeValue != null) {
            this.delegate.setAttribute(renderElement, name, attributeValue, ns);
        }
        else {
            this.delegate.removeAttribute(renderElement, name, ns);
        }
    }
    /**
     * @param {?} renderElement
     * @param {?} propertyName
     * @param {?} propertyValue
     * @return {?}
     */
    setBindingDebugInfo(renderElement, propertyName, propertyValue) { }
    /**
     * @param {?} renderElement
     * @param {?} className
     * @param {?} isAdd
     * @return {?}
     */
    setElementClass(renderElement, className, isAdd) {
        if (isAdd) {
            this.delegate.addClass(renderElement, className);
        }
        else {
            this.delegate.removeClass(renderElement, className);
        }
    }
    /**
     * @param {?} renderElement
     * @param {?} styleName
     * @param {?=} styleValue
     * @return {?}
     */
    setElementStyle(renderElement, styleName, styleValue) {
        if (styleValue != null) {
            this.delegate.setStyle(renderElement, styleName, styleValue);
        }
        else {
            this.delegate.removeStyle(renderElement, styleName);
        }
    }
    /**
     * @param {?} renderElement
     * @param {?} methodName
     * @param {?} args
     * @return {?}
     */
    invokeElementMethod(renderElement, methodName, args) {
        ((/** @type {?} */ (renderElement)))[methodName].apply(renderElement, args);
    }
    /**
     * @param {?} renderNode
     * @param {?} text
     * @return {?}
     */
    setText(renderNode, text) { this.delegate.setValue(renderNode, text); }
    /**
     * @return {?}
     */
    animate() { throw new Error('Renderer.animate is no longer supported!'); }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    RendererAdapter.prototype.delegate;
}
/**
 * @param {?} moduleType
 * @param {?} parent
 * @param {?} bootstrapComponents
 * @param {?} def
 * @return {?}
 */
export function createNgModuleRef(moduleType, parent, bootstrapComponents, def) {
    return new NgModuleRef_(moduleType, parent, bootstrapComponents, def);
}
class NgModuleRef_ {
    /**
     * @param {?} _moduleType
     * @param {?} _parent
     * @param {?} _bootstrapComponents
     * @param {?} _def
     */
    constructor(_moduleType, _parent, _bootstrapComponents, _def) {
        this._moduleType = _moduleType;
        this._parent = _parent;
        this._bootstrapComponents = _bootstrapComponents;
        this._def = _def;
        this._destroyListeners = [];
        this._destroyed = false;
        this.injector = this;
        initNgModule(this);
    }
    /**
     * @param {?} token
     * @param {?=} notFoundValue
     * @param {?=} injectFlags
     * @return {?}
     */
    get(token, notFoundValue = Injector.THROW_IF_NOT_FOUND, injectFlags = InjectFlags.Default) {
        /** @type {?} */
        let flags = 0 /* None */;
        if (injectFlags & InjectFlags.SkipSelf) {
            flags |= 1 /* SkipSelf */;
        }
        else if (injectFlags & InjectFlags.Self) {
            flags |= 4 /* Self */;
        }
        return resolveNgModuleDep(this, { token: token, tokenKey: tokenKey(token), flags: flags }, notFoundValue);
    }
    /**
     * @return {?}
     */
    get instance() { return this.get(this._moduleType); }
    /**
     * @return {?}
     */
    get componentFactoryResolver() { return this.get(ComponentFactoryResolver); }
    /**
     * @return {?}
     */
    destroy() {
        if (this._destroyed) {
            throw new Error(`The ng module ${stringify(this.instance.constructor)} has already been destroyed.`);
        }
        this._destroyed = true;
        callNgModuleLifecycle(this, 131072 /* OnDestroy */);
        this._destroyListeners.forEach((/**
         * @param {?} listener
         * @return {?}
         */
        (listener) => listener()));
    }
    /**
     * @param {?} callback
     * @return {?}
     */
    onDestroy(callback) { this._destroyListeners.push(callback); }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    NgModuleRef_.prototype._destroyListeners;
    /**
     * @type {?}
     * @private
     */
    NgModuleRef_.prototype._destroyed;
    /**
     * \@internal
     * @type {?}
     */
    NgModuleRef_.prototype._providers;
    /**
     * \@internal
     * @type {?}
     */
    NgModuleRef_.prototype._modules;
    /** @type {?} */
    NgModuleRef_.prototype.injector;
    /**
     * @type {?}
     * @private
     */
    NgModuleRef_.prototype._moduleType;
    /** @type {?} */
    NgModuleRef_.prototype._parent;
    /** @type {?} */
    NgModuleRef_.prototype._bootstrapComponents;
    /** @type {?} */
    NgModuleRef_.prototype._def;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmcy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvcmVmcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVVBLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUN4QyxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0sMEJBQTBCLENBQUM7QUFFckQsT0FBTyxFQUFDLGdCQUFnQixFQUFFLFlBQVksRUFBQyxNQUFNLDZCQUE2QixDQUFDO0FBQzNFLE9BQU8sRUFBQyw2QkFBNkIsRUFBRSx3QkFBd0IsRUFBQyxNQUFNLHNDQUFzQyxDQUFDO0FBQzdHLE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSx1QkFBdUIsQ0FBQztBQUNqRCxPQUFPLEVBQXNCLFdBQVcsRUFBQyxNQUFNLDZCQUE2QixDQUFDO0FBQzdFLE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSx3QkFBd0IsQ0FBQztBQUluRCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDNUMsT0FBTyxFQUFDLE9BQU8sRUFBQyxNQUFNLFlBQVksQ0FBQztBQUVuQyxPQUFPLEVBQUMscUJBQXFCLEVBQUUsWUFBWSxFQUFFLGtCQUFrQixFQUFDLE1BQU0sYUFBYSxDQUFDO0FBQ3BGLE9BQU8sRUFBOEUsUUFBUSxFQUErRSxhQUFhLEVBQUUsY0FBYyxFQUFFLFVBQVUsRUFBQyxNQUFNLFNBQVMsQ0FBQztBQUN0TyxPQUFPLEVBQUMsdUJBQXVCLEVBQUUsaUJBQWlCLEVBQUUsZUFBZSxFQUFFLGNBQWMsRUFBRSxRQUFRLEVBQUUsWUFBWSxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBQzNILE9BQU8sRUFBQyxrQkFBa0IsRUFBRSxrQkFBa0IsRUFBRSxnQkFBZ0IsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLGVBQWUsQ0FBQzs7TUFFbkcsYUFBYSxHQUFHLElBQUksTUFBTSxFQUFF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ckIsSUFBSSxFQUFFLEVBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxRQUFRLEVBQUUsUUFBUSxDQUFDLEtBQUssQ0FBQyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUMsRUFBRSxhQUFhLENBQUMsQ0FBQztJQUNwRixDQUFDOzs7O0lBRUQsSUFBSSxRQUFRLEtBQUssT0FBTyxJQUFJLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7SUFFckQsSUFBSSx3QkFBd0IsS0FBSyxPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMsd0JBQXdCLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7SUFFN0UsT0FBTztRQUNMLElBQUksSUFBSSxDQUFDLFVBQVUsRUFBRTtZQUNuQixNQUFNLElBQUksS0FBSyxDQUNYLGlCQUFpQixTQUFTLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsOEJBQThCLENBQUMsQ0FBQztTQUMxRjtRQUNELElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDO1FBQ3ZCLHFCQUFxQixDQUFDLElBQUkseUJBQXNCLENBQUM7UUFDakQsSUFBSSxDQUFDLGlCQUFpQixDQUFDLE9BQU87Ozs7UUFBQyxDQUFDLFFBQVEsRUFBRSxFQUFFLENBQUMsUUFBUSxFQUFFLEVBQUMsQ0FBQztJQUMzRCxDQUFDOzs7OztJQUVELFNBQVMsQ0FBQyxRQUFvQixJQUFVLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDO0NBQ2pGOzs7Ozs7SUE1Q0MseUNBQStDOzs7OztJQUMvQyxrQ0FBb0M7Ozs7O0lBR3BDLGtDQUFvQjs7Ozs7SUFHcEIsZ0NBQWtCOztJQUVsQixnQ0FBbUM7Ozs7O0lBRy9CLG1DQUE4Qjs7SUFBRSwrQkFBd0I7O0lBQ3hELDRDQUF3Qzs7SUFBRSw0QkFBK0IiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QXBwbGljYXRpb25SZWZ9IGZyb20gJy4uL2FwcGxpY2F0aW9uX3JlZic7XG5pbXBvcnQge0NoYW5nZURldGVjdG9yUmVmfSBmcm9tICcuLi9jaGFuZ2VfZGV0ZWN0aW9uL2NoYW5nZV9kZXRlY3Rpb24nO1xuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnLi4vZGkvaW5qZWN0b3InO1xuaW1wb3J0IHtJbmplY3RGbGFnc30gZnJvbSAnLi4vZGkvaW50ZXJmYWNlL2luamVjdG9yJztcbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtDb21wb25lbnRGYWN0b3J5LCBDb21wb25lbnRSZWZ9IGZyb20gJy4uL2xpbmtlci9jb21wb25lbnRfZmFjdG9yeSc7XG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlCb3VuZFRvTW9kdWxlLCBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJ9IGZyb20gJy4uL2xpbmtlci9jb21wb25lbnRfZmFjdG9yeV9yZXNvbHZlcic7XG5pbXBvcnQge0VsZW1lbnRSZWZ9IGZyb20gJy4uL2xpbmtlci9lbGVtZW50X3JlZic7XG5pbXBvcnQge0ludGVybmFsTmdNb2R1bGVSZWYsIE5nTW9kdWxlUmVmfSBmcm9tICcuLi9saW5rZXIvbmdfbW9kdWxlX2ZhY3RvcnknO1xuaW1wb3J0IHtUZW1wbGF0ZVJlZn0gZnJvbSAnLi4vbGlua2VyL3RlbXBsYXRlX3JlZic7XG5pbXBvcnQge1ZpZXdDb250YWluZXJSZWZ9IGZyb20gJy4uL2xpbmtlci92aWV3X2NvbnRhaW5lcl9yZWYnO1xuaW1wb3J0IHtFbWJlZGRlZFZpZXdSZWYsIEludGVybmFsVmlld1JlZiwgVmlld1JlZn0gZnJvbSAnLi4vbGlua2VyL3ZpZXdfcmVmJztcbmltcG9ydCB7UmVuZGVyZXIgYXMgUmVuZGVyZXJWMSwgUmVuZGVyZXIyfSBmcm9tICcuLi9yZW5kZXIvYXBpJztcbmltcG9ydCB7c3RyaW5naWZ5fSBmcm9tICcuLi91dGlsL3N0cmluZ2lmeSc7XG5pbXBvcnQge1ZFUlNJT059IGZyb20gJy4uL3ZlcnNpb24nO1xuXG5pbXBvcnQge2NhbGxOZ01vZHVsZUxpZmVjeWNsZSwgaW5pdE5nTW9kdWxlLCByZXNvbHZlTmdNb2R1bGVEZXB9IGZyb20gJy4vbmdfbW9kdWxlJztcbmltcG9ydCB7RGVwRmxhZ3MsIEVsZW1lbnREYXRhLCBOZ01vZHVsZURhdGEsIE5nTW9kdWxlRGVmaW5pdGlvbiwgTm9kZURlZiwgTm9kZUZsYWdzLCBTZXJ2aWNlcywgVGVtcGxhdGVEYXRhLCBWaWV3Q29udGFpbmVyRGF0YSwgVmlld0RhdGEsIFZpZXdEZWZpbml0aW9uRmFjdG9yeSwgVmlld1N0YXRlLCBhc0VsZW1lbnREYXRhLCBhc1Byb3ZpZGVyRGF0YSwgYXNUZXh0RGF0YX0gZnJvbSAnLi90eXBlcyc7XG5pbXBvcnQge21hcmtQYXJlbnRWaWV3c0ZvckNoZWNrLCByZXNvbHZlRGVmaW5pdGlvbiwgcm9vdFJlbmRlck5vZGVzLCBzcGxpdE5hbWVzcGFjZSwgdG9rZW5LZXksIHZpZXdQYXJlbnRFbH0gZnJvbSAnLi91dGlsJztcbmltcG9ydCB7YXR0YWNoRW1iZWRkZWRWaWV3LCBkZXRhY2hFbWJlZGRlZFZpZXcsIG1vdmVFbWJlZGRlZFZpZXcsIHJlbmRlckRldGFjaFZpZXd9IGZyb20gJy4vdmlld19hdHRhY2gnO1xuXG5jb25zdCBFTVBUWV9DT05URVhUID0gbmV3IE9iamVjdCgpO1xuXG4vLyBBdHRlbnRpb246IHRoaXMgZnVuY3Rpb24gaXMgY2FsbGVkIGFzIHRvcCBsZXZlbCBmdW5jdGlvbi5cbi8vIFB1dHRpbmcgYW55IGxvZ2ljIGluIGhlcmUgd2lsbCBkZXN0cm95IGNsb3N1cmUgdHJlZSBzaGFraW5nIVxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZUNvbXBvbmVudEZhY3RvcnkoXG4gICAgc2VsZWN0b3I6IHN0cmluZywgY29tcG9uZW50VHlwZTogVHlwZTxhbnk+LCB2aWV3RGVmRmFjdG9yeTogVmlld0RlZmluaXRpb25GYWN0b3J5LFxuICAgIGlucHV0czoge1twcm9wTmFtZTogc3RyaW5nXTogc3RyaW5nfSB8IG51bGwsIG91dHB1dHM6IHtbcHJvcE5hbWU6IHN0cmluZ106IHN0cmluZ30sXG4gICAgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXSk6IENvbXBvbmVudEZhY3Rvcnk8YW55PiB7XG4gIHJldHVybiBuZXcgQ29tcG9uZW50RmFjdG9yeV8oXG4gICAgICBzZWxlY3RvciwgY29tcG9uZW50VHlwZSwgdmlld0RlZkZhY3RvcnksIGlucHV0cywgb3V0cHV0cywgbmdDb250ZW50U2VsZWN0b3JzKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbXBvbmVudFZpZXdEZWZpbml0aW9uRmFjdG9yeShjb21wb25lbnRGYWN0b3J5OiBDb21wb25lbnRGYWN0b3J5PGFueT4pOlxuICAgIFZpZXdEZWZpbml0aW9uRmFjdG9yeSB7XG4gIHJldHVybiAoY29tcG9uZW50RmFjdG9yeSBhcyBDb21wb25lbnRGYWN0b3J5Xykudmlld0RlZkZhY3Rvcnk7XG59XG5cbmNsYXNzIENvbXBvbmVudEZhY3RvcnlfIGV4dGVuZHMgQ29tcG9uZW50RmFjdG9yeTxhbnk+IHtcbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgdmlld0RlZkZhY3Rvcnk6IFZpZXdEZWZpbml0aW9uRmFjdG9yeTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBzZWxlY3Rvcjogc3RyaW5nLCBwdWJsaWMgY29tcG9uZW50VHlwZTogVHlwZTxhbnk+LFxuICAgICAgdmlld0RlZkZhY3Rvcnk6IFZpZXdEZWZpbml0aW9uRmFjdG9yeSwgcHJpdmF0ZSBfaW5wdXRzOiB7W3Byb3BOYW1lOiBzdHJpbmddOiBzdHJpbmd9fG51bGwsXG4gICAgICBwcml2YXRlIF9vdXRwdXRzOiB7W3Byb3BOYW1lOiBzdHJpbmddOiBzdHJpbmd9LCBwdWJsaWMgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXSkge1xuICAgIC8vIEF0dGVudGlvbjogdGhpcyBjdG9yIGlzIGNhbGxlZCBhcyB0b3AgbGV2ZWwgZnVuY3Rpb24uXG4gICAgLy8gUHV0dGluZyBhbnkgbG9naWMgaW4gaGVyZSB3aWxsIGRlc3Ryb3kgY2xvc3VyZSB0cmVlIHNoYWtpbmchXG4gICAgc3VwZXIoKTtcbiAgICB0aGlzLnZpZXdEZWZGYWN0b3J5ID0gdmlld0RlZkZhY3Rvcnk7XG4gIH1cblxuICBnZXQgaW5wdXRzKCkge1xuICAgIGNvbnN0IGlucHV0c0Fycjoge3Byb3BOYW1lOiBzdHJpbmcsIHRlbXBsYXRlTmFtZTogc3RyaW5nfVtdID0gW107XG4gICAgY29uc3QgaW5wdXRzID0gdGhpcy5faW5wdXRzICE7XG4gICAgZm9yIChsZXQgcHJvcE5hbWUgaW4gaW5wdXRzKSB7XG4gICAgICBjb25zdCB0ZW1wbGF0ZU5hbWUgPSBpbnB1dHNbcHJvcE5hbWVdO1xuICAgICAgaW5wdXRzQXJyLnB1c2goe3Byb3BOYW1lLCB0ZW1wbGF0ZU5hbWV9KTtcbiAgICB9XG4gICAgcmV0dXJuIGlucHV0c0FycjtcbiAgfVxuXG4gIGdldCBvdXRwdXRzKCkge1xuICAgIGNvbnN0IG91dHB1dHNBcnI6IHtwcm9wTmFtZTogc3RyaW5nLCB0ZW1wbGF0ZU5hbWU6IHN0cmluZ31bXSA9IFtdO1xuICAgIGZvciAobGV0IHByb3BOYW1lIGluIHRoaXMuX291dHB1dHMpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlTmFtZSA9IHRoaXMuX291dHB1dHNbcHJvcE5hbWVdO1xuICAgICAgb3V0cHV0c0Fyci5wdXNoKHtwcm9wTmFtZSwgdGVtcGxhdGVOYW1lfSk7XG4gICAgfVxuICAgIHJldHVybiBvdXRwdXRzQXJyO1xuICB9XG5cbiAgLyoqXG4gICAqIENyZWF0ZXMgYSBuZXcgY29tcG9uZW50LlxuICAgKi9cbiAgY3JlYXRlKFxuICAgICAgaW5qZWN0b3I6IEluamVjdG9yLCBwcm9qZWN0YWJsZU5vZGVzPzogYW55W11bXSwgcm9vdFNlbGVjdG9yT3JOb2RlPzogc3RyaW5nfGFueSxcbiAgICAgIG5nTW9kdWxlPzogTmdNb2R1bGVSZWY8YW55Pik6IENvbXBvbmVudFJlZjxhbnk+IHtcbiAgICBpZiAoIW5nTW9kdWxlKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ25nTW9kdWxlIHNob3VsZCBiZSBwcm92aWRlZCcpO1xuICAgIH1cbiAgICBjb25zdCB2aWV3RGVmID0gcmVzb2x2ZURlZmluaXRpb24odGhpcy52aWV3RGVmRmFjdG9yeSk7XG4gICAgY29uc3QgY29tcG9uZW50Tm9kZUluZGV4ID0gdmlld0RlZi5ub2Rlc1swXS5lbGVtZW50ICEuY29tcG9uZW50UHJvdmlkZXIgIS5ub2RlSW5kZXg7XG4gICAgY29uc3QgdmlldyA9IFNlcnZpY2VzLmNyZWF0ZVJvb3RWaWV3KFxuICAgICAgICBpbmplY3RvciwgcHJvamVjdGFibGVOb2RlcyB8fCBbXSwgcm9vdFNlbGVjdG9yT3JOb2RlLCB2aWV3RGVmLCBuZ01vZHVsZSwgRU1QVFlfQ09OVEVYVCk7XG4gICAgY29uc3QgY29tcG9uZW50ID0gYXNQcm92aWRlckRhdGEodmlldywgY29tcG9uZW50Tm9kZUluZGV4KS5pbnN0YW5jZTtcbiAgICBpZiAocm9vdFNlbGVjdG9yT3JOb2RlKSB7XG4gICAgICB2aWV3LnJlbmRlcmVyLnNldEF0dHJpYnV0ZShhc0VsZW1lbnREYXRhKHZpZXcsIDApLnJlbmRlckVsZW1lbnQsICduZy12ZXJzaW9uJywgVkVSU0lPTi5mdWxsKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbmV3IENvbXBvbmVudFJlZl8odmlldywgbmV3IFZpZXdSZWZfKHZpZXcpLCBjb21wb25lbnQpO1xuICB9XG59XG5cbmNsYXNzIENvbXBvbmVudFJlZl8gZXh0ZW5kcyBDb21wb25lbnRSZWY8YW55PiB7XG4gIHB1YmxpYyByZWFkb25seSBob3N0VmlldzogVmlld1JlZjtcbiAgcHVibGljIHJlYWRvbmx5IGluc3RhbmNlOiBhbnk7XG4gIHB1YmxpYyByZWFkb25seSBjaGFuZ2VEZXRlY3RvclJlZjogQ2hhbmdlRGV0ZWN0b3JSZWY7XG4gIHByaXZhdGUgX2VsRGVmOiBOb2RlRGVmO1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF92aWV3OiBWaWV3RGF0YSwgcHJpdmF0ZSBfdmlld1JlZjogVmlld1JlZiwgcHJpdmF0ZSBfY29tcG9uZW50OiBhbnkpIHtcbiAgICBzdXBlcigpO1xuICAgIHRoaXMuX2VsRGVmID0gdGhpcy5fdmlldy5kZWYubm9kZXNbMF07XG4gICAgdGhpcy5ob3N0VmlldyA9IF92aWV3UmVmO1xuICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYgPSBfdmlld1JlZjtcbiAgICB0aGlzLmluc3RhbmNlID0gX2NvbXBvbmVudDtcbiAgfVxuICBnZXQgbG9jYXRpb24oKTogRWxlbWVudFJlZiB7XG4gICAgcmV0dXJuIG5ldyBFbGVtZW50UmVmKGFzRWxlbWVudERhdGEodGhpcy5fdmlldywgdGhpcy5fZWxEZWYubm9kZUluZGV4KS5yZW5kZXJFbGVtZW50KTtcbiAgfVxuICBnZXQgaW5qZWN0b3IoKTogSW5qZWN0b3IgeyByZXR1cm4gbmV3IEluamVjdG9yXyh0aGlzLl92aWV3LCB0aGlzLl9lbERlZik7IH1cbiAgZ2V0IGNvbXBvbmVudFR5cGUoKTogVHlwZTxhbnk+IHsgcmV0dXJuIDxhbnk+dGhpcy5fY29tcG9uZW50LmNvbnN0cnVjdG9yOyB9XG5cbiAgZGVzdHJveSgpOiB2b2lkIHsgdGhpcy5fdmlld1JlZi5kZXN0cm95KCk7IH1cbiAgb25EZXN0cm95KGNhbGxiYWNrOiBGdW5jdGlvbik6IHZvaWQgeyB0aGlzLl92aWV3UmVmLm9uRGVzdHJveShjYWxsYmFjayk7IH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVZpZXdDb250YWluZXJEYXRhKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBlbERlZjogTm9kZURlZiwgZWxEYXRhOiBFbGVtZW50RGF0YSk6IFZpZXdDb250YWluZXJEYXRhIHtcbiAgcmV0dXJuIG5ldyBWaWV3Q29udGFpbmVyUmVmXyh2aWV3LCBlbERlZiwgZWxEYXRhKTtcbn1cblxuY2xhc3MgVmlld0NvbnRhaW5lclJlZl8gaW1wbGVtZW50cyBWaWV3Q29udGFpbmVyRGF0YSB7XG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIF9lbWJlZGRlZFZpZXdzOiBWaWV3RGF0YVtdID0gW107XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3ZpZXc6IFZpZXdEYXRhLCBwcml2YXRlIF9lbERlZjogTm9kZURlZiwgcHJpdmF0ZSBfZGF0YTogRWxlbWVudERhdGEpIHt9XG5cbiAgZ2V0IGVsZW1lbnQoKTogRWxlbWVudFJlZiB7IHJldHVybiBuZXcgRWxlbWVudFJlZih0aGlzLl9kYXRhLnJlbmRlckVsZW1lbnQpOyB9XG5cbiAgZ2V0IGluamVjdG9yKCk6IEluamVjdG9yIHsgcmV0dXJuIG5ldyBJbmplY3Rvcl8odGhpcy5fdmlldywgdGhpcy5fZWxEZWYpOyB9XG5cbiAgLyoqIEBkZXByZWNhdGVkIE5vIHJlcGxhY2VtZW50ICovXG4gIGdldCBwYXJlbnRJbmplY3RvcigpOiBJbmplY3RvciB7XG4gICAgbGV0IHZpZXcgPSB0aGlzLl92aWV3O1xuICAgIGxldCBlbERlZiA9IHRoaXMuX2VsRGVmLnBhcmVudDtcbiAgICB3aGlsZSAoIWVsRGVmICYmIHZpZXcpIHtcbiAgICAgIGVsRGVmID0gdmlld1BhcmVudEVsKHZpZXcpO1xuICAgICAgdmlldyA9IHZpZXcucGFyZW50ICE7XG4gICAgfVxuXG4gICAgcmV0dXJuIHZpZXcgPyBuZXcgSW5qZWN0b3JfKHZpZXcsIGVsRGVmKSA6IG5ldyBJbmplY3Rvcl8odGhpcy5fdmlldywgbnVsbCk7XG4gIH1cblxuICBjbGVhcigpOiB2b2lkIHtcbiAgICBjb25zdCBsZW4gPSB0aGlzLl9lbWJlZGRlZFZpZXdzLmxlbmd0aDtcbiAgICBmb3IgKGxldCBpID0gbGVuIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICAgIGNvbnN0IHZpZXcgPSBkZXRhY2hFbWJlZGRlZFZpZXcodGhpcy5fZGF0YSwgaSkgITtcbiAgICAgIFNlcnZpY2VzLmRlc3Ryb3lWaWV3KHZpZXcpO1xuICAgIH1cbiAgfVxuXG4gIGdldChpbmRleDogbnVtYmVyKTogVmlld1JlZnxudWxsIHtcbiAgICBjb25zdCB2aWV3ID0gdGhpcy5fZW1iZWRkZWRWaWV3c1tpbmRleF07XG4gICAgaWYgKHZpZXcpIHtcbiAgICAgIGNvbnN0IHJlZiA9IG5ldyBWaWV3UmVmXyh2aWV3KTtcbiAgICAgIHJlZi5hdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodGhpcyk7XG4gICAgICByZXR1cm4gcmVmO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGdldCBsZW5ndGgoKTogbnVtYmVyIHsgcmV0dXJuIHRoaXMuX2VtYmVkZGVkVmlld3MubGVuZ3RoOyB9XG5cbiAgY3JlYXRlRW1iZWRkZWRWaWV3PEM+KHRlbXBsYXRlUmVmOiBUZW1wbGF0ZVJlZjxDPiwgY29udGV4dD86IEMsIGluZGV4PzogbnVtYmVyKTpcbiAgICAgIEVtYmVkZGVkVmlld1JlZjxDPiB7XG4gICAgY29uc3Qgdmlld1JlZiA9IHRlbXBsYXRlUmVmLmNyZWF0ZUVtYmVkZGVkVmlldyhjb250ZXh0IHx8IDxhbnk+e30pO1xuICAgIHRoaXMuaW5zZXJ0KHZpZXdSZWYsIGluZGV4KTtcbiAgICByZXR1cm4gdmlld1JlZjtcbiAgfVxuXG4gIGNyZWF0ZUNvbXBvbmVudDxDPihcbiAgICAgIGNvbXBvbmVudEZhY3Rvcnk6IENvbXBvbmVudEZhY3Rvcnk8Qz4sIGluZGV4PzogbnVtYmVyLCBpbmplY3Rvcj86IEluamVjdG9yLFxuICAgICAgcHJvamVjdGFibGVOb2Rlcz86IGFueVtdW10sIG5nTW9kdWxlUmVmPzogTmdNb2R1bGVSZWY8YW55Pik6IENvbXBvbmVudFJlZjxDPiB7XG4gICAgY29uc3QgY29udGV4dEluamVjdG9yID0gaW5qZWN0b3IgfHwgdGhpcy5wYXJlbnRJbmplY3RvcjtcbiAgICBpZiAoIW5nTW9kdWxlUmVmICYmICEoY29tcG9uZW50RmFjdG9yeSBpbnN0YW5jZW9mIENvbXBvbmVudEZhY3RvcnlCb3VuZFRvTW9kdWxlKSkge1xuICAgICAgbmdNb2R1bGVSZWYgPSBjb250ZXh0SW5qZWN0b3IuZ2V0KE5nTW9kdWxlUmVmKTtcbiAgICB9XG4gICAgY29uc3QgY29tcG9uZW50UmVmID1cbiAgICAgICAgY29tcG9uZW50RmFjdG9yeS5jcmVhdGUoY29udGV4dEluamVjdG9yLCBwcm9qZWN0YWJsZU5vZGVzLCB1bmRlZmluZWQsIG5nTW9kdWxlUmVmKTtcbiAgICB0aGlzLmluc2VydChjb21wb25lbnRSZWYuaG9zdFZpZXcsIGluZGV4KTtcbiAgICByZXR1cm4gY29tcG9uZW50UmVmO1xuICB9XG5cbiAgaW5zZXJ0KHZpZXdSZWY6IFZpZXdSZWYsIGluZGV4PzogbnVtYmVyKTogVmlld1JlZiB7XG4gICAgaWYgKHZpZXdSZWYuZGVzdHJveWVkKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0Nhbm5vdCBpbnNlcnQgYSBkZXN0cm95ZWQgVmlldyBpbiBhIFZpZXdDb250YWluZXIhJyk7XG4gICAgfVxuICAgIGNvbnN0IHZpZXdSZWZfID0gPFZpZXdSZWZfPnZpZXdSZWY7XG4gICAgY29uc3Qgdmlld0RhdGEgPSB2aWV3UmVmXy5fdmlldztcbiAgICBhdHRhY2hFbWJlZGRlZFZpZXcodGhpcy5fdmlldywgdGhpcy5fZGF0YSwgaW5kZXgsIHZpZXdEYXRhKTtcbiAgICB2aWV3UmVmXy5hdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodGhpcyk7XG4gICAgcmV0dXJuIHZpZXdSZWY7XG4gIH1cblxuICBtb3ZlKHZpZXdSZWY6IFZpZXdSZWZfLCBjdXJyZW50SW5kZXg6IG51bWJlcik6IFZpZXdSZWYge1xuICAgIGlmICh2aWV3UmVmLmRlc3Ryb3llZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdDYW5ub3QgbW92ZSBhIGRlc3Ryb3llZCBWaWV3IGluIGEgVmlld0NvbnRhaW5lciEnKTtcbiAgICB9XG4gICAgY29uc3QgcHJldmlvdXNJbmRleCA9IHRoaXMuX2VtYmVkZGVkVmlld3MuaW5kZXhPZih2aWV3UmVmLl92aWV3KTtcbiAgICBtb3ZlRW1iZWRkZWRWaWV3KHRoaXMuX2RhdGEsIHByZXZpb3VzSW5kZXgsIGN1cnJlbnRJbmRleCk7XG4gICAgcmV0dXJuIHZpZXdSZWY7XG4gIH1cblxuICBpbmRleE9mKHZpZXdSZWY6IFZpZXdSZWYpOiBudW1iZXIge1xuICAgIHJldHVybiB0aGlzLl9lbWJlZGRlZFZpZXdzLmluZGV4T2YoKDxWaWV3UmVmXz52aWV3UmVmKS5fdmlldyk7XG4gIH1cblxuICByZW1vdmUoaW5kZXg/OiBudW1iZXIpOiB2b2lkIHtcbiAgICBjb25zdCB2aWV3RGF0YSA9IGRldGFjaEVtYmVkZGVkVmlldyh0aGlzLl9kYXRhLCBpbmRleCk7XG4gICAgaWYgKHZpZXdEYXRhKSB7XG4gICAgICBTZXJ2aWNlcy5kZXN0cm95Vmlldyh2aWV3RGF0YSk7XG4gICAgfVxuICB9XG5cbiAgZGV0YWNoKGluZGV4PzogbnVtYmVyKTogVmlld1JlZnxudWxsIHtcbiAgICBjb25zdCB2aWV3ID0gZGV0YWNoRW1iZWRkZWRWaWV3KHRoaXMuX2RhdGEsIGluZGV4KTtcbiAgICByZXR1cm4gdmlldyA/IG5ldyBWaWV3UmVmXyh2aWV3KSA6IG51bGw7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZUNoYW5nZURldGVjdG9yUmVmKHZpZXc6IFZpZXdEYXRhKTogQ2hhbmdlRGV0ZWN0b3JSZWYge1xuICByZXR1cm4gbmV3IFZpZXdSZWZfKHZpZXcpO1xufVxuXG5leHBvcnQgY2xhc3MgVmlld1JlZl8gaW1wbGVtZW50cyBFbWJlZGRlZFZpZXdSZWY8YW55PiwgSW50ZXJuYWxWaWV3UmVmIHtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfdmlldzogVmlld0RhdGE7XG4gIHByaXZhdGUgX3ZpZXdDb250YWluZXJSZWY6IFZpZXdDb250YWluZXJSZWZ8bnVsbDtcbiAgcHJpdmF0ZSBfYXBwUmVmOiBBcHBsaWNhdGlvblJlZnxudWxsO1xuXG4gIGNvbnN0cnVjdG9yKF92aWV3OiBWaWV3RGF0YSkge1xuICAgIHRoaXMuX3ZpZXcgPSBfdmlldztcbiAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmID0gbnVsbDtcbiAgICB0aGlzLl9hcHBSZWYgPSBudWxsO1xuICB9XG5cbiAgZ2V0IHJvb3ROb2RlcygpOiBhbnlbXSB7IHJldHVybiByb290UmVuZGVyTm9kZXModGhpcy5fdmlldyk7IH1cblxuICBnZXQgY29udGV4dCgpIHsgcmV0dXJuIHRoaXMuX3ZpZXcuY29udGV4dDsgfVxuXG4gIGdldCBkZXN0cm95ZWQoKTogYm9vbGVhbiB7IHJldHVybiAodGhpcy5fdmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5EZXN0cm95ZWQpICE9PSAwOyB9XG5cbiAgbWFya0ZvckNoZWNrKCk6IHZvaWQgeyBtYXJrUGFyZW50Vmlld3NGb3JDaGVjayh0aGlzLl92aWV3KTsgfVxuICBkZXRhY2goKTogdm9pZCB7IHRoaXMuX3ZpZXcuc3RhdGUgJj0gflZpZXdTdGF0ZS5BdHRhY2hlZDsgfVxuICBkZXRlY3RDaGFuZ2VzKCk6IHZvaWQge1xuICAgIGNvbnN0IGZzID0gdGhpcy5fdmlldy5yb290LnJlbmRlcmVyRmFjdG9yeTtcbiAgICBpZiAoZnMuYmVnaW4pIHtcbiAgICAgIGZzLmJlZ2luKCk7XG4gICAgfVxuICAgIHRyeSB7XG4gICAgICBTZXJ2aWNlcy5jaGVja0FuZFVwZGF0ZVZpZXcodGhpcy5fdmlldyk7XG4gICAgfSBmaW5hbGx5IHtcbiAgICAgIGlmIChmcy5lbmQpIHtcbiAgICAgICAgZnMuZW5kKCk7XG4gICAgICB9XG4gICAgfVxuICB9XG4gIGNoZWNrTm9DaGFuZ2VzKCk6IHZvaWQgeyBTZXJ2aWNlcy5jaGVja05vQ2hhbmdlc1ZpZXcodGhpcy5fdmlldyk7IH1cblxuICByZWF0dGFjaCgpOiB2b2lkIHsgdGhpcy5fdmlldy5zdGF0ZSB8PSBWaWV3U3RhdGUuQXR0YWNoZWQ7IH1cbiAgb25EZXN0cm95KGNhbGxiYWNrOiBGdW5jdGlvbikge1xuICAgIGlmICghdGhpcy5fdmlldy5kaXNwb3NhYmxlcykge1xuICAgICAgdGhpcy5fdmlldy5kaXNwb3NhYmxlcyA9IFtdO1xuICAgIH1cbiAgICB0aGlzLl92aWV3LmRpc3Bvc2FibGVzLnB1c2goPGFueT5jYWxsYmFjayk7XG4gIH1cblxuICBkZXN0cm95KCkge1xuICAgIGlmICh0aGlzLl9hcHBSZWYpIHtcbiAgICAgIHRoaXMuX2FwcFJlZi5kZXRhY2hWaWV3KHRoaXMpO1xuICAgIH0gZWxzZSBpZiAodGhpcy5fdmlld0NvbnRhaW5lclJlZikge1xuICAgICAgdGhpcy5fdmlld0NvbnRhaW5lclJlZi5kZXRhY2godGhpcy5fdmlld0NvbnRhaW5lclJlZi5pbmRleE9mKHRoaXMpKTtcbiAgICB9XG4gICAgU2VydmljZXMuZGVzdHJveVZpZXcodGhpcy5fdmlldyk7XG4gIH1cblxuICBkZXRhY2hGcm9tQXBwUmVmKCkge1xuICAgIHRoaXMuX2FwcFJlZiA9IG51bGw7XG4gICAgcmVuZGVyRGV0YWNoVmlldyh0aGlzLl92aWV3KTtcbiAgICBTZXJ2aWNlcy5kaXJ0eVBhcmVudFF1ZXJpZXModGhpcy5fdmlldyk7XG4gIH1cblxuICBhdHRhY2hUb0FwcFJlZihhcHBSZWY6IEFwcGxpY2F0aW9uUmVmKSB7XG4gICAgaWYgKHRoaXMuX3ZpZXdDb250YWluZXJSZWYpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignVGhpcyB2aWV3IGlzIGFscmVhZHkgYXR0YWNoZWQgdG8gYSBWaWV3Q29udGFpbmVyIScpO1xuICAgIH1cbiAgICB0aGlzLl9hcHBSZWYgPSBhcHBSZWY7XG4gIH1cblxuICBhdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodmNSZWY6IFZpZXdDb250YWluZXJSZWYpIHtcbiAgICBpZiAodGhpcy5fYXBwUmVmKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1RoaXMgdmlldyBpcyBhbHJlYWR5IGF0dGFjaGVkIGRpcmVjdGx5IHRvIHRoZSBBcHBsaWNhdGlvblJlZiEnKTtcbiAgICB9XG4gICAgdGhpcy5fdmlld0NvbnRhaW5lclJlZiA9IHZjUmVmO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVUZW1wbGF0ZURhdGEodmlldzogVmlld0RhdGEsIGRlZjogTm9kZURlZik6IFRlbXBsYXRlRGF0YSB7XG4gIHJldHVybiBuZXcgVGVtcGxhdGVSZWZfKHZpZXcsIGRlZik7XG59XG5cbmNsYXNzIFRlbXBsYXRlUmVmXyBleHRlbmRzIFRlbXBsYXRlUmVmPGFueT4gaW1wbGVtZW50cyBUZW1wbGF0ZURhdGEge1xuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgX3Byb2plY3RlZFZpZXdzICE6IFZpZXdEYXRhW107XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfcGFyZW50VmlldzogVmlld0RhdGEsIHByaXZhdGUgX2RlZjogTm9kZURlZikgeyBzdXBlcigpOyB9XG5cbiAgY3JlYXRlRW1iZWRkZWRWaWV3KGNvbnRleHQ6IGFueSk6IEVtYmVkZGVkVmlld1JlZjxhbnk+IHtcbiAgICByZXR1cm4gbmV3IFZpZXdSZWZfKFNlcnZpY2VzLmNyZWF0ZUVtYmVkZGVkVmlldyhcbiAgICAgICAgdGhpcy5fcGFyZW50VmlldywgdGhpcy5fZGVmLCB0aGlzLl9kZWYuZWxlbWVudCAhLnRlbXBsYXRlICEsIGNvbnRleHQpKTtcbiAgfVxuXG4gIGdldCBlbGVtZW50UmVmKCk6IEVsZW1lbnRSZWYge1xuICAgIHJldHVybiBuZXcgRWxlbWVudFJlZihhc0VsZW1lbnREYXRhKHRoaXMuX3BhcmVudFZpZXcsIHRoaXMuX2RlZi5ub2RlSW5kZXgpLnJlbmRlckVsZW1lbnQpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVJbmplY3Rvcih2aWV3OiBWaWV3RGF0YSwgZWxEZWY6IE5vZGVEZWYpOiBJbmplY3RvciB7XG4gIHJldHVybiBuZXcgSW5qZWN0b3JfKHZpZXcsIGVsRGVmKTtcbn1cblxuY2xhc3MgSW5qZWN0b3JfIGltcGxlbWVudHMgSW5qZWN0b3Ige1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHZpZXc6IFZpZXdEYXRhLCBwcml2YXRlIGVsRGVmOiBOb2RlRGVmfG51bGwpIHt9XG4gIGdldCh0b2tlbjogYW55LCBub3RGb3VuZFZhbHVlOiBhbnkgPSBJbmplY3Rvci5USFJPV19JRl9OT1RfRk9VTkQpOiBhbnkge1xuICAgIGNvbnN0IGFsbG93UHJpdmF0ZVNlcnZpY2VzID1cbiAgICAgICAgdGhpcy5lbERlZiA/ICh0aGlzLmVsRGVmLmZsYWdzICYgTm9kZUZsYWdzLkNvbXBvbmVudFZpZXcpICE9PSAwIDogZmFsc2U7XG4gICAgcmV0dXJuIFNlcnZpY2VzLnJlc29sdmVEZXAoXG4gICAgICAgIHRoaXMudmlldywgdGhpcy5lbERlZiwgYWxsb3dQcml2YXRlU2VydmljZXMsXG4gICAgICAgIHtmbGFnczogRGVwRmxhZ3MuTm9uZSwgdG9rZW4sIHRva2VuS2V5OiB0b2tlbktleSh0b2tlbil9LCBub3RGb3VuZFZhbHVlKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gbm9kZVZhbHVlKHZpZXc6IFZpZXdEYXRhLCBpbmRleDogbnVtYmVyKTogYW55IHtcbiAgY29uc3QgZGVmID0gdmlldy5kZWYubm9kZXNbaW5kZXhdO1xuICBpZiAoZGVmLmZsYWdzICYgTm9kZUZsYWdzLlR5cGVFbGVtZW50KSB7XG4gICAgY29uc3QgZWxEYXRhID0gYXNFbGVtZW50RGF0YSh2aWV3LCBkZWYubm9kZUluZGV4KTtcbiAgICByZXR1cm4gZGVmLmVsZW1lbnQgIS50ZW1wbGF0ZSA/IGVsRGF0YS50ZW1wbGF0ZSA6IGVsRGF0YS5yZW5kZXJFbGVtZW50O1xuICB9IGVsc2UgaWYgKGRlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlVGV4dCkge1xuICAgIHJldHVybiBhc1RleHREYXRhKHZpZXcsIGRlZi5ub2RlSW5kZXgpLnJlbmRlclRleHQ7XG4gIH0gZWxzZSBpZiAoZGVmLmZsYWdzICYgKE5vZGVGbGFncy5DYXRQcm92aWRlciB8IE5vZGVGbGFncy5UeXBlUGlwZSkpIHtcbiAgICByZXR1cm4gYXNQcm92aWRlckRhdGEodmlldywgZGVmLm5vZGVJbmRleCkuaW5zdGFuY2U7XG4gIH1cbiAgdGhyb3cgbmV3IEVycm9yKGBJbGxlZ2FsIHN0YXRlOiByZWFkIG5vZGVWYWx1ZSBmb3Igbm9kZSBpbmRleCAke2luZGV4fWApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlUmVuZGVyZXJWMSh2aWV3OiBWaWV3RGF0YSk6IFJlbmRlcmVyVjEge1xuICByZXR1cm4gbmV3IFJlbmRlcmVyQWRhcHRlcih2aWV3LnJlbmRlcmVyKTtcbn1cblxuY2xhc3MgUmVuZGVyZXJBZGFwdGVyIGltcGxlbWVudHMgUmVuZGVyZXJWMSB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZGVsZWdhdGU6IFJlbmRlcmVyMikge31cbiAgc2VsZWN0Um9vdEVsZW1lbnQoc2VsZWN0b3JPck5vZGU6IHN0cmluZ3xFbGVtZW50KTogRWxlbWVudCB7XG4gICAgcmV0dXJuIHRoaXMuZGVsZWdhdGUuc2VsZWN0Um9vdEVsZW1lbnQoc2VsZWN0b3JPck5vZGUpO1xuICB9XG5cbiAgY3JlYXRlRWxlbWVudChwYXJlbnQ6IEVsZW1lbnR8RG9jdW1lbnRGcmFnbWVudCwgbmFtZXNwYWNlQW5kTmFtZTogc3RyaW5nKTogRWxlbWVudCB7XG4gICAgY29uc3QgW25zLCBuYW1lXSA9IHNwbGl0TmFtZXNwYWNlKG5hbWVzcGFjZUFuZE5hbWUpO1xuICAgIGNvbnN0IGVsID0gdGhpcy5kZWxlZ2F0ZS5jcmVhdGVFbGVtZW50KG5hbWUsIG5zKTtcbiAgICBpZiAocGFyZW50KSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLmFwcGVuZENoaWxkKHBhcmVudCwgZWwpO1xuICAgIH1cbiAgICByZXR1cm4gZWw7XG4gIH1cblxuICBjcmVhdGVWaWV3Um9vdChob3N0RWxlbWVudDogRWxlbWVudCk6IEVsZW1lbnR8RG9jdW1lbnRGcmFnbWVudCB7IHJldHVybiBob3N0RWxlbWVudDsgfVxuXG4gIGNyZWF0ZVRlbXBsYXRlQW5jaG9yKHBhcmVudEVsZW1lbnQ6IEVsZW1lbnR8RG9jdW1lbnRGcmFnbWVudCk6IENvbW1lbnQge1xuICAgIGNvbnN0IGNvbW1lbnQgPSB0aGlzLmRlbGVnYXRlLmNyZWF0ZUNvbW1lbnQoJycpO1xuICAgIGlmIChwYXJlbnRFbGVtZW50KSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLmFwcGVuZENoaWxkKHBhcmVudEVsZW1lbnQsIGNvbW1lbnQpO1xuICAgIH1cbiAgICByZXR1cm4gY29tbWVudDtcbiAgfVxuXG4gIGNyZWF0ZVRleHQocGFyZW50RWxlbWVudDogRWxlbWVudHxEb2N1bWVudEZyYWdtZW50LCB2YWx1ZTogc3RyaW5nKTogYW55IHtcbiAgICBjb25zdCBub2RlID0gdGhpcy5kZWxlZ2F0ZS5jcmVhdGVUZXh0KHZhbHVlKTtcbiAgICBpZiAocGFyZW50RWxlbWVudCkge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5hcHBlbmRDaGlsZChwYXJlbnRFbGVtZW50LCBub2RlKTtcbiAgICB9XG4gICAgcmV0dXJuIG5vZGU7XG4gIH1cblxuICBwcm9qZWN0Tm9kZXMocGFyZW50RWxlbWVudDogRWxlbWVudHxEb2N1bWVudEZyYWdtZW50LCBub2RlczogTm9kZVtdKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBub2Rlcy5sZW5ndGg7IGkrKykge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5hcHBlbmRDaGlsZChwYXJlbnRFbGVtZW50LCBub2Rlc1tpXSk7XG4gICAgfVxuICB9XG5cbiAgYXR0YWNoVmlld0FmdGVyKG5vZGU6IE5vZGUsIHZpZXdSb290Tm9kZXM6IE5vZGVbXSkge1xuICAgIGNvbnN0IHBhcmVudEVsZW1lbnQgPSB0aGlzLmRlbGVnYXRlLnBhcmVudE5vZGUobm9kZSk7XG4gICAgY29uc3QgbmV4dFNpYmxpbmcgPSB0aGlzLmRlbGVnYXRlLm5leHRTaWJsaW5nKG5vZGUpO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdmlld1Jvb3ROb2Rlcy5sZW5ndGg7IGkrKykge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5pbnNlcnRCZWZvcmUocGFyZW50RWxlbWVudCwgdmlld1Jvb3ROb2Rlc1tpXSwgbmV4dFNpYmxpbmcpO1xuICAgIH1cbiAgfVxuXG4gIGRldGFjaFZpZXcodmlld1Jvb3ROb2RlczogKEVsZW1lbnR8VGV4dHxDb21tZW50KVtdKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB2aWV3Um9vdE5vZGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBub2RlID0gdmlld1Jvb3ROb2Rlc1tpXTtcbiAgICAgIGNvbnN0IHBhcmVudEVsZW1lbnQgPSB0aGlzLmRlbGVnYXRlLnBhcmVudE5vZGUobm9kZSk7XG4gICAgICB0aGlzLmRlbGVnYXRlLnJlbW92ZUNoaWxkKHBhcmVudEVsZW1lbnQsIG5vZGUpO1xuICAgIH1cbiAgfVxuXG4gIGRlc3Ryb3lWaWV3KGhvc3RFbGVtZW50OiBFbGVtZW50fERvY3VtZW50RnJhZ21lbnQsIHZpZXdBbGxOb2RlczogTm9kZVtdKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB2aWV3QWxsTm9kZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHRoaXMuZGVsZWdhdGUuZGVzdHJveU5vZGUgISh2aWV3QWxsTm9kZXNbaV0pO1xuICAgIH1cbiAgfVxuXG4gIGxpc3RlbihyZW5kZXJFbGVtZW50OiBhbnksIG5hbWU6IHN0cmluZywgY2FsbGJhY2s6IEZ1bmN0aW9uKTogRnVuY3Rpb24ge1xuICAgIHJldHVybiB0aGlzLmRlbGVnYXRlLmxpc3RlbihyZW5kZXJFbGVtZW50LCBuYW1lLCA8YW55PmNhbGxiYWNrKTtcbiAgfVxuXG4gIGxpc3Rlbkdsb2JhbCh0YXJnZXQ6IHN0cmluZywgbmFtZTogc3RyaW5nLCBjYWxsYmFjazogRnVuY3Rpb24pOiBGdW5jdGlvbiB7XG4gICAgcmV0dXJuIHRoaXMuZGVsZWdhdGUubGlzdGVuKHRhcmdldCwgbmFtZSwgPGFueT5jYWxsYmFjayk7XG4gIH1cblxuICBzZXRFbGVtZW50UHJvcGVydHkoXG4gICAgICByZW5kZXJFbGVtZW50OiBFbGVtZW50fERvY3VtZW50RnJhZ21lbnQsIHByb3BlcnR5TmFtZTogc3RyaW5nLCBwcm9wZXJ0eVZhbHVlOiBhbnkpOiB2b2lkIHtcbiAgICB0aGlzLmRlbGVnYXRlLnNldFByb3BlcnR5KHJlbmRlckVsZW1lbnQsIHByb3BlcnR5TmFtZSwgcHJvcGVydHlWYWx1ZSk7XG4gIH1cblxuICBzZXRFbGVtZW50QXR0cmlidXRlKHJlbmRlckVsZW1lbnQ6IEVsZW1lbnQsIG5hbWVzcGFjZUFuZE5hbWU6IHN0cmluZywgYXR0cmlidXRlVmFsdWU/OiBzdHJpbmcpOlxuICAgICAgdm9pZCB7XG4gICAgY29uc3QgW25zLCBuYW1lXSA9IHNwbGl0TmFtZXNwYWNlKG5hbWVzcGFjZUFuZE5hbWUpO1xuICAgIGlmIChhdHRyaWJ1dGVWYWx1ZSAhPSBudWxsKSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLnNldEF0dHJpYnV0ZShyZW5kZXJFbGVtZW50LCBuYW1lLCBhdHRyaWJ1dGVWYWx1ZSwgbnMpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLnJlbW92ZUF0dHJpYnV0ZShyZW5kZXJFbGVtZW50LCBuYW1lLCBucyk7XG4gICAgfVxuICB9XG5cbiAgc2V0QmluZGluZ0RlYnVnSW5mbyhyZW5kZXJFbGVtZW50OiBFbGVtZW50LCBwcm9wZXJ0eU5hbWU6IHN0cmluZywgcHJvcGVydHlWYWx1ZTogc3RyaW5nKTogdm9pZCB7fVxuXG4gIHNldEVsZW1lbnRDbGFzcyhyZW5kZXJFbGVtZW50OiBFbGVtZW50LCBjbGFzc05hbWU6IHN0cmluZywgaXNBZGQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAoaXNBZGQpIHtcbiAgICAgIHRoaXMuZGVsZWdhdGUuYWRkQ2xhc3MocmVuZGVyRWxlbWVudCwgY2xhc3NOYW1lKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5kZWxlZ2F0ZS5yZW1vdmVDbGFzcyhyZW5kZXJFbGVtZW50LCBjbGFzc05hbWUpO1xuICAgIH1cbiAgfVxuXG4gIHNldEVsZW1lbnRTdHlsZShyZW5kZXJFbGVtZW50OiBIVE1MRWxlbWVudCwgc3R5bGVOYW1lOiBzdHJpbmcsIHN0eWxlVmFsdWU/OiBzdHJpbmcpOiB2b2lkIHtcbiAgICBpZiAoc3R5bGVWYWx1ZSAhPSBudWxsKSB7XG4gICAgICB0aGlzLmRlbGVnYXRlLnNldFN0eWxlKHJlbmRlckVsZW1lbnQsIHN0eWxlTmFtZSwgc3R5bGVWYWx1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuZGVsZWdhdGUucmVtb3ZlU3R5bGUocmVuZGVyRWxlbWVudCwgc3R5bGVOYW1lKTtcbiAgICB9XG4gIH1cblxuICBpbnZva2VFbGVtZW50TWV0aG9kKHJlbmRlckVsZW1lbnQ6IEVsZW1lbnQsIG1ldGhvZE5hbWU6IHN0cmluZywgYXJnczogYW55W10pOiB2b2lkIHtcbiAgICAocmVuZGVyRWxlbWVudCBhcyBhbnkpW21ldGhvZE5hbWVdLmFwcGx5KHJlbmRlckVsZW1lbnQsIGFyZ3MpO1xuICB9XG5cbiAgc2V0VGV4dChyZW5kZXJOb2RlOiBUZXh0LCB0ZXh0OiBzdHJpbmcpOiB2b2lkIHsgdGhpcy5kZWxlZ2F0ZS5zZXRWYWx1ZShyZW5kZXJOb2RlLCB0ZXh0KTsgfVxuXG4gIGFuaW1hdGUoKTogYW55IHsgdGhyb3cgbmV3IEVycm9yKCdSZW5kZXJlci5hbmltYXRlIGlzIG5vIGxvbmdlciBzdXBwb3J0ZWQhJyk7IH1cbn1cblxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlTmdNb2R1bGVSZWYoXG4gICAgbW9kdWxlVHlwZTogVHlwZTxhbnk+LCBwYXJlbnQ6IEluamVjdG9yLCBib290c3RyYXBDb21wb25lbnRzOiBUeXBlPGFueT5bXSxcbiAgICBkZWY6IE5nTW9kdWxlRGVmaW5pdGlvbik6IE5nTW9kdWxlUmVmPGFueT4ge1xuICByZXR1cm4gbmV3IE5nTW9kdWxlUmVmXyhtb2R1bGVUeXBlLCBwYXJlbnQsIGJvb3RzdHJhcENvbXBvbmVudHMsIGRlZik7XG59XG5cbmNsYXNzIE5nTW9kdWxlUmVmXyBpbXBsZW1lbnRzIE5nTW9kdWxlRGF0YSwgSW50ZXJuYWxOZ01vZHVsZVJlZjxhbnk+IHtcbiAgcHJpdmF0ZSBfZGVzdHJveUxpc3RlbmVyczogKCgpID0+IHZvaWQpW10gPSBbXTtcbiAgcHJpdmF0ZSBfZGVzdHJveWVkOiBib29sZWFuID0gZmFsc2U7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9wcm92aWRlcnMgITogYW55W107XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9tb2R1bGVzICE6IGFueVtdO1xuXG4gIHJlYWRvbmx5IGluamVjdG9yOiBJbmplY3RvciA9IHRoaXM7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9tb2R1bGVUeXBlOiBUeXBlPGFueT4sIHB1YmxpYyBfcGFyZW50OiBJbmplY3RvcixcbiAgICAgIHB1YmxpYyBfYm9vdHN0cmFwQ29tcG9uZW50czogVHlwZTxhbnk+W10sIHB1YmxpYyBfZGVmOiBOZ01vZHVsZURlZmluaXRpb24pIHtcbiAgICBpbml0TmdNb2R1bGUodGhpcyk7XG4gIH1cblxuICBnZXQodG9rZW46IGFueSwgbm90Rm91bmRWYWx1ZTogYW55ID0gSW5qZWN0b3IuVEhST1dfSUZfTk9UX0ZPVU5ELFxuICAgICAgaW5qZWN0RmxhZ3M6IEluamVjdEZsYWdzID0gSW5qZWN0RmxhZ3MuRGVmYXVsdCk6IGFueSB7XG4gICAgbGV0IGZsYWdzID0gRGVwRmxhZ3MuTm9uZTtcbiAgICBpZiAoaW5qZWN0RmxhZ3MgJiBJbmplY3RGbGFncy5Ta2lwU2VsZikge1xuICAgICAgZmxhZ3MgfD0gRGVwRmxhZ3MuU2tpcFNlbGY7XG4gICAgfSBlbHNlIGlmIChpbmplY3RGbGFncyAmIEluamVjdEZsYWdzLlNlbGYpIHtcbiAgICAgIGZsYWdzIHw9IERlcEZsYWdzLlNlbGY7XG4gICAgfVxuICAgIHJldHVybiByZXNvbHZlTmdNb2R1bGVEZXAoXG4gICAgICAgIHRoaXMsIHt0b2tlbjogdG9rZW4sIHRva2VuS2V5OiB0b2tlbktleSh0b2tlbiksIGZsYWdzOiBmbGFnc30sIG5vdEZvdW5kVmFsdWUpO1xuICB9XG5cbiAgZ2V0IGluc3RhbmNlKCkgeyByZXR1cm4gdGhpcy5nZXQodGhpcy5fbW9kdWxlVHlwZSk7IH1cblxuICBnZXQgY29tcG9uZW50RmFjdG9yeVJlc29sdmVyKCkgeyByZXR1cm4gdGhpcy5nZXQoQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyKTsgfVxuXG4gIGRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2Rlc3Ryb3llZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBUaGUgbmcgbW9kdWxlICR7c3RyaW5naWZ5KHRoaXMuaW5zdGFuY2UuY29uc3RydWN0b3IpfSBoYXMgYWxyZWFkeSBiZWVuIGRlc3Ryb3llZC5gKTtcbiAgICB9XG4gICAgdGhpcy5fZGVzdHJveWVkID0gdHJ1ZTtcbiAgICBjYWxsTmdNb2R1bGVMaWZlY3ljbGUodGhpcywgTm9kZUZsYWdzLk9uRGVzdHJveSk7XG4gICAgdGhpcy5fZGVzdHJveUxpc3RlbmVycy5mb3JFYWNoKChsaXN0ZW5lcikgPT4gbGlzdGVuZXIoKSk7XG4gIH1cblxuICBvbkRlc3Ryb3koY2FsbGJhY2s6ICgpID0+IHZvaWQpOiB2b2lkIHsgdGhpcy5fZGVzdHJveUxpc3RlbmVycy5wdXNoKGNhbGxiYWNrKTsgfVxufVxuIl19