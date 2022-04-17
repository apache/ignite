/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { ResourceLoader, core, DirectiveResolver, NgModuleResolver, PipeResolver } from '@angular/compiler';

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * A mock implementation of {\@link ResourceLoader} that allows outgoing requests to be mocked
 * and responded to within a single test, without going to the network.
 */
class MockResourceLoader extends ResourceLoader {
    constructor() {
        super(...arguments);
        this._expectations = [];
        this._definitions = new Map();
        this._requests = [];
    }
    /**
     * @param {?} url
     * @return {?}
     */
    get(url) {
        /** @type {?} */
        const request = new _PendingRequest(url);
        this._requests.push(request);
        return request.getPromise();
    }
    /**
     * @return {?}
     */
    hasPendingRequests() { return !!this._requests.length; }
    /**
     * Add an expectation for the given URL. Incoming requests will be checked against
     * the next expectation (in FIFO order). The `verifyNoOutstandingExpectations` method
     * can be used to check if any expectations have not yet been met.
     *
     * The response given will be returned if the expectation matches.
     * @param {?} url
     * @param {?} response
     * @return {?}
     */
    expect(url, response) {
        /** @type {?} */
        const expectation = new _Expectation(url, response);
        this._expectations.push(expectation);
    }
    /**
     * Add a definition for the given URL to return the given response. Unlike expectations,
     * definitions have no order and will satisfy any matching request at any time. Also
     * unlike expectations, unused definitions do not cause `verifyNoOutstandingExpectations`
     * to return an error.
     * @param {?} url
     * @param {?} response
     * @return {?}
     */
    when(url, response) { this._definitions.set(url, response); }
    /**
     * Process pending requests and verify there are no outstanding expectations. Also fails
     * if no requests are pending.
     * @return {?}
     */
    flush() {
        if (this._requests.length === 0) {
            throw new Error('No pending requests to flush');
        }
        do {
            this._processRequest((/** @type {?} */ (this._requests.shift())));
        } while (this._requests.length > 0);
        this.verifyNoOutstandingExpectations();
    }
    /**
     * Throw an exception if any expectations have not been satisfied.
     * @return {?}
     */
    verifyNoOutstandingExpectations() {
        if (this._expectations.length === 0)
            return;
        /** @type {?} */
        const urls = [];
        for (let i = 0; i < this._expectations.length; i++) {
            /** @type {?} */
            const expectation = this._expectations[i];
            urls.push(expectation.url);
        }
        throw new Error(`Unsatisfied requests: ${urls.join(', ')}`);
    }
    /**
     * @private
     * @param {?} request
     * @return {?}
     */
    _processRequest(request) {
        /** @type {?} */
        const url = request.url;
        if (this._expectations.length > 0) {
            /** @type {?} */
            const expectation = this._expectations[0];
            if (expectation.url == url) {
                remove(this._expectations, expectation);
                request.complete(expectation.response);
                return;
            }
        }
        if (this._definitions.has(url)) {
            /** @type {?} */
            const response = this._definitions.get(url);
            request.complete(response == null ? null : response);
            return;
        }
        throw new Error(`Unexpected request ${url}`);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockResourceLoader.prototype._expectations;
    /**
     * @type {?}
     * @private
     */
    MockResourceLoader.prototype._definitions;
    /**
     * @type {?}
     * @private
     */
    MockResourceLoader.prototype._requests;
}
class _PendingRequest {
    /**
     * @param {?} url
     */
    constructor(url) {
        this.url = url;
        this.promise = new Promise((/**
         * @param {?} res
         * @param {?} rej
         * @return {?}
         */
        (res, rej) => {
            this.resolve = res;
            this.reject = rej;
        }));
    }
    /**
     * @param {?} response
     * @return {?}
     */
    complete(response) {
        if (response == null) {
            this.reject(`Failed to load ${this.url}`);
        }
        else {
            this.resolve(response);
        }
    }
    /**
     * @return {?}
     */
    getPromise() { return this.promise; }
}
if (false) {
    /** @type {?} */
    _PendingRequest.prototype.resolve;
    /** @type {?} */
    _PendingRequest.prototype.reject;
    /** @type {?} */
    _PendingRequest.prototype.promise;
    /** @type {?} */
    _PendingRequest.prototype.url;
}
class _Expectation {
    /**
     * @param {?} url
     * @param {?} response
     */
    constructor(url, response) {
        this.url = url;
        this.response = response;
    }
}
if (false) {
    /** @type {?} */
    _Expectation.prototype.url;
    /** @type {?} */
    _Expectation.prototype.response;
}
/**
 * @template T
 * @param {?} list
 * @param {?} el
 * @return {?}
 */
function remove(list, el) {
    /** @type {?} */
    const index = list.indexOf(el);
    if (index > -1) {
        list.splice(index, 1);
    }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class MockSchemaRegistry {
    /**
     * @param {?} existingProperties
     * @param {?} attrPropMapping
     * @param {?} existingElements
     * @param {?} invalidProperties
     * @param {?} invalidAttributes
     */
    constructor(existingProperties, attrPropMapping, existingElements, invalidProperties, invalidAttributes) {
        this.existingProperties = existingProperties;
        this.attrPropMapping = attrPropMapping;
        this.existingElements = existingElements;
        this.invalidProperties = invalidProperties;
        this.invalidAttributes = invalidAttributes;
    }
    /**
     * @param {?} tagName
     * @param {?} property
     * @param {?} schemas
     * @return {?}
     */
    hasProperty(tagName, property, schemas) {
        /** @type {?} */
        const value = this.existingProperties[property];
        return value === void 0 ? true : value;
    }
    /**
     * @param {?} tagName
     * @param {?} schemaMetas
     * @return {?}
     */
    hasElement(tagName, schemaMetas) {
        /** @type {?} */
        const value = this.existingElements[tagName.toLowerCase()];
        return value === void 0 ? true : value;
    }
    /**
     * @return {?}
     */
    allKnownElementNames() { return Object.keys(this.existingElements); }
    /**
     * @param {?} selector
     * @param {?} property
     * @param {?} isAttribute
     * @return {?}
     */
    securityContext(selector, property, isAttribute) {
        return core.SecurityContext.NONE;
    }
    /**
     * @param {?} attrName
     * @return {?}
     */
    getMappedPropName(attrName) { return this.attrPropMapping[attrName] || attrName; }
    /**
     * @return {?}
     */
    getDefaultComponentElementName() { return 'ng-component'; }
    /**
     * @param {?} name
     * @return {?}
     */
    validateProperty(name) {
        if (this.invalidProperties.indexOf(name) > -1) {
            return { error: true, msg: `Binding to property '${name}' is disallowed for security reasons` };
        }
        else {
            return { error: false };
        }
    }
    /**
     * @param {?} name
     * @return {?}
     */
    validateAttribute(name) {
        if (this.invalidAttributes.indexOf(name) > -1) {
            return {
                error: true,
                msg: `Binding to attribute '${name}' is disallowed for security reasons`
            };
        }
        else {
            return { error: false };
        }
    }
    /**
     * @param {?} propName
     * @return {?}
     */
    normalizeAnimationStyleProperty(propName) { return propName; }
    /**
     * @param {?} camelCaseProp
     * @param {?} userProvidedProp
     * @param {?} val
     * @return {?}
     */
    normalizeAnimationStyleValue(camelCaseProp, userProvidedProp, val) {
        return { error: (/** @type {?} */ (null)), value: val.toString() };
    }
}
if (false) {
    /** @type {?} */
    MockSchemaRegistry.prototype.existingProperties;
    /** @type {?} */
    MockSchemaRegistry.prototype.attrPropMapping;
    /** @type {?} */
    MockSchemaRegistry.prototype.existingElements;
    /** @type {?} */
    MockSchemaRegistry.prototype.invalidProperties;
    /** @type {?} */
    MockSchemaRegistry.prototype.invalidAttributes;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * An implementation of {\@link DirectiveResolver} that allows overriding
 * various properties of directives.
 */
class MockDirectiveResolver extends DirectiveResolver {
    /**
     * @param {?} reflector
     */
    constructor(reflector) {
        super(reflector);
        this._directives = new Map();
    }
    /**
     * @param {?} type
     * @param {?=} throwIfNotFound
     * @return {?}
     */
    resolve(type, throwIfNotFound = true) {
        return this._directives.get(type) || super.resolve(type, throwIfNotFound);
    }
    /**
     * Overrides the {\@link core.Directive} for a directive.
     * @param {?} type
     * @param {?} metadata
     * @return {?}
     */
    setDirective(type, metadata) {
        this._directives.set(type, metadata);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockDirectiveResolver.prototype._directives;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class MockNgModuleResolver extends NgModuleResolver {
    /**
     * @param {?} reflector
     */
    constructor(reflector) {
        super(reflector);
        this._ngModules = new Map();
    }
    /**
     * Overrides the {\@link NgModule} for a module.
     * @param {?} type
     * @param {?} metadata
     * @return {?}
     */
    setNgModule(type, metadata) {
        this._ngModules.set(type, metadata);
    }
    /**
     * Returns the {\@link NgModule} for a module:
     * - Set the {\@link NgModule} to the overridden view when it exists or fallback to the
     * default
     * `NgModuleResolver`, see `setNgModule`.
     * @param {?} type
     * @param {?=} throwIfNotFound
     * @return {?}
     */
    resolve(type, throwIfNotFound = true) {
        return this._ngModules.get(type) || (/** @type {?} */ (super.resolve(type, throwIfNotFound)));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockNgModuleResolver.prototype._ngModules;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class MockPipeResolver extends PipeResolver {
    /**
     * @param {?} refector
     */
    constructor(refector) {
        super(refector);
        this._pipes = new Map();
    }
    /**
     * Overrides the {\@link Pipe} for a pipe.
     * @param {?} type
     * @param {?} metadata
     * @return {?}
     */
    setPipe(type, metadata) { this._pipes.set(type, metadata); }
    /**
     * Returns the {\@link Pipe} for a pipe:
     * - Set the {\@link Pipe} to the overridden view when it exists or fallback to the
     * default
     * `PipeResolver`, see `setPipe`.
     * @param {?} type
     * @param {?=} throwIfNotFound
     * @return {?}
     */
    resolve(type, throwIfNotFound = true) {
        /** @type {?} */
        let metadata = this._pipes.get(type);
        if (!metadata) {
            metadata = (/** @type {?} */ (super.resolve(type, throwIfNotFound)));
        }
        return metadata;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockPipeResolver.prototype._pipes;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * Generated bundle index. Do not edit.
 */

export { MockResourceLoader, MockSchemaRegistry, MockDirectiveResolver, MockNgModuleResolver, MockPipeResolver };
//# sourceMappingURL=testing.js.map
