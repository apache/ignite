/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { ComponentRef } from '@angular/core';
import { DebugElement } from '@angular/core';
import { DebugNode } from '@angular/core';
import { ErrorHandler } from '@angular/core';
import { GetTestability } from '@angular/core';
import { InjectionToken } from '@angular/core';
import { Injector } from '@angular/core';
import { LocationChangeListener } from '@angular/common';
import { ModuleWithProviders } from '@angular/core';
import { NgProbeToken } from '@angular/core';
import { NgZone } from '@angular/core';
import { OnDestroy } from '@angular/core';
import { PlatformLocation } from '@angular/common';
import { PlatformRef } from '@angular/core';
import { Predicate } from '@angular/core';
import { Provider } from '@angular/core';
import { Renderer2 } from '@angular/core';
import { RendererFactory2 } from '@angular/core';
import { RendererType2 } from '@angular/core';
import { Sanitizer } from '@angular/core';
import { SecurityContext } from '@angular/core';
import { StaticProvider } from '@angular/core';
import { Testability } from '@angular/core';
import { TestabilityRegistry } from '@angular/core';
import { Type } from '@angular/core';
import { Version } from '@angular/core';
import { ɵConsole } from '@angular/core';

/**
 * Exports required infrastructure for all Angular apps.
 * Included by default in all Angular apps created with the CLI
 * `new` command.
 * Re-exports `CommonModule` and `ApplicationModule`, making their
 * exports and providers available to all apps.
 *
 * @publicApi
 */
export declare class BrowserModule {
    constructor(parentModule: BrowserModule | null);
    /**
     * Configures a browser-based app to transition from a server-rendered app, if
     * one is present on the page.
     *
     * @param params An object containing an identifier for the app to transition.
     * The ID must match between the client and server versions of the app.
     * @returns The reconfigured `BrowserModule` to import into the app's root `AppModule`.
     */
    static withServerTransition(params: {
        appId: string;
    }): ModuleWithProviders<BrowserModule>;
}

/**
 * NgModule to install on the client side while using the `TransferState` to transfer state from
 * server to client.
 *
 * @publicApi
 */
export declare class BrowserTransferStateModule {
}

/**
 * Predicates for use with {@link DebugElement}'s query functions.
 *
 * @publicApi
 */
export declare class By {
    /**
     * Match all nodes.
     *
     * @usageNotes
     * ### Example
     *
     * {@example platform-browser/dom/debug/ts/by/by.ts region='by_all'}
     */
    static all(): Predicate<DebugNode>;
    /**
     * Match elements by the given CSS selector.
     *
     * @usageNotes
     * ### Example
     *
     * {@example platform-browser/dom/debug/ts/by/by.ts region='by_css'}
     */
    static css(selector: string): Predicate<DebugElement>;
    /**
     * Match nodes that have the given directive present.
     *
     * @usageNotes
     * ### Example
     *
     * {@example platform-browser/dom/debug/ts/by/by.ts region='by_directive'}
     */
    static directive(type: Type<any>): Predicate<DebugNode>;
}

/**
 * Disables Angular tools.
 *
 * @publicApi
 */
export declare function disableDebugTools(): void;

/**
 * DomSanitizer helps preventing Cross Site Scripting Security bugs (XSS) by sanitizing
 * values to be safe to use in the different DOM contexts.
 *
 * For example, when binding a URL in an `<a [href]="someValue">` hyperlink, `someValue` will be
 * sanitized so that an attacker cannot inject e.g. a `javascript:` URL that would execute code on
 * the website.
 *
 * In specific situations, it might be necessary to disable sanitization, for example if the
 * application genuinely needs to produce a `javascript:` style link with a dynamic value in it.
 * Users can bypass security by constructing a value with one of the `bypassSecurityTrust...`
 * methods, and then binding to that value from the template.
 *
 * These situations should be very rare, and extraordinary care must be taken to avoid creating a
 * Cross Site Scripting (XSS) security bug!
 *
 * When using `bypassSecurityTrust...`, make sure to call the method as early as possible and as
 * close as possible to the source of the value, to make it easy to verify no security bug is
 * created by its use.
 *
 * It is not required (and not recommended) to bypass security if the value is safe, e.g. a URL that
 * does not start with a suspicious protocol, or an HTML snippet that does not contain dangerous
 * code. The sanitizer leaves safe values intact.
 *
 * @security Calling any of the `bypassSecurityTrust...` APIs disables Angular's built-in
 * sanitization for the value passed in. Carefully check and audit all values and code paths going
 * into this call. Make sure any user data is appropriately escaped for this security context.
 * For more detail, see the [Security Guide](http://g.co/ng/security).
 *
 * @publicApi
 */
export declare abstract class DomSanitizer implements Sanitizer {
    /**
     * Sanitizes a value for use in the given SecurityContext.
     *
     * If value is trusted for the context, this method will unwrap the contained safe value and use
     * it directly. Otherwise, value will be sanitized to be safe in the given context, for example
     * by replacing URLs that have an unsafe protocol part (such as `javascript:`). The implementation
     * is responsible to make sure that the value can definitely be safely used in the given context.
     */
    abstract sanitize(context: SecurityContext, value: SafeValue | string | null): string | null;
    /**
     * Bypass security and trust the given value to be safe HTML. Only use this when the bound HTML
     * is unsafe (e.g. contains `<script>` tags) and the code should be executed. The sanitizer will
     * leave safe HTML intact, so in most situations this method should not be used.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     */
    abstract bypassSecurityTrustHtml(value: string): SafeHtml;
    /**
     * Bypass security and trust the given value to be safe style value (CSS).
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     */
    abstract bypassSecurityTrustStyle(value: string): SafeStyle;
    /**
     * Bypass security and trust the given value to be safe JavaScript.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     */
    abstract bypassSecurityTrustScript(value: string): SafeScript;
    /**
     * Bypass security and trust the given value to be a safe style URL, i.e. a value that can be used
     * in hyperlinks or `<img src>`.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     */
    abstract bypassSecurityTrustUrl(value: string): SafeUrl;
    /**
     * Bypass security and trust the given value to be a safe resource URL, i.e. a location that may
     * be used to load executable code from, like `<script src>`, or `<iframe src>`.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     */
    abstract bypassSecurityTrustResourceUrl(value: string): SafeResourceUrl;
}

/**
 * Enabled Angular debug tools that are accessible via your browser's
 * developer console.
 *
 * Usage:
 *
 * 1. Open developer console (e.g. in Chrome Ctrl + Shift + j)
 * 1. Type `ng.` (usually the console will show auto-complete suggestion)
 * 1. Try the change detection profiler `ng.profiler.timeChangeDetection()`
 *    then hit Enter.
 *
 * @publicApi
 */
export declare function enableDebugTools<T>(ref: ComponentRef<T>): ComponentRef<T>;

/**
 * The injection token for the event-manager plug-in service.
 *
 * @publicApi
 */
export declare const EVENT_MANAGER_PLUGINS: InjectionToken<ɵangular_packages_platform_browser_platform_browser_g[]>;

/**
 * An injectable service that provides event management for Angular
 * through a browser plug-in.
 *
 * @publicApi
 */
export declare class EventManager {
    private _zone;
    private _plugins;
    private _eventNameToPlugin;
    /**
     * Initializes an instance of the event-manager service.
     */
    constructor(plugins: ɵangular_packages_platform_browser_platform_browser_g[], _zone: NgZone);
    /**
     * Registers a handler for a specific element and event.
     *
     * @param element The HTML element to receive event notifications.
     * @param eventName The name of the event to listen for.
     * @param handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @returns  A callback function that can be used to remove the handler.
     */
    addEventListener(element: HTMLElement, eventName: string, handler: Function): Function;
    /**
     * Registers a global handler for an event in a target view.
     *
     * @param target A target for global event notifications. One of "window", "document", or "body".
     * @param eventName The name of the event to listen for.
     * @param handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @returns A callback function that can be used to remove the handler.
     */
    addGlobalEventListener(target: string, eventName: string, handler: Function): Function;
    /**
     * Retrieves the compilation zone in which event listeners are registered.
     */
    getZone(): NgZone;
}

/**
 * DI token for providing [HammerJS](http://hammerjs.github.io/) support to Angular.
 * @see `HammerGestureConfig`
 *
 * @publicApi
 */
export declare const HAMMER_GESTURE_CONFIG: InjectionToken<HammerGestureConfig>;

/**
 * Injection token used to provide a {@link HammerLoader} to Angular.
 *
 * @publicApi
 */
export declare const HAMMER_LOADER: InjectionToken<HammerLoader>;

/**
 * An injectable [HammerJS Manager](http://hammerjs.github.io/api/#hammer.manager)
 * for gesture recognition. Configures specific event recognition.
 * @publicApi
 */
export declare class HammerGestureConfig {
    /**
     * A set of supported event names for gestures to be used in Angular.
     * Angular supports all built-in recognizers, as listed in
     * [HammerJS documentation](http://hammerjs.github.io/).
     */
    events: string[];
    /**
    * Maps gesture event names to a set of configuration options
    * that specify overrides to the default values for specific properties.
    *
    * The key is a supported event name to be configured,
    * and the options object contains a set of properties, with override values
    * to be applied to the named recognizer event.
    * For example, to disable recognition of the rotate event, specify
    *  `{"rotate": {"enable": false}}`.
    *
    * Properties that are not present take the HammerJS default values.
    * For information about which properties are supported for which events,
    * and their allowed and default values, see
    * [HammerJS documentation](http://hammerjs.github.io/).
    *
    */
    overrides: {
        [key: string]: Object;
    };
    /**
     * Properties whose default values can be overridden for a given event.
     * Different sets of properties apply to different events.
     * For information about which properties are supported for which events,
     * and their allowed and default values, see
     * [HammerJS documentation](http://hammerjs.github.io/).
     */
    options?: {
        cssProps?: any;
        domEvents?: boolean;
        enable?: boolean | ((manager: any) => boolean);
        preset?: any[];
        touchAction?: string;
        recognizers?: any[];
        inputClass?: any;
        inputTarget?: EventTarget;
    };
    /**
     * Creates a [HammerJS Manager](http://hammerjs.github.io/api/#hammer.manager)
     * and attaches it to a given HTML element.
     * @param element The element that will recognize gestures.
     * @returns A HammerJS event-manager object.
     */
    buildHammer(element: HTMLElement): HammerInstance;
}

declare interface HammerInstance {
    on(eventName: string, callback?: Function): void;
    off(eventName: string, callback?: Function): void;
    destroy?(): void;
}

/**
 * Function that loads HammerJS, returning a promise that is resolved once HammerJs is loaded.
 *
 * @publicApi
 */
export declare type HammerLoader = () => Promise<void>;

/**
 * Create a `StateKey<T>` that can be used to store value of type T with `TransferState`.
 *
 * Example:
 *
 * ```
 * const COUNTER_KEY = makeStateKey<number>('counter');
 * let value = 10;
 *
 * transferState.set(COUNTER_KEY, value);
 * ```
 *
 * @publicApi
 */
export declare function makeStateKey<T = void>(key: string): StateKey<T>;

/**
 * A service that can be used to get and add meta tags.
 *
 * @publicApi
 */
export declare class Meta {
    private _doc;
    private _dom;
    constructor(_doc: any);
    addTag(tag: MetaDefinition, forceCreation?: boolean): HTMLMetaElement | null;
    addTags(tags: MetaDefinition[], forceCreation?: boolean): HTMLMetaElement[];
    getTag(attrSelector: string): HTMLMetaElement | null;
    getTags(attrSelector: string): HTMLMetaElement[];
    updateTag(tag: MetaDefinition, selector?: string): HTMLMetaElement | null;
    removeTag(attrSelector: string): void;
    removeTagElement(meta: HTMLMetaElement): void;
    private _getOrCreateElement;
    private _setMetaElementAttributes;
    private _parseSelector;
    private _containsAttributes;
}


/**
 * Represents a meta element.
 *
 * @publicApi
 */
export declare type MetaDefinition = {
    charset?: string;
    content?: string;
    httpEquiv?: string;
    id?: string;
    itemprop?: string;
    name?: string;
    property?: string;
    scheme?: string;
    url?: string;
} & {
    [prop: string]: string;
};

/**
 * @publicApi
 */
export declare const platformBrowser: (extraProviders?: StaticProvider[]) => PlatformRef;

/**
 * Marker interface for a value that's safe to use as HTML.
 *
 * @publicApi
 */
export declare interface SafeHtml extends SafeValue {
}

/**
 * Marker interface for a value that's safe to use as a URL to load executable code from.
 *
 * @publicApi
 */
export declare interface SafeResourceUrl extends SafeValue {
}

/**
 * Marker interface for a value that's safe to use as JavaScript.
 *
 * @publicApi
 */
export declare interface SafeScript extends SafeValue {
}

/**
 * Marker interface for a value that's safe to use as style (CSS).
 *
 * @publicApi
 */
export declare interface SafeStyle extends SafeValue {
}

/**
 * Marker interface for a value that's safe to use as a URL linking to a document.
 *
 * @publicApi
 */
export declare interface SafeUrl extends SafeValue {
}

/**
 * Marker interface for a value that's safe to use in a particular context.
 *
 * @publicApi
 */
export declare interface SafeValue {
}

/**
 * A type-safe key to use with `TransferState`.
 *
 * Example:
 *
 * ```
 * const COUNTER_KEY = makeStateKey<number>('counter');
 * let value = 10;
 *
 * transferState.set(COUNTER_KEY, value);
 * ```
 *
 * @publicApi
 */
export declare type StateKey<T> = string & {
    __not_a_string: never;
};

/**
 * A service that can be used to get and set the title of a current HTML document.
 *
 * Since an Angular application can't be bootstrapped on the entire HTML document (`<html>` tag)
 * it is not possible to bind to the `text` property of the `HTMLTitleElement` elements
 * (representing the `<title>` tag). Instead, this service can be used to set and get the current
 * title value.
 *
 * @publicApi
 */
export declare class Title {
    private _doc;
    constructor(_doc: any);
    /**
     * Get the title of the current HTML document.
     */
    getTitle(): string;
    /**
     * Set the title of the current HTML document.
     * @param newTitle
     */
    setTitle(newTitle: string): void;
}

/**
 * A key value store that is transferred from the application on the server side to the application
 * on the client side.
 *
 * `TransferState` will be available as an injectable token. To use it import
 * `ServerTransferStateModule` on the server and `BrowserTransferStateModule` on the client.
 *
 * The values in the store are serialized/deserialized using JSON.stringify/JSON.parse. So only
 * boolean, number, string, null and non-class objects will be serialized and deserialzied in a
 * non-lossy manner.
 *
 * @publicApi
 */
export declare class TransferState {
    private store;
    private onSerializeCallbacks;
    /**
     * Get the value corresponding to a key. Return `defaultValue` if key is not found.
     */
    get<T>(key: StateKey<T>, defaultValue: T): T;
    /**
     * Set the value corresponding to a key.
     */
    set<T>(key: StateKey<T>, value: T): void;
    /**
     * Remove a key from the store.
     */
    remove<T>(key: StateKey<T>): void;
    /**
     * Test whether a key exists in the store.
     */
    hasKey<T>(key: StateKey<T>): boolean;
    /**
     * Register a callback to provide the value for a key when `toJson` is called.
     */
    onSerialize<T>(key: StateKey<T>, callback: () => T): void;
    /**
     * Serialize the current state of the store to JSON.
     */
    toJson(): string;
}

/**
 * @publicApi
 */
export declare const VERSION: Version;

export declare function ɵangular_packages_platform_browser_platform_browser_a(): ErrorHandler;

export declare function ɵangular_packages_platform_browser_platform_browser_b(): any;

export declare const ɵangular_packages_platform_browser_platform_browser_c: StaticProvider[];

/**
 * Factory to create Meta service.
 */
export declare function ɵangular_packages_platform_browser_platform_browser_d(): Meta;


/**
 * Factory to create Title service.
 */
export declare function ɵangular_packages_platform_browser_platform_browser_e(): Title;

export declare function ɵangular_packages_platform_browser_platform_browser_f(doc: Document, appId: string): TransferState;

export declare abstract class ɵangular_packages_platform_browser_platform_browser_g {
    private _doc;
    constructor(_doc: any);
    manager: EventManager;
    abstract supports(eventName: string): boolean;
    abstract addEventListener(element: HTMLElement, eventName: string, handler: Function): Function;
    addGlobalEventListener(element: string, eventName: string, handler: Function): Function;
}

export declare function ɵangular_packages_platform_browser_platform_browser_h(transitionId: string, document: any, injector: Injector): () => void;

export declare const ɵangular_packages_platform_browser_platform_browser_i: StaticProvider[];

export declare function ɵangular_packages_platform_browser_platform_browser_j(coreTokens: NgProbeToken[]): any;

/**
 * Providers which support debugging Angular applications (e.g. via `ng.probe`).
 */
export declare const ɵangular_packages_platform_browser_platform_browser_k: Provider[];

/**
 * Provides DOM operations in any browser environment.
 *
 * @security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 */
export declare abstract class ɵangular_packages_platform_browser_platform_browser_l extends ɵDomAdapter {
    private _animationPrefix;
    private _transitionEnd;
    constructor();
    getDistributedNodes(el: HTMLElement): Node[];
    resolveAndSetHref(el: HTMLAnchorElement, baseUrl: string, href: string): void;
    supportsDOMEvents(): boolean;
    supportsNativeShadowDOM(): boolean;
    getAnimationPrefix(): string;
    getTransitionEnd(): string;
    supportsAnimation(): boolean;
}

/**
 * @security Replacing built-in sanitization providers exposes the application to XSS risks.
 * Attacker-controlled data introduced by an unsanitized provider could expose your
 * application to XSS risks. For more detail, see the [Security Guide](http://g.co/ng/security).
 * @publicApi
 */
export declare const ɵBROWSER_SANITIZATION_PROVIDERS: StaticProvider[];

/**
 * A `DomAdapter` powered by full browser DOM APIs.
 *
 * @security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 */
export declare class ɵBrowserDomAdapter extends ɵangular_packages_platform_browser_platform_browser_l {
    parse(templateHtml: string): void;
    static makeCurrent(): void;
    hasProperty(element: Node, name: string): boolean;
    setProperty(el: Node, name: string, value: any): void;
    getProperty(el: Node, name: string): any;
    invoke(el: Node, methodName: string, args: any[]): any;
    logError(error: string): void;
    log(error: string): void;
    logGroup(error: string): void;
    logGroupEnd(): void;
    readonly attrToPropMap: any;
    contains(nodeA: any, nodeB: any): boolean;
    querySelector(el: HTMLElement, selector: string): any;
    querySelectorAll(el: any, selector: string): any[];
    on(el: Node, evt: any, listener: any): void;
    onAndCancel(el: Node, evt: any, listener: any): Function;
    dispatchEvent(el: Node, evt: any): void;
    createMouseEvent(eventType: string): MouseEvent;
    createEvent(eventType: any): Event;
    preventDefault(evt: Event): void;
    isPrevented(evt: Event): boolean;
    getInnerHTML(el: HTMLElement): string;
    getTemplateContent(el: Node): Node | null;
    getOuterHTML(el: HTMLElement): string;
    nodeName(node: Node): string;
    nodeValue(node: Node): string | null;
    type(node: HTMLInputElement): string;
    content(node: Node): Node;
    firstChild(el: Node): Node | null;
    nextSibling(el: Node): Node | null;
    parentElement(el: Node): Node | null;
    childNodes(el: any): Node[];
    childNodesAsList(el: Node): any[];
    clearNodes(el: Node): void;
    appendChild(el: Node, node: Node): void;
    removeChild(el: Node, node: Node): void;
    replaceChild(el: Node, newChild: Node, oldChild: Node): void;
    remove(node: Node): Node;
    insertBefore(parent: Node, ref: Node, node: Node): void;
    insertAllBefore(parent: Node, ref: Node, nodes: Node[]): void;
    insertAfter(parent: Node, ref: Node, node: any): void;
    setInnerHTML(el: Element, value: string): void;
    getText(el: Node): string | null;
    setText(el: Node, value: string): void;
    getValue(el: any): string;
    setValue(el: any, value: string): void;
    getChecked(el: any): boolean;
    setChecked(el: any, value: boolean): void;
    createComment(text: string): Comment;
    createTemplate(html: any): HTMLElement;
    createElement(tagName: string, doc?: Document): HTMLElement;
    createElementNS(ns: string, tagName: string, doc?: Document): Element;
    createTextNode(text: string, doc?: Document): Text;
    createScriptTag(attrName: string, attrValue: string, doc?: Document): HTMLScriptElement;
    createStyleElement(css: string, doc?: Document): HTMLStyleElement;
    createShadowRoot(el: HTMLElement): DocumentFragment;
    getShadowRoot(el: HTMLElement): DocumentFragment;
    getHost(el: HTMLElement): HTMLElement;
    clone(node: Node): Node;
    getElementsByClassName(element: any, name: string): HTMLElement[];
    getElementsByTagName(element: any, name: string): HTMLElement[];
    classList(element: any): any[];
    addClass(element: any, className: string): void;
    removeClass(element: any, className: string): void;
    hasClass(element: any, className: string): boolean;
    setStyle(element: any, styleName: string, styleValue: string): void;
    removeStyle(element: any, stylename: string): void;
    getStyle(element: any, stylename: string): string;
    hasStyle(element: any, styleName: string, styleValue?: string | null): boolean;
    tagName(element: any): string;
    attributeMap(element: any): Map<string, string>;
    hasAttribute(element: Element, attribute: string): boolean;
    hasAttributeNS(element: Element, ns: string, attribute: string): boolean;
    getAttribute(element: Element, attribute: string): string | null;
    getAttributeNS(element: Element, ns: string, name: string): string | null;
    setAttribute(element: Element, name: string, value: string): void;
    setAttributeNS(element: Element, ns: string, name: string, value: string): void;
    removeAttribute(element: Element, attribute: string): void;
    removeAttributeNS(element: Element, ns: string, name: string): void;
    templateAwareRoot(el: Node): any;
    createHtmlDocument(): HTMLDocument;
    getDefaultDocument(): Document;
    getBoundingClientRect(el: Element): any;
    getTitle(doc: Document): string;
    setTitle(doc: Document, newTitle: string): void;
    elementMatches(n: any, selector: string): boolean;
    isTemplateElement(el: Node): boolean;
    isTextNode(node: Node): boolean;
    isCommentNode(node: Node): boolean;
    isElementNode(node: Node): boolean;
    hasShadowRoot(node: any): boolean;
    isShadowRoot(node: any): boolean;
    importIntoDoc(node: Node): any;
    adoptNode(node: Node): any;
    getHref(el: Element): string;
    getEventKey(event: any): string;
    getGlobalEventTarget(doc: Document, target: string): EventTarget | null;
    getHistory(): History;
    getLocation(): Location;
    getBaseHref(doc: Document): string | null;
    resetBaseElement(): void;
    getUserAgent(): string;
    setData(element: Element, name: string, value: string): void;
    getData(element: Element, name: string): string | null;
    getComputedStyle(element: any): any;
    supportsWebAnimation(): boolean;
    performanceNow(): number;
    supportsCookies(): boolean;
    getCookie(name: string): string | null;
    setCookie(name: string, value: string): void;
}

export declare class ɵBrowserGetTestability implements GetTestability {
    static init(): void;
    addToWindow(registry: TestabilityRegistry): void;
    findTestabilityInTree(registry: TestabilityRegistry, elem: any, findInAncestors: boolean): Testability | null;
}

/**
 * `PlatformLocation` encapsulates all of the direct calls to platform APIs.
 * This class should not be used directly by an application developer. Instead, use
 * {@link Location}.
 */
export declare class ɵBrowserPlatformLocation extends PlatformLocation {
    private _doc;
    readonly location: Location;
    private _history;
    constructor(_doc: any);
    getBaseHrefFromDOM(): string;
    onPopState(fn: LocationChangeListener): void;
    onHashChange(fn: LocationChangeListener): void;
    readonly href: string;
    readonly protocol: string;
    readonly hostname: string;
    readonly port: string;
    pathname: string;
    readonly search: string;
    readonly hash: string;
    pushState(state: any, title: string, url: string): void;
    replaceState(state: any, title: string, url: string): void;
    forward(): void;
    back(): void;
    getState(): unknown;
}

/**
 * Provides DOM operations in an environment-agnostic way.
 *
 * @security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 */
export declare abstract class ɵDomAdapter {
    resourceLoaderType: Type<any>;
    abstract hasProperty(element: any, name: string): boolean;
    abstract setProperty(el: Element, name: string, value: any): any;
    abstract getProperty(el: Element, name: string): any;
    abstract invoke(el: Element, methodName: string, args: any[]): any;
    abstract logError(error: any): any;
    abstract log(error: any): any;
    abstract logGroup(error: any): any;
    abstract logGroupEnd(): any;
    /**
     * Maps attribute names to their corresponding property names for cases
     * where attribute name doesn't match property name.
     */
    attrToPropMap: {
        [key: string]: string;
    };
    abstract contains(nodeA: any, nodeB: any): boolean;
    abstract parse(templateHtml: string): any;
    abstract querySelector(el: any, selector: string): any;
    abstract querySelectorAll(el: any, selector: string): any[];
    abstract on(el: any, evt: any, listener: any): any;
    abstract onAndCancel(el: any, evt: any, listener: any): Function;
    abstract dispatchEvent(el: any, evt: any): any;
    abstract createMouseEvent(eventType: any): any;
    abstract createEvent(eventType: string): any;
    abstract preventDefault(evt: any): any;
    abstract isPrevented(evt: any): boolean;
    abstract getInnerHTML(el: any): string;
    /** Returns content if el is a <template> element, null otherwise. */
    abstract getTemplateContent(el: any): any;
    abstract getOuterHTML(el: any): string;
    abstract nodeName(node: any): string;
    abstract nodeValue(node: any): string | null;
    abstract type(node: any): string;
    abstract content(node: any): any;
    abstract firstChild(el: any): Node | null;
    abstract nextSibling(el: any): Node | null;
    abstract parentElement(el: any): Node | null;
    abstract childNodes(el: any): Node[];
    abstract childNodesAsList(el: any): Node[];
    abstract clearNodes(el: any): any;
    abstract appendChild(el: any, node: any): any;
    abstract removeChild(el: any, node: any): any;
    abstract replaceChild(el: any, newNode: any, oldNode: any): any;
    abstract remove(el: any): Node;
    abstract insertBefore(parent: any, ref: any, node: any): any;
    abstract insertAllBefore(parent: any, ref: any, nodes: any): any;
    abstract insertAfter(parent: any, el: any, node: any): any;
    abstract setInnerHTML(el: any, value: any): any;
    abstract getText(el: any): string | null;
    abstract setText(el: any, value: string): any;
    abstract getValue(el: any): string;
    abstract setValue(el: any, value: string): any;
    abstract getChecked(el: any): boolean;
    abstract setChecked(el: any, value: boolean): any;
    abstract createComment(text: string): any;
    abstract createTemplate(html: any): HTMLElement;
    abstract createElement(tagName: any, doc?: any): HTMLElement;
    abstract createElementNS(ns: string, tagName: string, doc?: any): Element;
    abstract createTextNode(text: string, doc?: any): Text;
    abstract createScriptTag(attrName: string, attrValue: string, doc?: any): HTMLElement;
    abstract createStyleElement(css: string, doc?: any): HTMLStyleElement;
    abstract createShadowRoot(el: any): any;
    abstract getShadowRoot(el: any): any;
    abstract getHost(el: any): any;
    abstract getDistributedNodes(el: any): Node[];
    abstract clone(node: Node): Node;
    abstract getElementsByClassName(element: any, name: string): HTMLElement[];
    abstract getElementsByTagName(element: any, name: string): HTMLElement[];
    abstract classList(element: any): any[];
    abstract addClass(element: any, className: string): any;
    abstract removeClass(element: any, className: string): any;
    abstract hasClass(element: any, className: string): boolean;
    abstract setStyle(element: any, styleName: string, styleValue: string): any;
    abstract removeStyle(element: any, styleName: string): any;
    abstract getStyle(element: any, styleName: string): string;
    abstract hasStyle(element: any, styleName: string, styleValue?: string): boolean;
    abstract tagName(element: any): string;
    abstract attributeMap(element: any): Map<string, string>;
    abstract hasAttribute(element: any, attribute: string): boolean;
    abstract hasAttributeNS(element: any, ns: string, attribute: string): boolean;
    abstract getAttribute(element: any, attribute: string): string | null;
    abstract getAttributeNS(element: any, ns: string, attribute: string): string | null;
    abstract setAttribute(element: any, name: string, value: string): any;
    abstract setAttributeNS(element: any, ns: string, name: string, value: string): any;
    abstract removeAttribute(element: any, attribute: string): any;
    abstract removeAttributeNS(element: any, ns: string, attribute: string): any;
    abstract templateAwareRoot(el: any): any;
    abstract createHtmlDocument(): HTMLDocument;
    abstract getDefaultDocument(): Document;
    abstract getBoundingClientRect(el: any): any;
    abstract getTitle(doc: Document): string;
    abstract setTitle(doc: Document, newTitle: string): any;
    abstract elementMatches(n: any, selector: string): boolean;
    abstract isTemplateElement(el: any): boolean;
    abstract isTextNode(node: any): boolean;
    abstract isCommentNode(node: any): boolean;
    abstract isElementNode(node: any): boolean;
    abstract hasShadowRoot(node: any): boolean;
    abstract isShadowRoot(node: any): boolean;
    abstract importIntoDoc(node: Node): Node;
    abstract adoptNode(node: Node): Node;
    abstract getHref(element: any): string;
    abstract getEventKey(event: any): string;
    abstract resolveAndSetHref(element: any, baseUrl: string, href: string): any;
    abstract supportsDOMEvents(): boolean;
    abstract supportsNativeShadowDOM(): boolean;
    abstract getGlobalEventTarget(doc: Document, target: string): any;
    abstract getHistory(): History;
    abstract getLocation(): Location;
    abstract getBaseHref(doc: Document): string | null;
    abstract resetBaseElement(): void;
    abstract getUserAgent(): string;
    abstract setData(element: any, name: string, value: string): any;
    abstract getComputedStyle(element: any): any;
    abstract getData(element: any, name: string): string | null;
    abstract supportsWebAnimation(): boolean;
    abstract performanceNow(): number;
    abstract getAnimationPrefix(): string;
    abstract getTransitionEnd(): string;
    abstract supportsAnimation(): boolean;
    abstract supportsCookies(): boolean;
    abstract getCookie(name: string): string | null;
    abstract setCookie(name: string, value: string): any;
}

export declare class ɵDomEventsPlugin extends ɵangular_packages_platform_browser_platform_browser_g {
    private ngZone;
    constructor(doc: any, ngZone: NgZone, platformId: {} | null);
    private patchEvent;
    supports(eventName: string): boolean;
    addEventListener(element: HTMLElement, eventName: string, handler: Function): Function;
    removeEventListener(target: any, eventName: string, callback: Function): void;
}

export declare class ɵDomRendererFactory2 implements RendererFactory2 {
    private eventManager;
    private sharedStylesHost;
    private appId;
    private rendererByCompId;
    private defaultRenderer;
    constructor(eventManager: EventManager, sharedStylesHost: ɵDomSharedStylesHost, appId: string);
    createRenderer(element: any, type: RendererType2 | null): Renderer2;
    begin(): void;
    end(): void;
}

export declare class ɵDomSanitizerImpl extends DomSanitizer {
    private _doc;
    constructor(_doc: any);
    sanitize(ctx: SecurityContext, value: SafeValue | string | null): string | null;
    private checkNotSafeValue;
    bypassSecurityTrustHtml(value: string): SafeHtml;
    bypassSecurityTrustStyle(value: string): SafeStyle;
    bypassSecurityTrustScript(value: string): SafeScript;
    bypassSecurityTrustUrl(value: string): SafeUrl;
    bypassSecurityTrustResourceUrl(value: string): SafeResourceUrl;
}

export declare class ɵDomSharedStylesHost extends ɵSharedStylesHost implements OnDestroy {
    private _doc;
    private _hostNodes;
    private _styleNodes;
    constructor(_doc: any);
    private _addStylesToHost;
    addHost(hostNode: Node): void;
    removeHost(hostNode: Node): void;
    onStylesAdded(additions: Set<string>): void;
    ngOnDestroy(): void;
}

export declare const ɵELEMENT_PROBE_PROVIDERS: Provider[];

/**
 * In Ivy, we don't support NgProbe because we have our own set of testing utilities
 * with more robust functionality.
 *
 * We shouldn't bring in NgProbe because it prevents DebugNode and friends from
 * tree-shaking properly.
 */
export declare const ɵELEMENT_PROBE_PROVIDERS__POST_R3__: never[];


export declare function ɵescapeHtml(text: string): string;

export declare function ɵflattenStyles(compId: string, styles: Array<any | any[]>, target: string[]): string[];

export declare function ɵgetDOM(): ɵDomAdapter;

export declare class ɵHammerGesturesPlugin extends ɵangular_packages_platform_browser_platform_browser_g {
    private _config;
    private console;
    private loader?;
    constructor(doc: any, _config: HammerGestureConfig, console: ɵConsole, loader?: HammerLoader | null | undefined);
    supports(eventName: string): boolean;
    addEventListener(element: HTMLElement, eventName: string, handler: Function): Function;
    isCustomEvent(eventName: string): boolean;
}

export declare function ɵinitDomAdapter(): void;

export declare const ɵINTERNAL_BROWSER_PLATFORM_PROVIDERS: StaticProvider[];

/**
 * @publicApi
 * A browser plug-in that provides support for handling of key events in Angular.
 */
export declare class ɵKeyEventsPlugin extends ɵangular_packages_platform_browser_platform_browser_g {
    /**
     * Initializes an instance of the browser plug-in.
     * @param doc The document in which key events will be detected.
     */
    constructor(doc: any);
    /**
      * Reports whether a named key event is supported.
      * @param eventName The event name to query.
      * @return True if the named key event is supported.
     */
    supports(eventName: string): boolean;
    /**
     * Registers a handler for a specific element and key event.
     * @param element The HTML element to receive event notifications.
     * @param eventName The name of the key event to listen for.
     * @param handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @returns The key event that was registered.
    */
    addEventListener(element: HTMLElement, eventName: string, handler: Function): Function;
    static parseEventName(eventName: string): {
        [key: string]: string;
    } | null;
    static getEventFullKey(event: KeyboardEvent): string;
    /**
     * Configures a handler callback for a key event.
     * @param fullKey The event name that combines all simultaneous keystrokes.
     * @param handler The function that responds to the key event.
     * @param zone The zone in which the event occurred.
     * @returns A callback function.
     */
    static eventCallback(fullKey: any, handler: Function, zone: NgZone): Function;
}

export declare const ɵNAMESPACE_URIS: {
    [ns: string]: string;
};

export declare function ɵsetRootDomAdapter(adapter: ɵDomAdapter): void;

export declare class ɵSharedStylesHost {
    addStyles(styles: string[]): void;
    onStylesAdded(additions: Set<string>): void;
    getAllStyles(): string[];
}

export declare function ɵshimContentAttribute(componentShortId: string): string;

export declare function ɵshimHostAttribute(componentShortId: string): string;

/**
 * An id that identifies a particular application being bootstrapped, that should
 * match across the client/server boundary.
 */
export declare const ɵTRANSITION_ID: InjectionToken<unknown>;

export { }
