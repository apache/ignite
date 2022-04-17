/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { StaticSymbol } from './aot/static_symbol';
import { splitAtColon, stringify } from './util';
// group 0: "[prop] or (event) or @trigger"
// group 1: "prop" from "[prop]"
// group 2: "event" from "(event)"
// group 3: "@trigger" from "@trigger"
const HOST_REG_EXP = /^(?:(?:\[([^\]]+)\])|(?:\(([^\)]+)\)))|(\@[-\w]+)$/;
export function sanitizeIdentifier(name) {
    return name.replace(/\W/g, '_');
}
let _anonymousTypeIndex = 0;
export function identifierName(compileIdentifier) {
    if (!compileIdentifier || !compileIdentifier.reference) {
        return null;
    }
    const ref = compileIdentifier.reference;
    if (ref instanceof StaticSymbol) {
        return ref.name;
    }
    if (ref['__anonymousType']) {
        return ref['__anonymousType'];
    }
    let identifier = stringify(ref);
    if (identifier.indexOf('(') >= 0) {
        // case: anonymous functions!
        identifier = `anonymous_${_anonymousTypeIndex++}`;
        ref['__anonymousType'] = identifier;
    }
    else {
        identifier = sanitizeIdentifier(identifier);
    }
    return identifier;
}
export function identifierModuleUrl(compileIdentifier) {
    const ref = compileIdentifier.reference;
    if (ref instanceof StaticSymbol) {
        return ref.filePath;
    }
    // Runtime type
    return `./${stringify(ref)}`;
}
export function viewClassName(compType, embeddedTemplateIndex) {
    return `View_${identifierName({ reference: compType })}_${embeddedTemplateIndex}`;
}
export function rendererTypeName(compType) {
    return `RenderType_${identifierName({ reference: compType })}`;
}
export function hostViewClassName(compType) {
    return `HostView_${identifierName({ reference: compType })}`;
}
export function componentFactoryName(compType) {
    return `${identifierName({ reference: compType })}NgFactory`;
}
export var CompileSummaryKind;
(function (CompileSummaryKind) {
    CompileSummaryKind[CompileSummaryKind["Pipe"] = 0] = "Pipe";
    CompileSummaryKind[CompileSummaryKind["Directive"] = 1] = "Directive";
    CompileSummaryKind[CompileSummaryKind["NgModule"] = 2] = "NgModule";
    CompileSummaryKind[CompileSummaryKind["Injectable"] = 3] = "Injectable";
})(CompileSummaryKind || (CompileSummaryKind = {}));
export function tokenName(token) {
    return token.value != null ? sanitizeIdentifier(token.value) : identifierName(token.identifier);
}
export function tokenReference(token) {
    if (token.identifier != null) {
        return token.identifier.reference;
    }
    else {
        return token.value;
    }
}
/**
 * Metadata about a stylesheet
 */
export class CompileStylesheetMetadata {
    constructor({ moduleUrl, styles, styleUrls } = {}) {
        this.moduleUrl = moduleUrl || null;
        this.styles = _normalizeArray(styles);
        this.styleUrls = _normalizeArray(styleUrls);
    }
}
/**
 * Metadata regarding compilation of a template.
 */
export class CompileTemplateMetadata {
    constructor({ encapsulation, template, templateUrl, htmlAst, styles, styleUrls, externalStylesheets, animations, ngContentSelectors, interpolation, isInline, preserveWhitespaces }) {
        this.encapsulation = encapsulation;
        this.template = template;
        this.templateUrl = templateUrl;
        this.htmlAst = htmlAst;
        this.styles = _normalizeArray(styles);
        this.styleUrls = _normalizeArray(styleUrls);
        this.externalStylesheets = _normalizeArray(externalStylesheets);
        this.animations = animations ? flatten(animations) : [];
        this.ngContentSelectors = ngContentSelectors || [];
        if (interpolation && interpolation.length != 2) {
            throw new Error(`'interpolation' should have a start and an end symbol.`);
        }
        this.interpolation = interpolation;
        this.isInline = isInline;
        this.preserveWhitespaces = preserveWhitespaces;
    }
    toSummary() {
        return {
            ngContentSelectors: this.ngContentSelectors,
            encapsulation: this.encapsulation,
            styles: this.styles,
            animations: this.animations
        };
    }
}
/**
 * Metadata regarding compilation of a directive.
 */
export class CompileDirectiveMetadata {
    static create({ isHost, type, isComponent, selector, exportAs, changeDetection, inputs, outputs, host, providers, viewProviders, queries, guards, viewQueries, entryComponents, template, componentViewType, rendererType, componentFactory }) {
        const hostListeners = {};
        const hostProperties = {};
        const hostAttributes = {};
        if (host != null) {
            Object.keys(host).forEach(key => {
                const value = host[key];
                const matches = key.match(HOST_REG_EXP);
                if (matches === null) {
                    hostAttributes[key] = value;
                }
                else if (matches[1] != null) {
                    hostProperties[matches[1]] = value;
                }
                else if (matches[2] != null) {
                    hostListeners[matches[2]] = value;
                }
            });
        }
        const inputsMap = {};
        if (inputs != null) {
            inputs.forEach((bindConfig) => {
                // canonical syntax: `dirProp: elProp`
                // if there is no `:`, use dirProp = elProp
                const parts = splitAtColon(bindConfig, [bindConfig, bindConfig]);
                inputsMap[parts[0]] = parts[1];
            });
        }
        const outputsMap = {};
        if (outputs != null) {
            outputs.forEach((bindConfig) => {
                // canonical syntax: `dirProp: elProp`
                // if there is no `:`, use dirProp = elProp
                const parts = splitAtColon(bindConfig, [bindConfig, bindConfig]);
                outputsMap[parts[0]] = parts[1];
            });
        }
        return new CompileDirectiveMetadata({
            isHost,
            type,
            isComponent: !!isComponent, selector, exportAs, changeDetection,
            inputs: inputsMap,
            outputs: outputsMap,
            hostListeners,
            hostProperties,
            hostAttributes,
            providers,
            viewProviders,
            queries,
            guards,
            viewQueries,
            entryComponents,
            template,
            componentViewType,
            rendererType,
            componentFactory,
        });
    }
    constructor({ isHost, type, isComponent, selector, exportAs, changeDetection, inputs, outputs, hostListeners, hostProperties, hostAttributes, providers, viewProviders, queries, guards, viewQueries, entryComponents, template, componentViewType, rendererType, componentFactory }) {
        this.isHost = !!isHost;
        this.type = type;
        this.isComponent = isComponent;
        this.selector = selector;
        this.exportAs = exportAs;
        this.changeDetection = changeDetection;
        this.inputs = inputs;
        this.outputs = outputs;
        this.hostListeners = hostListeners;
        this.hostProperties = hostProperties;
        this.hostAttributes = hostAttributes;
        this.providers = _normalizeArray(providers);
        this.viewProviders = _normalizeArray(viewProviders);
        this.queries = _normalizeArray(queries);
        this.guards = guards;
        this.viewQueries = _normalizeArray(viewQueries);
        this.entryComponents = _normalizeArray(entryComponents);
        this.template = template;
        this.componentViewType = componentViewType;
        this.rendererType = rendererType;
        this.componentFactory = componentFactory;
    }
    toSummary() {
        return {
            summaryKind: CompileSummaryKind.Directive,
            type: this.type,
            isComponent: this.isComponent,
            selector: this.selector,
            exportAs: this.exportAs,
            inputs: this.inputs,
            outputs: this.outputs,
            hostListeners: this.hostListeners,
            hostProperties: this.hostProperties,
            hostAttributes: this.hostAttributes,
            providers: this.providers,
            viewProviders: this.viewProviders,
            queries: this.queries,
            guards: this.guards,
            viewQueries: this.viewQueries,
            entryComponents: this.entryComponents,
            changeDetection: this.changeDetection,
            template: this.template && this.template.toSummary(),
            componentViewType: this.componentViewType,
            rendererType: this.rendererType,
            componentFactory: this.componentFactory
        };
    }
}
export class CompilePipeMetadata {
    constructor({ type, name, pure }) {
        this.type = type;
        this.name = name;
        this.pure = !!pure;
    }
    toSummary() {
        return {
            summaryKind: CompileSummaryKind.Pipe,
            type: this.type,
            name: this.name,
            pure: this.pure
        };
    }
}
export class CompileShallowModuleMetadata {
}
/**
 * Metadata regarding compilation of a module.
 */
export class CompileNgModuleMetadata {
    constructor({ type, providers, declaredDirectives, exportedDirectives, declaredPipes, exportedPipes, entryComponents, bootstrapComponents, importedModules, exportedModules, schemas, transitiveModule, id }) {
        this.type = type || null;
        this.declaredDirectives = _normalizeArray(declaredDirectives);
        this.exportedDirectives = _normalizeArray(exportedDirectives);
        this.declaredPipes = _normalizeArray(declaredPipes);
        this.exportedPipes = _normalizeArray(exportedPipes);
        this.providers = _normalizeArray(providers);
        this.entryComponents = _normalizeArray(entryComponents);
        this.bootstrapComponents = _normalizeArray(bootstrapComponents);
        this.importedModules = _normalizeArray(importedModules);
        this.exportedModules = _normalizeArray(exportedModules);
        this.schemas = _normalizeArray(schemas);
        this.id = id || null;
        this.transitiveModule = transitiveModule || null;
    }
    toSummary() {
        const module = this.transitiveModule;
        return {
            summaryKind: CompileSummaryKind.NgModule,
            type: this.type,
            entryComponents: module.entryComponents,
            providers: module.providers,
            modules: module.modules,
            exportedDirectives: module.exportedDirectives,
            exportedPipes: module.exportedPipes
        };
    }
}
export class TransitiveCompileNgModuleMetadata {
    constructor() {
        this.directivesSet = new Set();
        this.directives = [];
        this.exportedDirectivesSet = new Set();
        this.exportedDirectives = [];
        this.pipesSet = new Set();
        this.pipes = [];
        this.exportedPipesSet = new Set();
        this.exportedPipes = [];
        this.modulesSet = new Set();
        this.modules = [];
        this.entryComponentsSet = new Set();
        this.entryComponents = [];
        this.providers = [];
    }
    addProvider(provider, module) {
        this.providers.push({ provider: provider, module: module });
    }
    addDirective(id) {
        if (!this.directivesSet.has(id.reference)) {
            this.directivesSet.add(id.reference);
            this.directives.push(id);
        }
    }
    addExportedDirective(id) {
        if (!this.exportedDirectivesSet.has(id.reference)) {
            this.exportedDirectivesSet.add(id.reference);
            this.exportedDirectives.push(id);
        }
    }
    addPipe(id) {
        if (!this.pipesSet.has(id.reference)) {
            this.pipesSet.add(id.reference);
            this.pipes.push(id);
        }
    }
    addExportedPipe(id) {
        if (!this.exportedPipesSet.has(id.reference)) {
            this.exportedPipesSet.add(id.reference);
            this.exportedPipes.push(id);
        }
    }
    addModule(id) {
        if (!this.modulesSet.has(id.reference)) {
            this.modulesSet.add(id.reference);
            this.modules.push(id);
        }
    }
    addEntryComponent(ec) {
        if (!this.entryComponentsSet.has(ec.componentType)) {
            this.entryComponentsSet.add(ec.componentType);
            this.entryComponents.push(ec);
        }
    }
}
function _normalizeArray(obj) {
    return obj || [];
}
export class ProviderMeta {
    constructor(token, { useClass, useValue, useExisting, useFactory, deps, multi }) {
        this.token = token;
        this.useClass = useClass || null;
        this.useValue = useValue;
        this.useExisting = useExisting;
        this.useFactory = useFactory || null;
        this.dependencies = deps || null;
        this.multi = !!multi;
    }
}
export function flatten(list) {
    return list.reduce((flat, item) => {
        const flatItem = Array.isArray(item) ? flatten(item) : item;
        return flat.concat(flatItem);
    }, []);
}
function jitSourceUrl(url) {
    // Note: We need 3 "/" so that ng shows up as a separate domain
    // in the chrome dev tools.
    return url.replace(/(\w+:\/\/[\w:-]+)?(\/+)?/, 'ng:///');
}
export function templateSourceUrl(ngModuleType, compMeta, templateMeta) {
    let url;
    if (templateMeta.isInline) {
        if (compMeta.type.reference instanceof StaticSymbol) {
            // Note: a .ts file might contain multiple components with inline templates,
            // so we need to give them unique urls, as these will be used for sourcemaps.
            url = `${compMeta.type.reference.filePath}.${compMeta.type.reference.name}.html`;
        }
        else {
            url = `${identifierName(ngModuleType)}/${identifierName(compMeta.type)}.html`;
        }
    }
    else {
        url = templateMeta.templateUrl;
    }
    return compMeta.type.reference instanceof StaticSymbol ? url : jitSourceUrl(url);
}
export function sharedStylesheetJitUrl(meta, id) {
    const pathParts = meta.moduleUrl.split(/\/\\/g);
    const baseName = pathParts[pathParts.length - 1];
    return jitSourceUrl(`css/${id}${baseName}.ngstyle.js`);
}
export function ngModuleJitUrl(moduleMeta) {
    return jitSourceUrl(`${identifierName(moduleMeta.type)}/module.ngfactory.js`);
}
export function templateJitUrl(ngModuleType, compMeta) {
    return jitSourceUrl(`${identifierName(ngModuleType)}/${identifierName(compMeta.type)}.ngfactory.js`);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZV9tZXRhZGF0YS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jb21waWxlX21ldGFkYXRhLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBQyxZQUFZLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUlqRCxPQUFPLEVBQUMsWUFBWSxFQUFFLFNBQVMsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUUvQywyQ0FBMkM7QUFDM0MsZ0NBQWdDO0FBQ2hDLGtDQUFrQztBQUNsQyxzQ0FBc0M7QUFDdEMsTUFBTSxZQUFZLEdBQUcsb0RBQW9ELENBQUM7QUFFMUUsTUFBTSxVQUFVLGtCQUFrQixDQUFDLElBQVk7SUFDN0MsT0FBTyxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FBQztBQUNsQyxDQUFDO0FBRUQsSUFBSSxtQkFBbUIsR0FBRyxDQUFDLENBQUM7QUFFNUIsTUFBTSxVQUFVLGNBQWMsQ0FBQyxpQkFBK0Q7SUFFNUYsSUFBSSxDQUFDLGlCQUFpQixJQUFJLENBQUMsaUJBQWlCLENBQUMsU0FBUyxFQUFFO1FBQ3RELE9BQU8sSUFBSSxDQUFDO0tBQ2I7SUFDRCxNQUFNLEdBQUcsR0FBRyxpQkFBaUIsQ0FBQyxTQUFTLENBQUM7SUFDeEMsSUFBSSxHQUFHLFlBQVksWUFBWSxFQUFFO1FBQy9CLE9BQU8sR0FBRyxDQUFDLElBQUksQ0FBQztLQUNqQjtJQUNELElBQUksR0FBRyxDQUFDLGlCQUFpQixDQUFDLEVBQUU7UUFDMUIsT0FBTyxHQUFHLENBQUMsaUJBQWlCLENBQUMsQ0FBQztLQUMvQjtJQUNELElBQUksVUFBVSxHQUFHLFNBQVMsQ0FBQyxHQUFHLENBQUMsQ0FBQztJQUNoQyxJQUFJLFVBQVUsQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxFQUFFO1FBQ2hDLDZCQUE2QjtR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QUFDLENBQUM7QUFDdkYsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtTdGF0aWNTeW1ib2x9IGZyb20gJy4vYW90L3N0YXRpY19zeW1ib2wnO1xuaW1wb3J0IHtDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgU2NoZW1hTWV0YWRhdGEsIFR5cGUsIFZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuL2NvcmUnO1xuaW1wb3J0IHtMaWZlY3ljbGVIb29rc30gZnJvbSAnLi9saWZlY3ljbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7UGFyc2VUcmVlUmVzdWx0IGFzIEh0bWxQYXJzZVRyZWVSZXN1bHR9IGZyb20gJy4vbWxfcGFyc2VyL3BhcnNlcic7XG5pbXBvcnQge3NwbGl0QXRDb2xvbiwgc3RyaW5naWZ5fSBmcm9tICcuL3V0aWwnO1xuXG4vLyBncm91cCAwOiBcIltwcm9wXSBvciAoZXZlbnQpIG9yIEB0cmlnZ2VyXCJcbi8vIGdyb3VwIDE6IFwicHJvcFwiIGZyb20gXCJbcHJvcF1cIlxuLy8gZ3JvdXAgMjogXCJldmVudFwiIGZyb20gXCIoZXZlbnQpXCJcbi8vIGdyb3VwIDM6IFwiQHRyaWdnZXJcIiBmcm9tIFwiQHRyaWdnZXJcIlxuY29uc3QgSE9TVF9SRUdfRVhQID0gL14oPzooPzpcXFsoW15cXF1dKylcXF0pfCg/OlxcKChbXlxcKV0rKVxcKSkpfChcXEBbLVxcd10rKSQvO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVJZGVudGlmaWVyKG5hbWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBuYW1lLnJlcGxhY2UoL1xcVy9nLCAnXycpO1xufVxuXG5sZXQgX2Fub255bW91c1R5cGVJbmRleCA9IDA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpZGVudGlmaWVyTmFtZShjb21waWxlSWRlbnRpZmllcjogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSB8IG51bGwgfCB1bmRlZmluZWQpOlxuICAgIHN0cmluZ3xudWxsIHtcbiAgaWYgKCFjb21waWxlSWRlbnRpZmllciB8fCAhY29tcGlsZUlkZW50aWZpZXIucmVmZXJlbmNlKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgY29uc3QgcmVmID0gY29tcGlsZUlkZW50aWZpZXIucmVmZXJlbmNlO1xuICBpZiAocmVmIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgcmV0dXJuIHJlZi5uYW1lO1xuICB9XG4gIGlmIChyZWZbJ19fYW5vbnltb3VzVHlwZSddKSB7XG4gICAgcmV0dXJuIHJlZlsnX19hbm9ueW1vdXNUeXBlJ107XG4gIH1cbiAgbGV0IGlkZW50aWZpZXIgPSBzdHJpbmdpZnkocmVmKTtcbiAgaWYgKGlkZW50aWZpZXIuaW5kZXhPZignKCcpID49IDApIHtcbiAgICAvLyBjYXNlOiBhbm9ueW1vdXMgZnVuY3Rpb25zIVxuICAgIGlkZW50aWZpZXIgPSBgYW5vbnltb3VzXyR7X2Fub255bW91c1R5cGVJbmRleCsrfWA7XG4gICAgcmVmWydfX2Fub255bW91c1R5cGUnXSA9IGlkZW50aWZpZXI7XG4gIH0gZWxzZSB7XG4gICAgaWRlbnRpZmllciA9IHNhbml0aXplSWRlbnRpZmllcihpZGVudGlmaWVyKTtcbiAgfVxuICByZXR1cm4gaWRlbnRpZmllcjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlkZW50aWZpZXJNb2R1bGVVcmwoY29tcGlsZUlkZW50aWZpZXI6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEpOiBzdHJpbmcge1xuICBjb25zdCByZWYgPSBjb21waWxlSWRlbnRpZmllci5yZWZlcmVuY2U7XG4gIGlmIChyZWYgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICByZXR1cm4gcmVmLmZpbGVQYXRoO1xuICB9XG4gIC8vIFJ1bnRpbWUgdHlwZVxuICByZXR1cm4gYC4vJHtzdHJpbmdpZnkocmVmKX1gO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmlld0NsYXNzTmFtZShjb21wVHlwZTogYW55LCBlbWJlZGRlZFRlbXBsYXRlSW5kZXg6IG51bWJlcik6IHN0cmluZyB7XG4gIHJldHVybiBgVmlld18ke2lkZW50aWZpZXJOYW1lKHtyZWZlcmVuY2U6IGNvbXBUeXBlfSl9XyR7ZW1iZWRkZWRUZW1wbGF0ZUluZGV4fWA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW5kZXJlclR5cGVOYW1lKGNvbXBUeXBlOiBhbnkpOiBzdHJpbmcge1xuICByZXR1cm4gYFJlbmRlclR5cGVfJHtpZGVudGlmaWVyTmFtZSh7cmVmZXJlbmNlOiBjb21wVHlwZX0pfWA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBob3N0Vmlld0NsYXNzTmFtZShjb21wVHlwZTogYW55KTogc3RyaW5nIHtcbiAgcmV0dXJuIGBIb3N0Vmlld18ke2lkZW50aWZpZXJOYW1lKHtyZWZlcmVuY2U6IGNvbXBUeXBlfSl9YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNvbXBvbmVudEZhY3RvcnlOYW1lKGNvbXBUeXBlOiBhbnkpOiBzdHJpbmcge1xuICByZXR1cm4gYCR7aWRlbnRpZmllck5hbWUoe3JlZmVyZW5jZTogY29tcFR5cGV9KX1OZ0ZhY3RvcnlgO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIFByb3h5Q2xhc3MgeyBzZXREZWxlZ2F0ZShkZWxlZ2F0ZTogYW55KTogdm9pZDsgfVxuXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEgeyByZWZlcmVuY2U6IGFueTsgfVxuXG5leHBvcnQgZW51bSBDb21waWxlU3VtbWFyeUtpbmQge1xuICBQaXBlLFxuICBEaXJlY3RpdmUsXG4gIE5nTW9kdWxlLFxuICBJbmplY3RhYmxlXG59XG5cbi8qKlxuICogQSBDb21waWxlU3VtbWFyeSBpcyB0aGUgZGF0YSBuZWVkZWQgdG8gdXNlIGEgZGlyZWN0aXZlIC8gcGlwZSAvIG1vZHVsZVxuICogaW4gb3RoZXIgbW9kdWxlcyAvIGNvbXBvbmVudHMuIEhvd2V2ZXIsIHRoaXMgZGF0YSBpcyBub3QgZW5vdWdoIHRvIGNvbXBpbGVcbiAqIHRoZSBkaXJlY3RpdmUgLyBtb2R1bGUgaXRzZWxmLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVUeXBlU3VtbWFyeSB7XG4gIHN1bW1hcnlLaW5kOiBDb21waWxlU3VtbWFyeUtpbmR8bnVsbDtcbiAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGEge1xuICBpc0F0dHJpYnV0ZT86IGJvb2xlYW47XG4gIGlzU2VsZj86IGJvb2xlYW47XG4gIGlzSG9zdD86IGJvb2xlYW47XG4gIGlzU2tpcFNlbGY/OiBib29sZWFuO1xuICBpc09wdGlvbmFsPzogYm9vbGVhbjtcbiAgaXNWYWx1ZT86IGJvb2xlYW47XG4gIHRva2VuPzogQ29tcGlsZVRva2VuTWV0YWRhdGE7XG4gIHZhbHVlPzogYW55O1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVQcm92aWRlck1ldGFkYXRhIHtcbiAgdG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhO1xuICB1c2VDbGFzcz86IENvbXBpbGVUeXBlTWV0YWRhdGE7XG4gIHVzZVZhbHVlPzogYW55O1xuICB1c2VFeGlzdGluZz86IENvbXBpbGVUb2tlbk1ldGFkYXRhO1xuICB1c2VGYWN0b3J5PzogQ29tcGlsZUZhY3RvcnlNZXRhZGF0YTtcbiAgZGVwcz86IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YVtdO1xuICBtdWx0aT86IGJvb2xlYW47XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZUZhY3RvcnlNZXRhZGF0YSBleHRlbmRzIENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEge1xuICBkaURlcHM6IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YVtdO1xuICByZWZlcmVuY2U6IGFueTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHRva2VuTmFtZSh0b2tlbjogQ29tcGlsZVRva2VuTWV0YWRhdGEpIHtcbiAgcmV0dXJuIHRva2VuLnZhbHVlICE9IG51bGwgPyBzYW5pdGl6ZUlkZW50aWZpZXIodG9rZW4udmFsdWUpIDogaWRlbnRpZmllck5hbWUodG9rZW4uaWRlbnRpZmllcik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0b2tlblJlZmVyZW5jZSh0b2tlbjogQ29tcGlsZVRva2VuTWV0YWRhdGEpIHtcbiAgaWYgKHRva2VuLmlkZW50aWZpZXIgIT0gbnVsbCkge1xuICAgIHJldHVybiB0b2tlbi5pZGVudGlmaWVyLnJlZmVyZW5jZTtcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gdG9rZW4udmFsdWU7XG4gIH1cbn1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlVG9rZW5NZXRhZGF0YSB7XG4gIHZhbHVlPzogYW55O1xuICBpZGVudGlmaWVyPzogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YXxDb21waWxlVHlwZU1ldGFkYXRhO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGEge1xuICBzeW1ib2w6IFN0YXRpY1N5bWJvbDtcbiAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YTtcblxuICBwcm92aWRlZEluPzogU3RhdGljU3ltYm9sO1xuXG4gIHVzZVZhbHVlPzogYW55O1xuICB1c2VDbGFzcz86IFN0YXRpY1N5bWJvbDtcbiAgdXNlRXhpc3Rpbmc/OiBTdGF0aWNTeW1ib2w7XG4gIHVzZUZhY3Rvcnk/OiBTdGF0aWNTeW1ib2w7XG4gIGRlcHM/OiBhbnlbXTtcbn1cblxuLyoqXG4gKiBNZXRhZGF0YSByZWdhcmRpbmcgY29tcGlsYXRpb24gb2YgYSB0eXBlLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVUeXBlTWV0YWRhdGEgZXh0ZW5kcyBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhIHtcbiAgZGlEZXBzOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGFbXTtcbiAgbGlmZWN5Y2xlSG9va3M6IExpZmVjeWNsZUhvb2tzW107XG4gIHJlZmVyZW5jZTogYW55O1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVRdWVyeU1ldGFkYXRhIHtcbiAgc2VsZWN0b3JzOiBBcnJheTxDb21waWxlVG9rZW5NZXRhZGF0YT47XG4gIGRlc2NlbmRhbnRzOiBib29sZWFuO1xuICBmaXJzdDogYm9vbGVhbjtcbiAgcHJvcGVydHlOYW1lOiBzdHJpbmc7XG4gIHJlYWQ6IENvbXBpbGVUb2tlbk1ldGFkYXRhO1xuICBzdGF0aWM/OiBib29sZWFuO1xufVxuXG4vKipcbiAqIE1ldGFkYXRhIGFib3V0IGEgc3R5bGVzaGVldFxuICovXG5leHBvcnQgY2xhc3MgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YSB7XG4gIG1vZHVsZVVybDogc3RyaW5nfG51bGw7XG4gIHN0eWxlczogc3RyaW5nW107XG4gIHN0eWxlVXJsczogc3RyaW5nW107XG4gIGNvbnN0cnVjdG9yKFxuICAgICAge21vZHVsZVVybCwgc3R5bGVzLFxuICAgICAgIHN0eWxlVXJsc306IHttb2R1bGVVcmw/OiBzdHJpbmcsIHN0eWxlcz86IHN0cmluZ1tdLCBzdHlsZVVybHM/OiBzdHJpbmdbXX0gPSB7fSkge1xuICAgIHRoaXMubW9kdWxlVXJsID0gbW9kdWxlVXJsIHx8IG51bGw7XG4gICAgdGhpcy5zdHlsZXMgPSBfbm9ybWFsaXplQXJyYXkoc3R5bGVzKTtcbiAgICB0aGlzLnN0eWxlVXJscyA9IF9ub3JtYWxpemVBcnJheShzdHlsZVVybHMpO1xuICB9XG59XG5cbi8qKlxuICogU3VtbWFyeSBNZXRhZGF0YSByZWdhcmRpbmcgY29tcGlsYXRpb24gb2YgYSB0ZW1wbGF0ZS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDb21waWxlVGVtcGxhdGVTdW1tYXJ5IHtcbiAgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXTtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb258bnVsbDtcbiAgc3R5bGVzOiBzdHJpbmdbXTtcbiAgYW5pbWF0aW9uczogYW55W118bnVsbDtcbn1cblxuLyoqXG4gKiBNZXRhZGF0YSByZWdhcmRpbmcgY29tcGlsYXRpb24gb2YgYSB0ZW1wbGF0ZS5cbiAqL1xuZXhwb3J0IGNsYXNzIENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhIHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb258bnVsbDtcbiAgdGVtcGxhdGU6IHN0cmluZ3xudWxsO1xuICB0ZW1wbGF0ZVVybDogc3RyaW5nfG51bGw7XG4gIGh0bWxBc3Q6IEh0bWxQYXJzZVRyZWVSZXN1bHR8bnVsbDtcbiAgaXNJbmxpbmU6IGJvb2xlYW47XG4gIHN0eWxlczogc3RyaW5nW107XG4gIHN0eWxlVXJsczogc3RyaW5nW107XG4gIGV4dGVybmFsU3R5bGVzaGVldHM6IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGFbXTtcbiAgYW5pbWF0aW9uczogYW55W107XG4gIG5nQ29udGVudFNlbGVjdG9yczogc3RyaW5nW107XG4gIGludGVycG9sYXRpb246IFtzdHJpbmcsIHN0cmluZ118bnVsbDtcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogYm9vbGVhbjtcbiAgY29uc3RydWN0b3Ioe2VuY2Fwc3VsYXRpb24sIHRlbXBsYXRlLCB0ZW1wbGF0ZVVybCwgaHRtbEFzdCwgc3R5bGVzLCBzdHlsZVVybHMsXG4gICAgICAgICAgICAgICBleHRlcm5hbFN0eWxlc2hlZXRzLCBhbmltYXRpb25zLCBuZ0NvbnRlbnRTZWxlY3RvcnMsIGludGVycG9sYXRpb24sIGlzSW5saW5lLFxuICAgICAgICAgICAgICAgcHJlc2VydmVXaGl0ZXNwYWNlc306IHtcbiAgICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbiB8IG51bGwsXG4gICAgdGVtcGxhdGU6IHN0cmluZ3xudWxsLFxuICAgIHRlbXBsYXRlVXJsOiBzdHJpbmd8bnVsbCxcbiAgICBodG1sQXN0OiBIdG1sUGFyc2VUcmVlUmVzdWx0fG51bGwsXG4gICAgc3R5bGVzOiBzdHJpbmdbXSxcbiAgICBzdHlsZVVybHM6IHN0cmluZ1tdLFxuICAgIGV4dGVybmFsU3R5bGVzaGVldHM6IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGFbXSxcbiAgICBuZ0NvbnRlbnRTZWxlY3RvcnM6IHN0cmluZ1tdLFxuICAgIGFuaW1hdGlvbnM6IGFueVtdLFxuICAgIGludGVycG9sYXRpb246IFtzdHJpbmcsIHN0cmluZ118bnVsbCxcbiAgICBpc0lubGluZTogYm9vbGVhbixcbiAgICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBib29sZWFuXG4gIH0pIHtcbiAgICB0aGlzLmVuY2Fwc3VsYXRpb24gPSBlbmNhcHN1bGF0aW9uO1xuICAgIHRoaXMudGVtcGxhdGUgPSB0ZW1wbGF0ZTtcbiAgICB0aGlzLnRlbXBsYXRlVXJsID0gdGVtcGxhdGVVcmw7XG4gICAgdGhpcy5odG1sQXN0ID0gaHRtbEFzdDtcbiAgICB0aGlzLnN0eWxlcyA9IF9ub3JtYWxpemVBcnJheShzdHlsZXMpO1xuICAgIHRoaXMuc3R5bGVVcmxzID0gX25vcm1hbGl6ZUFycmF5KHN0eWxlVXJscyk7XG4gICAgdGhpcy5leHRlcm5hbFN0eWxlc2hlZXRzID0gX25vcm1hbGl6ZUFycmF5KGV4dGVybmFsU3R5bGVzaGVldHMpO1xuICAgIHRoaXMuYW5pbWF0aW9ucyA9IGFuaW1hdGlvbnMgPyBmbGF0dGVuKGFuaW1hdGlvbnMpIDogW107XG4gICAgdGhpcy5uZ0NvbnRlbnRTZWxlY3RvcnMgPSBuZ0NvbnRlbnRTZWxlY3RvcnMgfHwgW107XG4gICAgaWYgKGludGVycG9sYXRpb24gJiYgaW50ZXJwb2xhdGlvbi5sZW5ndGggIT0gMikge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGAnaW50ZXJwb2xhdGlvbicgc2hvdWxkIGhhdmUgYSBzdGFydCBhbmQgYW4gZW5kIHN5bWJvbC5gKTtcbiAgICB9XG4gICAgdGhpcy5pbnRlcnBvbGF0aW9uID0gaW50ZXJwb2xhdGlvbjtcbiAgICB0aGlzLmlzSW5saW5lID0gaXNJbmxpbmU7XG4gICAgdGhpcy5wcmVzZXJ2ZVdoaXRlc3BhY2VzID0gcHJlc2VydmVXaGl0ZXNwYWNlcztcbiAgfVxuXG4gIHRvU3VtbWFyeSgpOiBDb21waWxlVGVtcGxhdGVTdW1tYXJ5IHtcbiAgICByZXR1cm4ge1xuICAgICAgbmdDb250ZW50U2VsZWN0b3JzOiB0aGlzLm5nQ29udGVudFNlbGVjdG9ycyxcbiAgICAgIGVuY2Fwc3VsYXRpb246IHRoaXMuZW5jYXBzdWxhdGlvbixcbiAgICAgIHN0eWxlczogdGhpcy5zdHlsZXMsXG4gICAgICBhbmltYXRpb25zOiB0aGlzLmFuaW1hdGlvbnNcbiAgICB9O1xuICB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGEge1xuICBjb21wb25lbnRUeXBlOiBhbnk7XG4gIGNvbXBvbmVudEZhY3Rvcnk6IFN0YXRpY1N5bWJvbHxvYmplY3Q7XG59XG5cbi8vIE5vdGU6IFRoaXMgc2hvdWxkIG9ubHkgdXNlIGludGVyZmFjZXMgYXMgbmVzdGVkIGRhdGEgdHlwZXNcbi8vIGFzIHdlIG5lZWQgdG8gYmUgYWJsZSB0byBzZXJpYWxpemUgdGhpcyBmcm9tL3RvIEpTT04hXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5IGV4dGVuZHMgQ29tcGlsZVR5cGVTdW1tYXJ5IHtcbiAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YTtcbiAgaXNDb21wb25lbnQ6IGJvb2xlYW47XG4gIHNlbGVjdG9yOiBzdHJpbmd8bnVsbDtcbiAgZXhwb3J0QXM6IHN0cmluZ3xudWxsO1xuICBpbnB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBvdXRwdXRzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgaG9zdExpc3RlbmVyczoge1trZXk6IHN0cmluZ106IHN0cmluZ307XG4gIGhvc3RQcm9wZXJ0aWVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgaG9zdEF0dHJpYnV0ZXM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW107XG4gIHZpZXdQcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW107XG4gIHF1ZXJpZXM6IENvbXBpbGVRdWVyeU1ldGFkYXRhW107XG4gIGd1YXJkczoge1trZXk6IHN0cmluZ106IGFueX07XG4gIHZpZXdRdWVyaWVzOiBDb21waWxlUXVlcnlNZXRhZGF0YVtdO1xuICBlbnRyeUNvbXBvbmVudHM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW107XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3l8bnVsbDtcbiAgdGVtcGxhdGU6IENvbXBpbGVUZW1wbGF0ZVN1bW1hcnl8bnVsbDtcbiAgY29tcG9uZW50Vmlld1R5cGU6IFN0YXRpY1N5bWJvbHxQcm94eUNsYXNzfG51bGw7XG4gIHJlbmRlcmVyVHlwZTogU3RhdGljU3ltYm9sfG9iamVjdHxudWxsO1xuICBjb21wb25lbnRGYWN0b3J5OiBTdGF0aWNTeW1ib2x8b2JqZWN0fG51bGw7XG59XG5cbi8qKlxuICogTWV0YWRhdGEgcmVnYXJkaW5nIGNvbXBpbGF0aW9uIG9mIGEgZGlyZWN0aXZlLlxuICovXG5leHBvcnQgY2xhc3MgQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhIHtcbiAgc3RhdGljIGNyZWF0ZSh7aXNIb3N0LCB0eXBlLCBpc0NvbXBvbmVudCwgc2VsZWN0b3IsIGV4cG9ydEFzLCBjaGFuZ2VEZXRlY3Rpb24sIGlucHV0cywgb3V0cHV0cyxcbiAgICAgICAgICAgICAgICAgaG9zdCwgcHJvdmlkZXJzLCB2aWV3UHJvdmlkZXJzLCBxdWVyaWVzLCBndWFyZHMsIHZpZXdRdWVyaWVzLCBlbnRyeUNvbXBvbmVudHMsXG4gICAgICAgICAgICAgICAgIHRlbXBsYXRlLCBjb21wb25lbnRWaWV3VHlwZSwgcmVuZGVyZXJUeXBlLCBjb21wb25lbnRGYWN0b3J5fToge1xuICAgIGlzSG9zdDogYm9vbGVhbixcbiAgICB0eXBlOiBDb21waWxlVHlwZU1ldGFkYXRhLFxuICAgIGlzQ29tcG9uZW50OiBib29sZWFuLFxuICAgIHNlbGVjdG9yOiBzdHJpbmd8bnVsbCxcbiAgICBleHBvcnRBczogc3RyaW5nfG51bGwsXG4gICAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneXxudWxsLFxuICAgIGlucHV0czogc3RyaW5nW10sXG4gICAgb3V0cHV0czogc3RyaW5nW10sXG4gICAgaG9zdDoge1trZXk6IHN0cmluZ106IHN0cmluZ30sXG4gICAgcHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdLFxuICAgIHZpZXdQcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW10sXG4gICAgcXVlcmllczogQ29tcGlsZVF1ZXJ5TWV0YWRhdGFbXSxcbiAgICBndWFyZHM6IHtba2V5OiBzdHJpbmddOiBhbnl9O1xuICAgIHZpZXdRdWVyaWVzOiBDb21waWxlUXVlcnlNZXRhZGF0YVtdLFxuICAgIGVudHJ5Q29tcG9uZW50czogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXSxcbiAgICB0ZW1wbGF0ZTogQ29tcGlsZVRlbXBsYXRlTWV0YWRhdGEsXG4gICAgY29tcG9uZW50Vmlld1R5cGU6IFN0YXRpY1N5bWJvbHxQcm94eUNsYXNzfG51bGwsXG4gICAgcmVuZGVyZXJUeXBlOiBTdGF0aWNTeW1ib2x8b2JqZWN0fG51bGwsXG4gICAgY29tcG9uZW50RmFjdG9yeTogU3RhdGljU3ltYm9sfG9iamVjdHxudWxsLFxuICB9KTogQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhIHtcbiAgICBjb25zdCBob3N0TGlzdGVuZXJzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICAgIGNvbnN0IGhvc3RQcm9wZXJ0aWVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICAgIGNvbnN0IGhvc3RBdHRyaWJ1dGVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICAgIGlmIChob3N0ICE9IG51bGwpIHtcbiAgICAgIE9iamVjdC5rZXlzKGhvc3QpLmZvckVhY2goa2V5ID0+IHtcbiAgICAgICAgY29uc3QgdmFsdWUgPSBob3N0W2tleV07XG4gICAgICAgIGNvbnN0IG1hdGNoZXMgPSBrZXkubWF0Y2goSE9TVF9SRUdfRVhQKTtcbiAgICAgICAgaWYgKG1hdGNoZXMgPT09IG51bGwpIHtcbiAgICAgICAgICBob3N0QXR0cmlidXRlc1trZXldID0gdmFsdWU7XG4gICAgICAgIH0gZWxzZSBpZiAobWF0Y2hlc1sxXSAhPSBudWxsKSB7XG4gICAgICAgICAgaG9zdFByb3BlcnRpZXNbbWF0Y2hlc1sxXV0gPSB2YWx1ZTtcbiAgICAgICAgfSBlbHNlIGlmIChtYXRjaGVzWzJdICE9IG51bGwpIHtcbiAgICAgICAgICBob3N0TGlzdGVuZXJzW21hdGNoZXNbMl1dID0gdmFsdWU7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgICBjb25zdCBpbnB1dHNNYXA6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9ID0ge307XG4gICAgaWYgKGlucHV0cyAhPSBudWxsKSB7XG4gICAgICBpbnB1dHMuZm9yRWFjaCgoYmluZENvbmZpZzogc3RyaW5nKSA9PiB7XG4gICAgICAgIC8vIGNhbm9uaWNhbCBzeW50YXg6IGBkaXJQcm9wOiBlbFByb3BgXG4gICAgICAgIC8vIGlmIHRoZXJlIGlzIG5vIGA6YCwgdXNlIGRpclByb3AgPSBlbFByb3BcbiAgICAgICAgY29uc3QgcGFydHMgPSBzcGxpdEF0Q29sb24oYmluZENvbmZpZywgW2JpbmRDb25maWcsIGJpbmRDb25maWddKTtcbiAgICAgICAgaW5wdXRzTWFwW3BhcnRzWzBdXSA9IHBhcnRzWzFdO1xuICAgICAgfSk7XG4gICAgfVxuICAgIGNvbnN0IG91dHB1dHNNYXA6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9ID0ge307XG4gICAgaWYgKG91dHB1dHMgIT0gbnVsbCkge1xuICAgICAgb3V0cHV0cy5mb3JFYWNoKChiaW5kQ29uZmlnOiBzdHJpbmcpID0+IHtcbiAgICAgICAgLy8gY2Fub25pY2FsIHN5bnRheDogYGRpclByb3A6IGVsUHJvcGBcbiAgICAgICAgLy8gaWYgdGhlcmUgaXMgbm8gYDpgLCB1c2UgZGlyUHJvcCA9IGVsUHJvcFxuICAgICAgICBjb25zdCBwYXJ0cyA9IHNwbGl0QXRDb2xvbihiaW5kQ29uZmlnLCBbYmluZENvbmZpZywgYmluZENvbmZpZ10pO1xuICAgICAgICBvdXRwdXRzTWFwW3BhcnRzWzBdXSA9IHBhcnRzWzFdO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIG5ldyBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEoe1xuICAgICAgaXNIb3N0LFxuICAgICAgdHlwZSxcbiAgICAgIGlzQ29tcG9uZW50OiAhIWlzQ29tcG9uZW50LCBzZWxlY3RvciwgZXhwb3J0QXMsIGNoYW5nZURldGVjdGlvbixcbiAgICAgIGlucHV0czogaW5wdXRzTWFwLFxuICAgICAgb3V0cHV0czogb3V0cHV0c01hcCxcbiAgICAgIGhvc3RMaXN0ZW5lcnMsXG4gICAgICBob3N0UHJvcGVydGllcyxcbiAgICAgIGhvc3RBdHRyaWJ1dGVzLFxuICAgICAgcHJvdmlkZXJzLFxuICAgICAgdmlld1Byb3ZpZGVycyxcbiAgICAgIHF1ZXJpZXMsXG4gICAgICBndWFyZHMsXG4gICAgICB2aWV3UXVlcmllcyxcbiAgICAgIGVudHJ5Q29tcG9uZW50cyxcbiAgICAgIHRlbXBsYXRlLFxuICAgICAgY29tcG9uZW50Vmlld1R5cGUsXG4gICAgICByZW5kZXJlclR5cGUsXG4gICAgICBjb21wb25lbnRGYWN0b3J5LFxuICAgIH0pO1xuICB9XG4gIGlzSG9zdDogYm9vbGVhbjtcbiAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YTtcbiAgaXNDb21wb25lbnQ6IGJvb2xlYW47XG4gIHNlbGVjdG9yOiBzdHJpbmd8bnVsbDtcbiAgZXhwb3J0QXM6IHN0cmluZ3xudWxsO1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5fG51bGw7XG4gIGlucHV0czoge1trZXk6IHN0cmluZ106IHN0cmluZ307XG4gIG91dHB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBob3N0TGlzdGVuZXJzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgaG9zdFByb3BlcnRpZXM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBob3N0QXR0cmlidXRlczoge1trZXk6IHN0cmluZ106IHN0cmluZ307XG4gIHByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXTtcbiAgdmlld1Byb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXTtcbiAgcXVlcmllczogQ29tcGlsZVF1ZXJ5TWV0YWRhdGFbXTtcbiAgZ3VhcmRzOiB7W2tleTogc3RyaW5nXTogYW55fTtcbiAgdmlld1F1ZXJpZXM6IENvbXBpbGVRdWVyeU1ldGFkYXRhW107XG4gIGVudHJ5Q29tcG9uZW50czogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXTtcblxuICB0ZW1wbGF0ZTogQ29tcGlsZVRlbXBsYXRlTWV0YWRhdGF8bnVsbDtcblxuICBjb21wb25lbnRWaWV3VHlwZTogU3RhdGljU3ltYm9sfFByb3h5Q2xhc3N8bnVsbDtcbiAgcmVuZGVyZXJUeXBlOiBTdGF0aWNTeW1ib2x8b2JqZWN0fG51bGw7XG4gIGNvbXBvbmVudEZhY3Rvcnk6IFN0YXRpY1N5bWJvbHxvYmplY3R8bnVsbDtcblxuICBjb25zdHJ1Y3Rvcih7aXNIb3N0LFxuICAgICAgICAgICAgICAgdHlwZSxcbiAgICAgICAgICAgICAgIGlzQ29tcG9uZW50LFxuICAgICAgICAgICAgICAgc2VsZWN0b3IsXG4gICAgICAgICAgICAgICBleHBvcnRBcyxcbiAgICAgICAgICAgICAgIGNoYW5nZURldGVjdGlvbixcbiAgICAgICAgICAgICAgIGlucHV0cyxcbiAgICAgICAgICAgICAgIG91dHB1dHMsXG4gICAgICAgICAgICAgICBob3N0TGlzdGVuZXJzLFxuICAgICAgICAgICAgICAgaG9zdFByb3BlcnRpZXMsXG4gICAgICAgICAgICAgICBob3N0QXR0cmlidXRlcyxcbiAgICAgICAgICAgICAgIHByb3ZpZGVycyxcbiAgICAgICAgICAgICAgIHZpZXdQcm92aWRlcnMsXG4gICAgICAgICAgICAgICBxdWVyaWVzLFxuICAgICAgICAgICAgICAgZ3VhcmRzLFxuICAgICAgICAgICAgICAgdmlld1F1ZXJpZXMsXG4gICAgICAgICAgICAgICBlbnRyeUNvbXBvbmVudHMsXG4gICAgICAgICAgICAgICB0ZW1wbGF0ZSxcbiAgICAgICAgICAgICAgIGNvbXBvbmVudFZpZXdUeXBlLFxuICAgICAgICAgICAgICAgcmVuZGVyZXJUeXBlLFxuICAgICAgICAgICAgICAgY29tcG9uZW50RmFjdG9yeX06IHtcbiAgICBpc0hvc3Q6IGJvb2xlYW4sXG4gICAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YSxcbiAgICBpc0NvbXBvbmVudDogYm9vbGVhbixcbiAgICBzZWxlY3Rvcjogc3RyaW5nfG51bGwsXG4gICAgZXhwb3J0QXM6IHN0cmluZ3xudWxsLFxuICAgIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3l8bnVsbCxcbiAgICBpbnB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9LFxuICAgIG91dHB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9LFxuICAgIGhvc3RMaXN0ZW5lcnM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9LFxuICAgIGhvc3RQcm9wZXJ0aWVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSxcbiAgICBob3N0QXR0cmlidXRlczoge1trZXk6IHN0cmluZ106IHN0cmluZ30sXG4gICAgcHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdLFxuICAgIHZpZXdQcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW10sXG4gICAgcXVlcmllczogQ29tcGlsZVF1ZXJ5TWV0YWRhdGFbXSxcbiAgICBndWFyZHM6IHtba2V5OiBzdHJpbmddOiBhbnl9LFxuICAgIHZpZXdRdWVyaWVzOiBDb21waWxlUXVlcnlNZXRhZGF0YVtdLFxuICAgIGVudHJ5Q29tcG9uZW50czogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXSxcbiAgICB0ZW1wbGF0ZTogQ29tcGlsZVRlbXBsYXRlTWV0YWRhdGF8bnVsbCxcbiAgICBjb21wb25lbnRWaWV3VHlwZTogU3RhdGljU3ltYm9sfFByb3h5Q2xhc3N8bnVsbCxcbiAgICByZW5kZXJlclR5cGU6IFN0YXRpY1N5bWJvbHxvYmplY3R8bnVsbCxcbiAgICBjb21wb25lbnRGYWN0b3J5OiBTdGF0aWNTeW1ib2x8b2JqZWN0fG51bGwsXG4gIH0pIHtcbiAgICB0aGlzLmlzSG9zdCA9ICEhaXNIb3N0O1xuICAgIHRoaXMudHlwZSA9IHR5cGU7XG4gICAgdGhpcy5pc0NvbXBvbmVudCA9IGlzQ29tcG9uZW50O1xuICAgIHRoaXMuc2VsZWN0b3IgPSBzZWxlY3RvcjtcbiAgICB0aGlzLmV4cG9ydEFzID0gZXhwb3J0QXM7XG4gICAgdGhpcy5jaGFuZ2VEZXRlY3Rpb24gPSBjaGFuZ2VEZXRlY3Rpb247XG4gICAgdGhpcy5pbnB1dHMgPSBpbnB1dHM7XG4gICAgdGhpcy5vdXRwdXRzID0gb3V0cHV0cztcbiAgICB0aGlzLmhvc3RMaXN0ZW5lcnMgPSBob3N0TGlzdGVuZXJzO1xuICAgIHRoaXMuaG9zdFByb3BlcnRpZXMgPSBob3N0UHJvcGVydGllcztcbiAgICB0aGlzLmhvc3RBdHRyaWJ1dGVzID0gaG9zdEF0dHJpYnV0ZXM7XG4gICAgdGhpcy5wcm92aWRlcnMgPSBfbm9ybWFsaXplQXJyYXkocHJvdmlkZXJzKTtcbiAgICB0aGlzLnZpZXdQcm92aWRlcnMgPSBfbm9ybWFsaXplQXJyYXkodmlld1Byb3ZpZGVycyk7XG4gICAgdGhpcy5xdWVyaWVzID0gX25vcm1hbGl6ZUFycmF5KHF1ZXJpZXMpO1xuICAgIHRoaXMuZ3VhcmRzID0gZ3VhcmRzO1xuICAgIHRoaXMudmlld1F1ZXJpZXMgPSBfbm9ybWFsaXplQXJyYXkodmlld1F1ZXJpZXMpO1xuICAgIHRoaXMuZW50cnlDb21wb25lbnRzID0gX25vcm1hbGl6ZUFycmF5KGVudHJ5Q29tcG9uZW50cyk7XG4gICAgdGhpcy50ZW1wbGF0ZSA9IHRlbXBsYXRlO1xuXG4gICAgdGhpcy5jb21wb25lbnRWaWV3VHlwZSA9IGNvbXBvbmVudFZpZXdUeXBlO1xuICAgIHRoaXMucmVuZGVyZXJUeXBlID0gcmVuZGVyZXJUeXBlO1xuICAgIHRoaXMuY29tcG9uZW50RmFjdG9yeSA9IGNvbXBvbmVudEZhY3Rvcnk7XG4gIH1cblxuICB0b1N1bW1hcnkoKTogQ29tcGlsZURpcmVjdGl2ZVN1bW1hcnkge1xuICAgIHJldHVybiB7XG4gICAgICBzdW1tYXJ5S2luZDogQ29tcGlsZVN1bW1hcnlLaW5kLkRpcmVjdGl2ZSxcbiAgICAgIHR5cGU6IHRoaXMudHlwZSxcbiAgICAgIGlzQ29tcG9uZW50OiB0aGlzLmlzQ29tcG9uZW50LFxuICAgICAgc2VsZWN0b3I6IHRoaXMuc2VsZWN0b3IsXG4gICAgICBleHBvcnRBczogdGhpcy5leHBvcnRBcyxcbiAgICAgIGlucHV0czogdGhpcy5pbnB1dHMsXG4gICAgICBvdXRwdXRzOiB0aGlzLm91dHB1dHMsXG4gICAgICBob3N0TGlzdGVuZXJzOiB0aGlzLmhvc3RMaXN0ZW5lcnMsXG4gICAgICBob3N0UHJvcGVydGllczogdGhpcy5ob3N0UHJvcGVydGllcyxcbiAgICAgIGhvc3RBdHRyaWJ1dGVzOiB0aGlzLmhvc3RBdHRyaWJ1dGVzLFxuICAgICAgcHJvdmlkZXJzOiB0aGlzLnByb3ZpZGVycyxcbiAgICAgIHZpZXdQcm92aWRlcnM6IHRoaXMudmlld1Byb3ZpZGVycyxcbiAgICAgIHF1ZXJpZXM6IHRoaXMucXVlcmllcyxcbiAgICAgIGd1YXJkczogdGhpcy5ndWFyZHMsXG4gICAgICB2aWV3UXVlcmllczogdGhpcy52aWV3UXVlcmllcyxcbiAgICAgIGVudHJ5Q29tcG9uZW50czogdGhpcy5lbnRyeUNvbXBvbmVudHMsXG4gICAgICBjaGFuZ2VEZXRlY3Rpb246IHRoaXMuY2hhbmdlRGV0ZWN0aW9uLFxuICAgICAgdGVtcGxhdGU6IHRoaXMudGVtcGxhdGUgJiYgdGhpcy50ZW1wbGF0ZS50b1N1bW1hcnkoKSxcbiAgICAgIGNvbXBvbmVudFZpZXdUeXBlOiB0aGlzLmNvbXBvbmVudFZpZXdUeXBlLFxuICAgICAgcmVuZGVyZXJUeXBlOiB0aGlzLnJlbmRlcmVyVHlwZSxcbiAgICAgIGNvbXBvbmVudEZhY3Rvcnk6IHRoaXMuY29tcG9uZW50RmFjdG9yeVxuICAgIH07XG4gIH1cbn1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlUGlwZVN1bW1hcnkgZXh0ZW5kcyBDb21waWxlVHlwZVN1bW1hcnkge1xuICB0eXBlOiBDb21waWxlVHlwZU1ldGFkYXRhO1xuICBuYW1lOiBzdHJpbmc7XG4gIHB1cmU6IGJvb2xlYW47XG59XG5cbmV4cG9ydCBjbGFzcyBDb21waWxlUGlwZU1ldGFkYXRhIHtcbiAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YTtcbiAgbmFtZTogc3RyaW5nO1xuICBwdXJlOiBib29sZWFuO1xuXG4gIGNvbnN0cnVjdG9yKHt0eXBlLCBuYW1lLCBwdXJlfToge1xuICAgIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGEsXG4gICAgbmFtZTogc3RyaW5nLFxuICAgIHB1cmU6IGJvb2xlYW4sXG4gIH0pIHtcbiAgICB0aGlzLnR5cGUgPSB0eXBlO1xuICAgIHRoaXMubmFtZSA9IG5hbWU7XG4gICAgdGhpcy5wdXJlID0gISFwdXJlO1xuICB9XG5cbiAgdG9TdW1tYXJ5KCk6IENvbXBpbGVQaXBlU3VtbWFyeSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN1bW1hcnlLaW5kOiBDb21waWxlU3VtbWFyeUtpbmQuUGlwZSxcbiAgICAgIHR5cGU6IHRoaXMudHlwZSxcbiAgICAgIG5hbWU6IHRoaXMubmFtZSxcbiAgICAgIHB1cmU6IHRoaXMucHVyZVxuICAgIH07XG4gIH1cbn1cblxuLy8gTm90ZTogVGhpcyBzaG91bGQgb25seSB1c2UgaW50ZXJmYWNlcyBhcyBuZXN0ZWQgZGF0YSB0eXBlc1xuLy8gYXMgd2UgbmVlZCB0byBiZSBhYmxlIHRvIHNlcmlhbGl6ZSB0aGlzIGZyb20vdG8gSlNPTiFcbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZU5nTW9kdWxlU3VtbWFyeSBleHRlbmRzIENvbXBpbGVUeXBlU3VtbWFyeSB7XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG5cbiAgLy8gTm90ZTogVGhpcyBpcyB0cmFuc2l0aXZlIG92ZXIgdGhlIGV4cG9ydGVkIG1vZHVsZXMuXG4gIGV4cG9ydGVkRGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdO1xuICAvLyBOb3RlOiBUaGlzIGlzIHRyYW5zaXRpdmUgb3ZlciB0aGUgZXhwb3J0ZWQgbW9kdWxlcy5cbiAgZXhwb3J0ZWRQaXBlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdO1xuXG4gIC8vIE5vdGU6IFRoaXMgaXMgdHJhbnNpdGl2ZS5cbiAgZW50cnlDb21wb25lbnRzOiBDb21waWxlRW50cnlDb21wb25lbnRNZXRhZGF0YVtdO1xuICAvLyBOb3RlOiBUaGlzIGlzIHRyYW5zaXRpdmUuXG4gIHByb3ZpZGVyczoge3Byb3ZpZGVyOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSwgbW9kdWxlOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhfVtdO1xuICAvLyBOb3RlOiBUaGlzIGlzIHRyYW5zaXRpdmUuXG4gIG1vZHVsZXM6IENvbXBpbGVUeXBlTWV0YWRhdGFbXTtcbn1cblxuZXhwb3J0IGNsYXNzIENvbXBpbGVTaGFsbG93TW9kdWxlTWV0YWRhdGEge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgdHlwZSAhOiBDb21waWxlVHlwZU1ldGFkYXRhO1xuXG4gIHJhd0V4cG9ydHM6IGFueTtcbiAgcmF3SW1wb3J0czogYW55O1xuICByYXdQcm92aWRlcnM6IGFueTtcbn1cblxuLyoqXG4gKiBNZXRhZGF0YSByZWdhcmRpbmcgY29tcGlsYXRpb24gb2YgYSBtb2R1bGUuXG4gKi9cbmV4cG9ydCBjbGFzcyBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSB7XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG4gIGRlY2xhcmVkRGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdO1xuICBleHBvcnRlZERpcmVjdGl2ZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXTtcbiAgZGVjbGFyZWRQaXBlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdO1xuXG4gIGV4cG9ydGVkUGlwZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXTtcbiAgZW50cnlDb21wb25lbnRzOiBDb21waWxlRW50cnlDb21wb25lbnRNZXRhZGF0YVtdO1xuICBib290c3RyYXBDb21wb25lbnRzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW107XG4gIHByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXTtcblxuICBpbXBvcnRlZE1vZHVsZXM6IENvbXBpbGVOZ01vZHVsZVN1bW1hcnlbXTtcbiAgZXhwb3J0ZWRNb2R1bGVzOiBDb21waWxlTmdNb2R1bGVTdW1tYXJ5W107XG4gIHNjaGVtYXM6IFNjaGVtYU1ldGFkYXRhW107XG4gIGlkOiBzdHJpbmd8bnVsbDtcblxuICB0cmFuc2l0aXZlTW9kdWxlOiBUcmFuc2l0aXZlQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGE7XG5cbiAgY29uc3RydWN0b3Ioe3R5cGUsIHByb3ZpZGVycywgZGVjbGFyZWREaXJlY3RpdmVzLCBleHBvcnRlZERpcmVjdGl2ZXMsIGRlY2xhcmVkUGlwZXMsXG4gICAgICAgICAgICAgICBleHBvcnRlZFBpcGVzLCBlbnRyeUNvbXBvbmVudHMsIGJvb3RzdHJhcENvbXBvbmVudHMsIGltcG9ydGVkTW9kdWxlcyxcbiAgICAgICAgICAgICAgIGV4cG9ydGVkTW9kdWxlcywgc2NoZW1hcywgdHJhbnNpdGl2ZU1vZHVsZSwgaWR9OiB7XG4gICAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YSxcbiAgICBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW10sXG4gICAgZGVjbGFyZWREaXJlY3RpdmVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10sXG4gICAgZXhwb3J0ZWREaXJlY3RpdmVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10sXG4gICAgZGVjbGFyZWRQaXBlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdLFxuICAgIGV4cG9ydGVkUGlwZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXSxcbiAgICBlbnRyeUNvbXBvbmVudHM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW10sXG4gICAgYm9vdHN0cmFwQ29tcG9uZW50czogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdLFxuICAgIGltcG9ydGVkTW9kdWxlczogQ29tcGlsZU5nTW9kdWxlU3VtbWFyeVtdLFxuICAgIGV4cG9ydGVkTW9kdWxlczogQ29tcGlsZU5nTW9kdWxlU3VtbWFyeVtdLFxuICAgIHRyYW5zaXRpdmVNb2R1bGU6IFRyYW5zaXRpdmVDb21waWxlTmdNb2R1bGVNZXRhZGF0YSxcbiAgICBzY2hlbWFzOiBTY2hlbWFNZXRhZGF0YVtdLFxuICAgIGlkOiBzdHJpbmd8bnVsbFxuICB9KSB7XG4gICAgdGhpcy50eXBlID0gdHlwZSB8fCBudWxsO1xuICAgIHRoaXMuZGVjbGFyZWREaXJlY3RpdmVzID0gX25vcm1hbGl6ZUFycmF5KGRlY2xhcmVkRGlyZWN0aXZlcyk7XG4gICAgdGhpcy5leHBvcnRlZERpcmVjdGl2ZXMgPSBfbm9ybWFsaXplQXJyYXkoZXhwb3J0ZWREaXJlY3RpdmVzKTtcbiAgICB0aGlzLmRlY2xhcmVkUGlwZXMgPSBfbm9ybWFsaXplQXJyYXkoZGVjbGFyZWRQaXBlcyk7XG4gICAgdGhpcy5leHBvcnRlZFBpcGVzID0gX25vcm1hbGl6ZUFycmF5KGV4cG9ydGVkUGlwZXMpO1xuICAgIHRoaXMucHJvdmlkZXJzID0gX25vcm1hbGl6ZUFycmF5KHByb3ZpZGVycyk7XG4gICAgdGhpcy5lbnRyeUNvbXBvbmVudHMgPSBfbm9ybWFsaXplQXJyYXkoZW50cnlDb21wb25lbnRzKTtcbiAgICB0aGlzLmJvb3RzdHJhcENvbXBvbmVudHMgPSBfbm9ybWFsaXplQXJyYXkoYm9vdHN0cmFwQ29tcG9uZW50cyk7XG4gICAgdGhpcy5pbXBvcnRlZE1vZHVsZXMgPSBfbm9ybWFsaXplQXJyYXkoaW1wb3J0ZWRNb2R1bGVzKTtcbiAgICB0aGlzLmV4cG9ydGVkTW9kdWxlcyA9IF9ub3JtYWxpemVBcnJheShleHBvcnRlZE1vZHVsZXMpO1xuICAgIHRoaXMuc2NoZW1hcyA9IF9ub3JtYWxpemVBcnJheShzY2hlbWFzKTtcbiAgICB0aGlzLmlkID0gaWQgfHwgbnVsbDtcbiAgICB0aGlzLnRyYW5zaXRpdmVNb2R1bGUgPSB0cmFuc2l0aXZlTW9kdWxlIHx8IG51bGw7XG4gIH1cblxuICB0b1N1bW1hcnkoKTogQ29tcGlsZU5nTW9kdWxlU3VtbWFyeSB7XG4gICAgY29uc3QgbW9kdWxlID0gdGhpcy50cmFuc2l0aXZlTW9kdWxlICE7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN1bW1hcnlLaW5kOiBDb21waWxlU3VtbWFyeUtpbmQuTmdNb2R1bGUsXG4gICAgICB0eXBlOiB0aGlzLnR5cGUsXG4gICAgICBlbnRyeUNvbXBvbmVudHM6IG1vZHVsZS5lbnRyeUNvbXBvbmVudHMsXG4gICAgICBwcm92aWRlcnM6IG1vZHVsZS5wcm92aWRlcnMsXG4gICAgICBtb2R1bGVzOiBtb2R1bGUubW9kdWxlcyxcbiAgICAgIGV4cG9ydGVkRGlyZWN0aXZlczogbW9kdWxlLmV4cG9ydGVkRGlyZWN0aXZlcyxcbiAgICAgIGV4cG9ydGVkUGlwZXM6IG1vZHVsZS5leHBvcnRlZFBpcGVzXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVHJhbnNpdGl2ZUNvbXBpbGVOZ01vZHVsZU1ldGFkYXRhIHtcbiAgZGlyZWN0aXZlc1NldCA9IG5ldyBTZXQ8YW55PigpO1xuICBkaXJlY3RpdmVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10gPSBbXTtcbiAgZXhwb3J0ZWREaXJlY3RpdmVzU2V0ID0gbmV3IFNldDxhbnk+KCk7XG4gIGV4cG9ydGVkRGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gIHBpcGVzU2V0ID0gbmV3IFNldDxhbnk+KCk7XG4gIHBpcGVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10gPSBbXTtcbiAgZXhwb3J0ZWRQaXBlc1NldCA9IG5ldyBTZXQ8YW55PigpO1xuICBleHBvcnRlZFBpcGVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10gPSBbXTtcbiAgbW9kdWxlc1NldCA9IG5ldyBTZXQ8YW55PigpO1xuICBtb2R1bGVzOiBDb21waWxlVHlwZU1ldGFkYXRhW10gPSBbXTtcbiAgZW50cnlDb21wb25lbnRzU2V0ID0gbmV3IFNldDxhbnk+KCk7XG4gIGVudHJ5Q29tcG9uZW50czogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXSA9IFtdO1xuXG4gIHByb3ZpZGVyczoge3Byb3ZpZGVyOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSwgbW9kdWxlOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhfVtdID0gW107XG5cbiAgYWRkUHJvdmlkZXIocHJvdmlkZXI6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhLCBtb2R1bGU6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEpIHtcbiAgICB0aGlzLnByb3ZpZGVycy5wdXNoKHtwcm92aWRlcjogcHJvdmlkZXIsIG1vZHVsZTogbW9kdWxlfSk7XG4gIH1cblxuICBhZGREaXJlY3RpdmUoaWQ6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEpIHtcbiAgICBpZiAoIXRoaXMuZGlyZWN0aXZlc1NldC5oYXMoaWQucmVmZXJlbmNlKSkge1xuICAgICAgdGhpcy5kaXJlY3RpdmVzU2V0LmFkZChpZC5yZWZlcmVuY2UpO1xuICAgICAgdGhpcy5kaXJlY3RpdmVzLnB1c2goaWQpO1xuICAgIH1cbiAgfVxuICBhZGRFeHBvcnRlZERpcmVjdGl2ZShpZDogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSkge1xuICAgIGlmICghdGhpcy5leHBvcnRlZERpcmVjdGl2ZXNTZXQuaGFzKGlkLnJlZmVyZW5jZSkpIHtcbiAgICAgIHRoaXMuZXhwb3J0ZWREaXJlY3RpdmVzU2V0LmFkZChpZC5yZWZlcmVuY2UpO1xuICAgICAgdGhpcy5leHBvcnRlZERpcmVjdGl2ZXMucHVzaChpZCk7XG4gICAgfVxuICB9XG4gIGFkZFBpcGUoaWQ6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEpIHtcbiAgICBpZiAoIXRoaXMucGlwZXNTZXQuaGFzKGlkLnJlZmVyZW5jZSkpIHtcbiAgICAgIHRoaXMucGlwZXNTZXQuYWRkKGlkLnJlZmVyZW5jZSk7XG4gICAgICB0aGlzLnBpcGVzLnB1c2goaWQpO1xuICAgIH1cbiAgfVxuICBhZGRFeHBvcnRlZFBpcGUoaWQ6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEpIHtcbiAgICBpZiAoIXRoaXMuZXhwb3J0ZWRQaXBlc1NldC5oYXMoaWQucmVmZXJlbmNlKSkge1xuICAgICAgdGhpcy5leHBvcnRlZFBpcGVzU2V0LmFkZChpZC5yZWZlcmVuY2UpO1xuICAgICAgdGhpcy5leHBvcnRlZFBpcGVzLnB1c2goaWQpO1xuICAgIH1cbiAgfVxuICBhZGRNb2R1bGUoaWQ6IENvbXBpbGVUeXBlTWV0YWRhdGEpIHtcbiAgICBpZiAoIXRoaXMubW9kdWxlc1NldC5oYXMoaWQucmVmZXJlbmNlKSkge1xuICAgICAgdGhpcy5tb2R1bGVzU2V0LmFkZChpZC5yZWZlcmVuY2UpO1xuICAgICAgdGhpcy5tb2R1bGVzLnB1c2goaWQpO1xuICAgIH1cbiAgfVxuICBhZGRFbnRyeUNvbXBvbmVudChlYzogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGEpIHtcbiAgICBpZiAoIXRoaXMuZW50cnlDb21wb25lbnRzU2V0LmhhcyhlYy5jb21wb25lbnRUeXBlKSkge1xuICAgICAgdGhpcy5lbnRyeUNvbXBvbmVudHNTZXQuYWRkKGVjLmNvbXBvbmVudFR5cGUpO1xuICAgICAgdGhpcy5lbnRyeUNvbXBvbmVudHMucHVzaChlYyk7XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIF9ub3JtYWxpemVBcnJheShvYmo6IGFueVtdIHwgdW5kZWZpbmVkIHwgbnVsbCk6IGFueVtdIHtcbiAgcmV0dXJuIG9iaiB8fCBbXTtcbn1cblxuZXhwb3J0IGNsYXNzIFByb3ZpZGVyTWV0YSB7XG4gIHRva2VuOiBhbnk7XG4gIHVzZUNsYXNzOiBUeXBlfG51bGw7XG4gIHVzZVZhbHVlOiBhbnk7XG4gIHVzZUV4aXN0aW5nOiBhbnk7XG4gIHVzZUZhY3Rvcnk6IEZ1bmN0aW9ufG51bGw7XG4gIGRlcGVuZGVuY2llczogT2JqZWN0W118bnVsbDtcbiAgbXVsdGk6IGJvb2xlYW47XG5cbiAgY29uc3RydWN0b3IodG9rZW46IGFueSwge3VzZUNsYXNzLCB1c2VWYWx1ZSwgdXNlRXhpc3RpbmcsIHVzZUZhY3RvcnksIGRlcHMsIG11bHRpfToge1xuICAgIHVzZUNsYXNzPzogVHlwZSxcbiAgICB1c2VWYWx1ZT86IGFueSxcbiAgICB1c2VFeGlzdGluZz86IGFueSxcbiAgICB1c2VGYWN0b3J5PzogRnVuY3Rpb258bnVsbCxcbiAgICBkZXBzPzogT2JqZWN0W118bnVsbCxcbiAgICBtdWx0aT86IGJvb2xlYW5cbiAgfSkge1xuICAgIHRoaXMudG9rZW4gPSB0b2tlbjtcbiAgICB0aGlzLnVzZUNsYXNzID0gdXNlQ2xhc3MgfHwgbnVsbDtcbiAgICB0aGlzLnVzZVZhbHVlID0gdXNlVmFsdWU7XG4gICAgdGhpcy51c2VFeGlzdGluZyA9IHVzZUV4aXN0aW5nO1xuICAgIHRoaXMudXNlRmFjdG9yeSA9IHVzZUZhY3RvcnkgfHwgbnVsbDtcbiAgICB0aGlzLmRlcGVuZGVuY2llcyA9IGRlcHMgfHwgbnVsbDtcbiAgICB0aGlzLm11bHRpID0gISFtdWx0aTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZmxhdHRlbjxUPihsaXN0OiBBcnJheTxUfFRbXT4pOiBUW10ge1xuICByZXR1cm4gbGlzdC5yZWR1Y2UoKGZsYXQ6IGFueVtdLCBpdGVtOiBUIHwgVFtdKTogVFtdID0+IHtcbiAgICBjb25zdCBmbGF0SXRlbSA9IEFycmF5LmlzQXJyYXkoaXRlbSkgPyBmbGF0dGVuKGl0ZW0pIDogaXRlbTtcbiAgICByZXR1cm4gKDxUW10+ZmxhdCkuY29uY2F0KGZsYXRJdGVtKTtcbiAgfSwgW10pO1xufVxuXG5mdW5jdGlvbiBqaXRTb3VyY2VVcmwodXJsOiBzdHJpbmcpIHtcbiAgLy8gTm90ZTogV2UgbmVlZCAzIFwiL1wiIHNvIHRoYXQgbmcgc2hvd3MgdXAgYXMgYSBzZXBhcmF0ZSBkb21haW5cbiAgLy8gaW4gdGhlIGNocm9tZSBkZXYgdG9vbHMuXG4gIHJldHVybiB1cmwucmVwbGFjZSgvKFxcdys6XFwvXFwvW1xcdzotXSspPyhcXC8rKT8vLCAnbmc6Ly8vJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0ZW1wbGF0ZVNvdXJjZVVybChcbiAgICBuZ01vZHVsZVR5cGU6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEsIGNvbXBNZXRhOiB7dHlwZTogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YX0sXG4gICAgdGVtcGxhdGVNZXRhOiB7aXNJbmxpbmU6IGJvb2xlYW4sIHRlbXBsYXRlVXJsOiBzdHJpbmcgfCBudWxsfSkge1xuICBsZXQgdXJsOiBzdHJpbmc7XG4gIGlmICh0ZW1wbGF0ZU1ldGEuaXNJbmxpbmUpIHtcbiAgICBpZiAoY29tcE1ldGEudHlwZS5yZWZlcmVuY2UgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgIC8vIE5vdGU6IGEgLnRzIGZpbGUgbWlnaHQgY29udGFpbiBtdWx0aXBsZSBjb21wb25lbnRzIHdpdGggaW5saW5lIHRlbXBsYXRlcyxcbiAgICAgIC8vIHNvIHdlIG5lZWQgdG8gZ2l2ZSB0aGVtIHVuaXF1ZSB1cmxzLCBhcyB0aGVzZSB3aWxsIGJlIHVzZWQgZm9yIHNvdXJjZW1hcHMuXG4gICAgICB1cmwgPSBgJHtjb21wTWV0YS50eXBlLnJlZmVyZW5jZS5maWxlUGF0aH0uJHtjb21wTWV0YS50eXBlLnJlZmVyZW5jZS5uYW1lfS5odG1sYDtcbiAgICB9IGVsc2Uge1xuICAgICAgdXJsID0gYCR7aWRlbnRpZmllck5hbWUobmdNb2R1bGVUeXBlKX0vJHtpZGVudGlmaWVyTmFtZShjb21wTWV0YS50eXBlKX0uaHRtbGA7XG4gICAgfVxuICB9IGVsc2Uge1xuICAgIHVybCA9IHRlbXBsYXRlTWV0YS50ZW1wbGF0ZVVybCAhO1xuICB9XG4gIHJldHVybiBjb21wTWV0YS50eXBlLnJlZmVyZW5jZSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCA/IHVybCA6IGppdFNvdXJjZVVybCh1cmwpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hhcmVkU3R5bGVzaGVldEppdFVybChtZXRhOiBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhLCBpZDogbnVtYmVyKSB7XG4gIGNvbnN0IHBhdGhQYXJ0cyA9IG1ldGEubW9kdWxlVXJsICEuc3BsaXQoL1xcL1xcXFwvZyk7XG4gIGNvbnN0IGJhc2VOYW1lID0gcGF0aFBhcnRzW3BhdGhQYXJ0cy5sZW5ndGggLSAxXTtcbiAgcmV0dXJuIGppdFNvdXJjZVVybChgY3NzLyR7aWR9JHtiYXNlTmFtZX0ubmdzdHlsZS5qc2ApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbmdNb2R1bGVKaXRVcmwobW9kdWxlTWV0YTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEpOiBzdHJpbmcge1xuICByZXR1cm4gaml0U291cmNlVXJsKGAke2lkZW50aWZpZXJOYW1lKG1vZHVsZU1ldGEudHlwZSl9L21vZHVsZS5uZ2ZhY3RvcnkuanNgKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHRlbXBsYXRlSml0VXJsKFxuICAgIG5nTW9kdWxlVHlwZTogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSwgY29tcE1ldGE6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSk6IHN0cmluZyB7XG4gIHJldHVybiBqaXRTb3VyY2VVcmwoXG4gICAgICBgJHtpZGVudGlmaWVyTmFtZShuZ01vZHVsZVR5cGUpfS8ke2lkZW50aWZpZXJOYW1lKGNvbXBNZXRhLnR5cGUpfS5uZ2ZhY3RvcnkuanNgKTtcbn1cbiJdfQ==