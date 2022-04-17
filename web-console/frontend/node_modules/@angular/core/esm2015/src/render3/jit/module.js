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
import { getCompilerFacade } from '../../compiler/compiler_facade';
import { resolveForwardRef } from '../../di/forward_ref';
import { NG_INJECTOR_DEF } from '../../di/interface/defs';
import { reflectDependencies } from '../../di/jit/util';
import { deepForEach, flatten } from '../../util/array_utils';
import { assertDefined } from '../../util/assert';
import { getComponentDef, getDirectiveDef, getNgModuleDef, getPipeDef } from '../definition';
import { NG_COMPONENT_DEF, NG_DIRECTIVE_DEF, NG_MODULE_DEF, NG_PIPE_DEF } from '../fields';
import { maybeUnwrapFn, stringifyForError } from '../util/misc_utils';
import { angularCoreEnv } from './environment';
/** @type {?} */
const EMPTY_ARRAY = [];
/**
 * @record
 */
function ModuleQueueItem() { }
if (false) {
    /** @type {?} */
    ModuleQueueItem.prototype.moduleType;
    /** @type {?} */
    ModuleQueueItem.prototype.ngModule;
}
/** @type {?} */
const moduleQueue = [];
/**
 * Enqueues moduleDef to be checked later to see if scope can be set on its
 * component declarations.
 * @param {?} moduleType
 * @param {?} ngModule
 * @return {?}
 */
function enqueueModuleForDelayedScoping(moduleType, ngModule) {
    moduleQueue.push({ moduleType, ngModule });
}
/** @type {?} */
let flushingModuleQueue = false;
/**
 * Loops over queued module definitions, if a given module definition has all of its
 * declarations resolved, it dequeues that module definition and sets the scope on
 * its declarations.
 * @return {?}
 */
export function flushModuleScopingQueueAsMuchAsPossible() {
    if (!flushingModuleQueue) {
        flushingModuleQueue = true;
        try {
            for (let i = moduleQueue.length - 1; i >= 0; i--) {
                const { moduleType, ngModule } = moduleQueue[i];
                if (ngModule.declarations && ngModule.declarations.every(isResolvedDeclaration)) {
                    // dequeue
                    moduleQueue.splice(i, 1);
                    setScopeOnDeclaredComponents(moduleType, ngModule);
                }
            }
        }
        finally {
            flushingModuleQueue = false;
        }
    }
}
/**
 * Returns truthy if a declaration has resolved. If the declaration happens to be
 * an array of declarations, it will recurse to check each declaration in that array
 * (which may also be arrays).
 * @param {?} declaration
 * @return {?}
 */
function isResolvedDeclaration(declaration) {
    if (Array.isArray(declaration)) {
        return declaration.every(isResolvedDeclaration);
    }
    return !!resolveForwardRef(declaration);
}
/**
 * Compiles a module in JIT mode.
 *
 * This function automatically gets called when a class has a `\@NgModule` decorator.
 * @param {?} moduleType
 * @param {?=} ngModule
 * @return {?}
 */
export function compileNgModule(moduleType, ngModule = {}) {
    compileNgModuleDefs((/** @type {?} */ (moduleType)), ngModule);
    // Because we don't know if all declarations have resolved yet at the moment the
    // NgModule decorator is executing, we're enqueueing the setting of module scope
    // on its declarations to be run at a later time when all declarations for the module,
    // including forward refs, have resolved.
    enqueueModuleForDelayedScoping(moduleType, ngModule);
}
/**
 * Compiles and adds the `ngModuleDef` and `ngInjectorDef` properties to the module class.
 *
 * It's possible to compile a module via this API which will allow duplicate declarations in its
 * root.
 * @param {?} moduleType
 * @param {?} ngModule
 * @param {?=} allowDuplicateDeclarationsInRoot
 * @return {?}
 */
export function compileNgModuleDefs(moduleType, ngModule, allowDuplicateDeclarationsInRoot = false) {
    ngDevMode && assertDefined(moduleType, 'Required value moduleType');
    ngDevMode && assertDefined(ngModule, 'Required value ngModule');
    /** @type {?} */
    const declarations = flatten(ngModule.declarations || EMPTY_ARRAY);
    /** @type {?} */
    /** @nocollapse */ let ngModuleDef = null;
    Object.defineProperty(moduleType, NG_MODULE_DEF, {
        configurable: true,
        get: (/**
         * @return {?}
         */
        () => {
            if (ngModuleDef === null) {
                if (ngDevMode && ngModule.imports && ngModule.imports.indexOf(moduleType) > -1) {
                    // We need to assert this immediately, because allowing it to continue will cause it to
                    // go into an infinite loop before we've reached the point where we throw all the errors.
                    throw new Error(`'${stringifyForError(moduleType)}' module can't import itself`);
                }
                ngModuleDef = getCompilerFacade().compileNgModule(angularCoreEnv, `ng:///${moduleType.name}/ngModuleDef.js`, {
                    type: moduleType,
                    bootstrap: flatten(ngModule.bootstrap || EMPTY_ARRAY).map(resolveForwardRef),
                    declarations: declarations.map(resolveForwardRef),
                    imports: flatten(ngModule.imports || EMPTY_ARRAY)
                        .map(resolveForwardRef)
                        .map(expandModuleWithProviders),
                    exports: flatten(ngModule.exports || EMPTY_ARRAY)
                        .map(resolveForwardRef)
                        .map(expandModuleWithProviders),
                    emitInline: true,
                    schemas: ngModule.schemas ? flatten(ngModule.schemas) : null,
                    id: ngModule.id || null,
                });
            }
            return ngModuleDef;
        })
    });
    /** @type {?} */
    /** @nocollapse */ let ngInjectorDef = null;
    Object.defineProperty(moduleType, NG_INJECTOR_DEF, {
        get: (/**
         * @return {?}
         */
        () => {
            if (ngInjectorDef === null) {
                ngDevMode && verifySemanticsOfNgModuleDef((/** @type {?} */ ((/** @type {?} */ (moduleType)))), allowDuplicateDeclarationsInRoot);
                /** @type {?} */
                const meta = {
                    name: moduleType.name,
                    type: moduleType,
                    deps: reflectDependencies(moduleType),
                    providers: ngModule.providers || EMPTY_ARRAY,
                    imports: [
                        (ngModule.imports || EMPTY_ARRAY).map(resolveForwardRef),
                        (ngModule.exports || EMPTY_ARRAY).map(resolveForwardRef),
                    ],
                };
                ngInjectorDef = getCompilerFacade().compileInjector(angularCoreEnv, `ng:///${moduleType.name}/ngInjectorDef.js`, meta);
            }
            return ngInjectorDef;
        }),
        // Make the property configurable in dev mode to allow overriding in tests
        configurable: !!ngDevMode,
    });
}
/**
 * @param {?} moduleType
 * @param {?} allowDuplicateDeclarationsInRoot
 * @param {?=} importingModule
 * @return {?}
 */
function verifySemanticsOfNgModuleDef(moduleType, allowDuplicateDeclarationsInRoot, importingModule) {
    if (verifiedNgModule.get(moduleType))
        return;
    verifiedNgModule.set(moduleType, true);
    moduleType = resolveForwardRef(moduleType);
    /** @type {?} */
    let ngModuleDef;
    if (importingModule) {
        ngModuleDef = (/** @type {?} */ (getNgModuleDef(moduleType)));
        if (!ngModuleDef) {
            throw new Error(`Unexpected value '${moduleType.name}' imported by the module '${importingModule.name}'. Please add an @NgModule annotation.`);
        }
    }
    else {
        ngModuleDef = getNgModuleDef(moduleType, true);
    }
    /** @type {?} */
    const errors = [];
    /** @type {?} */
    const declarations = maybeUnwrapFn(ngModuleDef.declarations);
    /** @type {?} */
    const imports = maybeUnwrapFn(ngModuleDef.imports);
    flatten(imports).map(unwrapModuleWithProvidersImports).forEach((/**
     * @param {?} mod
     * @return {?}
     */
    mod => {
        verifySemanticsOfNgModuleImport(mod, moduleType);
        verifySemanticsOfNgModuleDef(mod, false, moduleType);
    }));
    /** @type {?} */
    const exports = maybeUnwrapFn(ngModuleDef.exports);
    declarations.forEach(verifyDeclarationsHaveDefinitions);
    /** @type {?} */
    const combinedDeclarations = [
        ...declarations.map(resolveForwardRef),
        ...flatten(imports.map(computeCombinedExports)).map(resolveForwardRef),
    ];
    exports.forEach(verifyExportsAreDeclaredOrReExported);
    declarations.forEach((/**
     * @param {?} decl
     * @return {?}
     */
    decl => verifyDeclarationIsUnique(decl, allowDuplicateDeclarationsInRoot)));
    declarations.forEach(verifyComponentEntryComponentsIsPartOfNgModule);
    /** @type {?} */
    const ngModule = getAnnotation(moduleType, 'NgModule');
    if (ngModule) {
        ngModule.imports &&
            flatten(ngModule.imports).map(unwrapModuleWithProvidersImports).forEach((/**
             * @param {?} mod
             * @return {?}
             */
            mod => {
                verifySemanticsOfNgModuleImport(mod, moduleType);
                verifySemanticsOfNgModuleDef(mod, false, moduleType);
            }));
        ngModule.bootstrap && deepForEach(ngModule.bootstrap, verifyCorrectBootstrapType);
        ngModule.bootstrap && deepForEach(ngModule.bootstrap, verifyComponentIsPartOfNgModule);
        ngModule.entryComponents &&
            deepForEach(ngModule.entryComponents, verifyComponentIsPartOfNgModule);
    }
    // Throw Error if any errors were detected.
    if (errors.length) {
        throw new Error(errors.join('\n'));
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @param {?} type
     * @return {?}
     */
    function verifyDeclarationsHaveDefinitions(type) {
        type = resolveForwardRef(type);
        /** @type {?} */
        const def = getComponentDef(type) || getDirectiveDef(type) || getPipeDef(type);
        if (!def) {
            errors.push(`Unexpected value '${stringifyForError(type)}' declared by the module '${stringifyForError(moduleType)}'. Please add a @Pipe/@Directive/@Component annotation.`);
        }
    }
    /**
     * @param {?} type
     * @return {?}
     */
    function verifyExportsAreDeclaredOrReExported(type) {
        type = resolveForwardRef(type);
        /** @type {?} */
        const kind = getComponentDef(type) && 'component' || getDirectiveDef(type) && 'directive' ||
            getPipeDef(type) && 'pipe';
        if (kind) {
            // only checked if we are declared as Component, Directive, or Pipe
            // Modules don't need to be declared or imported.
            if (combinedDeclarations.lastIndexOf(type) === -1) {
                // We are exporting something which we don't explicitly declare or import.
                errors.push(`Can't export ${kind} ${stringifyForError(type)} from ${stringifyForError(moduleType)} as it was neither declared nor imported!`);
            }
        }
    }
    /**
     * @param {?} type
     * @param {?} suppressErrors
     * @return {?}
     */
    function verifyDeclarationIsUnique(type, suppressErrors) {
        type = resolveForwardRef(type);
        /** @type {?} */
        const existingModule = ownerNgModule.get(type);
        if (existingModule && existingModule !== moduleType) {
            if (!suppressErrors) {
                /** @type {?} */
                const modules = [existingModule, moduleType].map(stringifyForError).sort();
                errors.push(`Type ${stringifyForError(type)} is part of the declarations of 2 modules: ${modules[0]} and ${modules[1]}! ` +
                    `Please consider moving ${stringifyForError(type)} to a higher module that imports ${modules[0]} and ${modules[1]}. ` +
                    `You can also create a new NgModule that exports and includes ${stringifyForError(type)} then import that NgModule in ${modules[0]} and ${modules[1]}.`);
            }
        }
        else {
            // Mark type as having owner.
            ownerNgModule.set(type, moduleType);
        }
    }
    /**
     * @param {?} type
     * @return {?}
     */
    function verifyComponentIsPartOfNgModule(type) {
        type = resolveForwardRef(type);
        /** @type {?} */
        const existingModule = ownerNgModule.get(type);
        if (!existingModule) {
            errors.push(`Component ${stringifyForError(type)} is not part of any NgModule or the module has not been imported into your module.`);
        }
    }
    /**
     * @param {?} type
     * @return {?}
     */
    function verifyCorrectBootstrapType(type) {
        type = resolveForwardRef(type);
        if (!getComponentDef(type)) {
            errors.push(`${stringifyForError(type)} cannot be used as an entry component.`);
        }
    }
    /**
     * @param {?} type
     * @return {?}
     */
    function verifyComponentEntryComponentsIsPartOfNgModule(type) {
        type = resolveForwardRef(type);
        if (getComponentDef(type)) {
            // We know we are component
            /** @type {?} */
            const component = getAnnotation(type, 'Component');
            if (component && component.entryComponents) {
                deepForEach(component.entryComponents, verifyComponentIsPartOfNgModule);
            }
        }
    }
    /**
     * @param {?} type
     * @param {?} importingModule
     * @return {?}
     */
    function verifySemanticsOfNgModuleImport(type, importingModule) {
        type = resolveForwardRef(type);
        if (getComponentDef(type) || getDirectiveDef(type)) {
            throw new Error(`Unexpected directive '${type.name}' imported by the module '${importingModule.name}'. Please add an @NgModule annotation.`);
        }
        if (getPipeDef(type)) {
            throw new Error(`Unexpected pipe '${type.name}' imported by the module '${importingModule.name}'. Please add an @NgModule annotation.`);
        }
    }
}
/**
 * @param {?} typeOrWithProviders
 * @return {?}
 */
function unwrapModuleWithProvidersImports(typeOrWithProviders) {
    typeOrWithProviders = resolveForwardRef(typeOrWithProviders);
    return ((/** @type {?} */ (typeOrWithProviders))).ngModule || typeOrWithProviders;
}
/**
 * @template T
 * @param {?} type
 * @param {?} name
 * @return {?}
 */
function getAnnotation(type, name) {
    /** @type {?} */
    let annotation = null;
    collect(type.__annotations__);
    collect(type.decorators);
    return annotation;
    /**
     * @param {?} annotations
     * @return {?}
     */
    function collect(annotations) {
        if (annotations) {
            annotations.forEach(readAnnotation);
        }
    }
    /**
     * @param {?} decorator
     * @return {?}
     */
    function readAnnotation(decorator) {
        if (!annotation) {
            /** @type {?} */
            const proto = Object.getPrototypeOf(decorator);
            if (proto.ngMetadataName == name) {
                annotation = (/** @type {?} */ (decorator));
            }
            else if (decorator.type) {
                /** @type {?} */
                const proto = Object.getPrototypeOf(decorator.type);
                if (proto.ngMetadataName == name) {
                    annotation = decorator.args[0];
                }
            }
        }
    }
}
/**
 * Keep track of compiled components. This is needed because in tests we often want to compile the
 * same component with more than one NgModule. This would cause an error unless we reset which
 * NgModule the component belongs to. We keep the list of compiled components here so that the
 * TestBed can reset it later.
 * @type {?}
 */
let ownerNgModule = new Map();
/** @type {?} */
let verifiedNgModule = new Map();
/**
 * @return {?}
 */
export function resetCompiledComponents() {
    ownerNgModule = new Map();
    verifiedNgModule = new Map();
    moduleQueue.length = 0;
}
/**
 * Computes the combined declarations of explicit declarations, as well as declarations inherited by
 * traversing the exports of imported modules.
 * @param {?} type
 * @return {?}
 */
function computeCombinedExports(type) {
    type = resolveForwardRef(type);
    /** @type {?} */
    /** @nocollapse */ const ngModuleDef = getNgModuleDef(type, true);
    return [...flatten(maybeUnwrapFn(ngModuleDef.exports).map((/**
         * @param {?} type
         * @return {?}
         */
        (type) => {
            /** @type {?} */
            /** @nocollapse */ const ngModuleDef = getNgModuleDef(type);
            if (ngModuleDef) {
                verifySemanticsOfNgModuleDef((/** @type {?} */ ((/** @type {?} */ (type)))), false);
                return computeCombinedExports(type);
            }
            else {
                return type;
            }
        })))];
}
/**
 * Some declared components may be compiled asynchronously, and thus may not have their
 * ngComponentDef set yet. If this is the case, then a reference to the module is written into
 * the `ngSelectorScope` property of the declared type.
 * @param {?} moduleType
 * @param {?} ngModule
 * @return {?}
 */
function setScopeOnDeclaredComponents(moduleType, ngModule) {
    /** @type {?} */
    const declarations = flatten(ngModule.declarations || EMPTY_ARRAY);
    /** @type {?} */
    const transitiveScopes = transitiveScopesFor(moduleType);
    declarations.forEach((/**
     * @param {?} declaration
     * @return {?}
     */
    declaration => {
        if (declaration.hasOwnProperty(NG_COMPONENT_DEF)) {
            // An `ngComponentDef` field exists - go ahead and patch the component directly.
            /** @type {?} */
            const component = (/** @type {?} */ (declaration));
            /** @type {?} */
            const componentDef = (/** @type {?} */ (getComponentDef(component)));
            patchComponentDefWithScope(componentDef, transitiveScopes);
        }
        else if (!declaration.hasOwnProperty(NG_DIRECTIVE_DEF) && !declaration.hasOwnProperty(NG_PIPE_DEF)) {
            // Set `ngSelectorScope` for future reference when the component compilation finishes.
            ((/** @type {?} */ (declaration))).ngSelectorScope = moduleType;
        }
    }));
}
/**
 * Patch the definition of a component with directives and pipes from the compilation scope of
 * a given module.
 * @template C
 * @param {?} componentDef
 * @param {?} transitiveScopes
 * @return {?}
 */
export function patchComponentDefWithScope(componentDef, transitiveScopes) {
    componentDef.directiveDefs = (/**
     * @return {?}
     */
    () => Array.from(transitiveScopes.compilation.directives)
        .map((/**
     * @param {?} dir
     * @return {?}
     */
    dir => dir.hasOwnProperty(NG_COMPONENT_DEF) ? (/** @type {?} */ (getComponentDef(dir))) :
        (/** @type {?} */ (getDirectiveDef(dir)))))
        .filter((/**
     * @param {?} def
     * @return {?}
     */
    def => !!def)));
    componentDef.pipeDefs = (/**
     * @return {?}
     */
    () => Array.from(transitiveScopes.compilation.pipes).map((/**
     * @param {?} pipe
     * @return {?}
     */
    pipe => (/** @type {?} */ (getPipeDef(pipe))))));
    componentDef.schemas = transitiveScopes.schemas;
    // Since we avoid Components/Directives/Pipes recompiling in case there are no overrides, we
    // may face a problem where previously compiled defs available to a given Component/Directive
    // are cached in TView and may become stale (in case any of these defs gets recompiled). In
    // order to avoid this problem, we force fresh TView to be created.
    componentDef.tView = null;
}
/**
 * Compute the pair of transitive scopes (compilation scope and exported scope) for a given module.
 *
 * This operation is memoized and the result is cached on the module's definition. It can be called
 * on modules with components that have not fully compiled yet, but the result should not be used
 * until they have.
 * @template T
 * @param {?} moduleType
 * @param {?=} processNgModuleFn
 * @return {?}
 */
export function transitiveScopesFor(moduleType, processNgModuleFn) {
    if (!isNgModule(moduleType)) {
        throw new Error(`${moduleType.name} does not have an ngModuleDef`);
    }
    /** @type {?} */
    const def = (/** @type {?} */ (getNgModuleDef(moduleType)));
    if (def.transitiveCompileScopes !== null) {
        return def.transitiveCompileScopes;
    }
    /** @type {?} */
    const scopes = {
        schemas: def.schemas || null,
        compilation: {
            directives: new Set(),
            pipes: new Set(),
        },
        exported: {
            directives: new Set(),
            pipes: new Set(),
        },
    };
    maybeUnwrapFn(def.declarations).forEach((/**
     * @param {?} declared
     * @return {?}
     */
    declared => {
        /** @type {?} */
        const declaredWithDefs = (/** @type {?} */ (declared));
        if (getPipeDef(declaredWithDefs)) {
            scopes.compilation.pipes.add(declared);
        }
        else {
            // Either declared has an ngComponentDef or ngDirectiveDef, or it's a component which hasn't
            // had its template compiled yet. In either case, it gets added to the compilation's
            // directives.
            scopes.compilation.directives.add(declared);
        }
    }));
    maybeUnwrapFn(def.imports).forEach((/**
     * @template I
     * @param {?} imported
     * @return {?}
     */
    (imported) => {
        /** @type {?} */
        const importedType = (/** @type {?} */ (imported));
        if (!isNgModule(importedType)) {
            throw new Error(`Importing ${importedType.name} which does not have an ngModuleDef`);
        }
        if (processNgModuleFn) {
            processNgModuleFn((/** @type {?} */ (importedType)));
        }
        // When this module imports another, the imported module's exported directives and pipes are
        // added to the compilation scope of this module.
        /** @type {?} */
        const importedScope = transitiveScopesFor(importedType, processNgModuleFn);
        importedScope.exported.directives.forEach((/**
         * @param {?} entry
         * @return {?}
         */
        entry => scopes.compilation.directives.add(entry)));
        importedScope.exported.pipes.forEach((/**
         * @param {?} entry
         * @return {?}
         */
        entry => scopes.compilation.pipes.add(entry)));
    }));
    maybeUnwrapFn(def.exports).forEach((/**
     * @template E
     * @param {?} exported
     * @return {?}
     */
    (exported) => {
        /** @type {?} */
        const exportedType = (/** @type {?} */ (exported));
        // Either the type is a module, a pipe, or a component/directive (which may not have an
        // ngComponentDef as it might be compiled asynchronously).
        if (isNgModule(exportedType)) {
            // When this module exports another, the exported module's exported directives and pipes are
            // added to both the compilation and exported scopes of this module.
            /** @type {?} */
            const exportedScope = transitiveScopesFor(exportedType, processNgModuleFn);
            exportedScope.exported.directives.forEach((/**
             * @param {?} entry
             * @return {?}
             */
            entry => {
                scopes.compilation.directives.add(entry);
                scopes.exported.directives.add(entry);
            }));
            exportedScope.exported.pipes.forEach((/**
             * @param {?} entry
             * @return {?}
             */
            entry => {
                scopes.compilation.pipes.add(entry);
                scopes.exported.pipes.add(entry);
            }));
        }
        else if (getPipeDef(exportedType)) {
            scopes.exported.pipes.add(exportedType);
        }
        else {
            scopes.exported.directives.add(exportedType);
        }
    }));
    def.transitiveCompileScopes = scopes;
    return scopes;
}
/**
 * @param {?} value
 * @return {?}
 */
function expandModuleWithProviders(value) {
    if (isModuleWithProviders(value)) {
        return value.ngModule;
    }
    return value;
}
/**
 * @param {?} value
 * @return {?}
 */
function isModuleWithProviders(value) {
    return ((/** @type {?} */ (value))).ngModule !== undefined;
}
/**
 * @template T
 * @param {?} value
 * @return {?}
 */
function isNgModule(value) {
    return !!getNgModuleDef(value);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9kdWxlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9qaXQvbW9kdWxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUEyQixpQkFBaUIsRUFBQyxNQUFNLGdDQUFnQyxDQUFDO0FBQzNGLE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQ3ZELE9BQU8sRUFBQyxlQUFlLEVBQUMsTUFBTSx5QkFBeUIsQ0FBQztBQUN4RCxPQUFPLEVBQUMsbUJBQW1CLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUl0RCxPQUFPLEVBQUMsV0FBVyxFQUFFLE9BQU8sRUFBQyxNQUFNLHdCQUF3QixDQUFDO0FBQzVELE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUNoRCxPQUFPLEVBQUMsZUFBZSxFQUFFLGVBQWUsRUFBRSxjQUFjLEVBQUUsVUFBVSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQzNGLE9BQU8sRUFBQyxnQkFBZ0IsRUFBRSxnQkFBZ0IsRUFBRSxhQUFhLEVBQUUsV0FBVyxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBR3pGLE9BQU8sRUFBQyxhQUFhLEVBQUUsaUJBQWlCLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUVwRSxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sZUFBZSxDQUFDOztNQUV2QyxXQUFXLEdBQWdCLEVBQUU7Ozs7QUFFbkMsOEJBR0M7OztJQUZDLHFDQUFzQjs7SUFDdEIsbUNBQW1COzs7TUFHZixXQUFXLEdBQXNCLEVBQUU7Ozs7Ozs7O0FBTXpDLFNBQVMsOEJBQThCLENBQUMsVUFBcUIsRUFBRSxRQUFrQjtJQUMvRSxXQUFXLENBQUMsSUFBSSxDQUFDLEVBQUMsVUFBVSxFQUFFLFFBQVEsRUFBQyxDQUFDLENBQUM7QUFDM0MsQ0FBQzs7SUFFRyxtQkFBbUIsR0FB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QUMsRUFBRTtRQUNoQyxPQUFPLEtBQUssQ0FBQyxRQUFRLENBQUM7S0FDdkI7SUFDRCxPQUFPLEtBQUssQ0FBQztBQUNmLENBQUM7Ozs7O0FBRUQsU0FBUyxxQkFBcUIsQ0FBQyxLQUFVO0lBQ3ZDLE9BQU8sQ0FBQyxtQkFBQSxLQUFLLEVBQW1CLENBQUMsQ0FBQyxRQUFRLEtBQUssU0FBUyxDQUFDO0FBQzNELENBQUM7Ozs7OztBQUVELFNBQVMsVUFBVSxDQUFJLEtBQWM7SUFDbkMsT0FBTyxDQUFDLENBQUMsY0FBYyxDQUFDLEtBQUssQ0FBQyxDQUFDO0FBQ2pDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7UjNJbmplY3Rvck1ldGFkYXRhRmFjYWRlLCBnZXRDb21waWxlckZhY2FkZX0gZnJvbSAnLi4vLi4vY29tcGlsZXIvY29tcGlsZXJfZmFjYWRlJztcbmltcG9ydCB7cmVzb2x2ZUZvcndhcmRSZWZ9IGZyb20gJy4uLy4uL2RpL2ZvcndhcmRfcmVmJztcbmltcG9ydCB7TkdfSU5KRUNUT1JfREVGfSBmcm9tICcuLi8uLi9kaS9pbnRlcmZhY2UvZGVmcyc7XG5pbXBvcnQge3JlZmxlY3REZXBlbmRlbmNpZXN9IGZyb20gJy4uLy4uL2RpL2ppdC91dGlsJztcbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtDb21wb25lbnR9IGZyb20gJy4uLy4uL21ldGFkYXRhJztcbmltcG9ydCB7TW9kdWxlV2l0aFByb3ZpZGVycywgTmdNb2R1bGUsIE5nTW9kdWxlRGVmLCBOZ01vZHVsZVRyYW5zaXRpdmVTY29wZXN9IGZyb20gJy4uLy4uL21ldGFkYXRhL25nX21vZHVsZSc7XG5pbXBvcnQge2RlZXBGb3JFYWNoLCBmbGF0dGVufSBmcm9tICcuLi8uLi91dGlsL2FycmF5X3V0aWxzJztcbmltcG9ydCB7YXNzZXJ0RGVmaW5lZH0gZnJvbSAnLi4vLi4vdXRpbC9hc3NlcnQnO1xuaW1wb3J0IHtnZXRDb21wb25lbnREZWYsIGdldERpcmVjdGl2ZURlZiwgZ2V0TmdNb2R1bGVEZWYsIGdldFBpcGVEZWZ9IGZyb20gJy4uL2RlZmluaXRpb24nO1xuaW1wb3J0IHtOR19DT01QT05FTlRfREVGLCBOR19ESVJFQ1RJVkVfREVGLCBOR19NT0RVTEVfREVGLCBOR19QSVBFX0RFRn0gZnJvbSAnLi4vZmllbGRzJztcbmltcG9ydCB7Q29tcG9uZW50RGVmfSBmcm9tICcuLi9pbnRlcmZhY2VzL2RlZmluaXRpb24nO1xuaW1wb3J0IHtOZ01vZHVsZVR5cGV9IGZyb20gJy4uL25nX21vZHVsZV9yZWYnO1xuaW1wb3J0IHttYXliZVVud3JhcEZuLCBzdHJpbmdpZnlGb3JFcnJvcn0gZnJvbSAnLi4vdXRpbC9taXNjX3V0aWxzJztcblxuaW1wb3J0IHthbmd1bGFyQ29yZUVudn0gZnJvbSAnLi9lbnZpcm9ubWVudCc7XG5cbmNvbnN0IEVNUFRZX0FSUkFZOiBUeXBlPGFueT5bXSA9IFtdO1xuXG5pbnRlcmZhY2UgTW9kdWxlUXVldWVJdGVtIHtcbiAgbW9kdWxlVHlwZTogVHlwZTxhbnk+O1xuICBuZ01vZHVsZTogTmdNb2R1bGU7XG59XG5cbmNvbnN0IG1vZHVsZVF1ZXVlOiBNb2R1bGVRdWV1ZUl0ZW1bXSA9IFtdO1xuXG4vKipcbiAqIEVucXVldWVzIG1vZHVsZURlZiB0byBiZSBjaGVja2VkIGxhdGVyIHRvIHNlZSBpZiBzY29wZSBjYW4gYmUgc2V0IG9uIGl0c1xuICogY29tcG9uZW50IGRlY2xhcmF0aW9ucy5cbiAqL1xuZnVuY3Rpb24gZW5xdWV1ZU1vZHVsZUZvckRlbGF5ZWRTY29waW5nKG1vZHVsZVR5cGU6IFR5cGU8YW55PiwgbmdNb2R1bGU6IE5nTW9kdWxlKSB7XG4gIG1vZHVsZVF1ZXVlLnB1c2goe21vZHVsZVR5cGUsIG5nTW9kdWxlfSk7XG59XG5cbmxldCBmbHVzaGluZ01vZHVsZVF1ZXVlID0gZmFsc2U7XG4vKipcbiAqIExvb3BzIG92ZXIgcXVldWVkIG1vZHVsZSBkZWZpbml0aW9ucywgaWYgYSBnaXZlbiBtb2R1bGUgZGVmaW5pdGlvbiBoYXMgYWxsIG9mIGl0c1xuICogZGVjbGFyYXRpb25zIHJlc29sdmVkLCBpdCBkZXF1ZXVlcyB0aGF0IG1vZHVsZSBkZWZpbml0aW9uIGFuZCBzZXRzIHRoZSBzY29wZSBvblxuICogaXRzIGRlY2xhcmF0aW9ucy5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGZsdXNoTW9kdWxlU2NvcGluZ1F1ZXVlQXNNdWNoQXNQb3NzaWJsZSgpIHtcbiAgaWYgKCFmbHVzaGluZ01vZHVsZVF1ZXVlKSB7XG4gICAgZmx1c2hpbmdNb2R1bGVRdWV1ZSA9IHRydWU7XG4gICAgdHJ5IHtcbiAgICAgIGZvciAobGV0IGkgPSBtb2R1bGVRdWV1ZS5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgICAgICBjb25zdCB7bW9kdWxlVHlwZSwgbmdNb2R1bGV9ID0gbW9kdWxlUXVldWVbaV07XG5cbiAgICAgICAgaWYgKG5nTW9kdWxlLmRlY2xhcmF0aW9ucyAmJiBuZ01vZHVsZS5kZWNsYXJhdGlvbnMuZXZlcnkoaXNSZXNvbHZlZERlY2xhcmF0aW9uKSkge1xuICAgICAgICAgIC8vIGRlcXVldWVcbiAgICAgICAgICBtb2R1bGVRdWV1ZS5zcGxpY2UoaSwgMSk7XG4gICAgICAgICAgc2V0U2NvcGVPbkRlY2xhcmVkQ29tcG9uZW50cyhtb2R1bGVUeXBlLCBuZ01vZHVsZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9IGZpbmFsbHkge1xuICAgICAgZmx1c2hpbmdNb2R1bGVRdWV1ZSA9IGZhbHNlO1xuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIFJldHVybnMgdHJ1dGh5IGlmIGEgZGVjbGFyYXRpb24gaGFzIHJlc29sdmVkLiBJZiB0aGUgZGVjbGFyYXRpb24gaGFwcGVucyB0byBiZVxuICogYW4gYXJyYXkgb2YgZGVjbGFyYXRpb25zLCBpdCB3aWxsIHJlY3Vyc2UgdG8gY2hlY2sgZWFjaCBkZWNsYXJhdGlvbiBpbiB0aGF0IGFycmF5XG4gKiAod2hpY2ggbWF5IGFsc28gYmUgYXJyYXlzKS5cbiAqL1xuZnVuY3Rpb24gaXNSZXNvbHZlZERlY2xhcmF0aW9uKGRlY2xhcmF0aW9uOiBhbnlbXSB8IFR5cGU8YW55Pik6IGJvb2xlYW4ge1xuICBpZiAoQXJyYXkuaXNBcnJheShkZWNsYXJhdGlvbikpIHtcbiAgICByZXR1cm4gZGVjbGFyYXRpb24uZXZlcnkoaXNSZXNvbHZlZERlY2xhcmF0aW9uKTtcbiAgfVxuICByZXR1cm4gISFyZXNvbHZlRm9yd2FyZFJlZihkZWNsYXJhdGlvbik7XG59XG5cbi8qKlxuICogQ29tcGlsZXMgYSBtb2R1bGUgaW4gSklUIG1vZGUuXG4gKlxuICogVGhpcyBmdW5jdGlvbiBhdXRvbWF0aWNhbGx5IGdldHMgY2FsbGVkIHdoZW4gYSBjbGFzcyBoYXMgYSBgQE5nTW9kdWxlYCBkZWNvcmF0b3IuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjb21waWxlTmdNb2R1bGUobW9kdWxlVHlwZTogVHlwZTxhbnk+LCBuZ01vZHVsZTogTmdNb2R1bGUgPSB7fSk6IHZvaWQge1xuICBjb21waWxlTmdNb2R1bGVEZWZzKG1vZHVsZVR5cGUgYXMgTmdNb2R1bGVUeXBlLCBuZ01vZHVsZSk7XG5cbiAgLy8gQmVjYXVzZSB3ZSBkb24ndCBrbm93IGlmIGFsbCBkZWNsYXJhdGlvbnMgaGF2ZSByZXNvbHZlZCB5ZXQgYXQgdGhlIG1vbWVudCB0aGVcbiAgLy8gTmdNb2R1bGUgZGVjb3JhdG9yIGlzIGV4ZWN1dGluZywgd2UncmUgZW5xdWV1ZWluZyB0aGUgc2V0dGluZyBvZiBtb2R1bGUgc2NvcGVcbiAgLy8gb24gaXRzIGRlY2xhcmF0aW9ucyB0byBiZSBydW4gYXQgYSBsYXRlciB0aW1lIHdoZW4gYWxsIGRlY2xhcmF0aW9ucyBmb3IgdGhlIG1vZHVsZSxcbiAgLy8gaW5jbHVkaW5nIGZvcndhcmQgcmVmcywgaGF2ZSByZXNvbHZlZC5cbiAgZW5xdWV1ZU1vZHVsZUZvckRlbGF5ZWRTY29waW5nKG1vZHVsZVR5cGUsIG5nTW9kdWxlKTtcbn1cblxuLyoqXG4gKiBDb21waWxlcyBhbmQgYWRkcyB0aGUgYG5nTW9kdWxlRGVmYCBhbmQgYG5nSW5qZWN0b3JEZWZgIHByb3BlcnRpZXMgdG8gdGhlIG1vZHVsZSBjbGFzcy5cbiAqXG4gKiBJdCdzIHBvc3NpYmxlIHRvIGNvbXBpbGUgYSBtb2R1bGUgdmlhIHRoaXMgQVBJIHdoaWNoIHdpbGwgYWxsb3cgZHVwbGljYXRlIGRlY2xhcmF0aW9ucyBpbiBpdHNcbiAqIHJvb3QuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjb21waWxlTmdNb2R1bGVEZWZzKFxuICAgIG1vZHVsZVR5cGU6IE5nTW9kdWxlVHlwZSwgbmdNb2R1bGU6IE5nTW9kdWxlLFxuICAgIGFsbG93RHVwbGljYXRlRGVjbGFyYXRpb25zSW5Sb290OiBib29sZWFuID0gZmFsc2UpOiB2b2lkIHtcbiAgbmdEZXZNb2RlICYmIGFzc2VydERlZmluZWQobW9kdWxlVHlwZSwgJ1JlcXVpcmVkIHZhbHVlIG1vZHVsZVR5cGUnKTtcbiAgbmdEZXZNb2RlICYmIGFzc2VydERlZmluZWQobmdNb2R1bGUsICdSZXF1aXJlZCB2YWx1ZSBuZ01vZHVsZScpO1xuICBjb25zdCBkZWNsYXJhdGlvbnM6IFR5cGU8YW55PltdID0gZmxhdHRlbihuZ01vZHVsZS5kZWNsYXJhdGlvbnMgfHwgRU1QVFlfQVJSQVkpO1xuICBsZXQgbmdNb2R1bGVEZWY6IGFueSA9IG51bGw7XG4gIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShtb2R1bGVUeXBlLCBOR19NT0RVTEVfREVGLCB7XG4gICAgY29uZmlndXJhYmxlOiB0cnVlLFxuICAgIGdldDogKCkgPT4ge1xuICAgICAgaWYgKG5nTW9kdWxlRGVmID09PSBudWxsKSB7XG4gICAgICAgIGlmIChuZ0Rldk1vZGUgJiYgbmdNb2R1bGUuaW1wb3J0cyAmJiBuZ01vZHVsZS5pbXBvcnRzLmluZGV4T2YobW9kdWxlVHlwZSkgPiAtMSkge1xuICAgICAgICAgIC8vIFdlIG5lZWQgdG8gYXNzZXJ0IHRoaXMgaW1tZWRpYXRlbHksIGJlY2F1c2UgYWxsb3dpbmcgaXQgdG8gY29udGludWUgd2lsbCBjYXVzZSBpdCB0b1xuICAgICAgICAgIC8vIGdvIGludG8gYW4gaW5maW5pdGUgbG9vcCBiZWZvcmUgd2UndmUgcmVhY2hlZCB0aGUgcG9pbnQgd2hlcmUgd2UgdGhyb3cgYWxsIHRoZSBlcnJvcnMuXG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGAnJHtzdHJpbmdpZnlGb3JFcnJvcihtb2R1bGVUeXBlKX0nIG1vZHVsZSBjYW4ndCBpbXBvcnQgaXRzZWxmYCk7XG4gICAgICAgIH1cbiAgICAgICAgbmdNb2R1bGVEZWYgPSBnZXRDb21waWxlckZhY2FkZSgpLmNvbXBpbGVOZ01vZHVsZShcbiAgICAgICAgICAgIGFuZ3VsYXJDb3JlRW52LCBgbmc6Ly8vJHttb2R1bGVUeXBlLm5hbWV9L25nTW9kdWxlRGVmLmpzYCwge1xuICAgICAgICAgICAgICB0eXBlOiBtb2R1bGVUeXBlLFxuICAgICAgICAgICAgICBib290c3RyYXA6IGZsYXR0ZW4obmdNb2R1bGUuYm9vdHN0cmFwIHx8IEVNUFRZX0FSUkFZKS5tYXAocmVzb2x2ZUZvcndhcmRSZWYpLFxuICAgICAgICAgICAgICBkZWNsYXJhdGlvbnM6IGRlY2xhcmF0aW9ucy5tYXAocmVzb2x2ZUZvcndhcmRSZWYpLFxuICAgICAgICAgICAgICBpbXBvcnRzOiBmbGF0dGVuKG5nTW9kdWxlLmltcG9ydHMgfHwgRU1QVFlfQVJSQVkpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAubWFwKHJlc29sdmVGb3J3YXJkUmVmKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcChleHBhbmRNb2R1bGVXaXRoUHJvdmlkZXJzKSxcbiAgICAgICAgICAgICAgZXhwb3J0czogZmxhdHRlbihuZ01vZHVsZS5leHBvcnRzIHx8IEVNUFRZX0FSUkFZKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcChyZXNvbHZlRm9yd2FyZFJlZilcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAoZXhwYW5kTW9kdWxlV2l0aFByb3ZpZGVycyksXG4gICAgICAgICAgICAgIGVtaXRJbmxpbmU6IHRydWUsXG4gICAgICAgICAgICAgIHNjaGVtYXM6IG5nTW9kdWxlLnNjaGVtYXMgPyBmbGF0dGVuKG5nTW9kdWxlLnNjaGVtYXMpIDogbnVsbCxcbiAgICAgICAgICAgICAgaWQ6IG5nTW9kdWxlLmlkIHx8IG51bGwsXG4gICAgICAgICAgICB9KTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBuZ01vZHVsZURlZjtcbiAgICB9XG4gIH0pO1xuXG4gIGxldCBuZ0luamVjdG9yRGVmOiBhbnkgPSBudWxsO1xuICBPYmplY3QuZGVmaW5lUHJvcGVydHkobW9kdWxlVHlwZSwgTkdfSU5KRUNUT1JfREVGLCB7XG4gICAgZ2V0OiAoKSA9PiB7XG4gICAgICBpZiAobmdJbmplY3RvckRlZiA9PT0gbnVsbCkge1xuICAgICAgICBuZ0Rldk1vZGUgJiYgdmVyaWZ5U2VtYW50aWNzT2ZOZ01vZHVsZURlZihcbiAgICAgICAgICAgICAgICAgICAgICAgICBtb2R1bGVUeXBlIGFzIGFueSBhcyBOZ01vZHVsZVR5cGUsIGFsbG93RHVwbGljYXRlRGVjbGFyYXRpb25zSW5Sb290KTtcbiAgICAgICAgY29uc3QgbWV0YTogUjNJbmplY3Rvck1ldGFkYXRhRmFjYWRlID0ge1xuICAgICAgICAgIG5hbWU6IG1vZHVsZVR5cGUubmFtZSxcbiAgICAgICAgICB0eXBlOiBtb2R1bGVUeXBlLFxuICAgICAgICAgIGRlcHM6IHJlZmxlY3REZXBlbmRlbmNpZXMobW9kdWxlVHlwZSksXG4gICAgICAgICAgcHJvdmlkZXJzOiBuZ01vZHVsZS5wcm92aWRlcnMgfHwgRU1QVFlfQVJSQVksXG4gICAgICAgICAgaW1wb3J0czogW1xuICAgICAgICAgICAgKG5nTW9kdWxlLmltcG9ydHMgfHwgRU1QVFlfQVJSQVkpLm1hcChyZXNvbHZlRm9yd2FyZFJlZiksXG4gICAgICAgICAgICAobmdNb2R1bGUuZXhwb3J0cyB8fCBFTVBUWV9BUlJBWSkubWFwKHJlc29sdmVGb3J3YXJkUmVmKSxcbiAgICAgICAgICBdLFxuICAgICAgICB9O1xuICAgICAgICBuZ0luamVjdG9yRGVmID0gZ2V0Q29tcGlsZXJGYWNhZGUoKS5jb21waWxlSW5qZWN0b3IoXG4gICAgICAgICAgICBhbmd1bGFyQ29yZUVudiwgYG5nOi8vLyR7bW9kdWxlVHlwZS5uYW1lfS9uZ0luamVjdG9yRGVmLmpzYCwgbWV0YSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gbmdJbmplY3RvckRlZjtcbiAgICB9LFxuICAgIC8vIE1ha2UgdGhlIHByb3BlcnR5IGNvbmZpZ3VyYWJsZSBpbiBkZXYgbW9kZSB0byBhbGxvdyBvdmVycmlkaW5nIGluIHRlc3RzXG4gICAgY29uZmlndXJhYmxlOiAhIW5nRGV2TW9kZSxcbiAgfSk7XG59XG5cbmZ1bmN0aW9uIHZlcmlmeVNlbWFudGljc09mTmdNb2R1bGVEZWYoXG4gICAgbW9kdWxlVHlwZTogTmdNb2R1bGVUeXBlLCBhbGxvd0R1cGxpY2F0ZURlY2xhcmF0aW9uc0luUm9vdDogYm9vbGVhbixcbiAgICBpbXBvcnRpbmdNb2R1bGU/OiBOZ01vZHVsZVR5cGUpOiB2b2lkIHtcbiAgaWYgKHZlcmlmaWVkTmdNb2R1bGUuZ2V0KG1vZHVsZVR5cGUpKSByZXR1cm47XG4gIHZlcmlmaWVkTmdNb2R1bGUuc2V0KG1vZHVsZVR5cGUsIHRydWUpO1xuICBtb2R1bGVUeXBlID0gcmVzb2x2ZUZvcndhcmRSZWYobW9kdWxlVHlwZSk7XG4gIGxldCBuZ01vZHVsZURlZjogTmdNb2R1bGVEZWY8YW55PjtcbiAgaWYgKGltcG9ydGluZ01vZHVsZSkge1xuICAgIG5nTW9kdWxlRGVmID0gZ2V0TmdNb2R1bGVEZWYobW9kdWxlVHlwZSkgITtcbiAgICBpZiAoIW5nTW9kdWxlRGVmKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYFVuZXhwZWN0ZWQgdmFsdWUgJyR7bW9kdWxlVHlwZS5uYW1lfScgaW1wb3J0ZWQgYnkgdGhlIG1vZHVsZSAnJHtpbXBvcnRpbmdNb2R1bGUubmFtZX0nLiBQbGVhc2UgYWRkIGFuIEBOZ01vZHVsZSBhbm5vdGF0aW9uLmApO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBuZ01vZHVsZURlZiA9IGdldE5nTW9kdWxlRGVmKG1vZHVsZVR5cGUsIHRydWUpO1xuICB9XG4gIGNvbnN0IGVycm9yczogc3RyaW5nW10gPSBbXTtcbiAgY29uc3QgZGVjbGFyYXRpb25zID0gbWF5YmVVbndyYXBGbihuZ01vZHVsZURlZi5kZWNsYXJhdGlvbnMpO1xuICBjb25zdCBpbXBvcnRzID0gbWF5YmVVbndyYXBGbihuZ01vZHVsZURlZi5pbXBvcnRzKTtcbiAgZmxhdHRlbihpbXBvcnRzKS5tYXAodW53cmFwTW9kdWxlV2l0aFByb3ZpZGVyc0ltcG9ydHMpLmZvckVhY2gobW9kID0+IHtcbiAgICB2ZXJpZnlTZW1hbnRpY3NPZk5nTW9kdWxlSW1wb3J0KG1vZCwgbW9kdWxlVHlwZSk7XG4gICAgdmVyaWZ5U2VtYW50aWNzT2ZOZ01vZHVsZURlZihtb2QsIGZhbHNlLCBtb2R1bGVUeXBlKTtcbiAgfSk7XG4gIGNvbnN0IGV4cG9ydHMgPSBtYXliZVVud3JhcEZuKG5nTW9kdWxlRGVmLmV4cG9ydHMpO1xuICBkZWNsYXJhdGlvbnMuZm9yRWFjaCh2ZXJpZnlEZWNsYXJhdGlvbnNIYXZlRGVmaW5pdGlvbnMpO1xuICBjb25zdCBjb21iaW5lZERlY2xhcmF0aW9uczogVHlwZTxhbnk+W10gPSBbXG4gICAgLi4uZGVjbGFyYXRpb25zLm1hcChyZXNvbHZlRm9yd2FyZFJlZiksXG4gICAgLi4uZmxhdHRlbihpbXBvcnRzLm1hcChjb21wdXRlQ29tYmluZWRFeHBvcnRzKSkubWFwKHJlc29sdmVGb3J3YXJkUmVmKSxcbiAgXTtcbiAgZXhwb3J0cy5mb3JFYWNoKHZlcmlmeUV4cG9ydHNBcmVEZWNsYXJlZE9yUmVFeHBvcnRlZCk7XG4gIGRlY2xhcmF0aW9ucy5mb3JFYWNoKGRlY2wgPT4gdmVyaWZ5RGVjbGFyYXRpb25Jc1VuaXF1ZShkZWNsLCBhbGxvd0R1cGxpY2F0ZURlY2xhcmF0aW9uc0luUm9vdCkpO1xuICBkZWNsYXJhdGlvbnMuZm9yRWFjaCh2ZXJpZnlDb21wb25lbnRFbnRyeUNvbXBvbmVudHNJc1BhcnRPZk5nTW9kdWxlKTtcblxuICBjb25zdCBuZ01vZHVsZSA9IGdldEFubm90YXRpb248TmdNb2R1bGU+KG1vZHVsZVR5cGUsICdOZ01vZHVsZScpO1xuICBpZiAobmdNb2R1bGUpIHtcbiAgICBuZ01vZHVsZS5pbXBvcnRzICYmXG4gICAgICAgIGZsYXR0ZW4obmdNb2R1bGUuaW1wb3J0cykubWFwKHVud3JhcE1vZHVsZVdpdGhQcm92aWRlcnNJbXBvcnRzKS5mb3JFYWNoKG1vZCA9PiB7XG4gICAgICAgICAgdmVyaWZ5U2VtYW50aWNzT2ZOZ01vZHVsZUltcG9ydChtb2QsIG1vZHVsZVR5cGUpO1xuICAgICAgICAgIHZlcmlmeVNlbWFudGljc09mTmdNb2R1bGVEZWYobW9kLCBmYWxzZSwgbW9kdWxlVHlwZSk7XG4gICAgICAgIH0pO1xuICAgIG5nTW9kdWxlLmJvb3RzdHJhcCAmJiBkZWVwRm9yRWFjaChuZ01vZHVsZS5ib290c3RyYXAsIHZlcmlmeUNvcnJlY3RCb290c3RyYXBUeXBlKTtcbiAgICBuZ01vZHVsZS5ib290c3RyYXAgJiYgZGVlcEZvckVhY2gobmdNb2R1bGUuYm9vdHN0cmFwLCB2ZXJpZnlDb21wb25lbnRJc1BhcnRPZk5nTW9kdWxlKTtcbiAgICBuZ01vZHVsZS5lbnRyeUNvbXBvbmVudHMgJiZcbiAgICAgICAgZGVlcEZvckVhY2gobmdNb2R1bGUuZW50cnlDb21wb25lbnRzLCB2ZXJpZnlDb21wb25lbnRJc1BhcnRPZk5nTW9kdWxlKTtcbiAgfVxuXG4gIC8vIFRocm93IEVycm9yIGlmIGFueSBlcnJvcnMgd2VyZSBkZXRlY3RlZC5cbiAgaWYgKGVycm9ycy5sZW5ndGgpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoZXJyb3JzLmpvaW4oJ1xcbicpKTtcbiAgfVxuICAvLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy9cbiAgZnVuY3Rpb24gdmVyaWZ5RGVjbGFyYXRpb25zSGF2ZURlZmluaXRpb25zKHR5cGU6IFR5cGU8YW55Pik6IHZvaWQge1xuICAgIHR5cGUgPSByZXNvbHZlRm9yd2FyZFJlZih0eXBlKTtcbiAgICBjb25zdCBkZWYgPSBnZXRDb21wb25lbnREZWYodHlwZSkgfHwgZ2V0RGlyZWN0aXZlRGVmKHR5cGUpIHx8IGdldFBpcGVEZWYodHlwZSk7XG4gICAgaWYgKCFkZWYpIHtcbiAgICAgIGVycm9ycy5wdXNoKFxuICAgICAgICAgIGBVbmV4cGVjdGVkIHZhbHVlICcke3N0cmluZ2lmeUZvckVycm9yKHR5cGUpfScgZGVjbGFyZWQgYnkgdGhlIG1vZHVsZSAnJHtzdHJpbmdpZnlGb3JFcnJvcihtb2R1bGVUeXBlKX0nLiBQbGVhc2UgYWRkIGEgQFBpcGUvQERpcmVjdGl2ZS9AQ29tcG9uZW50IGFubm90YXRpb24uYCk7XG4gICAgfVxuICB9XG5cbiAgZnVuY3Rpb24gdmVyaWZ5RXhwb3J0c0FyZURlY2xhcmVkT3JSZUV4cG9ydGVkKHR5cGU6IFR5cGU8YW55Pikge1xuICAgIHR5cGUgPSByZXNvbHZlRm9yd2FyZFJlZih0eXBlKTtcbiAgICBjb25zdCBraW5kID0gZ2V0Q29tcG9uZW50RGVmKHR5cGUpICYmICdjb21wb25lbnQnIHx8IGdldERpcmVjdGl2ZURlZih0eXBlKSAmJiAnZGlyZWN0aXZlJyB8fFxuICAgICAgICBnZXRQaXBlRGVmKHR5cGUpICYmICdwaXBlJztcbiAgICBpZiAoa2luZCkge1xuICAgICAgLy8gb25seSBjaGVja2VkIGlmIHdlIGFyZSBkZWNsYXJlZCBhcyBDb21wb25lbnQsIERpcmVjdGl2ZSwgb3IgUGlwZVxuICAgICAgLy8gTW9kdWxlcyBkb24ndCBuZWVkIHRvIGJlIGRlY2xhcmVkIG9yIGltcG9ydGVkLlxuICAgICAgaWYgKGNvbWJpbmVkRGVjbGFyYXRpb25zLmxhc3RJbmRleE9mKHR5cGUpID09PSAtMSkge1xuICAgICAgICAvLyBXZSBhcmUgZXhwb3J0aW5nIHNvbWV0aGluZyB3aGljaCB3ZSBkb24ndCBleHBsaWNpdGx5IGRlY2xhcmUgb3IgaW1wb3J0LlxuICAgICAgICBlcnJvcnMucHVzaChcbiAgICAgICAgICAgIGBDYW4ndCBleHBvcnQgJHtraW5kfSAke3N0cmluZ2lmeUZvckVycm9yKHR5cGUpfSBmcm9tICR7c3RyaW5naWZ5Rm9yRXJyb3IobW9kdWxlVHlwZSl9IGFzIGl0IHdhcyBuZWl0aGVyIGRlY2xhcmVkIG5vciBpbXBvcnRlZCFgKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBmdW5jdGlvbiB2ZXJpZnlEZWNsYXJhdGlvbklzVW5pcXVlKHR5cGU6IFR5cGU8YW55Piwgc3VwcHJlc3NFcnJvcnM6IGJvb2xlYW4pIHtcbiAgICB0eXBlID0gcmVzb2x2ZUZvcndhcmRSZWYodHlwZSk7XG4gICAgY29uc3QgZXhpc3RpbmdNb2R1bGUgPSBvd25lck5nTW9kdWxlLmdldCh0eXBlKTtcbiAgICBpZiAoZXhpc3RpbmdNb2R1bGUgJiYgZXhpc3RpbmdNb2R1bGUgIT09IG1vZHVsZVR5cGUpIHtcbiAgICAgIGlmICghc3VwcHJlc3NFcnJvcnMpIHtcbiAgICAgICAgY29uc3QgbW9kdWxlcyA9IFtleGlzdGluZ01vZHVsZSwgbW9kdWxlVHlwZV0ubWFwKHN0cmluZ2lmeUZvckVycm9yKS5zb3J0KCk7XG4gICAgICAgIGVycm9ycy5wdXNoKFxuICAgICAgICAgICAgYFR5cGUgJHtzdHJpbmdpZnlGb3JFcnJvcih0eXBlKX0gaXMgcGFydCBvZiB0aGUgZGVjbGFyYXRpb25zIG9mIDIgbW9kdWxlczogJHttb2R1bGVzWzBdfSBhbmQgJHttb2R1bGVzWzFdfSEgYCArXG4gICAgICAgICAgICBgUGxlYXNlIGNvbnNpZGVyIG1vdmluZyAke3N0cmluZ2lmeUZvckVycm9yKHR5cGUpfSB0byBhIGhpZ2hlciBtb2R1bGUgdGhhdCBpbXBvcnRzICR7bW9kdWxlc1swXX0gYW5kICR7bW9kdWxlc1sxXX0uIGAgK1xuICAgICAgICAgICAgYFlvdSBjYW4gYWxzbyBjcmVhdGUgYSBuZXcgTmdNb2R1bGUgdGhhdCBleHBvcnRzIGFuZCBpbmNsdWRlcyAke3N0cmluZ2lmeUZvckVycm9yKHR5cGUpfSB0aGVuIGltcG9ydCB0aGF0IE5nTW9kdWxlIGluICR7bW9kdWxlc1swXX0gYW5kICR7bW9kdWxlc1sxXX0uYCk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIE1hcmsgdHlwZSBhcyBoYXZpbmcgb3duZXIuXG4gICAgICBvd25lck5nTW9kdWxlLnNldCh0eXBlLCBtb2R1bGVUeXBlKTtcbiAgICB9XG4gIH1cblxuICBmdW5jdGlvbiB2ZXJpZnlDb21wb25lbnRJc1BhcnRPZk5nTW9kdWxlKHR5cGU6IFR5cGU8YW55Pikge1xuICAgIHR5cGUgPSByZXNvbHZlRm9yd2FyZFJlZih0eXBlKTtcbiAgICBjb25zdCBleGlzdGluZ01vZHVsZSA9IG93bmVyTmdNb2R1bGUuZ2V0KHR5cGUpO1xuICAgIGlmICghZXhpc3RpbmdNb2R1bGUpIHtcbiAgICAgIGVycm9ycy5wdXNoKFxuICAgICAgICAgIGBDb21wb25lbnQgJHtzdHJpbmdpZnlGb3JFcnJvcih0eXBlKX0gaXMgbm90IHBhcnQgb2YgYW55IE5nTW9kdWxlIG9yIHRoZSBtb2R1bGUgaGFzIG5vdCBiZWVuIGltcG9ydGVkIGludG8geW91ciBtb2R1bGUuYCk7XG4gICAgfVxuICB9XG5cbiAgZnVuY3Rpb24gdmVyaWZ5Q29ycmVjdEJvb3RzdHJhcFR5cGUodHlwZTogVHlwZTxhbnk+KSB7XG4gICAgdHlwZSA9IHJlc29sdmVGb3J3YXJkUmVmKHR5cGUpO1xuICAgIGlmICghZ2V0Q29tcG9uZW50RGVmKHR5cGUpKSB7XG4gICAgICBlcnJvcnMucHVzaChgJHtzdHJpbmdpZnlGb3JFcnJvcih0eXBlKX0gY2Fubm90IGJlIHVzZWQgYXMgYW4gZW50cnkgY29tcG9uZW50LmApO1xuICAgIH1cbiAgfVxuXG4gIGZ1bmN0aW9uIHZlcmlmeUNvbXBvbmVudEVudHJ5Q29tcG9uZW50c0lzUGFydE9mTmdNb2R1bGUodHlwZTogVHlwZTxhbnk+KSB7XG4gICAgdHlwZSA9IHJlc29sdmVGb3J3YXJkUmVmKHR5cGUpO1xuICAgIGlmIChnZXRDb21wb25lbnREZWYodHlwZSkpIHtcbiAgICAgIC8vIFdlIGtub3cgd2UgYXJlIGNvbXBvbmVudFxuICAgICAgY29uc3QgY29tcG9uZW50ID0gZ2V0QW5ub3RhdGlvbjxDb21wb25lbnQ+KHR5cGUsICdDb21wb25lbnQnKTtcbiAgICAgIGlmIChjb21wb25lbnQgJiYgY29tcG9uZW50LmVudHJ5Q29tcG9uZW50cykge1xuICAgICAgICBkZWVwRm9yRWFjaChjb21wb25lbnQuZW50cnlDb21wb25lbnRzLCB2ZXJpZnlDb21wb25lbnRJc1BhcnRPZk5nTW9kdWxlKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBmdW5jdGlvbiB2ZXJpZnlTZW1hbnRpY3NPZk5nTW9kdWxlSW1wb3J0KHR5cGU6IFR5cGU8YW55PiwgaW1wb3J0aW5nTW9kdWxlOiBUeXBlPGFueT4pIHtcbiAgICB0eXBlID0gcmVzb2x2ZUZvcndhcmRSZWYodHlwZSk7XG5cbiAgICBpZiAoZ2V0Q29tcG9uZW50RGVmKHR5cGUpIHx8IGdldERpcmVjdGl2ZURlZih0eXBlKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBVbmV4cGVjdGVkIGRpcmVjdGl2ZSAnJHt0eXBlLm5hbWV9JyBpbXBvcnRlZCBieSB0aGUgbW9kdWxlICcke2ltcG9ydGluZ01vZHVsZS5uYW1lfScuIFBsZWFzZSBhZGQgYW4gQE5nTW9kdWxlIGFubm90YXRpb24uYCk7XG4gICAgfVxuXG4gICAgaWYgKGdldFBpcGVEZWYodHlwZSkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgVW5leHBlY3RlZCBwaXBlICcke3R5cGUubmFtZX0nIGltcG9ydGVkIGJ5IHRoZSBtb2R1bGUgJyR7aW1wb3J0aW5nTW9kdWxlLm5hbWV9Jy4gUGxlYXNlIGFkZCBhbiBATmdNb2R1bGUgYW5ub3RhdGlvbi5gKTtcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gdW53cmFwTW9kdWxlV2l0aFByb3ZpZGVyc0ltcG9ydHMoXG4gICAgdHlwZU9yV2l0aFByb3ZpZGVyczogTmdNb2R1bGVUeXBlPGFueT58IHtuZ01vZHVsZTogTmdNb2R1bGVUeXBlPGFueT59KTogTmdNb2R1bGVUeXBlPGFueT4ge1xuICB0eXBlT3JXaXRoUHJvdmlkZXJzID0gcmVzb2x2ZUZvcndhcmRSZWYodHlwZU9yV2l0aFByb3ZpZGVycyk7XG4gIHJldHVybiAodHlwZU9yV2l0aFByb3ZpZGVycyBhcyBhbnkpLm5nTW9kdWxlIHx8IHR5cGVPcldpdGhQcm92aWRlcnM7XG59XG5cbmZ1bmN0aW9uIGdldEFubm90YXRpb248VD4odHlwZTogYW55LCBuYW1lOiBzdHJpbmcpOiBUfG51bGwge1xuICBsZXQgYW5ub3RhdGlvbjogVHxudWxsID0gbnVsbDtcbiAgY29sbGVjdCh0eXBlLl9fYW5ub3RhdGlvbnNfXyk7XG4gIGNvbGxlY3QodHlwZS5kZWNvcmF0b3JzKTtcbiAgcmV0dXJuIGFubm90YXRpb247XG5cbiAgZnVuY3Rpb24gY29sbGVjdChhbm5vdGF0aW9uczogYW55W10gfCBudWxsKSB7XG4gICAgaWYgKGFubm90YXRpb25zKSB7XG4gICAgICBhbm5vdGF0aW9ucy5mb3JFYWNoKHJlYWRBbm5vdGF0aW9uKTtcbiAgICB9XG4gIH1cblxuICBmdW5jdGlvbiByZWFkQW5ub3RhdGlvbihcbiAgICAgIGRlY29yYXRvcjoge3R5cGU6IHtwcm90b3R5cGU6IHtuZ01ldGFkYXRhTmFtZTogc3RyaW5nfSwgYXJnczogYW55W119LCBhcmdzOiBhbnl9KTogdm9pZCB7XG4gICAgaWYgKCFhbm5vdGF0aW9uKSB7XG4gICAgICBjb25zdCBwcm90byA9IE9iamVjdC5nZXRQcm90b3R5cGVPZihkZWNvcmF0b3IpO1xuICAgICAgaWYgKHByb3RvLm5nTWV0YWRhdGFOYW1lID09IG5hbWUpIHtcbiAgICAgICAgYW5ub3RhdGlvbiA9IGRlY29yYXRvciBhcyBhbnk7XG4gICAgICB9IGVsc2UgaWYgKGRlY29yYXRvci50eXBlKSB7XG4gICAgICAgIGNvbnN0IHByb3RvID0gT2JqZWN0LmdldFByb3RvdHlwZU9mKGRlY29yYXRvci50eXBlKTtcbiAgICAgICAgaWYgKHByb3RvLm5nTWV0YWRhdGFOYW1lID09IG5hbWUpIHtcbiAgICAgICAgICBhbm5vdGF0aW9uID0gZGVjb3JhdG9yLmFyZ3NbMF07XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuLyoqXG4gKiBLZWVwIHRyYWNrIG9mIGNvbXBpbGVkIGNvbXBvbmVudHMuIFRoaXMgaXMgbmVlZGVkIGJlY2F1c2UgaW4gdGVzdHMgd2Ugb2Z0ZW4gd2FudCB0byBjb21waWxlIHRoZVxuICogc2FtZSBjb21wb25lbnQgd2l0aCBtb3JlIHRoYW4gb25lIE5nTW9kdWxlLiBUaGlzIHdvdWxkIGNhdXNlIGFuIGVycm9yIHVubGVzcyB3ZSByZXNldCB3aGljaFxuICogTmdNb2R1bGUgdGhlIGNvbXBvbmVudCBiZWxvbmdzIHRvLiBXZSBrZWVwIHRoZSBsaXN0IG9mIGNvbXBpbGVkIGNvbXBvbmVudHMgaGVyZSBzbyB0aGF0IHRoZVxuICogVGVzdEJlZCBjYW4gcmVzZXQgaXQgbGF0ZXIuXG4gKi9cbmxldCBvd25lck5nTW9kdWxlID0gbmV3IE1hcDxUeXBlPGFueT4sIE5nTW9kdWxlVHlwZTxhbnk+PigpO1xubGV0IHZlcmlmaWVkTmdNb2R1bGUgPSBuZXcgTWFwPE5nTW9kdWxlVHlwZTxhbnk+LCBib29sZWFuPigpO1xuXG5leHBvcnQgZnVuY3Rpb24gcmVzZXRDb21waWxlZENvbXBvbmVudHMoKTogdm9pZCB7XG4gIG93bmVyTmdNb2R1bGUgPSBuZXcgTWFwPFR5cGU8YW55PiwgTmdNb2R1bGVUeXBlPGFueT4+KCk7XG4gIHZlcmlmaWVkTmdNb2R1bGUgPSBuZXcgTWFwPE5nTW9kdWxlVHlwZTxhbnk+LCBib29sZWFuPigpO1xuICBtb2R1bGVRdWV1ZS5sZW5ndGggPSAwO1xufVxuXG4vKipcbiAqIENvbXB1dGVzIHRoZSBjb21iaW5lZCBkZWNsYXJhdGlvbnMgb2YgZXhwbGljaXQgZGVjbGFyYXRpb25zLCBhcyB3ZWxsIGFzIGRlY2xhcmF0aW9ucyBpbmhlcml0ZWQgYnlcbiAqIHRyYXZlcnNpbmcgdGhlIGV4cG9ydHMgb2YgaW1wb3J0ZWQgbW9kdWxlcy5cbiAqIEBwYXJhbSB0eXBlXG4gKi9cbmZ1bmN0aW9uIGNvbXB1dGVDb21iaW5lZEV4cG9ydHModHlwZTogVHlwZTxhbnk+KTogVHlwZTxhbnk+W10ge1xuICB0eXBlID0gcmVzb2x2ZUZvcndhcmRSZWYodHlwZSk7XG4gIGNvbnN0IG5nTW9kdWxlRGVmID0gZ2V0TmdNb2R1bGVEZWYodHlwZSwgdHJ1ZSk7XG4gIHJldHVybiBbLi4uZmxhdHRlbihtYXliZVVud3JhcEZuKG5nTW9kdWxlRGVmLmV4cG9ydHMpLm1hcCgodHlwZSkgPT4ge1xuICAgIGNvbnN0IG5nTW9kdWxlRGVmID0gZ2V0TmdNb2R1bGVEZWYodHlwZSk7XG4gICAgaWYgKG5nTW9kdWxlRGVmKSB7XG4gICAgICB2ZXJpZnlTZW1hbnRpY3NPZk5nTW9kdWxlRGVmKHR5cGUgYXMgYW55IGFzIE5nTW9kdWxlVHlwZSwgZmFsc2UpO1xuICAgICAgcmV0dXJuIGNvbXB1dGVDb21iaW5lZEV4cG9ydHModHlwZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB0eXBlO1xuICAgIH1cbiAgfSkpXTtcbn1cblxuLyoqXG4gKiBTb21lIGRlY2xhcmVkIGNvbXBvbmVudHMgbWF5IGJlIGNvbXBpbGVkIGFzeW5jaHJvbm91c2x5LCBhbmQgdGh1cyBtYXkgbm90IGhhdmUgdGhlaXJcbiAqIG5nQ29tcG9uZW50RGVmIHNldCB5ZXQuIElmIHRoaXMgaXMgdGhlIGNhc2UsIHRoZW4gYSByZWZlcmVuY2UgdG8gdGhlIG1vZHVsZSBpcyB3cml0dGVuIGludG9cbiAqIHRoZSBgbmdTZWxlY3RvclNjb3BlYCBwcm9wZXJ0eSBvZiB0aGUgZGVjbGFyZWQgdHlwZS5cbiAqL1xuZnVuY3Rpb24gc2V0U2NvcGVPbkRlY2xhcmVkQ29tcG9uZW50cyhtb2R1bGVUeXBlOiBUeXBlPGFueT4sIG5nTW9kdWxlOiBOZ01vZHVsZSkge1xuICBjb25zdCBkZWNsYXJhdGlvbnM6IFR5cGU8YW55PltdID0gZmxhdHRlbihuZ01vZHVsZS5kZWNsYXJhdGlvbnMgfHwgRU1QVFlfQVJSQVkpO1xuXG4gIGNvbnN0IHRyYW5zaXRpdmVTY29wZXMgPSB0cmFuc2l0aXZlU2NvcGVzRm9yKG1vZHVsZVR5cGUpO1xuXG4gIGRlY2xhcmF0aW9ucy5mb3JFYWNoKGRlY2xhcmF0aW9uID0+IHtcbiAgICBpZiAoZGVjbGFyYXRpb24uaGFzT3duUHJvcGVydHkoTkdfQ09NUE9ORU5UX0RFRikpIHtcbiAgICAgIC8vIEFuIGBuZ0NvbXBvbmVudERlZmAgZmllbGQgZXhpc3RzIC0gZ28gYWhlYWQgYW5kIHBhdGNoIHRoZSBjb21wb25lbnQgZGlyZWN0bHkuXG4gICAgICBjb25zdCBjb21wb25lbnQgPSBkZWNsYXJhdGlvbiBhcyBUeXBlPGFueT4mIHtuZ0NvbXBvbmVudERlZjogQ29tcG9uZW50RGVmPGFueT59O1xuICAgICAgY29uc3QgY29tcG9uZW50RGVmID0gZ2V0Q29tcG9uZW50RGVmKGNvbXBvbmVudCkgITtcbiAgICAgIHBhdGNoQ29tcG9uZW50RGVmV2l0aFNjb3BlKGNvbXBvbmVudERlZiwgdHJhbnNpdGl2ZVNjb3Blcyk7XG4gICAgfSBlbHNlIGlmIChcbiAgICAgICAgIWRlY2xhcmF0aW9uLmhhc093blByb3BlcnR5KE5HX0RJUkVDVElWRV9ERUYpICYmICFkZWNsYXJhdGlvbi5oYXNPd25Qcm9wZXJ0eShOR19QSVBFX0RFRikpIHtcbiAgICAgIC8vIFNldCBgbmdTZWxlY3RvclNjb3BlYCBmb3IgZnV0dXJlIHJlZmVyZW5jZSB3aGVuIHRoZSBjb21wb25lbnQgY29tcGlsYXRpb24gZmluaXNoZXMuXG4gICAgICAoZGVjbGFyYXRpb24gYXMgVHlwZTxhbnk+JiB7bmdTZWxlY3RvclNjb3BlPzogYW55fSkubmdTZWxlY3RvclNjb3BlID0gbW9kdWxlVHlwZTtcbiAgICB9XG4gIH0pO1xufVxuXG4vKipcbiAqIFBhdGNoIHRoZSBkZWZpbml0aW9uIG9mIGEgY29tcG9uZW50IHdpdGggZGlyZWN0aXZlcyBhbmQgcGlwZXMgZnJvbSB0aGUgY29tcGlsYXRpb24gc2NvcGUgb2ZcbiAqIGEgZ2l2ZW4gbW9kdWxlLlxuICovXG5leHBvcnQgZnVuY3Rpb24gcGF0Y2hDb21wb25lbnREZWZXaXRoU2NvcGU8Qz4oXG4gICAgY29tcG9uZW50RGVmOiBDb21wb25lbnREZWY8Qz4sIHRyYW5zaXRpdmVTY29wZXM6IE5nTW9kdWxlVHJhbnNpdGl2ZVNjb3Blcykge1xuICBjb21wb25lbnREZWYuZGlyZWN0aXZlRGVmcyA9ICgpID0+XG4gICAgICBBcnJheS5mcm9tKHRyYW5zaXRpdmVTY29wZXMuY29tcGlsYXRpb24uZGlyZWN0aXZlcylcbiAgICAgICAgICAubWFwKFxuICAgICAgICAgICAgICBkaXIgPT4gZGlyLmhhc093blByb3BlcnR5KE5HX0NPTVBPTkVOVF9ERUYpID8gZ2V0Q29tcG9uZW50RGVmKGRpcikgISA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBnZXREaXJlY3RpdmVEZWYoZGlyKSAhKVxuICAgICAgICAgIC5maWx0ZXIoZGVmID0+ICEhZGVmKTtcbiAgY29tcG9uZW50RGVmLnBpcGVEZWZzID0gKCkgPT5cbiAgICAgIEFycmF5LmZyb20odHJhbnNpdGl2ZVNjb3Blcy5jb21waWxhdGlvbi5waXBlcykubWFwKHBpcGUgPT4gZ2V0UGlwZURlZihwaXBlKSAhKTtcbiAgY29tcG9uZW50RGVmLnNjaGVtYXMgPSB0cmFuc2l0aXZlU2NvcGVzLnNjaGVtYXM7XG5cbiAgLy8gU2luY2Ugd2UgYXZvaWQgQ29tcG9uZW50cy9EaXJlY3RpdmVzL1BpcGVzIHJlY29tcGlsaW5nIGluIGNhc2UgdGhlcmUgYXJlIG5vIG92ZXJyaWRlcywgd2VcbiAgLy8gbWF5IGZhY2UgYSBwcm9ibGVtIHdoZXJlIHByZXZpb3VzbHkgY29tcGlsZWQgZGVmcyBhdmFpbGFibGUgdG8gYSBnaXZlbiBDb21wb25lbnQvRGlyZWN0aXZlXG4gIC8vIGFyZSBjYWNoZWQgaW4gVFZpZXcgYW5kIG1heSBiZWNvbWUgc3RhbGUgKGluIGNhc2UgYW55IG9mIHRoZXNlIGRlZnMgZ2V0cyByZWNvbXBpbGVkKS4gSW5cbiAgLy8gb3JkZXIgdG8gYXZvaWQgdGhpcyBwcm9ibGVtLCB3ZSBmb3JjZSBmcmVzaCBUVmlldyB0byBiZSBjcmVhdGVkLlxuICBjb21wb25lbnREZWYudFZpZXcgPSBudWxsO1xufVxuXG4vKipcbiAqIENvbXB1dGUgdGhlIHBhaXIgb2YgdHJhbnNpdGl2ZSBzY29wZXMgKGNvbXBpbGF0aW9uIHNjb3BlIGFuZCBleHBvcnRlZCBzY29wZSkgZm9yIGEgZ2l2ZW4gbW9kdWxlLlxuICpcbiAqIFRoaXMgb3BlcmF0aW9uIGlzIG1lbW9pemVkIGFuZCB0aGUgcmVzdWx0IGlzIGNhY2hlZCBvbiB0aGUgbW9kdWxlJ3MgZGVmaW5pdGlvbi4gSXQgY2FuIGJlIGNhbGxlZFxuICogb24gbW9kdWxlcyB3aXRoIGNvbXBvbmVudHMgdGhhdCBoYXZlIG5vdCBmdWxseSBjb21waWxlZCB5ZXQsIGJ1dCB0aGUgcmVzdWx0IHNob3VsZCBub3QgYmUgdXNlZFxuICogdW50aWwgdGhleSBoYXZlLlxuICovXG5leHBvcnQgZnVuY3Rpb24gdHJhbnNpdGl2ZVNjb3Blc0ZvcjxUPihcbiAgICBtb2R1bGVUeXBlOiBUeXBlPFQ+LFxuICAgIHByb2Nlc3NOZ01vZHVsZUZuPzogKG5nTW9kdWxlOiBOZ01vZHVsZVR5cGUpID0+IHZvaWQpOiBOZ01vZHVsZVRyYW5zaXRpdmVTY29wZXMge1xuICBpZiAoIWlzTmdNb2R1bGUobW9kdWxlVHlwZSkpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYCR7bW9kdWxlVHlwZS5uYW1lfSBkb2VzIG5vdCBoYXZlIGFuIG5nTW9kdWxlRGVmYCk7XG4gIH1cbiAgY29uc3QgZGVmID0gZ2V0TmdNb2R1bGVEZWYobW9kdWxlVHlwZSkgITtcblxuICBpZiAoZGVmLnRyYW5zaXRpdmVDb21waWxlU2NvcGVzICE9PSBudWxsKSB7XG4gICAgcmV0dXJuIGRlZi50cmFuc2l0aXZlQ29tcGlsZVNjb3BlcztcbiAgfVxuXG4gIGNvbnN0IHNjb3BlczogTmdNb2R1bGVUcmFuc2l0aXZlU2NvcGVzID0ge1xuICAgIHNjaGVtYXM6IGRlZi5zY2hlbWFzIHx8IG51bGwsXG4gICAgY29tcGlsYXRpb246IHtcbiAgICAgIGRpcmVjdGl2ZXM6IG5ldyBTZXQ8YW55PigpLFxuICAgICAgcGlwZXM6IG5ldyBTZXQ8YW55PigpLFxuICAgIH0sXG4gICAgZXhwb3J0ZWQ6IHtcbiAgICAgIGRpcmVjdGl2ZXM6IG5ldyBTZXQ8YW55PigpLFxuICAgICAgcGlwZXM6IG5ldyBTZXQ8YW55PigpLFxuICAgIH0sXG4gIH07XG5cbiAgbWF5YmVVbndyYXBGbihkZWYuZGVjbGFyYXRpb25zKS5mb3JFYWNoKGRlY2xhcmVkID0+IHtcbiAgICBjb25zdCBkZWNsYXJlZFdpdGhEZWZzID0gZGVjbGFyZWQgYXMgVHlwZTxhbnk+JiB7IG5nUGlwZURlZj86IGFueTsgfTtcblxuICAgIGlmIChnZXRQaXBlRGVmKGRlY2xhcmVkV2l0aERlZnMpKSB7XG4gICAgICBzY29wZXMuY29tcGlsYXRpb24ucGlwZXMuYWRkKGRlY2xhcmVkKTtcbiAgICB9IGVsc2Uge1xuICAgICAgLy8gRWl0aGVyIGRlY2xhcmVkIGhhcyBhbiBuZ0NvbXBvbmVudERlZiBvciBuZ0RpcmVjdGl2ZURlZiwgb3IgaXQncyBhIGNvbXBvbmVudCB3aGljaCBoYXNuJ3RcbiAgICAgIC8vIGhhZCBpdHMgdGVtcGxhdGUgY29tcGlsZWQgeWV0LiBJbiBlaXRoZXIgY2FzZSwgaXQgZ2V0cyBhZGRlZCB0byB0aGUgY29tcGlsYXRpb24nc1xuICAgICAgLy8gZGlyZWN0aXZlcy5cbiAgICAgIHNjb3Blcy5jb21waWxhdGlvbi5kaXJlY3RpdmVzLmFkZChkZWNsYXJlZCk7XG4gICAgfVxuICB9KTtcblxuICBtYXliZVVud3JhcEZuKGRlZi5pbXBvcnRzKS5mb3JFYWNoKDxJPihpbXBvcnRlZDogVHlwZTxJPikgPT4ge1xuICAgIGNvbnN0IGltcG9ydGVkVHlwZSA9IGltcG9ydGVkIGFzIFR5cGU8ST4mIHtcbiAgICAgIC8vIElmIGltcG9ydGVkIGlzIGFuIEBOZ01vZHVsZTpcbiAgICAgIG5nTW9kdWxlRGVmPzogTmdNb2R1bGVEZWY8ST47XG4gICAgfTtcblxuICAgIGlmICghaXNOZ01vZHVsZTxJPihpbXBvcnRlZFR5cGUpKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYEltcG9ydGluZyAke2ltcG9ydGVkVHlwZS5uYW1lfSB3aGljaCBkb2VzIG5vdCBoYXZlIGFuIG5nTW9kdWxlRGVmYCk7XG4gICAgfVxuXG4gICAgaWYgKHByb2Nlc3NOZ01vZHVsZUZuKSB7XG4gICAgICBwcm9jZXNzTmdNb2R1bGVGbihpbXBvcnRlZFR5cGUgYXMgTmdNb2R1bGVUeXBlKTtcbiAgICB9XG5cbiAgICAvLyBXaGVuIHRoaXMgbW9kdWxlIGltcG9ydHMgYW5vdGhlciwgdGhlIGltcG9ydGVkIG1vZHVsZSdzIGV4cG9ydGVkIGRpcmVjdGl2ZXMgYW5kIHBpcGVzIGFyZVxuICAgIC8vIGFkZGVkIHRvIHRoZSBjb21waWxhdGlvbiBzY29wZSBvZiB0aGlzIG1vZHVsZS5cbiAgICBjb25zdCBpbXBvcnRlZFNjb3BlID0gdHJhbnNpdGl2ZVNjb3Blc0ZvcihpbXBvcnRlZFR5cGUsIHByb2Nlc3NOZ01vZHVsZUZuKTtcbiAgICBpbXBvcnRlZFNjb3BlLmV4cG9ydGVkLmRpcmVjdGl2ZXMuZm9yRWFjaChlbnRyeSA9PiBzY29wZXMuY29tcGlsYXRpb24uZGlyZWN0aXZlcy5hZGQoZW50cnkpKTtcbiAgICBpbXBvcnRlZFNjb3BlLmV4cG9ydGVkLnBpcGVzLmZvckVhY2goZW50cnkgPT4gc2NvcGVzLmNvbXBpbGF0aW9uLnBpcGVzLmFkZChlbnRyeSkpO1xuICB9KTtcblxuICBtYXliZVVud3JhcEZuKGRlZi5leHBvcnRzKS5mb3JFYWNoKDxFPihleHBvcnRlZDogVHlwZTxFPikgPT4ge1xuICAgIGNvbnN0IGV4cG9ydGVkVHlwZSA9IGV4cG9ydGVkIGFzIFR5cGU8RT4mIHtcbiAgICAgIC8vIENvbXBvbmVudHMsIERpcmVjdGl2ZXMsIE5nTW9kdWxlcywgYW5kIFBpcGVzIGNhbiBhbGwgYmUgZXhwb3J0ZWQuXG4gICAgICBuZ0NvbXBvbmVudERlZj86IGFueTtcbiAgICAgIG5nRGlyZWN0aXZlRGVmPzogYW55O1xuICAgICAgbmdNb2R1bGVEZWY/OiBOZ01vZHVsZURlZjxFPjtcbiAgICAgIG5nUGlwZURlZj86IGFueTtcbiAgICB9O1xuXG4gICAgLy8gRWl0aGVyIHRoZSB0eXBlIGlzIGEgbW9kdWxlLCBhIHBpcGUsIG9yIGEgY29tcG9uZW50L2RpcmVjdGl2ZSAod2hpY2ggbWF5IG5vdCBoYXZlIGFuXG4gICAgLy8gbmdDb21wb25lbnREZWYgYXMgaXQgbWlnaHQgYmUgY29tcGlsZWQgYXN5bmNocm9ub3VzbHkpLlxuICAgIGlmIChpc05nTW9kdWxlKGV4cG9ydGVkVHlwZSkpIHtcbiAgICAgIC8vIFdoZW4gdGhpcyBtb2R1bGUgZXhwb3J0cyBhbm90aGVyLCB0aGUgZXhwb3J0ZWQgbW9kdWxlJ3MgZXhwb3J0ZWQgZGlyZWN0aXZlcyBhbmQgcGlwZXMgYXJlXG4gICAgICAvLyBhZGRlZCB0byBib3RoIHRoZSBjb21waWxhdGlvbiBhbmQgZXhwb3J0ZWQgc2NvcGVzIG9mIHRoaXMgbW9kdWxlLlxuICAgICAgY29uc3QgZXhwb3J0ZWRTY29wZSA9IHRyYW5zaXRpdmVTY29wZXNGb3IoZXhwb3J0ZWRUeXBlLCBwcm9jZXNzTmdNb2R1bGVGbik7XG4gICAgICBleHBvcnRlZFNjb3BlLmV4cG9ydGVkLmRpcmVjdGl2ZXMuZm9yRWFjaChlbnRyeSA9PiB7XG4gICAgICAgIHNjb3Blcy5jb21waWxhdGlvbi5kaXJlY3RpdmVzLmFkZChlbnRyeSk7XG4gICAgICAgIHNjb3Blcy5leHBvcnRlZC5kaXJlY3RpdmVzLmFkZChlbnRyeSk7XG4gICAgICB9KTtcbiAgICAgIGV4cG9ydGVkU2NvcGUuZXhwb3J0ZWQucGlwZXMuZm9yRWFjaChlbnRyeSA9PiB7XG4gICAgICAgIHNjb3Blcy5jb21waWxhdGlvbi5waXBlcy5hZGQoZW50cnkpO1xuICAgICAgICBzY29wZXMuZXhwb3J0ZWQucGlwZXMuYWRkKGVudHJ5KTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSBpZiAoZ2V0UGlwZURlZihleHBvcnRlZFR5cGUpKSB7XG4gICAgICBzY29wZXMuZXhwb3J0ZWQucGlwZXMuYWRkKGV4cG9ydGVkVHlwZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHNjb3Blcy5leHBvcnRlZC5kaXJlY3RpdmVzLmFkZChleHBvcnRlZFR5cGUpO1xuICAgIH1cbiAgfSk7XG5cbiAgZGVmLnRyYW5zaXRpdmVDb21waWxlU2NvcGVzID0gc2NvcGVzO1xuICByZXR1cm4gc2NvcGVzO1xufVxuXG5mdW5jdGlvbiBleHBhbmRNb2R1bGVXaXRoUHJvdmlkZXJzKHZhbHVlOiBUeXBlPGFueT58IE1vZHVsZVdpdGhQcm92aWRlcnM8e30+KTogVHlwZTxhbnk+IHtcbiAgaWYgKGlzTW9kdWxlV2l0aFByb3ZpZGVycyh2YWx1ZSkpIHtcbiAgICByZXR1cm4gdmFsdWUubmdNb2R1bGU7XG4gIH1cbiAgcmV0dXJuIHZhbHVlO1xufVxuXG5mdW5jdGlvbiBpc01vZHVsZVdpdGhQcm92aWRlcnModmFsdWU6IGFueSk6IHZhbHVlIGlzIE1vZHVsZVdpdGhQcm92aWRlcnM8e30+IHtcbiAgcmV0dXJuICh2YWx1ZSBhc3tuZ01vZHVsZT86IGFueX0pLm5nTW9kdWxlICE9PSB1bmRlZmluZWQ7XG59XG5cbmZ1bmN0aW9uIGlzTmdNb2R1bGU8VD4odmFsdWU6IFR5cGU8VD4pOiB2YWx1ZSBpcyBUeXBlPFQ+JntuZ01vZHVsZURlZjogTmdNb2R1bGVEZWY8VD59IHtcbiAgcmV0dXJuICEhZ2V0TmdNb2R1bGVEZWYodmFsdWUpO1xufVxuIl19