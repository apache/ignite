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
import { compileInjectable } from '../../di/jit/injectable';
import { getReflect, reflectDependencies } from '../../di/jit/util';
import { componentNeedsResolution, maybeQueueResolutionOfComponentResources } from '../../metadata/resource_loading';
import { ViewEncapsulation } from '../../metadata/view';
import { getBaseDef, getComponentDef, getDirectiveDef } from '../definition';
import { EMPTY_ARRAY, EMPTY_OBJ } from '../empty';
import { NG_BASE_DEF, NG_COMPONENT_DEF, NG_DIRECTIVE_DEF } from '../fields';
import { stringifyForError } from '../util/misc_utils';
import { angularCoreEnv } from './environment';
import { flushModuleScopingQueueAsMuchAsPossible, patchComponentDefWithScope, transitiveScopesFor } from './module';
/**
 * Compile an Angular component according to its decorator metadata, and patch the resulting
 * ngComponentDef onto the component type.
 *
 * Compilation may be asynchronous (due to the need to resolve URLs for the component template or
 * other resources, for example). In the event that compilation is not immediate, `compileComponent`
 * will enqueue resource resolution into a global queue and will fail to return the `ngComponentDef`
 * until the global queue has been resolved with a call to `resolveComponentResources`.
 * @param {?} type
 * @param {?} metadata
 * @return {?}
 */
export function compileComponent(type, metadata) {
    /** @type {?} */
    /** @nocollapse */ let ngComponentDef = null;
    // Metadata may have resources which need to be resolved.
    maybeQueueResolutionOfComponentResources(type, metadata);
    Object.defineProperty(type, NG_COMPONENT_DEF, {
        get: (/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const compiler = getCompilerFacade();
            if (ngComponentDef === null) {
                if (componentNeedsResolution(metadata)) {
                    /** @type {?} */
                    const error = [`Component '${type.name}' is not resolved:`];
                    if (metadata.templateUrl) {
                        error.push(` - templateUrl: ${metadata.templateUrl}`);
                    }
                    if (metadata.styleUrls && metadata.styleUrls.length) {
                        error.push(` - styleUrls: ${JSON.stringify(metadata.styleUrls)}`);
                    }
                    error.push(`Did you run and wait for 'resolveComponentResources()'?`);
                    throw new Error(error.join('\n'));
                }
                /** @type {?} */
                const templateUrl = metadata.templateUrl || `ng:///${type.name}/template.html`;
                /** @type {?} */
                const meta = Object.assign({}, directiveMetadata(type, metadata), { typeSourceSpan: compiler.createParseSourceSpan('Component', type.name, templateUrl), template: metadata.template || '', preserveWhitespaces: metadata.preserveWhitespaces || false, styles: metadata.styles || EMPTY_ARRAY, animations: metadata.animations, directives: [], changeDetection: metadata.changeDetection, pipes: new Map(), encapsulation: metadata.encapsulation || ViewEncapsulation.Emulated, interpolation: metadata.interpolation, viewProviders: metadata.viewProviders || null });
                if (meta.usesInheritance) {
                    addBaseDefToUndecoratedParents(type);
                }
                ngComponentDef = compiler.compileComponent(angularCoreEnv, templateUrl, meta);
                // When NgModule decorator executed, we enqueued the module definition such that
                // it would only dequeue and add itself as module scope to all of its declarations,
                // but only if  if all of its declarations had resolved. This call runs the check
                // to see if any modules that are in the queue can be dequeued and add scope to
                // their declarations.
                flushModuleScopingQueueAsMuchAsPossible();
                // If component compilation is async, then the @NgModule annotation which declares the
                // component may execute and set an ngSelectorScope property on the component type. This
                // allows the component to patch itself with directiveDefs from the module after it
                // finishes compiling.
                if (hasSelectorScope(type)) {
                    /** @type {?} */
                    const scopes = transitiveScopesFor(type.ngSelectorScope);
                    patchComponentDefWithScope(ngComponentDef, scopes);
                }
            }
            return ngComponentDef;
        }),
        // Make the property configurable in dev mode to allow overriding in tests
        configurable: !!ngDevMode,
    });
    // Add ngInjectableDef so components are reachable through the module injector by default
    // This is mostly to support injecting components in tests. In real application code,
    // components should be retrieved through the node injector, so this isn't a problem.
    compileInjectable(type);
}
/**
 * @template T
 * @param {?} component
 * @return {?}
 */
function hasSelectorScope(component) {
    return ((/** @type {?} */ (component))).ngSelectorScope !== undefined;
}
/**
 * Compile an Angular directive according to its decorator metadata, and patch the resulting
 * ngDirectiveDef onto the component type.
 *
 * In the event that compilation is not immediate, `compileDirective` will return a `Promise` which
 * will resolve when compilation completes and the directive becomes usable.
 * @param {?} type
 * @param {?} directive
 * @return {?}
 */
export function compileDirective(type, directive) {
    /** @type {?} */
    /** @nocollapse */ let ngDirectiveDef = null;
    Object.defineProperty(type, NG_DIRECTIVE_DEF, {
        get: (/**
         * @return {?}
         */
        () => {
            if (ngDirectiveDef === null) {
                /** @type {?} */
                const name = type && type.name;
                /** @type {?} */
                const sourceMapUrl = `ng:///${name}/ngDirectiveDef.js`;
                /** @type {?} */
                const compiler = getCompilerFacade();
                /** @type {?} */
                const facade = directiveMetadata((/** @type {?} */ (type)), directive);
                facade.typeSourceSpan = compiler.createParseSourceSpan('Directive', name, sourceMapUrl);
                if (facade.usesInheritance) {
                    addBaseDefToUndecoratedParents(type);
                }
                ngDirectiveDef = compiler.compileDirective(angularCoreEnv, sourceMapUrl, facade);
            }
            return ngDirectiveDef;
        }),
        // Make the property configurable in dev mode to allow overriding in tests
        configurable: !!ngDevMode,
    });
    // Add ngInjectableDef so directives are reachable through the module injector by default
    // This is mostly to support injecting directives in tests. In real application code,
    // directives should be retrieved through the node injector, so this isn't a problem.
    compileInjectable(type);
}
/**
 * @param {?} type
 * @return {?}
 */
export function extendsDirectlyFromObject(type) {
    return Object.getPrototypeOf(type.prototype) === Object.prototype;
}
/**
 * Extract the `R3DirectiveMetadata` for a particular directive (either a `Directive` or a
 * `Component`).
 * @param {?} type
 * @param {?} metadata
 * @return {?}
 */
export function directiveMetadata(type, metadata) {
    // Reflect inputs and outputs.
    /** @type {?} */
    const propMetadata = getReflect().ownPropMetadata(type);
    return {
        name: type.name,
        type: type,
        typeArgumentCount: 0,
        selector: (/** @type {?} */ (metadata.selector)),
        deps: reflectDependencies(type),
        host: metadata.host || EMPTY_OBJ,
        propMetadata: propMetadata,
        inputs: metadata.inputs || EMPTY_ARRAY,
        outputs: metadata.outputs || EMPTY_ARRAY,
        queries: extractQueriesMetadata(type, propMetadata, isContentQuery),
        lifecycle: { usesOnChanges: type.prototype.hasOwnProperty('ngOnChanges') },
        typeSourceSpan: (/** @type {?} */ (null)),
        usesInheritance: !extendsDirectlyFromObject(type),
        exportAs: extractExportAs(metadata.exportAs),
        providers: metadata.providers || null,
        viewQueries: extractQueriesMetadata(type, propMetadata, isViewQuery),
    };
}
/**
 * Adds an `ngBaseDef` to all parent classes of a type that don't have an Angular decorator.
 * @param {?} type
 * @return {?}
 */
function addBaseDefToUndecoratedParents(type) {
    /** @type {?} */
    const objPrototype = Object.prototype;
    /** @type {?} */
    let parent = Object.getPrototypeOf(type);
    // Go up the prototype until we hit `Object`.
    while (parent && parent !== objPrototype) {
        // Since inheritance works if the class was annotated already, we only need to add
        // the base def if there are no annotations and the base def hasn't been created already.
        if (!getDirectiveDef(parent) && !getComponentDef(parent) && !getBaseDef(parent)) {
            /** @type {?} */
            const facade = extractBaseDefMetadata(parent);
            facade && compileBase(parent, facade);
        }
        parent = Object.getPrototypeOf(parent);
    }
}
/**
 * Compiles the base metadata into a base definition.
 * @param {?} type
 * @param {?} facade
 * @return {?}
 */
function compileBase(type, facade) {
    /** @type {?} */
    /** @nocollapse */ let ngBaseDef = null;
    Object.defineProperty(type, NG_BASE_DEF, {
        get: (/**
         * @return {?}
         */
        () => {
            if (ngBaseDef === null) {
                /** @type {?} */
                const name = type && type.name;
                /** @type {?} */
                const sourceMapUrl = `ng://${name}/ngBaseDef.js`;
                /** @type {?} */
                const compiler = getCompilerFacade();
                ngBaseDef = compiler.compileBase(angularCoreEnv, sourceMapUrl, facade);
            }
            return ngBaseDef;
        }),
        // Make the property configurable in dev mode to allow overriding in tests
        configurable: !!ngDevMode,
    });
}
/**
 * Extracts the metadata necessary to construct an `ngBaseDef` from a class.
 * @param {?} type
 * @return {?}
 */
function extractBaseDefMetadata(type) {
    /** @type {?} */
    const propMetadata = getReflect().ownPropMetadata(type);
    /** @type {?} */
    const viewQueries = extractQueriesMetadata(type, propMetadata, isViewQuery);
    /** @type {?} */
    const queries = extractQueriesMetadata(type, propMetadata, isContentQuery);
    /** @type {?} */
    let inputs;
    /** @type {?} */
    let outputs;
    // We only need to know whether there are any HostListener or HostBinding
    // decorators present, the parsing logic is in the compiler already.
    /** @type {?} */
    let hasHostDecorators = false;
    for (const field in propMetadata) {
        propMetadata[field].forEach((/**
         * @param {?} ann
         * @return {?}
         */
        ann => {
            /** @type {?} */
            const metadataName = ann.ngMetadataName;
            if (metadataName === 'Input') {
                inputs = inputs || {};
                inputs[field] = ann.bindingPropertyName ? [ann.bindingPropertyName, field] : field;
            }
            else if (metadataName === 'Output') {
                outputs = outputs || {};
                outputs[field] = ann.bindingPropertyName || field;
            }
            else if (metadataName === 'HostBinding' || metadataName === 'HostListener') {
                hasHostDecorators = true;
            }
        }));
    }
    // Only generate the base def if there's any info inside it.
    if (inputs || outputs || viewQueries.length || queries.length || hasHostDecorators) {
        return { name: type.name, type, inputs, outputs, viewQueries, queries, propMetadata };
    }
    return null;
}
/**
 * @param {?} selector
 * @return {?}
 */
function convertToR3QueryPredicate(selector) {
    return typeof selector === 'string' ? splitByComma(selector) : resolveForwardRef(selector);
}
/**
 * @param {?} propertyName
 * @param {?} ann
 * @return {?}
 */
export function convertToR3QueryMetadata(propertyName, ann) {
    return {
        propertyName: propertyName,
        predicate: convertToR3QueryPredicate(ann.selector),
        descendants: ann.descendants,
        first: ann.first,
        read: ann.read ? ann.read : null,
        static: !!ann.static
    };
}
/**
 * @param {?} type
 * @param {?} propMetadata
 * @param {?} isQueryAnn
 * @return {?}
 */
function extractQueriesMetadata(type, propMetadata, isQueryAnn) {
    /** @type {?} */
    const queriesMeta = [];
    for (const field in propMetadata) {
        if (propMetadata.hasOwnProperty(field)) {
            /** @type {?} */
            const annotations = propMetadata[field];
            annotations.forEach((/**
             * @param {?} ann
             * @return {?}
             */
            ann => {
                if (isQueryAnn(ann)) {
                    if (!ann.selector) {
                        throw new Error(`Can't construct a query for the property "${field}" of ` +
                            `"${stringifyForError(type)}" since the query selector wasn't defined.`);
                    }
                    if (annotations.some(isInputAnn)) {
                        throw new Error(`Cannot combine @Input decorators with query decorators`);
                    }
                    queriesMeta.push(convertToR3QueryMetadata(field, ann));
                }
            }));
        }
    }
    return queriesMeta;
}
/**
 * @param {?} exportAs
 * @return {?}
 */
function extractExportAs(exportAs) {
    if (exportAs === undefined) {
        return null;
    }
    return exportAs.split(',').map((/**
     * @param {?} part
     * @return {?}
     */
    part => part.trim()));
}
/**
 * @param {?} value
 * @return {?}
 */
function isContentQuery(value) {
    /** @type {?} */
    const name = value.ngMetadataName;
    return name === 'ContentChild' || name === 'ContentChildren';
}
/**
 * @param {?} value
 * @return {?}
 */
function isViewQuery(value) {
    /** @type {?} */
    const name = value.ngMetadataName;
    return name === 'ViewChild' || name === 'ViewChildren';
}
/**
 * @param {?} value
 * @return {?}
 */
function isInputAnn(value) {
    return value.ngMetadataName === 'Input';
}
/**
 * @param {?} value
 * @return {?}
 */
function splitByComma(value) {
    return value.split(',').map((/**
     * @param {?} piece
     * @return {?}
     */
    piece => piece.trim()));
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9qaXQvZGlyZWN0aXZlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUE0QixpQkFBaUIsRUFBQyxNQUFNLGdDQUFnQyxDQUFDO0FBRTVGLE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQ3ZELE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLHlCQUF5QixDQUFDO0FBQzFELE9BQU8sRUFBQyxVQUFVLEVBQUUsbUJBQW1CLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUlsRSxPQUFPLEVBQUMsd0JBQXdCLEVBQUUsd0NBQXdDLEVBQUMsTUFBTSxpQ0FBaUMsQ0FBQztBQUNuSCxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUN0RCxPQUFPLEVBQUMsVUFBVSxFQUFFLGVBQWUsRUFBRSxlQUFlLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFDM0UsT0FBTyxFQUFDLFdBQVcsRUFBRSxTQUFTLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFDaEQsT0FBTyxFQUFDLFdBQVcsRUFBRSxnQkFBZ0IsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLFdBQVcsQ0FBQztBQUUxRSxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUVyRCxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQzdDLE9BQU8sRUFBQyx1Q0FBdUMsRUFBRSwwQkFBMEIsRUFBRSxtQkFBbUIsRUFBQyxNQUFNLFVBQVUsQ0FBQzs7Ozs7Ozs7Ozs7OztBQWFsSCxNQUFNLFVBQVUsZ0JBQWdCLENBQUMsSUFBZSxFQUFFLFFBQW1COztRQUMvRCxjQUFjLEdBQVEsSUFBSTtJQUM5Qix5REFBeUQ7SUFDekQsd0NBQXdDLENBQUMsSUFB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MUMsQ0FBQzs7Ozs7QUFFRCxTQUFTLFlBQVksQ0FBQyxLQUFhO0lBQ2pDLE9BQU8sS0FBSyxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsQ0FBQyxHQUFHOzs7O0lBQUMsS0FBSyxDQUFDLEVBQUUsQ0FBQyxLQUFLLENBQUMsSUFBSSxFQUFFLEVBQUMsQ0FBQztBQUNyRCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1IzRGlyZWN0aXZlTWV0YWRhdGFGYWNhZGUsIGdldENvbXBpbGVyRmFjYWRlfSBmcm9tICcuLi8uLi9jb21waWxlci9jb21waWxlcl9mYWNhZGUnO1xuaW1wb3J0IHtSM0Jhc2VNZXRhZGF0YUZhY2FkZSwgUjNDb21wb25lbnRNZXRhZGF0YUZhY2FkZSwgUjNRdWVyeU1ldGFkYXRhRmFjYWRlfSBmcm9tICcuLi8uLi9jb21waWxlci9jb21waWxlcl9mYWNhZGVfaW50ZXJmYWNlJztcbmltcG9ydCB7cmVzb2x2ZUZvcndhcmRSZWZ9IGZyb20gJy4uLy4uL2RpL2ZvcndhcmRfcmVmJztcbmltcG9ydCB7Y29tcGlsZUluamVjdGFibGV9IGZyb20gJy4uLy4uL2RpL2ppdC9pbmplY3RhYmxlJztcbmltcG9ydCB7Z2V0UmVmbGVjdCwgcmVmbGVjdERlcGVuZGVuY2llc30gZnJvbSAnLi4vLi4vZGkvaml0L3V0aWwnO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi8uLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge1F1ZXJ5fSBmcm9tICcuLi8uLi9tZXRhZGF0YS9kaSc7XG5pbXBvcnQge0NvbXBvbmVudCwgRGlyZWN0aXZlLCBJbnB1dH0gZnJvbSAnLi4vLi4vbWV0YWRhdGEvZGlyZWN0aXZlcyc7XG5pbXBvcnQge2NvbXBvbmVudE5lZWRzUmVzb2x1dGlvbiwgbWF5YmVRdWV1ZVJlc29sdXRpb25PZkNvbXBvbmVudFJlc291cmNlc30gZnJvbSAnLi4vLi4vbWV0YWRhdGEvcmVzb3VyY2VfbG9hZGluZyc7XG5pbXBvcnQge1ZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuLi8uLi9tZXRhZGF0YS92aWV3JztcbmltcG9ydCB7Z2V0QmFzZURlZiwgZ2V0Q29tcG9uZW50RGVmLCBnZXREaXJlY3RpdmVEZWZ9IGZyb20gJy4uL2RlZmluaXRpb24nO1xuaW1wb3J0IHtFTVBUWV9BUlJBWSwgRU1QVFlfT0JKfSBmcm9tICcuLi9lbXB0eSc7XG5pbXBvcnQge05HX0JBU0VfREVGLCBOR19DT01QT05FTlRfREVGLCBOR19ESVJFQ1RJVkVfREVGfSBmcm9tICcuLi9maWVsZHMnO1xuaW1wb3J0IHtDb21wb25lbnRUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2VzL2RlZmluaXRpb24nO1xuaW1wb3J0IHtzdHJpbmdpZnlGb3JFcnJvcn0gZnJvbSAnLi4vdXRpbC9taXNjX3V0aWxzJztcblxuaW1wb3J0IHthbmd1bGFyQ29yZUVudn0gZnJvbSAnLi9lbnZpcm9ubWVudCc7XG5pbXBvcnQge2ZsdXNoTW9kdWxlU2NvcGluZ1F1ZXVlQXNNdWNoQXNQb3NzaWJsZSwgcGF0Y2hDb21wb25lbnREZWZXaXRoU2NvcGUsIHRyYW5zaXRpdmVTY29wZXNGb3J9IGZyb20gJy4vbW9kdWxlJztcblxuXG5cbi8qKlxuICogQ29tcGlsZSBhbiBBbmd1bGFyIGNvbXBvbmVudCBhY2NvcmRpbmcgdG8gaXRzIGRlY29yYXRvciBtZXRhZGF0YSwgYW5kIHBhdGNoIHRoZSByZXN1bHRpbmdcbiAqIG5nQ29tcG9uZW50RGVmIG9udG8gdGhlIGNvbXBvbmVudCB0eXBlLlxuICpcbiAqIENvbXBpbGF0aW9uIG1heSBiZSBhc3luY2hyb25vdXMgKGR1ZSB0byB0aGUgbmVlZCB0byByZXNvbHZlIFVSTHMgZm9yIHRoZSBjb21wb25lbnQgdGVtcGxhdGUgb3JcbiAqIG90aGVyIHJlc291cmNlcywgZm9yIGV4YW1wbGUpLiBJbiB0aGUgZXZlbnQgdGhhdCBjb21waWxhdGlvbiBpcyBub3QgaW1tZWRpYXRlLCBgY29tcGlsZUNvbXBvbmVudGBcbiAqIHdpbGwgZW5xdWV1ZSByZXNvdXJjZSByZXNvbHV0aW9uIGludG8gYSBnbG9iYWwgcXVldWUgYW5kIHdpbGwgZmFpbCB0byByZXR1cm4gdGhlIGBuZ0NvbXBvbmVudERlZmBcbiAqIHVudGlsIHRoZSBnbG9iYWwgcXVldWUgaGFzIGJlZW4gcmVzb2x2ZWQgd2l0aCBhIGNhbGwgdG8gYHJlc29sdmVDb21wb25lbnRSZXNvdXJjZXNgLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY29tcGlsZUNvbXBvbmVudCh0eXBlOiBUeXBlPGFueT4sIG1ldGFkYXRhOiBDb21wb25lbnQpOiB2b2lkIHtcbiAgbGV0IG5nQ29tcG9uZW50RGVmOiBhbnkgPSBudWxsO1xuICAvLyBNZXRhZGF0YSBtYXkgaGF2ZSByZXNvdXJjZXMgd2hpY2ggbmVlZCB0byBiZSByZXNvbHZlZC5cbiAgbWF5YmVRdWV1ZVJlc29sdXRpb25PZkNvbXBvbmVudFJlc291cmNlcyh0eXBlLCBtZXRhZGF0YSk7XG4gIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0eXBlLCBOR19DT01QT05FTlRfREVGLCB7XG4gICAgZ2V0OiAoKSA9PiB7XG4gICAgICBjb25zdCBjb21waWxlciA9IGdldENvbXBpbGVyRmFjYWRlKCk7XG4gICAgICBpZiAobmdDb21wb25lbnREZWYgPT09IG51bGwpIHtcbiAgICAgICAgaWYgKGNvbXBvbmVudE5lZWRzUmVzb2x1dGlvbihtZXRhZGF0YSkpIHtcbiAgICAgICAgICBjb25zdCBlcnJvciA9IFtgQ29tcG9uZW50ICcke3R5cGUubmFtZX0nIGlzIG5vdCByZXNvbHZlZDpgXTtcbiAgICAgICAgICBpZiAobWV0YWRhdGEudGVtcGxhdGVVcmwpIHtcbiAgICAgICAgICAgIGVycm9yLnB1c2goYCAtIHRlbXBsYXRlVXJsOiAke21ldGFkYXRhLnRlbXBsYXRlVXJsfWApO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAobWV0YWRhdGEuc3R5bGVVcmxzICYmIG1ldGFkYXRhLnN0eWxlVXJscy5sZW5ndGgpIHtcbiAgICAgICAgICAgIGVycm9yLnB1c2goYCAtIHN0eWxlVXJsczogJHtKU09OLnN0cmluZ2lmeShtZXRhZGF0YS5zdHlsZVVybHMpfWApO1xuICAgICAgICAgIH1cbiAgICAgICAgICBlcnJvci5wdXNoKGBEaWQgeW91IHJ1biBhbmQgd2FpdCBmb3IgJ3Jlc29sdmVDb21wb25lbnRSZXNvdXJjZXMoKSc/YCk7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGVycm9yLmpvaW4oJ1xcbicpKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHRlbXBsYXRlVXJsID0gbWV0YWRhdGEudGVtcGxhdGVVcmwgfHwgYG5nOi8vLyR7dHlwZS5uYW1lfS90ZW1wbGF0ZS5odG1sYDtcbiAgICAgICAgY29uc3QgbWV0YTogUjNDb21wb25lbnRNZXRhZGF0YUZhY2FkZSA9IHtcbiAgICAgICAgICAuLi5kaXJlY3RpdmVNZXRhZGF0YSh0eXBlLCBtZXRhZGF0YSksXG4gICAgICAgICAgdHlwZVNvdXJjZVNwYW46IGNvbXBpbGVyLmNyZWF0ZVBhcnNlU291cmNlU3BhbignQ29tcG9uZW50JywgdHlwZS5uYW1lLCB0ZW1wbGF0ZVVybCksXG4gICAgICAgICAgdGVtcGxhdGU6IG1ldGFkYXRhLnRlbXBsYXRlIHx8ICcnLFxuICAgICAgICAgIHByZXNlcnZlV2hpdGVzcGFjZXM6IG1ldGFkYXRhLnByZXNlcnZlV2hpdGVzcGFjZXMgfHwgZmFsc2UsXG4gICAgICAgICAgc3R5bGVzOiBtZXRhZGF0YS5zdHlsZXMgfHwgRU1QVFlfQVJSQVksXG4gICAgICAgICAgYW5pbWF0aW9uczogbWV0YWRhdGEuYW5pbWF0aW9ucyxcbiAgICAgICAgICBkaXJlY3RpdmVzOiBbXSxcbiAgICAgICAgICBjaGFuZ2VEZXRlY3Rpb246IG1ldGFkYXRhLmNoYW5nZURldGVjdGlvbixcbiAgICAgICAgICBwaXBlczogbmV3IE1hcCgpLFxuICAgICAgICAgIGVuY2Fwc3VsYXRpb246IG1ldGFkYXRhLmVuY2Fwc3VsYXRpb24gfHwgVmlld0VuY2Fwc3VsYXRpb24uRW11bGF0ZWQsXG4gICAgICAgICAgaW50ZXJwb2xhdGlvbjogbWV0YWRhdGEuaW50ZXJwb2xhdGlvbixcbiAgICAgICAgICB2aWV3UHJvdmlkZXJzOiBtZXRhZGF0YS52aWV3UHJvdmlkZXJzIHx8IG51bGwsXG4gICAgICAgIH07XG4gICAgICAgIGlmIChtZXRhLnVzZXNJbmhlcml0YW5jZSkge1xuICAgICAgICAgIGFkZEJhc2VEZWZUb1VuZGVjb3JhdGVkUGFyZW50cyh0eXBlKTtcbiAgICAgICAgfVxuICAgICAgICBuZ0NvbXBvbmVudERlZiA9IGNvbXBpbGVyLmNvbXBpbGVDb21wb25lbnQoYW5ndWxhckNvcmVFbnYsIHRlbXBsYXRlVXJsLCBtZXRhKTtcblxuICAgICAgICAvLyBXaGVuIE5nTW9kdWxlIGRlY29yYXRvciBleGVjdXRlZCwgd2UgZW5xdWV1ZWQgdGhlIG1vZHVsZSBkZWZpbml0aW9uIHN1Y2ggdGhhdFxuICAgICAgICAvLyBpdCB3b3VsZCBvbmx5IGRlcXVldWUgYW5kIGFkZCBpdHNlbGYgYXMgbW9kdWxlIHNjb3BlIHRvIGFsbCBvZiBpdHMgZGVjbGFyYXRpb25zLFxuICAgICAgICAvLyBidXQgb25seSBpZiAgaWYgYWxsIG9mIGl0cyBkZWNsYXJhdGlvbnMgaGFkIHJlc29sdmVkLiBUaGlzIGNhbGwgcnVucyB0aGUgY2hlY2tcbiAgICAgICAgLy8gdG8gc2VlIGlmIGFueSBtb2R1bGVzIHRoYXQgYXJlIGluIHRoZSBxdWV1ZSBjYW4gYmUgZGVxdWV1ZWQgYW5kIGFkZCBzY29wZSB0b1xuICAgICAgICAvLyB0aGVpciBkZWNsYXJhdGlvbnMuXG4gICAgICAgIGZsdXNoTW9kdWxlU2NvcGluZ1F1ZXVlQXNNdWNoQXNQb3NzaWJsZSgpO1xuXG4gICAgICAgIC8vIElmIGNvbXBvbmVudCBjb21waWxhdGlvbiBpcyBhc3luYywgdGhlbiB0aGUgQE5nTW9kdWxlIGFubm90YXRpb24gd2hpY2ggZGVjbGFyZXMgdGhlXG4gICAgICAgIC8vIGNvbXBvbmVudCBtYXkgZXhlY3V0ZSBhbmQgc2V0IGFuIG5nU2VsZWN0b3JTY29wZSBwcm9wZXJ0eSBvbiB0aGUgY29tcG9uZW50IHR5cGUuIFRoaXNcbiAgICAgICAgLy8gYWxsb3dzIHRoZSBjb21wb25lbnQgdG8gcGF0Y2ggaXRzZWxmIHdpdGggZGlyZWN0aXZlRGVmcyBmcm9tIHRoZSBtb2R1bGUgYWZ0ZXIgaXRcbiAgICAgICAgLy8gZmluaXNoZXMgY29tcGlsaW5nLlxuICAgICAgICBpZiAoaGFzU2VsZWN0b3JTY29wZSh0eXBlKSkge1xuICAgICAgICAgIGNvbnN0IHNjb3BlcyA9IHRyYW5zaXRpdmVTY29wZXNGb3IodHlwZS5uZ1NlbGVjdG9yU2NvcGUpO1xuICAgICAgICAgIHBhdGNoQ29tcG9uZW50RGVmV2l0aFNjb3BlKG5nQ29tcG9uZW50RGVmLCBzY29wZXMpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gbmdDb21wb25lbnREZWY7XG4gICAgfSxcbiAgICAvLyBNYWtlIHRoZSBwcm9wZXJ0eSBjb25maWd1cmFibGUgaW4gZGV2IG1vZGUgdG8gYWxsb3cgb3ZlcnJpZGluZyBpbiB0ZXN0c1xuICAgIGNvbmZpZ3VyYWJsZTogISFuZ0Rldk1vZGUsXG4gIH0pO1xuXG5cbiAgLy8gQWRkIG5nSW5qZWN0YWJsZURlZiBzbyBjb21wb25lbnRzIGFyZSByZWFjaGFibGUgdGhyb3VnaCB0aGUgbW9kdWxlIGluamVjdG9yIGJ5IGRlZmF1bHRcbiAgLy8gVGhpcyBpcyBtb3N0bHkgdG8gc3VwcG9ydCBpbmplY3RpbmcgY29tcG9uZW50cyBpbiB0ZXN0cy4gSW4gcmVhbCBhcHBsaWNhdGlvbiBjb2RlLFxuICAvLyBjb21wb25lbnRzIHNob3VsZCBiZSByZXRyaWV2ZWQgdGhyb3VnaCB0aGUgbm9kZSBpbmplY3Rvciwgc28gdGhpcyBpc24ndCBhIHByb2JsZW0uXG4gIGNvbXBpbGVJbmplY3RhYmxlKHR5cGUpO1xufVxuXG5mdW5jdGlvbiBoYXNTZWxlY3RvclNjb3BlPFQ+KGNvbXBvbmVudDogVHlwZTxUPik6IGNvbXBvbmVudCBpcyBUeXBlPFQ+JlxuICAgIHtuZ1NlbGVjdG9yU2NvcGU6IFR5cGU8YW55Pn0ge1xuICByZXR1cm4gKGNvbXBvbmVudCBhc3tuZ1NlbGVjdG9yU2NvcGU/OiBhbnl9KS5uZ1NlbGVjdG9yU2NvcGUgIT09IHVuZGVmaW5lZDtcbn1cblxuLyoqXG4gKiBDb21waWxlIGFuIEFuZ3VsYXIgZGlyZWN0aXZlIGFjY29yZGluZyB0byBpdHMgZGVjb3JhdG9yIG1ldGFkYXRhLCBhbmQgcGF0Y2ggdGhlIHJlc3VsdGluZ1xuICogbmdEaXJlY3RpdmVEZWYgb250byB0aGUgY29tcG9uZW50IHR5cGUuXG4gKlxuICogSW4gdGhlIGV2ZW50IHRoYXQgY29tcGlsYXRpb24gaXMgbm90IGltbWVkaWF0ZSwgYGNvbXBpbGVEaXJlY3RpdmVgIHdpbGwgcmV0dXJuIGEgYFByb21pc2VgIHdoaWNoXG4gKiB3aWxsIHJlc29sdmUgd2hlbiBjb21waWxhdGlvbiBjb21wbGV0ZXMgYW5kIHRoZSBkaXJlY3RpdmUgYmVjb21lcyB1c2FibGUuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjb21waWxlRGlyZWN0aXZlKHR5cGU6IFR5cGU8YW55PiwgZGlyZWN0aXZlOiBEaXJlY3RpdmUpOiB2b2lkIHtcbiAgbGV0IG5nRGlyZWN0aXZlRGVmOiBhbnkgPSBudWxsO1xuICBPYmplY3QuZGVmaW5lUHJvcGVydHkodHlwZSwgTkdfRElSRUNUSVZFX0RFRiwge1xuICAgIGdldDogKCkgPT4ge1xuICAgICAgaWYgKG5nRGlyZWN0aXZlRGVmID09PSBudWxsKSB7XG4gICAgICAgIGNvbnN0IG5hbWUgPSB0eXBlICYmIHR5cGUubmFtZTtcbiAgICAgICAgY29uc3Qgc291cmNlTWFwVXJsID0gYG5nOi8vLyR7bmFtZX0vbmdEaXJlY3RpdmVEZWYuanNgO1xuICAgICAgICBjb25zdCBjb21waWxlciA9IGdldENvbXBpbGVyRmFjYWRlKCk7XG4gICAgICAgIGNvbnN0IGZhY2FkZSA9IGRpcmVjdGl2ZU1ldGFkYXRhKHR5cGUgYXMgQ29tcG9uZW50VHlwZTxhbnk+LCBkaXJlY3RpdmUpO1xuICAgICAgICBmYWNhZGUudHlwZVNvdXJjZVNwYW4gPSBjb21waWxlci5jcmVhdGVQYXJzZVNvdXJjZVNwYW4oJ0RpcmVjdGl2ZScsIG5hbWUsIHNvdXJjZU1hcFVybCk7XG4gICAgICAgIGlmIChmYWNhZGUudXNlc0luaGVyaXRhbmNlKSB7XG4gICAgICAgICAgYWRkQmFzZURlZlRvVW5kZWNvcmF0ZWRQYXJlbnRzKHR5cGUpO1xuICAgICAgICB9XG4gICAgICAgIG5nRGlyZWN0aXZlRGVmID0gY29tcGlsZXIuY29tcGlsZURpcmVjdGl2ZShhbmd1bGFyQ29yZUVudiwgc291cmNlTWFwVXJsLCBmYWNhZGUpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIG5nRGlyZWN0aXZlRGVmO1xuICAgIH0sXG4gICAgLy8gTWFrZSB0aGUgcHJvcGVydHkgY29uZmlndXJhYmxlIGluIGRldiBtb2RlIHRvIGFsbG93IG92ZXJyaWRpbmcgaW4gdGVzdHNcbiAgICBjb25maWd1cmFibGU6ICEhbmdEZXZNb2RlLFxuICB9KTtcblxuICAvLyBBZGQgbmdJbmplY3RhYmxlRGVmIHNvIGRpcmVjdGl2ZXMgYXJlIHJlYWNoYWJsZSB0aHJvdWdoIHRoZSBtb2R1bGUgaW5qZWN0b3IgYnkgZGVmYXVsdFxuICAvLyBUaGlzIGlzIG1vc3RseSB0byBzdXBwb3J0IGluamVjdGluZyBkaXJlY3RpdmVzIGluIHRlc3RzLiBJbiByZWFsIGFwcGxpY2F0aW9uIGNvZGUsXG4gIC8vIGRpcmVjdGl2ZXMgc2hvdWxkIGJlIHJldHJpZXZlZCB0aHJvdWdoIHRoZSBub2RlIGluamVjdG9yLCBzbyB0aGlzIGlzbid0IGEgcHJvYmxlbS5cbiAgY29tcGlsZUluamVjdGFibGUodHlwZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRlbmRzRGlyZWN0bHlGcm9tT2JqZWN0KHR5cGU6IFR5cGU8YW55Pik6IGJvb2xlYW4ge1xuICByZXR1cm4gT2JqZWN0LmdldFByb3RvdHlwZU9mKHR5cGUucHJvdG90eXBlKSA9PT0gT2JqZWN0LnByb3RvdHlwZTtcbn1cblxuLyoqXG4gKiBFeHRyYWN0IHRoZSBgUjNEaXJlY3RpdmVNZXRhZGF0YWAgZm9yIGEgcGFydGljdWxhciBkaXJlY3RpdmUgKGVpdGhlciBhIGBEaXJlY3RpdmVgIG9yIGFcbiAqIGBDb21wb25lbnRgKS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGRpcmVjdGl2ZU1ldGFkYXRhKHR5cGU6IFR5cGU8YW55PiwgbWV0YWRhdGE6IERpcmVjdGl2ZSk6IFIzRGlyZWN0aXZlTWV0YWRhdGFGYWNhZGUge1xuICAvLyBSZWZsZWN0IGlucHV0cyBhbmQgb3V0cHV0cy5cbiAgY29uc3QgcHJvcE1ldGFkYXRhID0gZ2V0UmVmbGVjdCgpLm93blByb3BNZXRhZGF0YSh0eXBlKTtcblxuICByZXR1cm4ge1xuICAgIG5hbWU6IHR5cGUubmFtZSxcbiAgICB0eXBlOiB0eXBlLFxuICAgIHR5cGVBcmd1bWVudENvdW50OiAwLFxuICAgIHNlbGVjdG9yOiBtZXRhZGF0YS5zZWxlY3RvciAhLFxuICAgIGRlcHM6IHJlZmxlY3REZXBlbmRlbmNpZXModHlwZSksXG4gICAgaG9zdDogbWV0YWRhdGEuaG9zdCB8fCBFTVBUWV9PQkosXG4gICAgcHJvcE1ldGFkYXRhOiBwcm9wTWV0YWRhdGEsXG4gICAgaW5wdXRzOiBtZXRhZGF0YS5pbnB1dHMgfHwgRU1QVFlfQVJSQVksXG4gICAgb3V0cHV0czogbWV0YWRhdGEub3V0cHV0cyB8fCBFTVBUWV9BUlJBWSxcbiAgICBxdWVyaWVzOiBleHRyYWN0UXVlcmllc01ldGFkYXRhKHR5cGUsIHByb3BNZXRhZGF0YSwgaXNDb250ZW50UXVlcnkpLFxuICAgIGxpZmVjeWNsZToge3VzZXNPbkNoYW5nZXM6IHR5cGUucHJvdG90eXBlLmhhc093blByb3BlcnR5KCduZ09uQ2hhbmdlcycpfSxcbiAgICB0eXBlU291cmNlU3BhbjogbnVsbCAhLFxuICAgIHVzZXNJbmhlcml0YW5jZTogIWV4dGVuZHNEaXJlY3RseUZyb21PYmplY3QodHlwZSksXG4gICAgZXhwb3J0QXM6IGV4dHJhY3RFeHBvcnRBcyhtZXRhZGF0YS5leHBvcnRBcyksXG4gICAgcHJvdmlkZXJzOiBtZXRhZGF0YS5wcm92aWRlcnMgfHwgbnVsbCxcbiAgICB2aWV3UXVlcmllczogZXh0cmFjdFF1ZXJpZXNNZXRhZGF0YSh0eXBlLCBwcm9wTWV0YWRhdGEsIGlzVmlld1F1ZXJ5KSxcbiAgfTtcbn1cblxuLyoqXG4gKiBBZGRzIGFuIGBuZ0Jhc2VEZWZgIHRvIGFsbCBwYXJlbnQgY2xhc3NlcyBvZiBhIHR5cGUgdGhhdCBkb24ndCBoYXZlIGFuIEFuZ3VsYXIgZGVjb3JhdG9yLlxuICovXG5mdW5jdGlvbiBhZGRCYXNlRGVmVG9VbmRlY29yYXRlZFBhcmVudHModHlwZTogVHlwZTxhbnk+KSB7XG4gIGNvbnN0IG9ialByb3RvdHlwZSA9IE9iamVjdC5wcm90b3R5cGU7XG4gIGxldCBwYXJlbnQgPSBPYmplY3QuZ2V0UHJvdG90eXBlT2YodHlwZSk7XG5cbiAgLy8gR28gdXAgdGhlIHByb3RvdHlwZSB1bnRpbCB3ZSBoaXQgYE9iamVjdGAuXG4gIHdoaWxlIChwYXJlbnQgJiYgcGFyZW50ICE9PSBvYmpQcm90b3R5cGUpIHtcbiAgICAvLyBTaW5jZSBpbmhlcml0YW5jZSB3b3JrcyBpZiB0aGUgY2xhc3Mgd2FzIGFubm90YXRlZCBhbHJlYWR5LCB3ZSBvbmx5IG5lZWQgdG8gYWRkXG4gICAgLy8gdGhlIGJhc2UgZGVmIGlmIHRoZXJlIGFyZSBubyBhbm5vdGF0aW9ucyBhbmQgdGhlIGJhc2UgZGVmIGhhc24ndCBiZWVuIGNyZWF0ZWQgYWxyZWFkeS5cbiAgICBpZiAoIWdldERpcmVjdGl2ZURlZihwYXJlbnQpICYmICFnZXRDb21wb25lbnREZWYocGFyZW50KSAmJiAhZ2V0QmFzZURlZihwYXJlbnQpKSB7XG4gICAgICBjb25zdCBmYWNhZGUgPSBleHRyYWN0QmFzZURlZk1ldGFkYXRhKHBhcmVudCk7XG4gICAgICBmYWNhZGUgJiYgY29tcGlsZUJhc2UocGFyZW50LCBmYWNhZGUpO1xuICAgIH1cbiAgICBwYXJlbnQgPSBPYmplY3QuZ2V0UHJvdG90eXBlT2YocGFyZW50KTtcbiAgfVxufVxuXG4vKiogQ29tcGlsZXMgdGhlIGJhc2UgbWV0YWRhdGEgaW50byBhIGJhc2UgZGVmaW5pdGlvbi4gKi9cbmZ1bmN0aW9uIGNvbXBpbGVCYXNlKHR5cGU6IFR5cGU8YW55PiwgZmFjYWRlOiBSM0Jhc2VNZXRhZGF0YUZhY2FkZSk6IHZvaWQge1xuICBsZXQgbmdCYXNlRGVmOiBhbnkgPSBudWxsO1xuICBPYmplY3QuZGVmaW5lUHJvcGVydHkodHlwZSwgTkdfQkFTRV9ERUYsIHtcbiAgICBnZXQ6ICgpID0+IHtcbiAgICAgIGlmIChuZ0Jhc2VEZWYgPT09IG51bGwpIHtcbiAgICAgICAgY29uc3QgbmFtZSA9IHR5cGUgJiYgdHlwZS5uYW1lO1xuICAgICAgICBjb25zdCBzb3VyY2VNYXBVcmwgPSBgbmc6Ly8ke25hbWV9L25nQmFzZURlZi5qc2A7XG4gICAgICAgIGNvbnN0IGNvbXBpbGVyID0gZ2V0Q29tcGlsZXJGYWNhZGUoKTtcbiAgICAgICAgbmdCYXNlRGVmID0gY29tcGlsZXIuY29tcGlsZUJhc2UoYW5ndWxhckNvcmVFbnYsIHNvdXJjZU1hcFVybCwgZmFjYWRlKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBuZ0Jhc2VEZWY7XG4gICAgfSxcbiAgICAvLyBNYWtlIHRoZSBwcm9wZXJ0eSBjb25maWd1cmFibGUgaW4gZGV2IG1vZGUgdG8gYWxsb3cgb3ZlcnJpZGluZyBpbiB0ZXN0c1xuICAgIGNvbmZpZ3VyYWJsZTogISFuZ0Rldk1vZGUsXG4gIH0pO1xufVxuXG4vKiogRXh0cmFjdHMgdGhlIG1ldGFkYXRhIG5lY2Vzc2FyeSB0byBjb25zdHJ1Y3QgYW4gYG5nQmFzZURlZmAgZnJvbSBhIGNsYXNzLiAqL1xuZnVuY3Rpb24gZXh0cmFjdEJhc2VEZWZNZXRhZGF0YSh0eXBlOiBUeXBlPGFueT4pOiBSM0Jhc2VNZXRhZGF0YUZhY2FkZXxudWxsIHtcbiAgY29uc3QgcHJvcE1ldGFkYXRhID0gZ2V0UmVmbGVjdCgpLm93blByb3BNZXRhZGF0YSh0eXBlKTtcbiAgY29uc3Qgdmlld1F1ZXJpZXMgPSBleHRyYWN0UXVlcmllc01ldGFkYXRhKHR5cGUsIHByb3BNZXRhZGF0YSwgaXNWaWV3UXVlcnkpO1xuICBjb25zdCBxdWVyaWVzID0gZXh0cmFjdFF1ZXJpZXNNZXRhZGF0YSh0eXBlLCBwcm9wTWV0YWRhdGEsIGlzQ29udGVudFF1ZXJ5KTtcbiAgbGV0IGlucHV0czoge1trZXk6IHN0cmluZ106IHN0cmluZyB8IFtzdHJpbmcsIHN0cmluZ119fHVuZGVmaW5lZDtcbiAgbGV0IG91dHB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9fHVuZGVmaW5lZDtcbiAgLy8gV2Ugb25seSBuZWVkIHRvIGtub3cgd2hldGhlciB0aGVyZSBhcmUgYW55IEhvc3RMaXN0ZW5lciBvciBIb3N0QmluZGluZ1xuICAvLyBkZWNvcmF0b3JzIHByZXNlbnQsIHRoZSBwYXJzaW5nIGxvZ2ljIGlzIGluIHRoZSBjb21waWxlciBhbHJlYWR5LlxuICBsZXQgaGFzSG9zdERlY29yYXRvcnMgPSBmYWxzZTtcblxuICBmb3IgKGNvbnN0IGZpZWxkIGluIHByb3BNZXRhZGF0YSkge1xuICAgIHByb3BNZXRhZGF0YVtmaWVsZF0uZm9yRWFjaChhbm4gPT4ge1xuICAgICAgY29uc3QgbWV0YWRhdGFOYW1lID0gYW5uLm5nTWV0YWRhdGFOYW1lO1xuICAgICAgaWYgKG1ldGFkYXRhTmFtZSA9PT0gJ0lucHV0Jykge1xuICAgICAgICBpbnB1dHMgPSBpbnB1dHMgfHwge307XG4gICAgICAgIGlucHV0c1tmaWVsZF0gPSBhbm4uYmluZGluZ1Byb3BlcnR5TmFtZSA/IFthbm4uYmluZGluZ1Byb3BlcnR5TmFtZSwgZmllbGRdIDogZmllbGQ7XG4gICAgICB9IGVsc2UgaWYgKG1ldGFkYXRhTmFtZSA9PT0gJ091dHB1dCcpIHtcbiAgICAgICAgb3V0cHV0cyA9IG91dHB1dHMgfHwge307XG4gICAgICAgIG91dHB1dHNbZmllbGRdID0gYW5uLmJpbmRpbmdQcm9wZXJ0eU5hbWUgfHwgZmllbGQ7XG4gICAgICB9IGVsc2UgaWYgKG1ldGFkYXRhTmFtZSA9PT0gJ0hvc3RCaW5kaW5nJyB8fCBtZXRhZGF0YU5hbWUgPT09ICdIb3N0TGlzdGVuZXInKSB7XG4gICAgICAgIGhhc0hvc3REZWNvcmF0b3JzID0gdHJ1ZTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIC8vIE9ubHkgZ2VuZXJhdGUgdGhlIGJhc2UgZGVmIGlmIHRoZXJlJ3MgYW55IGluZm8gaW5zaWRlIGl0LlxuICBpZiAoaW5wdXRzIHx8IG91dHB1dHMgfHwgdmlld1F1ZXJpZXMubGVuZ3RoIHx8IHF1ZXJpZXMubGVuZ3RoIHx8IGhhc0hvc3REZWNvcmF0b3JzKSB7XG4gICAgcmV0dXJuIHtuYW1lOiB0eXBlLm5hbWUsIHR5cGUsIGlucHV0cywgb3V0cHV0cywgdmlld1F1ZXJpZXMsIHF1ZXJpZXMsIHByb3BNZXRhZGF0YX07XG4gIH1cblxuICByZXR1cm4gbnVsbDtcbn1cblxuZnVuY3Rpb24gY29udmVydFRvUjNRdWVyeVByZWRpY2F0ZShzZWxlY3RvcjogYW55KTogYW55fHN0cmluZ1tdIHtcbiAgcmV0dXJuIHR5cGVvZiBzZWxlY3RvciA9PT0gJ3N0cmluZycgPyBzcGxpdEJ5Q29tbWEoc2VsZWN0b3IpIDogcmVzb2x2ZUZvcndhcmRSZWYoc2VsZWN0b3IpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY29udmVydFRvUjNRdWVyeU1ldGFkYXRhKHByb3BlcnR5TmFtZTogc3RyaW5nLCBhbm46IFF1ZXJ5KTogUjNRdWVyeU1ldGFkYXRhRmFjYWRlIHtcbiAgcmV0dXJuIHtcbiAgICBwcm9wZXJ0eU5hbWU6IHByb3BlcnR5TmFtZSxcbiAgICBwcmVkaWNhdGU6IGNvbnZlcnRUb1IzUXVlcnlQcmVkaWNhdGUoYW5uLnNlbGVjdG9yKSxcbiAgICBkZXNjZW5kYW50czogYW5uLmRlc2NlbmRhbnRzLFxuICAgIGZpcnN0OiBhbm4uZmlyc3QsXG4gICAgcmVhZDogYW5uLnJlYWQgPyBhbm4ucmVhZCA6IG51bGwsXG4gICAgc3RhdGljOiAhIWFubi5zdGF0aWNcbiAgfTtcbn1cbmZ1bmN0aW9uIGV4dHJhY3RRdWVyaWVzTWV0YWRhdGEoXG4gICAgdHlwZTogVHlwZTxhbnk+LCBwcm9wTWV0YWRhdGE6IHtba2V5OiBzdHJpbmddOiBhbnlbXX0sXG4gICAgaXNRdWVyeUFubjogKGFubjogYW55KSA9PiBhbm4gaXMgUXVlcnkpOiBSM1F1ZXJ5TWV0YWRhdGFGYWNhZGVbXSB7XG4gIGNvbnN0IHF1ZXJpZXNNZXRhOiBSM1F1ZXJ5TWV0YWRhdGFGYWNhZGVbXSA9IFtdO1xuICBmb3IgKGNvbnN0IGZpZWxkIGluIHByb3BNZXRhZGF0YSkge1xuICAgIGlmIChwcm9wTWV0YWRhdGEuaGFzT3duUHJvcGVydHkoZmllbGQpKSB7XG4gICAgICBjb25zdCBhbm5vdGF0aW9ucyA9IHByb3BNZXRhZGF0YVtmaWVsZF07XG4gICAgICBhbm5vdGF0aW9ucy5mb3JFYWNoKGFubiA9PiB7XG4gICAgICAgIGlmIChpc1F1ZXJ5QW5uKGFubikpIHtcbiAgICAgICAgICBpZiAoIWFubi5zZWxlY3Rvcikge1xuICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgICAgICAgIGBDYW4ndCBjb25zdHJ1Y3QgYSBxdWVyeSBmb3IgdGhlIHByb3BlcnR5IFwiJHtmaWVsZH1cIiBvZiBgICtcbiAgICAgICAgICAgICAgICBgXCIke3N0cmluZ2lmeUZvckVycm9yKHR5cGUpfVwiIHNpbmNlIHRoZSBxdWVyeSBzZWxlY3RvciB3YXNuJ3QgZGVmaW5lZC5gKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGFubm90YXRpb25zLnNvbWUoaXNJbnB1dEFubikpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihgQ2Fubm90IGNvbWJpbmUgQElucHV0IGRlY29yYXRvcnMgd2l0aCBxdWVyeSBkZWNvcmF0b3JzYCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHF1ZXJpZXNNZXRhLnB1c2goY29udmVydFRvUjNRdWVyeU1ldGFkYXRhKGZpZWxkLCBhbm4pKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9XG4gIHJldHVybiBxdWVyaWVzTWV0YTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdEV4cG9ydEFzKGV4cG9ydEFzOiBzdHJpbmcgfCB1bmRlZmluZWQpOiBzdHJpbmdbXXxudWxsIHtcbiAgaWYgKGV4cG9ydEFzID09PSB1bmRlZmluZWQpIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHJldHVybiBleHBvcnRBcy5zcGxpdCgnLCcpLm1hcChwYXJ0ID0+IHBhcnQudHJpbSgpKTtcbn1cblxuZnVuY3Rpb24gaXNDb250ZW50UXVlcnkodmFsdWU6IGFueSk6IHZhbHVlIGlzIFF1ZXJ5IHtcbiAgY29uc3QgbmFtZSA9IHZhbHVlLm5nTWV0YWRhdGFOYW1lO1xuICByZXR1cm4gbmFtZSA9PT0gJ0NvbnRlbnRDaGlsZCcgfHwgbmFtZSA9PT0gJ0NvbnRlbnRDaGlsZHJlbic7XG59XG5cbmZ1bmN0aW9uIGlzVmlld1F1ZXJ5KHZhbHVlOiBhbnkpOiB2YWx1ZSBpcyBRdWVyeSB7XG4gIGNvbnN0IG5hbWUgPSB2YWx1ZS5uZ01ldGFkYXRhTmFtZTtcbiAgcmV0dXJuIG5hbWUgPT09ICdWaWV3Q2hpbGQnIHx8IG5hbWUgPT09ICdWaWV3Q2hpbGRyZW4nO1xufVxuXG5mdW5jdGlvbiBpc0lucHV0QW5uKHZhbHVlOiBhbnkpOiB2YWx1ZSBpcyBJbnB1dCB7XG4gIHJldHVybiB2YWx1ZS5uZ01ldGFkYXRhTmFtZSA9PT0gJ0lucHV0Jztcbn1cblxuZnVuY3Rpb24gc3BsaXRCeUNvbW1hKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmdbXSB7XG4gIHJldHVybiB2YWx1ZS5zcGxpdCgnLCcpLm1hcChwaWVjZSA9PiBwaWVjZS50cmltKCkpO1xufVxuIl19