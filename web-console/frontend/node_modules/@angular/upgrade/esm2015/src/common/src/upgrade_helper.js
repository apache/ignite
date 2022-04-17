/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { element as angularElement } from './angular1';
import { $COMPILE, $CONTROLLER, $HTTP_BACKEND, $INJECTOR, $TEMPLATE_CACHE } from './constants';
import { controllerKey, directiveNormalize, isFunction } from './util';
// Constants
const REQUIRE_PREFIX_RE = /^(\^\^?)?(\?)?(\^\^?)?/;
// Classes
export class UpgradeHelper {
    constructor(injector, name, elementRef, directive) {
        this.injector = injector;
        this.name = name;
        this.$injector = injector.get($INJECTOR);
        this.$compile = this.$injector.get($COMPILE);
        this.$controller = this.$injector.get($CONTROLLER);
        this.element = elementRef.nativeElement;
        this.$element = angularElement(this.element);
        this.directive = directive || UpgradeHelper.getDirective(this.$injector, name);
    }
    static getDirective($injector, name) {
        const directives = $injector.get(name + 'Directive');
        if (directives.length > 1) {
            throw new Error(`Only support single directive definition for: ${name}`);
        }
        const directive = directives[0];
        // AngularJS will transform `link: xyz` to `compile: () => xyz`. So we can only tell there was a
        // user-defined `compile` if there is no `link`. In other cases, we will just ignore `compile`.
        if (directive.compile && !directive.link)
            notSupported(name, 'compile');
        if (directive.replace)
            notSupported(name, 'replace');
        if (directive.terminal)
            notSupported(name, 'terminal');
        return directive;
    }
    static getTemplate($injector, directive, fetchRemoteTemplate = false, $element) {
        if (directive.template !== undefined) {
            return getOrCall(directive.template, $element);
        }
        else if (directive.templateUrl) {
            const $templateCache = $injector.get($TEMPLATE_CACHE);
            const url = getOrCall(directive.templateUrl, $element);
            const template = $templateCache.get(url);
            if (template !== undefined) {
                return template;
            }
            else if (!fetchRemoteTemplate) {
                throw new Error('loading directive templates asynchronously is not supported');
            }
            return new Promise((resolve, reject) => {
                const $httpBackend = $injector.get($HTTP_BACKEND);
                $httpBackend('GET', url, null, (status, response) => {
                    if (status === 200) {
                        resolve($templateCache.put(url, response));
                    }
                    else {
                        reject(`GET component template from '${url}' returned '${status}: ${response}'`);
                    }
                });
            });
        }
        else {
            throw new Error(`Directive '${directive.name}' is not a component, it is missing template.`);
        }
    }
    buildController(controllerType, $scope) {
        // TODO: Document that we do not pre-assign bindings on the controller instance.
        // Quoted properties below so that this code can be optimized with Closure Compiler.
        const locals = { '$scope': $scope, '$element': this.$element };
        const controller = this.$controller(controllerType, locals, null, this.directive.controllerAs);
        this.$element.data(controllerKey(this.directive.name), controller);
        return controller;
    }
    compileTemplate(template) {
        if (template === undefined) {
            template =
                UpgradeHelper.getTemplate(this.$injector, this.directive, false, this.$element);
        }
        return this.compileHtml(template);
    }
    onDestroy($scope, controllerInstance) {
        if (controllerInstance && isFunction(controllerInstance.$onDestroy)) {
            controllerInstance.$onDestroy();
        }
        $scope.$destroy();
        // Clean the jQuery/jqLite data on the component+child elements.
        // Equivelent to how jQuery/jqLite invoke `cleanData` on an Element (this.element)
        //  https://github.com/jquery/jquery/blob/e743cbd28553267f955f71ea7248377915613fd9/src/manipulation.js#L223
        //  https://github.com/angular/angular.js/blob/26ddc5f830f902a3d22f4b2aab70d86d4d688c82/src/jqLite.js#L306-L312
        // `cleanData` will invoke the AngularJS `$destroy` DOM event
        //  https://github.com/angular/angular.js/blob/26ddc5f830f902a3d22f4b2aab70d86d4d688c82/src/Angular.js#L1911-L1924
        angularElement.cleanData([this.element]);
        angularElement.cleanData(this.element.querySelectorAll('*'));
    }
    prepareTransclusion() {
        const transclude = this.directive.transclude;
        const contentChildNodes = this.extractChildNodes();
        const attachChildrenFn = (scope, cloneAttachFn) => {
            // Since AngularJS v1.5.8, `cloneAttachFn` will try to destroy the transclusion scope if
            // `$template` is empty. Since the transcluded content comes from Angular, not AngularJS,
            // there will be no transclusion scope here.
            // Provide a dummy `scope.$destroy()` method to prevent `cloneAttachFn` from throwing.
            scope = scope || { $destroy: () => undefined };
            return cloneAttachFn($template, scope);
        };
        let $template = contentChildNodes;
        if (transclude) {
            const slots = Object.create(null);
            if (typeof transclude === 'object') {
                $template = [];
                const slotMap = Object.create(null);
                const filledSlots = Object.create(null);
                // Parse the element selectors.
                Object.keys(transclude).forEach(slotName => {
                    let selector = transclude[slotName];
                    const optional = selector.charAt(0) === '?';
                    selector = optional ? selector.substring(1) : selector;
                    slotMap[selector] = slotName;
                    slots[slotName] = null; // `null`: Defined but not yet filled.
                    filledSlots[slotName] = optional; // Consider optional slots as filled.
                });
                // Add the matching elements into their slot.
                contentChildNodes.forEach(node => {
                    const slotName = slotMap[directiveNormalize(node.nodeName.toLowerCase())];
                    if (slotName) {
                        filledSlots[slotName] = true;
                        slots[slotName] = slots[slotName] || [];
                        slots[slotName].push(node);
                    }
                    else {
                        $template.push(node);
                    }
                });
                // Check for required slots that were not filled.
                Object.keys(filledSlots).forEach(slotName => {
                    if (!filledSlots[slotName]) {
                        throw new Error(`Required transclusion slot '${slotName}' on directive: ${this.name}`);
                    }
                });
                Object.keys(slots).filter(slotName => slots[slotName]).forEach(slotName => {
                    const nodes = slots[slotName];
                    slots[slotName] = (scope, cloneAttach) => {
                        return cloneAttach(nodes, scope);
                    };
                });
            }
            // Attach `$$slots` to default slot transclude fn.
            attachChildrenFn.$$slots = slots;
            // AngularJS v1.6+ ignores empty or whitespace-only transcluded text nodes. But Angular
            // removes all text content after the first interpolation and updates it later, after
            // evaluating the expressions. This would result in AngularJS failing to recognize text
            // nodes that start with an interpolation as transcluded content and use the fallback
            // content instead.
            // To avoid this issue, we add a
            // [zero-width non-joiner character](https://en.wikipedia.org/wiki/Zero-width_non-joiner)
            // to empty text nodes (which can only be a result of Angular removing their initial content).
            // NOTE: Transcluded text content that starts with whitespace followed by an interpolation
            //       will still fail to be detected by AngularJS v1.6+
            $template.forEach(node => {
                if (node.nodeType === Node.TEXT_NODE && !node.nodeValue) {
                    node.nodeValue = '\u200C';
                }
            });
        }
        return attachChildrenFn;
    }
    resolveAndBindRequiredControllers(controllerInstance) {
        const directiveRequire = this.getDirectiveRequire();
        const requiredControllers = this.resolveRequire(directiveRequire);
        if (controllerInstance && this.directive.bindToController && isMap(directiveRequire)) {
            const requiredControllersMap = requiredControllers;
            Object.keys(requiredControllersMap).forEach(key => {
                controllerInstance[key] = requiredControllersMap[key];
            });
        }
        return requiredControllers;
    }
    compileHtml(html) {
        this.element.innerHTML = html;
        return this.$compile(this.element.childNodes);
    }
    extractChildNodes() {
        const childNodes = [];
        let childNode;
        while (childNode = this.element.firstChild) {
            this.element.removeChild(childNode);
            childNodes.push(childNode);
        }
        return childNodes;
    }
    getDirectiveRequire() {
        const require = this.directive.require || (this.directive.controller && this.directive.name);
        if (isMap(require)) {
            Object.keys(require).forEach(key => {
                const value = require[key];
                const match = value.match(REQUIRE_PREFIX_RE);
                const name = value.substring(match[0].length);
                if (!name) {
                    require[key] = match[0] + key;
                }
            });
        }
        return require;
    }
    resolveRequire(require, controllerInstance) {
        if (!require) {
            return null;
        }
        else if (Array.isArray(require)) {
            return require.map(req => this.resolveRequire(req));
        }
        else if (typeof require === 'object') {
            const value = {};
            Object.keys(require).forEach(key => value[key] = this.resolveRequire(require[key]));
            return value;
        }
        else if (typeof require === 'string') {
            const match = require.match(REQUIRE_PREFIX_RE);
            const inheritType = match[1] || match[3];
            const name = require.substring(match[0].length);
            const isOptional = !!match[2];
            const searchParents = !!inheritType;
            const startOnParent = inheritType === '^^';
            const ctrlKey = controllerKey(name);
            const elem = startOnParent ? this.$element.parent() : this.$element;
            const value = searchParents ? elem.inheritedData(ctrlKey) : elem.data(ctrlKey);
            if (!value && !isOptional) {
                throw new Error(`Unable to find required '${require}' in upgraded directive '${this.name}'.`);
            }
            return value;
        }
        else {
            throw new Error(`Unrecognized 'require' syntax on upgraded directive '${this.name}': ${require}`);
        }
    }
}
function getOrCall(property, ...args) {
    return isFunction(property) ? property(...args) : property;
}
// NOTE: Only works for `typeof T !== 'object'`.
function isMap(value) {
    return value && !Array.isArray(value) && typeof value === 'object';
}
function notSupported(name, feature) {
    throw new Error(`Upgraded directive '${name}' contains unsupported feature: '${feature}'.`);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXBncmFkZV9oZWxwZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy91cGdyYWRlL3NyYy9jb21tb24vc3JjL3VwZ3JhZGVfaGVscGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUlILE9BQU8sRUFBbU8sT0FBTyxJQUFJLGNBQWMsRUFBQyxNQUFNLFlBQVksQ0FBQztBQUN2UixPQUFPLEVBQUMsUUFBUSxFQUFFLFdBQVcsRUFBRSxhQUFhLEVBQUUsU0FBUyxFQUFFLGVBQWUsRUFBQyxNQUFNLGFBQWEsQ0FBQztBQUM3RixPQUFPLEVBQUMsYUFBYSxFQUFFLGtCQUFrQixFQUFFLFVBQVUsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUlyRSxZQUFZO0FBQ1osTUFBTSxpQkFBaUIsR0FBRyx3QkFBd0IsQ0FBQztBQWVuRCxVQUFVO0FBQ1YsTUFBTSxPQUFPLGFBQWE7SUFTeEIsWUFDWSxRQUFrQixFQUFVLElBQVksRUFBRSxVQUFzQixFQUN4RSxTQUFzQjtRQURkLGFBQVEsR0FBUixRQUFRLENBQVU7UUFBVSxTQUFJLEdBQUosSUFBSSxDQUFRO1FBRWxELElBQUksQ0FBQyxTQUFTLEdBQUcsUUFBUSxDQUFDLEdBQUcsQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUN6QyxJQUFJLENBQUMsUUFBUSxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsR0FBRyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1FBQzdDLElBQUksQ0FBQyxXQUFXLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxHQUFHLENBQUMsV0FBVyxDQUFDLENBQUM7UUFFbkQsSUFBSSxDQUFDLE9BQU8sR0FBRyxVQUFVLENBQUMsYUFBYSxDQUFDO1FBQ3hDLElBQUksQ0FBQyxRQUFRLEdBQUcsY0FBYyxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUU3QyxJQUFJLENBQUMsU0FBUyxHQUFHLFNBQVMsSUFBSSxhQUFhLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxTQUFTLEVBQUUsSUFBSSxDQUFDLENBQUM7SUFDakYsQ0FBQztJQUVELE1BQU0sQ0FBQyxZQUFZLENBQUMsU0FBMkIsRUFBRSxJQUFZO1FBQzNELE1BQU0sVUFBVSxHQUFpQixTQUFTLENBQUMsR0FBRyxDQUFDLElBQUksR0FBRyxXQUFXLENBQUMsQ0FBQztRQUNuRSxJQUFJLFVBQVUsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFO1lBQ3pCLE1BQU0sSUFBSSxLQUFLLENBQUMsaURBQWlELElBQUksRUFBRSxDQUFDLENBQUM7U0FDMUU7UUFFRCxNQUFNLFNBQVMsR0FBRyxVQUFVLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFFaEMsZ0dBQWdHO1FBQ2hHLCtGQUErRjtRQUMvRixJQUFJLFNBQVMsQ0FBQyxPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSTtZQUFFLFlBQVksQ0FBQyxJQUFJLEVBQUUsU0FBUyxDQUFDLENBQUM7UUFDeEUsSUFBSSxTQUFTLENBQUMsT0FBTztZQUFFLFlBQVksQ0FBQyxJQUFJLEVBQUUsU0FBUyxDQUFDLENBQUM7UUFDckQsSUFBSSxTQUFTLENBQUMsUUFBUTtZQUFFLFlBQVksQ0FBQyxJQUFJLEVBQUUsVUFBVSxDQUFDLENBQUM7UUFFdkQsT0FBTyxTQUFTLENBQUM7SUFDbkIsQ0FBQztJQUVELE1BQU0sQ0FBQyxXQUFXLENBQ2QsU0FBMkIsRUFBRSxTQUFxQixFQUFFLG1CQUFtQixHQUFHLEtBQUssRUFDL0UsUUFBMkI7UUFDN0IsSUFBSSxTQUFTLENBQUMsUUFBUSxLQUFLLFNBQVMsRUFBRTtZQUNwQyxPQUFPLFNBQVMsQ0FBUyxTQUFTLENBQUMsUUFBUSxFQUFFLFFBQVEsQ0FBQyxDQUFDO1NBQ3hEO2FBQU0sSUFBSSxTQUFTLENBQUMsV0FBVyxFQUFFO1lBQ2hDLE1BQU0sY0FBYyxHQUFHLFNBQVMsQ0FBQyxHQUFHLENBQUMsZUFBZSxDQUEwQixDQUFDO1lBQy9FLE1BQU0sR0FBRyxHQUFHLFNBQVMsQ0FBUyxTQUFTLENBQUMsV0FBVyxFQUFFLFFBQVEsQ0FBQyxDQUFDO1lBQy9ELE1BQU0sUUFBUSxHQUFHLGNBQWMsQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDLENBQUM7WUFFekMsSUFBSSxRQUFRLEtBQUssU0FBUyxFQUFFO2dCQUMxQixPQUFPLFFBQVEsQ0FBQzthQUNqQjtpQkFBTSxJQUFJLENBQUMsbUJBQW1CLEVBQUU7Z0JBQy9CLE1BQU0sSUFBSSxLQUFLLENBQUMsNkRBQTZELENBQUMsQ0FBQzthQUNoRjtZQUVELE9BQU8sSUFBSSxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsTUFBTSxFQUFFLEVBQUU7Z0JBQ3JDLE1BQU0sWUFBWSxHQUFHLFNBQVMsQ0FBQyxHQUFHLENBQUMsYUFBYSxDQUF3QixDQUFDO2dCQUN6RSxZQUFZLENBQUMsS0FBSyxFQUFFLEdBQUcsRUFBRSxJQUFJLEVBQUUsQ0FBQyxNQUFjLEVBQUUsUUFBZ0IsRUFBRSxFQUFFO29CQUNsRSxJQUFJLE1BQU0sS0FBSyxHQUFHLEVBQUU7d0JBQ2xCLE9BQU8sQ0FBQyxjQUFjLENBQUMsR0FBRyxDQUFDLEdBQUcsRUFBRSxRQUFRLENBQUMsQ0FBQyxDQUFDO3FCQUM1Qzt5QkFBTTt3QkFDTCxNQUFNLENBQUMsZ0NBQWdDLEdBQUcsZUFBZSxNQUFNLEtBQUssUUFBUSxHQUFHLENBQUMsQ0FBQztxQkFDbEY7Z0JBQ0gsQ0FBQyxDQUFDLENBQUM7WUFDTCxDQUFDLENBQUMsQ0FBQztTQUNKO2FBQU07WUFDTCxNQUFNLElBQUksS0FBSyxDQUFDLGNBQWMsU0FBUyxDQUFDLElBQUksK0NBQStDLENBQUMsQ0FBQztTQUM5RjtJQUNILENBQUM7SUFFRCxlQUFlLENBQUMsY0FBMkIsRUFBRSxNQUFjO1FBQ3pELGdGQUFnRjtRQUNoRixvRkFBb0Y7UUFDcEYsTUFBTSxNQUFNLEdBQUcsRUFBQyxRQUFRLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxJQUFJLENBQUMsUUFBUSxFQUFDLENBQUM7UUFDN0QsTUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLFdBQVcsQ0FBQyxjQUFjLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxJQUFJLENBQUMsU0FBUyxDQUFDLFlBQVksQ0FBQyxDQUFDO1FBRS9GLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBTSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLElBQU0sQ0FBQyxFQUFFLFVBQVUsQ0FBQyxDQUFDO1FBRXZFLE9BQU8sVUFBVSxDQUFDO0lBQ3BCLENBQUM7SUFFRCxlQUFlLENBQUMsUUFBaUI7UUFDL0IsSUFBSSxRQUFRLEtBQUssU0FBUyxFQUFFO1lBQzFCLFFBQVE7Z0JBQ0osYUFBYSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFLElBQUksQ0FBQyxTQUFTLEVBQUUsS0FBSyxFQUFFLElBQUksQ0FBQyxRQUFRLENBQVcsQ0FBQztTQUMvRjtRQUVELE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUNwQyxDQUFDO0lBRUQsU0FBUyxDQUFDLE1BQWMsRUFBRSxrQkFBd0I7UUFDaEQsSUFBSSxrQkFBa0IsSUFBSSxVQUFVLENBQUMsa0JBQWtCLENBQUMsVUFBVSxDQUFDLEVBQUU7WUFDbkUsa0JBQWtCLENBQUMsVUFBVSxFQUFFLENBQUM7U0FDakM7UUFDRCxNQUFNLENBQUMsUUFBUSxFQUFFLENBQUM7UUFFbEIsZ0VBQWdFO1FBQ2hFLGtGQUFrRjtRQUNsRiwyR0FBMkc7UUFDM0csK0dBQStHO1FBQy9HLDZEQUE2RDtRQUM3RCxrSEFBa0g7UUFDbEgsY0FBYyxDQUFDLFNBQVMsQ0FBQyxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDO1FBQ3pDLGNBQWMsQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxnQkFBZ0IsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDO0lBQy9ELENBQUM7SUFFRCxtQkFBbUI7UUFDakIsTUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxVQUFVLENBQUM7UUFDN0MsTUFBTSxpQkFBaUIsR0FBRyxJQUFJLENBQUMsaUJBQWlCLEVBQUUsQ0FBQztRQUNuRCxNQUFNLGdCQUFnQixHQUFZLENBQUMsS0FBSyxFQUFFLGFBQWEsRUFBRSxFQUFFO1lBQ3pELHdGQUF3RjtZQUN4Rix5RkFBeUY7WUFDekYsNENBQTRDO1lBQzVDLHNGQUFzRjtZQUN0RixLQUFLLEdBQUcsS0FBSyxJQUFJLEVBQUMsUUFBUSxFQUFFLEdBQUcsRUFBRSxDQUFDLFNBQVMsRUFBQyxDQUFDO1lBQzdDLE9BQU8sYUFBZSxDQUFDLFNBQVMsRUFBRSxLQUFLLENBQUMsQ0FBQztRQUMzQyxDQUFDLENBQUM7UUFDRixJQUFJLFNBQVMsR0FBRyxpQkFBaUIsQ0FBQztRQUVsQyxJQUFJLFVBQVUsRUFBRTtZQUNkLE1BQU0sS0FBSyxHQUFHLE1BQU0sQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUM7WUFFbEMsSUFBSSxPQUFPLFVBQVUsS0FBSyxRQUFRLEVBQUU7Z0JBQ2xDLFNBQVMsR0FBRyxFQUFFLENBQUM7Z0JBRWYsTUFBTSxPQUFPLEdBQUcsTUFBTSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDcEMsTUFBTSxXQUFXLEdBQUcsTUFBTSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFFeEMsK0JBQStCO2dCQUMvQixNQUFNLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxRQUFRLENBQUMsRUFBRTtvQkFDekMsSUFBSSxRQUFRLEdBQUcsVUFBVSxDQUFDLFFBQVEsQ0FBQyxDQUFDO29CQUNwQyxNQUFNLFFBQVEsR0FBRyxRQUFRLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxLQUFLLEdBQUcsQ0FBQztvQkFDNUMsUUFBUSxHQUFHLFFBQVEsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDO29CQUV2RCxPQUFPLENBQUMsUUFBUSxDQUFDLEdBQUcsUUFBUSxDQUFDO29CQUM3QixLQUFLLENBQUMsUUFBUSxDQUFDLEdBQUcsSUFBSSxDQUFDLENBQVksc0NBQXNDO29CQUN6RSxXQUFXLENBQUMsUUFBUSxDQUFDLEdBQUcsUUFBUSxDQUFDLENBQUUscUNBQXFDO2dCQUMxRSxDQUFDLENBQUMsQ0FBQztnQkFFSCw2Q0FBNkM7Z0JBQzdDLGlCQUFpQixDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsRUFBRTtvQkFDL0IsTUFBTSxRQUFRLEdBQUcsT0FBTyxDQUFDLGtCQUFrQixDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsV0FBVyxFQUFFLENBQUMsQ0FBQyxDQUFDO29CQUMxRSxJQUFJLFFBQVEsRUFBRTt3QkFDWixXQUFXLENBQUMsUUFBUSxDQUFDLEdBQUcsSUFBSSxDQUFDO3dCQUM3QixLQUFLLENBQUMsUUFBUSxDQUFDLEdBQUcsS0FBSyxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsQ0FBQzt3QkFDeEMsS0FBSyxDQUFDLFFBQVEsQ0FBQyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztxQkFDNUI7eUJBQU07d0JBQ0wsU0FBUyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztxQkFDdEI7Z0JBQ0gsQ0FBQyxDQUFDLENBQUM7Z0JBRUgsaURBQWlEO2dCQUNqRCxNQUFNLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxRQUFRLENBQUMsRUFBRTtvQkFDMUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxRQUFRLENBQUMsRUFBRTt3QkFDMUIsTUFBTSxJQUFJLEtBQUssQ0FBQywrQkFBK0IsUUFBUSxtQkFBbUIsSUFBSSxDQUFDLElBQUksRUFBRSxDQUFDLENBQUM7cUJBQ3hGO2dCQUNILENBQUMsQ0FBQyxDQUFDO2dCQUVILE1BQU0sQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsTUFBTSxDQUFDLFFBQVEsQ0FBQyxFQUFFLENBQUMsS0FBSyxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUMsT0FBTyxDQUFDLFFBQVEsQ0FBQyxFQUFFO29CQUN4RSxNQUFNLEtBQUssR0FBRyxLQUFLLENBQUMsUUFBUSxDQUFDLENBQUM7b0JBQzlCLEtBQUssQ0FBQyxRQUFRLENBQUMsR0FBRyxDQUFDLEtBQWEsRUFBRSxXQUFpQyxFQUFFLEVBQUU7d0JBQ3JFLE9BQU8sV0FBYSxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQztvQkFDckMsQ0FBQyxDQUFDO2dCQUNKLENBQUMsQ0FBQyxDQUFDO2FBQ0o7WUFFRCxrREFBa0Q7WUFDbEQsZ0JBQWdCLENBQUMsT0FBTyxHQUFHLEtBQUssQ0FBQztZQUVqQyx1RkFBdUY7WUFDdkYscUZBQXFGO1lBQ3JGLHVGQUF1RjtZQUN2RixxRkFBcUY7WUFDckYsbUJBQW1CO1lBQ25CLGdDQUFnQztZQUNoQyx5RkFBeUY7WUFDekYsOEZBQThGO1lBQzlGLDBGQUEwRjtZQUMxRiwwREFBMEQ7WUFDMUQsU0FBUyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsRUFBRTtnQkFDdkIsSUFBSSxJQUFJLENBQUMsUUFBUSxLQUFLLElBQUksQ0FBQyxTQUFTLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFO29CQUN2RCxJQUFJLENBQUMsU0FBUyxHQUFHLFFBQVEsQ0FBQztpQkFDM0I7WUFDSCxDQUFDLENBQUMsQ0FBQztTQUNKO1FBRUQsT0FBTyxnQkFBZ0IsQ0FBQztJQUMxQixDQUFDO0lBRUQsaUNBQWlDLENBQUMsa0JBQTRDO1FBQzVFLE1BQU0sZ0JBQWdCLEdBQUcsSUFBSSxDQUFDLG1CQUFtQixFQUFFLENBQUM7UUFDcEQsTUFBTSxtQkFBbUIsR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLGdCQUFnQixDQUFDLENBQUM7UUFFbEUsSUFBSSxrQkFBa0IsSUFBSSxJQUFJLENBQUMsU0FBUyxDQUFDLGdCQUFnQixJQUFJLEtBQUssQ0FBQyxnQkFBZ0IsQ0FBQyxFQUFFO1lBQ3BGLE1BQU0sc0JBQXNCLEdBQUcsbUJBQTBELENBQUM7WUFDMUYsTUFBTSxDQUFDLElBQUksQ0FBQyxzQkFBc0IsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsRUFBRTtnQkFDaEQsa0JBQWtCLENBQUMsR0FBRyxDQUFDLEdBQUcsc0JBQXNCLENBQUMsR0FBRyxDQUFDLENBQUM7WUFDeEQsQ0FBQyxDQUFDLENBQUM7U0FDSjtRQUVELE9BQU8sbUJBQW1CLENBQUM7SUFDN0IsQ0FBQztJQUVPLFdBQVcsQ0FBQyxJQUFZO1FBQzlCLElBQUksQ0FBQyxPQUFPLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQztRQUM5QixPQUFPLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxVQUFVLENBQUMsQ0FBQztJQUNoRCxDQUFDO0lBRU8saUJBQWlCO1FBQ3ZCLE1BQU0sVUFBVSxHQUFXLEVBQUUsQ0FBQztRQUM5QixJQUFJLFNBQW9CLENBQUM7UUFFekIsT0FBTyxTQUFTLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxVQUFVLEVBQUU7WUFDMUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxXQUFXLENBQUMsU0FBUyxDQUFDLENBQUM7WUFDcEMsVUFBVSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUM1QjtRQUVELE9BQU8sVUFBVSxDQUFDO0lBQ3BCLENBQUM7SUFFTyxtQkFBbUI7UUFDekIsTUFBTSxPQUFPLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLFVBQVUsSUFBSSxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBRyxDQUFDO1FBRS9GLElBQUksS0FBSyxDQUFDLE9BQU8sQ0FBQyxFQUFFO1lBQ2xCLE1BQU0sQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxFQUFFO2dCQUNqQyxNQUFNLEtBQUssR0FBRyxPQUFPLENBQUMsR0FBRyxDQUFDLENBQUM7Z0JBQzNCLE1BQU0sS0FBSyxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUMsaUJBQWlCLENBQUcsQ0FBQztnQkFDL0MsTUFBTSxJQUFJLEdBQUcsS0FBSyxDQUFDLFNBQVMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUM7Z0JBRTlDLElBQUksQ0FBQyxJQUFJLEVBQUU7b0JBQ1QsT0FBTyxDQUFDLEdBQUcsQ0FBQyxHQUFHLEtBQUssQ0FBQyxDQUFDLENBQUMsR0FBRyxHQUFHLENBQUM7aUJBQy9CO1lBQ0gsQ0FBQyxDQUFDLENBQUM7U0FDSjtRQUVELE9BQU8sT0FBTyxDQUFDO0lBQ2pCLENBQUM7SUFFTyxjQUFjLENBQUMsT0FBaUMsRUFBRSxrQkFBd0I7UUFFaEYsSUFBSSxDQUFDLE9BQU8sRUFBRTtZQUNaLE9BQU8sSUFBSSxDQUFDO1NBQ2I7YUFBTSxJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsT0FBTyxDQUFDLEVBQUU7WUFDakMsT0FBTyxPQUFPLENBQUMsR0FBRyxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLGNBQWMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDO1NBQ3JEO2FBQU0sSUFBSSxPQUFPLE9BQU8sS0FBSyxRQUFRLEVBQUU7WUFDdEMsTUFBTSxLQUFLLEdBQXlDLEVBQUUsQ0FBQztZQUN2RCxNQUFNLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsQ0FBRyxDQUFDLENBQUM7WUFDdEYsT0FBTyxLQUFLLENBQUM7U0FDZDthQUFNLElBQUksT0FBTyxPQUFPLEtBQUssUUFBUSxFQUFFO1lBQ3RDLE1BQU0sS0FBSyxHQUFHLE9BQU8sQ0FBQyxLQUFLLENBQUMsaUJBQWlCLENBQUcsQ0FBQztZQUNqRCxNQUFNLFdBQVcsR0FBRyxLQUFLLENBQUMsQ0FBQyxDQUFDLElBQUksS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDO1lBRXpDLE1BQU0sSUFBSSxHQUFHLE9BQU8sQ0FBQyxTQUFTLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxDQUFDO1lBQ2hELE1BQU0sVUFBVSxHQUFHLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7WUFDOUIsTUFBTSxhQUFhLEdBQUcsQ0FBQyxDQUFDLFdBQVcsQ0FBQztZQUNwQyxNQUFNLGFBQWEsR0FBRyxXQUFXLEtBQUssSUFBSSxDQUFDO1lBRTNDLE1BQU0sT0FBTyxHQUFHLGFBQWEsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUNwQyxNQUFNLElBQUksR0FBRyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsTUFBUSxFQUFFLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUM7WUFDdEUsTUFBTSxLQUFLLEdBQUcsYUFBYSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsYUFBZSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsSUFBTSxDQUFDLE9BQU8sQ0FBQyxDQUFDO1lBRW5GLElBQUksQ0FBQyxLQUFLLElBQUksQ0FBQyxVQUFVLEVBQUU7Z0JBQ3pCLE1BQU0sSUFBSSxLQUFLLENBQ1gsNEJBQTRCLE9BQU8sNEJBQTRCLElBQUksQ0FBQyxJQUFJLElBQUksQ0FBQyxDQUFDO2FBQ25GO1lBRUQsT0FBTyxLQUFLLENBQUM7U0FDZDthQUFNO1lBQ0wsTUFBTSxJQUFJLEtBQUssQ0FDWCx3REFBd0QsSUFBSSxDQUFDLElBQUksTUFBTSxPQUFPLEVBQUUsQ0FBQyxDQUFDO1NBQ3ZGO0lBQ0gsQ0FBQztDQUNGO0FBRUQsU0FBUyxTQUFTLENBQUksUUFBc0IsRUFBRSxHQUFHLElBQVc7SUFDMUQsT0FBTyxVQUFVLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUM7QUFDN0QsQ0FBQztBQUVELGdEQUFnRDtBQUNoRCxTQUFTLEtBQUssQ0FBSSxLQUEyQjtJQUMzQyxPQUFPLEtBQUssSUFBSSxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLElBQUksT0FBTyxLQUFLLEtBQUssUUFBUSxDQUFDO0FBQ3JFLENBQUM7QUFFRCxTQUFTLFlBQVksQ0FBQyxJQUFZLEVBQUUsT0FBZTtJQUNqRCxNQUFNLElBQUksS0FBSyxDQUFDLHVCQUF1QixJQUFJLG9DQUFvQyxPQUFPLElBQUksQ0FBQyxDQUFDO0FBQzlGLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RWxlbWVudFJlZiwgSW5qZWN0b3IsIFNpbXBsZUNoYW5nZXN9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge0RpcmVjdGl2ZVJlcXVpcmVQcm9wZXJ0eSwgSUF1Z21lbnRlZEpRdWVyeSwgSUNsb25lQXR0YWNoRnVuY3Rpb24sIElDb21waWxlU2VydmljZSwgSUNvbnRyb2xsZXIsIElDb250cm9sbGVyU2VydmljZSwgSURpcmVjdGl2ZSwgSUh0dHBCYWNrZW5kU2VydmljZSwgSUluamVjdG9yU2VydmljZSwgSUxpbmtGbiwgSVNjb3BlLCBJVGVtcGxhdGVDYWNoZVNlcnZpY2UsIFNpbmdsZU9yTGlzdE9yTWFwLCBlbGVtZW50IGFzIGFuZ3VsYXJFbGVtZW50fSBmcm9tICcuL2FuZ3VsYXIxJztcbmltcG9ydCB7JENPTVBJTEUsICRDT05UUk9MTEVSLCAkSFRUUF9CQUNLRU5ELCAkSU5KRUNUT1IsICRURU1QTEFURV9DQUNIRX0gZnJvbSAnLi9jb25zdGFudHMnO1xuaW1wb3J0IHtjb250cm9sbGVyS2V5LCBkaXJlY3RpdmVOb3JtYWxpemUsIGlzRnVuY3Rpb259IGZyb20gJy4vdXRpbCc7XG5cblxuXG4vLyBDb25zdGFudHNcbmNvbnN0IFJFUVVJUkVfUFJFRklYX1JFID0gL14oXFxeXFxePyk/KFxcPyk/KFxcXlxcXj8pPy87XG5cbi8vIEludGVyZmFjZXNcbmV4cG9ydCBpbnRlcmZhY2UgSUJpbmRpbmdEZXN0aW5hdGlvbiB7XG4gIFtrZXk6IHN0cmluZ106IGFueTtcbiAgJG9uQ2hhbmdlcz86IChjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKSA9PiB2b2lkO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIElDb250cm9sbGVySW5zdGFuY2UgZXh0ZW5kcyBJQmluZGluZ0Rlc3RpbmF0aW9uIHtcbiAgJGRvQ2hlY2s/OiAoKSA9PiB2b2lkO1xuICAkb25EZXN0cm95PzogKCkgPT4gdm9pZDtcbiAgJG9uSW5pdD86ICgpID0+IHZvaWQ7XG4gICRwb3N0TGluaz86ICgpID0+IHZvaWQ7XG59XG5cbi8vIENsYXNzZXNcbmV4cG9ydCBjbGFzcyBVcGdyYWRlSGVscGVyIHtcbiAgcHVibGljIHJlYWRvbmx5ICRpbmplY3RvcjogSUluamVjdG9yU2VydmljZTtcbiAgcHVibGljIHJlYWRvbmx5IGVsZW1lbnQ6IEVsZW1lbnQ7XG4gIHB1YmxpYyByZWFkb25seSAkZWxlbWVudDogSUF1Z21lbnRlZEpRdWVyeTtcbiAgcHVibGljIHJlYWRvbmx5IGRpcmVjdGl2ZTogSURpcmVjdGl2ZTtcblxuICBwcml2YXRlIHJlYWRvbmx5ICRjb21waWxlOiBJQ29tcGlsZVNlcnZpY2U7XG4gIHByaXZhdGUgcmVhZG9ubHkgJGNvbnRyb2xsZXI6IElDb250cm9sbGVyU2VydmljZTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgaW5qZWN0b3I6IEluamVjdG9yLCBwcml2YXRlIG5hbWU6IHN0cmluZywgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICAgIGRpcmVjdGl2ZT86IElEaXJlY3RpdmUpIHtcbiAgICB0aGlzLiRpbmplY3RvciA9IGluamVjdG9yLmdldCgkSU5KRUNUT1IpO1xuICAgIHRoaXMuJGNvbXBpbGUgPSB0aGlzLiRpbmplY3Rvci5nZXQoJENPTVBJTEUpO1xuICAgIHRoaXMuJGNvbnRyb2xsZXIgPSB0aGlzLiRpbmplY3Rvci5nZXQoJENPTlRST0xMRVIpO1xuXG4gICAgdGhpcy5lbGVtZW50ID0gZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50O1xuICAgIHRoaXMuJGVsZW1lbnQgPSBhbmd1bGFyRWxlbWVudCh0aGlzLmVsZW1lbnQpO1xuXG4gICAgdGhpcy5kaXJlY3RpdmUgPSBkaXJlY3RpdmUgfHwgVXBncmFkZUhlbHBlci5nZXREaXJlY3RpdmUodGhpcy4kaW5qZWN0b3IsIG5hbWUpO1xuICB9XG5cbiAgc3RhdGljIGdldERpcmVjdGl2ZSgkaW5qZWN0b3I6IElJbmplY3RvclNlcnZpY2UsIG5hbWU6IHN0cmluZyk6IElEaXJlY3RpdmUge1xuICAgIGNvbnN0IGRpcmVjdGl2ZXM6IElEaXJlY3RpdmVbXSA9ICRpbmplY3Rvci5nZXQobmFtZSArICdEaXJlY3RpdmUnKTtcbiAgICBpZiAoZGlyZWN0aXZlcy5sZW5ndGggPiAxKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYE9ubHkgc3VwcG9ydCBzaW5nbGUgZGlyZWN0aXZlIGRlZmluaXRpb24gZm9yOiAke25hbWV9YCk7XG4gICAgfVxuXG4gICAgY29uc3QgZGlyZWN0aXZlID0gZGlyZWN0aXZlc1swXTtcblxuICAgIC8vIEFuZ3VsYXJKUyB3aWxsIHRyYW5zZm9ybSBgbGluazogeHl6YCB0byBgY29tcGlsZTogKCkgPT4geHl6YC4gU28gd2UgY2FuIG9ubHkgdGVsbCB0aGVyZSB3YXMgYVxuICAgIC8vIHVzZXItZGVmaW5lZCBgY29tcGlsZWAgaWYgdGhlcmUgaXMgbm8gYGxpbmtgLiBJbiBvdGhlciBjYXNlcywgd2Ugd2lsbCBqdXN0IGlnbm9yZSBgY29tcGlsZWAuXG4gICAgaWYgKGRpcmVjdGl2ZS5jb21waWxlICYmICFkaXJlY3RpdmUubGluaykgbm90U3VwcG9ydGVkKG5hbWUsICdjb21waWxlJyk7XG4gICAgaWYgKGRpcmVjdGl2ZS5yZXBsYWNlKSBub3RTdXBwb3J0ZWQobmFtZSwgJ3JlcGxhY2UnKTtcbiAgICBpZiAoZGlyZWN0aXZlLnRlcm1pbmFsKSBub3RTdXBwb3J0ZWQobmFtZSwgJ3Rlcm1pbmFsJyk7XG5cbiAgICByZXR1cm4gZGlyZWN0aXZlO1xuICB9XG5cbiAgc3RhdGljIGdldFRlbXBsYXRlKFxuICAgICAgJGluamVjdG9yOiBJSW5qZWN0b3JTZXJ2aWNlLCBkaXJlY3RpdmU6IElEaXJlY3RpdmUsIGZldGNoUmVtb3RlVGVtcGxhdGUgPSBmYWxzZSxcbiAgICAgICRlbGVtZW50PzogSUF1Z21lbnRlZEpRdWVyeSk6IHN0cmluZ3xQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmIChkaXJlY3RpdmUudGVtcGxhdGUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgcmV0dXJuIGdldE9yQ2FsbDxzdHJpbmc+KGRpcmVjdGl2ZS50ZW1wbGF0ZSwgJGVsZW1lbnQpO1xuICAgIH0gZWxzZSBpZiAoZGlyZWN0aXZlLnRlbXBsYXRlVXJsKSB7XG4gICAgICBjb25zdCAkdGVtcGxhdGVDYWNoZSA9ICRpbmplY3Rvci5nZXQoJFRFTVBMQVRFX0NBQ0hFKSBhcyBJVGVtcGxhdGVDYWNoZVNlcnZpY2U7XG4gICAgICBjb25zdCB1cmwgPSBnZXRPckNhbGw8c3RyaW5nPihkaXJlY3RpdmUudGVtcGxhdGVVcmwsICRlbGVtZW50KTtcbiAgICAgIGNvbnN0IHRlbXBsYXRlID0gJHRlbXBsYXRlQ2FjaGUuZ2V0KHVybCk7XG5cbiAgICAgIGlmICh0ZW1wbGF0ZSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgIHJldHVybiB0ZW1wbGF0ZTtcbiAgICAgIH0gZWxzZSBpZiAoIWZldGNoUmVtb3RlVGVtcGxhdGUpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdsb2FkaW5nIGRpcmVjdGl2ZSB0ZW1wbGF0ZXMgYXN5bmNocm9ub3VzbHkgaXMgbm90IHN1cHBvcnRlZCcpO1xuICAgICAgfVxuXG4gICAgICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUsIHJlamVjdCkgPT4ge1xuICAgICAgICBjb25zdCAkaHR0cEJhY2tlbmQgPSAkaW5qZWN0b3IuZ2V0KCRIVFRQX0JBQ0tFTkQpIGFzIElIdHRwQmFja2VuZFNlcnZpY2U7XG4gICAgICAgICRodHRwQmFja2VuZCgnR0VUJywgdXJsLCBudWxsLCAoc3RhdHVzOiBudW1iZXIsIHJlc3BvbnNlOiBzdHJpbmcpID0+IHtcbiAgICAgICAgICBpZiAoc3RhdHVzID09PSAyMDApIHtcbiAgICAgICAgICAgIHJlc29sdmUoJHRlbXBsYXRlQ2FjaGUucHV0KHVybCwgcmVzcG9uc2UpKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmVqZWN0KGBHRVQgY29tcG9uZW50IHRlbXBsYXRlIGZyb20gJyR7dXJsfScgcmV0dXJuZWQgJyR7c3RhdHVzfTogJHtyZXNwb25zZX0nYCk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYERpcmVjdGl2ZSAnJHtkaXJlY3RpdmUubmFtZX0nIGlzIG5vdCBhIGNvbXBvbmVudCwgaXQgaXMgbWlzc2luZyB0ZW1wbGF0ZS5gKTtcbiAgICB9XG4gIH1cblxuICBidWlsZENvbnRyb2xsZXIoY29udHJvbGxlclR5cGU6IElDb250cm9sbGVyLCAkc2NvcGU6IElTY29wZSkge1xuICAgIC8vIFRPRE86IERvY3VtZW50IHRoYXQgd2UgZG8gbm90IHByZS1hc3NpZ24gYmluZGluZ3Mgb24gdGhlIGNvbnRyb2xsZXIgaW5zdGFuY2UuXG4gICAgLy8gUXVvdGVkIHByb3BlcnRpZXMgYmVsb3cgc28gdGhhdCB0aGlzIGNvZGUgY2FuIGJlIG9wdGltaXplZCB3aXRoIENsb3N1cmUgQ29tcGlsZXIuXG4gICAgY29uc3QgbG9jYWxzID0geyckc2NvcGUnOiAkc2NvcGUsICckZWxlbWVudCc6IHRoaXMuJGVsZW1lbnR9O1xuICAgIGNvbnN0IGNvbnRyb2xsZXIgPSB0aGlzLiRjb250cm9sbGVyKGNvbnRyb2xsZXJUeXBlLCBsb2NhbHMsIG51bGwsIHRoaXMuZGlyZWN0aXZlLmNvbnRyb2xsZXJBcyk7XG5cbiAgICB0aGlzLiRlbGVtZW50LmRhdGEgIShjb250cm9sbGVyS2V5KHRoaXMuZGlyZWN0aXZlLm5hbWUgISksIGNvbnRyb2xsZXIpO1xuXG4gICAgcmV0dXJuIGNvbnRyb2xsZXI7XG4gIH1cblxuICBjb21waWxlVGVtcGxhdGUodGVtcGxhdGU/OiBzdHJpbmcpOiBJTGlua0ZuIHtcbiAgICBpZiAodGVtcGxhdGUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgdGVtcGxhdGUgPVxuICAgICAgICAgIFVwZ3JhZGVIZWxwZXIuZ2V0VGVtcGxhdGUodGhpcy4kaW5qZWN0b3IsIHRoaXMuZGlyZWN0aXZlLCBmYWxzZSwgdGhpcy4kZWxlbWVudCkgYXMgc3RyaW5nO1xuICAgIH1cblxuICAgIHJldHVybiB0aGlzLmNvbXBpbGVIdG1sKHRlbXBsYXRlKTtcbiAgfVxuXG4gIG9uRGVzdHJveSgkc2NvcGU6IElTY29wZSwgY29udHJvbGxlckluc3RhbmNlPzogYW55KSB7XG4gICAgaWYgKGNvbnRyb2xsZXJJbnN0YW5jZSAmJiBpc0Z1bmN0aW9uKGNvbnRyb2xsZXJJbnN0YW5jZS4kb25EZXN0cm95KSkge1xuICAgICAgY29udHJvbGxlckluc3RhbmNlLiRvbkRlc3Ryb3koKTtcbiAgICB9XG4gICAgJHNjb3BlLiRkZXN0cm95KCk7XG5cbiAgICAvLyBDbGVhbiB0aGUgalF1ZXJ5L2pxTGl0ZSBkYXRhIG9uIHRoZSBjb21wb25lbnQrY2hpbGQgZWxlbWVudHMuXG4gICAgLy8gRXF1aXZlbGVudCB0byBob3cgalF1ZXJ5L2pxTGl0ZSBpbnZva2UgYGNsZWFuRGF0YWAgb24gYW4gRWxlbWVudCAodGhpcy5lbGVtZW50KVxuICAgIC8vICBodHRwczovL2dpdGh1Yi5jb20vanF1ZXJ5L2pxdWVyeS9ibG9iL2U3NDNjYmQyODU1MzI2N2Y5NTVmNzFlYTcyNDgzNzc5MTU2MTNmZDkvc3JjL21hbmlwdWxhdGlvbi5qcyNMMjIzXG4gICAgLy8gIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIuanMvYmxvYi8yNmRkYzVmODMwZjkwMmEzZDIyZjRiMmFhYjcwZDg2ZDRkNjg4YzgyL3NyYy9qcUxpdGUuanMjTDMwNi1MMzEyXG4gICAgLy8gYGNsZWFuRGF0YWAgd2lsbCBpbnZva2UgdGhlIEFuZ3VsYXJKUyBgJGRlc3Ryb3lgIERPTSBldmVudFxuICAgIC8vICBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvMjZkZGM1ZjgzMGY5MDJhM2QyMmY0YjJhYWI3MGQ4NmQ0ZDY4OGM4Mi9zcmMvQW5ndWxhci5qcyNMMTkxMS1MMTkyNFxuICAgIGFuZ3VsYXJFbGVtZW50LmNsZWFuRGF0YShbdGhpcy5lbGVtZW50XSk7XG4gICAgYW5ndWxhckVsZW1lbnQuY2xlYW5EYXRhKHRoaXMuZWxlbWVudC5xdWVyeVNlbGVjdG9yQWxsKCcqJykpO1xuICB9XG5cbiAgcHJlcGFyZVRyYW5zY2x1c2lvbigpOiBJTGlua0ZufHVuZGVmaW5lZCB7XG4gICAgY29uc3QgdHJhbnNjbHVkZSA9IHRoaXMuZGlyZWN0aXZlLnRyYW5zY2x1ZGU7XG4gICAgY29uc3QgY29udGVudENoaWxkTm9kZXMgPSB0aGlzLmV4dHJhY3RDaGlsZE5vZGVzKCk7XG4gICAgY29uc3QgYXR0YWNoQ2hpbGRyZW5GbjogSUxpbmtGbiA9IChzY29wZSwgY2xvbmVBdHRhY2hGbikgPT4ge1xuICAgICAgLy8gU2luY2UgQW5ndWxhckpTIHYxLjUuOCwgYGNsb25lQXR0YWNoRm5gIHdpbGwgdHJ5IHRvIGRlc3Ryb3kgdGhlIHRyYW5zY2x1c2lvbiBzY29wZSBpZlxuICAgICAgLy8gYCR0ZW1wbGF0ZWAgaXMgZW1wdHkuIFNpbmNlIHRoZSB0cmFuc2NsdWRlZCBjb250ZW50IGNvbWVzIGZyb20gQW5ndWxhciwgbm90IEFuZ3VsYXJKUyxcbiAgICAgIC8vIHRoZXJlIHdpbGwgYmUgbm8gdHJhbnNjbHVzaW9uIHNjb3BlIGhlcmUuXG4gICAgICAvLyBQcm92aWRlIGEgZHVtbXkgYHNjb3BlLiRkZXN0cm95KClgIG1ldGhvZCB0byBwcmV2ZW50IGBjbG9uZUF0dGFjaEZuYCBmcm9tIHRocm93aW5nLlxuICAgICAgc2NvcGUgPSBzY29wZSB8fCB7JGRlc3Ryb3k6ICgpID0+IHVuZGVmaW5lZH07XG4gICAgICByZXR1cm4gY2xvbmVBdHRhY2hGbiAhKCR0ZW1wbGF0ZSwgc2NvcGUpO1xuICAgIH07XG4gICAgbGV0ICR0ZW1wbGF0ZSA9IGNvbnRlbnRDaGlsZE5vZGVzO1xuXG4gICAgaWYgKHRyYW5zY2x1ZGUpIHtcbiAgICAgIGNvbnN0IHNsb3RzID0gT2JqZWN0LmNyZWF0ZShudWxsKTtcblxuICAgICAgaWYgKHR5cGVvZiB0cmFuc2NsdWRlID09PSAnb2JqZWN0Jykge1xuICAgICAgICAkdGVtcGxhdGUgPSBbXTtcblxuICAgICAgICBjb25zdCBzbG90TWFwID0gT2JqZWN0LmNyZWF0ZShudWxsKTtcbiAgICAgICAgY29uc3QgZmlsbGVkU2xvdHMgPSBPYmplY3QuY3JlYXRlKG51bGwpO1xuXG4gICAgICAgIC8vIFBhcnNlIHRoZSBlbGVtZW50IHNlbGVjdG9ycy5cbiAgICAgICAgT2JqZWN0LmtleXModHJhbnNjbHVkZSkuZm9yRWFjaChzbG90TmFtZSA9PiB7XG4gICAgICAgICAgbGV0IHNlbGVjdG9yID0gdHJhbnNjbHVkZVtzbG90TmFtZV07XG4gICAgICAgICAgY29uc3Qgb3B0aW9uYWwgPSBzZWxlY3Rvci5jaGFyQXQoMCkgPT09ICc/JztcbiAgICAgICAgICBzZWxlY3RvciA9IG9wdGlvbmFsID8gc2VsZWN0b3Iuc3Vic3RyaW5nKDEpIDogc2VsZWN0b3I7XG5cbiAgICAgICAgICBzbG90TWFwW3NlbGVjdG9yXSA9IHNsb3ROYW1lO1xuICAgICAgICAgIHNsb3RzW3Nsb3ROYW1lXSA9IG51bGw7ICAgICAgICAgICAgLy8gYG51bGxgOiBEZWZpbmVkIGJ1dCBub3QgeWV0IGZpbGxlZC5cbiAgICAgICAgICBmaWxsZWRTbG90c1tzbG90TmFtZV0gPSBvcHRpb25hbDsgIC8vIENvbnNpZGVyIG9wdGlvbmFsIHNsb3RzIGFzIGZpbGxlZC5cbiAgICAgICAgfSk7XG5cbiAgICAgICAgLy8gQWRkIHRoZSBtYXRjaGluZyBlbGVtZW50cyBpbnRvIHRoZWlyIHNsb3QuXG4gICAgICAgIGNvbnRlbnRDaGlsZE5vZGVzLmZvckVhY2gobm9kZSA9PiB7XG4gICAgICAgICAgY29uc3Qgc2xvdE5hbWUgPSBzbG90TWFwW2RpcmVjdGl2ZU5vcm1hbGl6ZShub2RlLm5vZGVOYW1lLnRvTG93ZXJDYXNlKCkpXTtcbiAgICAgICAgICBpZiAoc2xvdE5hbWUpIHtcbiAgICAgICAgICAgIGZpbGxlZFNsb3RzW3Nsb3ROYW1lXSA9IHRydWU7XG4gICAgICAgICAgICBzbG90c1tzbG90TmFtZV0gPSBzbG90c1tzbG90TmFtZV0gfHwgW107XG4gICAgICAgICAgICBzbG90c1tzbG90TmFtZV0ucHVzaChub2RlKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgJHRlbXBsYXRlLnB1c2gobm9kZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICAvLyBDaGVjayBmb3IgcmVxdWlyZWQgc2xvdHMgdGhhdCB3ZXJlIG5vdCBmaWxsZWQuXG4gICAgICAgIE9iamVjdC5rZXlzKGZpbGxlZFNsb3RzKS5mb3JFYWNoKHNsb3ROYW1lID0+IHtcbiAgICAgICAgICBpZiAoIWZpbGxlZFNsb3RzW3Nsb3ROYW1lXSkge1xuICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBSZXF1aXJlZCB0cmFuc2NsdXNpb24gc2xvdCAnJHtzbG90TmFtZX0nIG9uIGRpcmVjdGl2ZTogJHt0aGlzLm5hbWV9YCk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICBPYmplY3Qua2V5cyhzbG90cykuZmlsdGVyKHNsb3ROYW1lID0+IHNsb3RzW3Nsb3ROYW1lXSkuZm9yRWFjaChzbG90TmFtZSA9PiB7XG4gICAgICAgICAgY29uc3Qgbm9kZXMgPSBzbG90c1tzbG90TmFtZV07XG4gICAgICAgICAgc2xvdHNbc2xvdE5hbWVdID0gKHNjb3BlOiBJU2NvcGUsIGNsb25lQXR0YWNoOiBJQ2xvbmVBdHRhY2hGdW5jdGlvbikgPT4ge1xuICAgICAgICAgICAgcmV0dXJuIGNsb25lQXR0YWNoICEobm9kZXMsIHNjb3BlKTtcbiAgICAgICAgICB9O1xuICAgICAgICB9KTtcbiAgICAgIH1cblxuICAgICAgLy8gQXR0YWNoIGAkJHNsb3RzYCB0byBkZWZhdWx0IHNsb3QgdHJhbnNjbHVkZSBmbi5cbiAgICAgIGF0dGFjaENoaWxkcmVuRm4uJCRzbG90cyA9IHNsb3RzO1xuXG4gICAgICAvLyBBbmd1bGFySlMgdjEuNisgaWdub3JlcyBlbXB0eSBvciB3aGl0ZXNwYWNlLW9ubHkgdHJhbnNjbHVkZWQgdGV4dCBub2Rlcy4gQnV0IEFuZ3VsYXJcbiAgICAgIC8vIHJlbW92ZXMgYWxsIHRleHQgY29udGVudCBhZnRlciB0aGUgZmlyc3QgaW50ZXJwb2xhdGlvbiBhbmQgdXBkYXRlcyBpdCBsYXRlciwgYWZ0ZXJcbiAgICAgIC8vIGV2YWx1YXRpbmcgdGhlIGV4cHJlc3Npb25zLiBUaGlzIHdvdWxkIHJlc3VsdCBpbiBBbmd1bGFySlMgZmFpbGluZyB0byByZWNvZ25pemUgdGV4dFxuICAgICAgLy8gbm9kZXMgdGhhdCBzdGFydCB3aXRoIGFuIGludGVycG9sYXRpb24gYXMgdHJhbnNjbHVkZWQgY29udGVudCBhbmQgdXNlIHRoZSBmYWxsYmFja1xuICAgICAgLy8gY29udGVudCBpbnN0ZWFkLlxuICAgICAgLy8gVG8gYXZvaWQgdGhpcyBpc3N1ZSwgd2UgYWRkIGFcbiAgICAgIC8vIFt6ZXJvLXdpZHRoIG5vbi1qb2luZXIgY2hhcmFjdGVyXShodHRwczovL2VuLndpa2lwZWRpYS5vcmcvd2lraS9aZXJvLXdpZHRoX25vbi1qb2luZXIpXG4gICAgICAvLyB0byBlbXB0eSB0ZXh0IG5vZGVzICh3aGljaCBjYW4gb25seSBiZSBhIHJlc3VsdCBvZiBBbmd1bGFyIHJlbW92aW5nIHRoZWlyIGluaXRpYWwgY29udGVudCkuXG4gICAgICAvLyBOT1RFOiBUcmFuc2NsdWRlZCB0ZXh0IGNvbnRlbnQgdGhhdCBzdGFydHMgd2l0aCB3aGl0ZXNwYWNlIGZvbGxvd2VkIGJ5IGFuIGludGVycG9sYXRpb25cbiAgICAgIC8vICAgICAgIHdpbGwgc3RpbGwgZmFpbCB0byBiZSBkZXRlY3RlZCBieSBBbmd1bGFySlMgdjEuNitcbiAgICAgICR0ZW1wbGF0ZS5mb3JFYWNoKG5vZGUgPT4ge1xuICAgICAgICBpZiAobm9kZS5ub2RlVHlwZSA9PT0gTm9kZS5URVhUX05PREUgJiYgIW5vZGUubm9kZVZhbHVlKSB7XG4gICAgICAgICAgbm9kZS5ub2RlVmFsdWUgPSAnXFx1MjAwQyc7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBhdHRhY2hDaGlsZHJlbkZuO1xuICB9XG5cbiAgcmVzb2x2ZUFuZEJpbmRSZXF1aXJlZENvbnRyb2xsZXJzKGNvbnRyb2xsZXJJbnN0YW5jZTogSUNvbnRyb2xsZXJJbnN0YW5jZXxudWxsKSB7XG4gICAgY29uc3QgZGlyZWN0aXZlUmVxdWlyZSA9IHRoaXMuZ2V0RGlyZWN0aXZlUmVxdWlyZSgpO1xuICAgIGNvbnN0IHJlcXVpcmVkQ29udHJvbGxlcnMgPSB0aGlzLnJlc29sdmVSZXF1aXJlKGRpcmVjdGl2ZVJlcXVpcmUpO1xuXG4gICAgaWYgKGNvbnRyb2xsZXJJbnN0YW5jZSAmJiB0aGlzLmRpcmVjdGl2ZS5iaW5kVG9Db250cm9sbGVyICYmIGlzTWFwKGRpcmVjdGl2ZVJlcXVpcmUpKSB7XG4gICAgICBjb25zdCByZXF1aXJlZENvbnRyb2xsZXJzTWFwID0gcmVxdWlyZWRDb250cm9sbGVycyBhc3tba2V5OiBzdHJpbmddOiBJQ29udHJvbGxlckluc3RhbmNlfTtcbiAgICAgIE9iamVjdC5rZXlzKHJlcXVpcmVkQ29udHJvbGxlcnNNYXApLmZvckVhY2goa2V5ID0+IHtcbiAgICAgICAgY29udHJvbGxlckluc3RhbmNlW2tleV0gPSByZXF1aXJlZENvbnRyb2xsZXJzTWFwW2tleV07XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVxdWlyZWRDb250cm9sbGVycztcbiAgfVxuXG4gIHByaXZhdGUgY29tcGlsZUh0bWwoaHRtbDogc3RyaW5nKTogSUxpbmtGbiB7XG4gICAgdGhpcy5lbGVtZW50LmlubmVySFRNTCA9IGh0bWw7XG4gICAgcmV0dXJuIHRoaXMuJGNvbXBpbGUodGhpcy5lbGVtZW50LmNoaWxkTm9kZXMpO1xuICB9XG5cbiAgcHJpdmF0ZSBleHRyYWN0Q2hpbGROb2RlcygpOiBOb2RlW10ge1xuICAgIGNvbnN0IGNoaWxkTm9kZXM6IE5vZGVbXSA9IFtdO1xuICAgIGxldCBjaGlsZE5vZGU6IE5vZGV8bnVsbDtcblxuICAgIHdoaWxlIChjaGlsZE5vZGUgPSB0aGlzLmVsZW1lbnQuZmlyc3RDaGlsZCkge1xuICAgICAgdGhpcy5lbGVtZW50LnJlbW92ZUNoaWxkKGNoaWxkTm9kZSk7XG4gICAgICBjaGlsZE5vZGVzLnB1c2goY2hpbGROb2RlKTtcbiAgICB9XG5cbiAgICByZXR1cm4gY2hpbGROb2RlcztcbiAgfVxuXG4gIHByaXZhdGUgZ2V0RGlyZWN0aXZlUmVxdWlyZSgpOiBEaXJlY3RpdmVSZXF1aXJlUHJvcGVydHkge1xuICAgIGNvbnN0IHJlcXVpcmUgPSB0aGlzLmRpcmVjdGl2ZS5yZXF1aXJlIHx8ICh0aGlzLmRpcmVjdGl2ZS5jb250cm9sbGVyICYmIHRoaXMuZGlyZWN0aXZlLm5hbWUpICE7XG5cbiAgICBpZiAoaXNNYXAocmVxdWlyZSkpIHtcbiAgICAgIE9iamVjdC5rZXlzKHJlcXVpcmUpLmZvckVhY2goa2V5ID0+IHtcbiAgICAgICAgY29uc3QgdmFsdWUgPSByZXF1aXJlW2tleV07XG4gICAgICAgIGNvbnN0IG1hdGNoID0gdmFsdWUubWF0Y2goUkVRVUlSRV9QUkVGSVhfUkUpICE7XG4gICAgICAgIGNvbnN0IG5hbWUgPSB2YWx1ZS5zdWJzdHJpbmcobWF0Y2hbMF0ubGVuZ3RoKTtcblxuICAgICAgICBpZiAoIW5hbWUpIHtcbiAgICAgICAgICByZXF1aXJlW2tleV0gPSBtYXRjaFswXSArIGtleTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlcXVpcmU7XG4gIH1cblxuICBwcml2YXRlIHJlc29sdmVSZXF1aXJlKHJlcXVpcmU6IERpcmVjdGl2ZVJlcXVpcmVQcm9wZXJ0eSwgY29udHJvbGxlckluc3RhbmNlPzogYW55KTpcbiAgICAgIFNpbmdsZU9yTGlzdE9yTWFwPElDb250cm9sbGVySW5zdGFuY2U+fG51bGwge1xuICAgIGlmICghcmVxdWlyZSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfSBlbHNlIGlmIChBcnJheS5pc0FycmF5KHJlcXVpcmUpKSB7XG4gICAgICByZXR1cm4gcmVxdWlyZS5tYXAocmVxID0+IHRoaXMucmVzb2x2ZVJlcXVpcmUocmVxKSk7XG4gICAgfSBlbHNlIGlmICh0eXBlb2YgcmVxdWlyZSA9PT0gJ29iamVjdCcpIHtcbiAgICAgIGNvbnN0IHZhbHVlOiB7W2tleTogc3RyaW5nXTogSUNvbnRyb2xsZXJJbnN0YW5jZX0gPSB7fTtcbiAgICAgIE9iamVjdC5rZXlzKHJlcXVpcmUpLmZvckVhY2goa2V5ID0+IHZhbHVlW2tleV0gPSB0aGlzLnJlc29sdmVSZXF1aXJlKHJlcXVpcmVba2V5XSkgISk7XG4gICAgICByZXR1cm4gdmFsdWU7XG4gICAgfSBlbHNlIGlmICh0eXBlb2YgcmVxdWlyZSA9PT0gJ3N0cmluZycpIHtcbiAgICAgIGNvbnN0IG1hdGNoID0gcmVxdWlyZS5tYXRjaChSRVFVSVJFX1BSRUZJWF9SRSkgITtcbiAgICAgIGNvbnN0IGluaGVyaXRUeXBlID0gbWF0Y2hbMV0gfHwgbWF0Y2hbM107XG5cbiAgICAgIGNvbnN0IG5hbWUgPSByZXF1aXJlLnN1YnN0cmluZyhtYXRjaFswXS5sZW5ndGgpO1xuICAgICAgY29uc3QgaXNPcHRpb25hbCA9ICEhbWF0Y2hbMl07XG4gICAgICBjb25zdCBzZWFyY2hQYXJlbnRzID0gISFpbmhlcml0VHlwZTtcbiAgICAgIGNvbnN0IHN0YXJ0T25QYXJlbnQgPSBpbmhlcml0VHlwZSA9PT0gJ15eJztcblxuICAgICAgY29uc3QgY3RybEtleSA9IGNvbnRyb2xsZXJLZXkobmFtZSk7XG4gICAgICBjb25zdCBlbGVtID0gc3RhcnRPblBhcmVudCA/IHRoaXMuJGVsZW1lbnQucGFyZW50ICEoKSA6IHRoaXMuJGVsZW1lbnQ7XG4gICAgICBjb25zdCB2YWx1ZSA9IHNlYXJjaFBhcmVudHMgPyBlbGVtLmluaGVyaXRlZERhdGEgIShjdHJsS2V5KSA6IGVsZW0uZGF0YSAhKGN0cmxLZXkpO1xuXG4gICAgICBpZiAoIXZhbHVlICYmICFpc09wdGlvbmFsKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICAgIGBVbmFibGUgdG8gZmluZCByZXF1aXJlZCAnJHtyZXF1aXJlfScgaW4gdXBncmFkZWQgZGlyZWN0aXZlICcke3RoaXMubmFtZX0nLmApO1xuICAgICAgfVxuXG4gICAgICByZXR1cm4gdmFsdWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgVW5yZWNvZ25pemVkICdyZXF1aXJlJyBzeW50YXggb24gdXBncmFkZWQgZGlyZWN0aXZlICcke3RoaXMubmFtZX0nOiAke3JlcXVpcmV9YCk7XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIGdldE9yQ2FsbDxUPihwcm9wZXJ0eTogVCB8IEZ1bmN0aW9uLCAuLi5hcmdzOiBhbnlbXSk6IFQge1xuICByZXR1cm4gaXNGdW5jdGlvbihwcm9wZXJ0eSkgPyBwcm9wZXJ0eSguLi5hcmdzKSA6IHByb3BlcnR5O1xufVxuXG4vLyBOT1RFOiBPbmx5IHdvcmtzIGZvciBgdHlwZW9mIFQgIT09ICdvYmplY3QnYC5cbmZ1bmN0aW9uIGlzTWFwPFQ+KHZhbHVlOiBTaW5nbGVPckxpc3RPck1hcDxUPik6IHZhbHVlIGlzIHtba2V5OiBzdHJpbmddOiBUfSB7XG4gIHJldHVybiB2YWx1ZSAmJiAhQXJyYXkuaXNBcnJheSh2YWx1ZSkgJiYgdHlwZW9mIHZhbHVlID09PSAnb2JqZWN0Jztcbn1cblxuZnVuY3Rpb24gbm90U3VwcG9ydGVkKG5hbWU6IHN0cmluZywgZmVhdHVyZTogc3RyaW5nKSB7XG4gIHRocm93IG5ldyBFcnJvcihgVXBncmFkZWQgZGlyZWN0aXZlICcke25hbWV9JyBjb250YWlucyB1bnN1cHBvcnRlZCBmZWF0dXJlOiAnJHtmZWF0dXJlfScuYCk7XG59XG4iXX0=