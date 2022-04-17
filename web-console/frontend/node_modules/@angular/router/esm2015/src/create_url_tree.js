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
import { PRIMARY_OUTLET } from './shared';
import { UrlSegment, UrlSegmentGroup, UrlTree } from './url_tree';
import { forEach, last, shallowEqual } from './utils/collection';
/**
 * @param {?} route
 * @param {?} urlTree
 * @param {?} commands
 * @param {?} queryParams
 * @param {?} fragment
 * @return {?}
 */
export function createUrlTree(route, urlTree, commands, queryParams, fragment) {
    if (commands.length === 0) {
        return tree(urlTree.root, urlTree.root, urlTree, queryParams, fragment);
    }
    /** @type {?} */
    const nav = computeNavigation(commands);
    if (nav.toRoot()) {
        return tree(urlTree.root, new UrlSegmentGroup([], {}), urlTree, queryParams, fragment);
    }
    /** @type {?} */
    const startingPosition = findStartingPosition(nav, urlTree, route);
    /** @type {?} */
    const segmentGroup = startingPosition.processChildren ?
        updateSegmentGroupChildren(startingPosition.segmentGroup, startingPosition.index, nav.commands) :
        updateSegmentGroup(startingPosition.segmentGroup, startingPosition.index, nav.commands);
    return tree(startingPosition.segmentGroup, segmentGroup, urlTree, queryParams, fragment);
}
/**
 * @param {?} command
 * @return {?}
 */
function isMatrixParams(command) {
    return typeof command === 'object' && command != null && !command.outlets && !command.segmentPath;
}
/**
 * @param {?} oldSegmentGroup
 * @param {?} newSegmentGroup
 * @param {?} urlTree
 * @param {?} queryParams
 * @param {?} fragment
 * @return {?}
 */
function tree(oldSegmentGroup, newSegmentGroup, urlTree, queryParams, fragment) {
    /** @type {?} */
    let qp = {};
    if (queryParams) {
        forEach(queryParams, (/**
         * @param {?} value
         * @param {?} name
         * @return {?}
         */
        (value, name) => {
            qp[name] = Array.isArray(value) ? value.map((/**
             * @param {?} v
             * @return {?}
             */
            (v) => `${v}`)) : `${value}`;
        }));
    }
    if (urlTree.root === oldSegmentGroup) {
        return new UrlTree(newSegmentGroup, qp, fragment);
    }
    return new UrlTree(replaceSegment(urlTree.root, oldSegmentGroup, newSegmentGroup), qp, fragment);
}
/**
 * @param {?} current
 * @param {?} oldSegment
 * @param {?} newSegment
 * @return {?}
 */
function replaceSegment(current, oldSegment, newSegment) {
    /** @type {?} */
    const children = {};
    forEach(current.children, (/**
     * @param {?} c
     * @param {?} outletName
     * @return {?}
     */
    (c, outletName) => {
        if (c === oldSegment) {
            children[outletName] = newSegment;
        }
        else {
            children[outletName] = replaceSegment(c, oldSegment, newSegment);
        }
    }));
    return new UrlSegmentGroup(current.segments, children);
}
class Navigation {
    /**
     * @param {?} isAbsolute
     * @param {?} numberOfDoubleDots
     * @param {?} commands
     */
    constructor(isAbsolute, numberOfDoubleDots, commands) {
        this.isAbsolute = isAbsolute;
        this.numberOfDoubleDots = numberOfDoubleDots;
        this.commands = commands;
        if (isAbsolute && commands.length > 0 && isMatrixParams(commands[0])) {
            throw new Error('Root segment cannot have matrix parameters');
        }
        /** @type {?} */
        const cmdWithOutlet = commands.find((/**
         * @param {?} c
         * @return {?}
         */
        c => typeof c === 'object' && c != null && c.outlets));
        if (cmdWithOutlet && cmdWithOutlet !== last(commands)) {
            throw new Error('{outlets:{}} has to be the last command');
        }
    }
    /**
     * @return {?}
     */
    toRoot() {
        return this.isAbsolute && this.commands.length === 1 && this.commands[0] == '/';
    }
}
if (false) {
    /** @type {?} */
    Navigation.prototype.isAbsolute;
    /** @type {?} */
    Navigation.prototype.numberOfDoubleDots;
    /** @type {?} */
    Navigation.prototype.commands;
}
/**
 * Transforms commands to a normalized `Navigation`
 * @param {?} commands
 * @return {?}
 */
function computeNavigation(commands) {
    if ((typeof commands[0] === 'string') && commands.length === 1 && commands[0] === '/') {
        return new Navigation(true, 0, commands);
    }
    /** @type {?} */
    let numberOfDoubleDots = 0;
    /** @type {?} */
    let isAbsolute = false;
    /** @type {?} */
    const res = commands.reduce((/**
     * @param {?} res
     * @param {?} cmd
     * @param {?} cmdIdx
     * @return {?}
     */
    (res, cmd, cmdIdx) => {
        if (typeof cmd === 'object' && cmd != null) {
            if (cmd.outlets) {
                /** @type {?} */
                const outlets = {};
                forEach(cmd.outlets, (/**
                 * @param {?} commands
                 * @param {?} name
                 * @return {?}
                 */
                (commands, name) => {
                    outlets[name] = typeof commands === 'string' ? commands.split('/') : commands;
                }));
                return [...res, { outlets }];
            }
            if (cmd.segmentPath) {
                return [...res, cmd.segmentPath];
            }
        }
        if (!(typeof cmd === 'string')) {
            return [...res, cmd];
        }
        if (cmdIdx === 0) {
            cmd.split('/').forEach((/**
             * @param {?} urlPart
             * @param {?} partIndex
             * @return {?}
             */
            (urlPart, partIndex) => {
                if (partIndex == 0 && urlPart === '.') {
                    // skip './a'
                }
                else if (partIndex == 0 && urlPart === '') { //  '/a'
                    isAbsolute = true;
                }
                else if (urlPart === '..') { //  '../a'
                    numberOfDoubleDots++;
                }
                else if (urlPart != '') {
                    res.push(urlPart);
                }
            }));
            return res;
        }
        return [...res, cmd];
    }), []);
    return new Navigation(isAbsolute, numberOfDoubleDots, res);
}
class Position {
    /**
     * @param {?} segmentGroup
     * @param {?} processChildren
     * @param {?} index
     */
    constructor(segmentGroup, processChildren, index) {
        this.segmentGroup = segmentGroup;
        this.processChildren = processChildren;
        this.index = index;
    }
}
if (false) {
    /** @type {?} */
    Position.prototype.segmentGroup;
    /** @type {?} */
    Position.prototype.processChildren;
    /** @type {?} */
    Position.prototype.index;
}
/**
 * @param {?} nav
 * @param {?} tree
 * @param {?} route
 * @return {?}
 */
function findStartingPosition(nav, tree, route) {
    if (nav.isAbsolute) {
        return new Position(tree.root, true, 0);
    }
    if (route.snapshot._lastPathIndex === -1) {
        return new Position(route.snapshot._urlSegment, true, 0);
    }
    /** @type {?} */
    const modifier = isMatrixParams(nav.commands[0]) ? 0 : 1;
    /** @type {?} */
    const index = route.snapshot._lastPathIndex + modifier;
    return createPositionApplyingDoubleDots(route.snapshot._urlSegment, index, nav.numberOfDoubleDots);
}
/**
 * @param {?} group
 * @param {?} index
 * @param {?} numberOfDoubleDots
 * @return {?}
 */
function createPositionApplyingDoubleDots(group, index, numberOfDoubleDots) {
    /** @type {?} */
    let g = group;
    /** @type {?} */
    let ci = index;
    /** @type {?} */
    let dd = numberOfDoubleDots;
    while (dd > ci) {
        dd -= ci;
        g = (/** @type {?} */ (g.parent));
        if (!g) {
            throw new Error('Invalid number of \'../\'');
        }
        ci = g.segments.length;
    }
    return new Position(g, false, ci - dd);
}
/**
 * @param {?} command
 * @return {?}
 */
function getPath(command) {
    if (typeof command === 'object' && command != null && command.outlets) {
        return command.outlets[PRIMARY_OUTLET];
    }
    return `${command}`;
}
/**
 * @param {?} commands
 * @return {?}
 */
function getOutlets(commands) {
    if (!(typeof commands[0] === 'object'))
        return { [PRIMARY_OUTLET]: commands };
    if (commands[0].outlets === undefined)
        return { [PRIMARY_OUTLET]: commands };
    return commands[0].outlets;
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function updateSegmentGroup(segmentGroup, startIndex, commands) {
    if (!segmentGroup) {
        segmentGroup = new UrlSegmentGroup([], {});
    }
    if (segmentGroup.segments.length === 0 && segmentGroup.hasChildren()) {
        return updateSegmentGroupChildren(segmentGroup, startIndex, commands);
    }
    /** @type {?} */
    const m = prefixedWith(segmentGroup, startIndex, commands);
    /** @type {?} */
    const slicedCommands = commands.slice(m.commandIndex);
    if (m.match && m.pathIndex < segmentGroup.segments.length) {
        /** @type {?} */
        const g = new UrlSegmentGroup(segmentGroup.segments.slice(0, m.pathIndex), {});
        g.children[PRIMARY_OUTLET] =
            new UrlSegmentGroup(segmentGroup.segments.slice(m.pathIndex), segmentGroup.children);
        return updateSegmentGroupChildren(g, 0, slicedCommands);
    }
    else if (m.match && slicedCommands.length === 0) {
        return new UrlSegmentGroup(segmentGroup.segments, {});
    }
    else if (m.match && !segmentGroup.hasChildren()) {
        return createNewSegmentGroup(segmentGroup, startIndex, commands);
    }
    else if (m.match) {
        return updateSegmentGroupChildren(segmentGroup, 0, slicedCommands);
    }
    else {
        return createNewSegmentGroup(segmentGroup, startIndex, commands);
    }
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function updateSegmentGroupChildren(segmentGroup, startIndex, commands) {
    if (commands.length === 0) {
        return new UrlSegmentGroup(segmentGroup.segments, {});
    }
    else {
        /** @type {?} */
        const outlets = getOutlets(commands);
        /** @type {?} */
        const children = {};
        forEach(outlets, (/**
         * @param {?} commands
         * @param {?} outlet
         * @return {?}
         */
        (commands, outlet) => {
            if (commands !== null) {
                children[outlet] = updateSegmentGroup(segmentGroup.children[outlet], startIndex, commands);
            }
        }));
        forEach(segmentGroup.children, (/**
         * @param {?} child
         * @param {?} childOutlet
         * @return {?}
         */
        (child, childOutlet) => {
            if (outlets[childOutlet] === undefined) {
                children[childOutlet] = child;
            }
        }));
        return new UrlSegmentGroup(segmentGroup.segments, children);
    }
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function prefixedWith(segmentGroup, startIndex, commands) {
    /** @type {?} */
    let currentCommandIndex = 0;
    /** @type {?} */
    let currentPathIndex = startIndex;
    /** @type {?} */
    const noMatch = { match: false, pathIndex: 0, commandIndex: 0 };
    while (currentPathIndex < segmentGroup.segments.length) {
        if (currentCommandIndex >= commands.length)
            return noMatch;
        /** @type {?} */
        const path = segmentGroup.segments[currentPathIndex];
        /** @type {?} */
        const curr = getPath(commands[currentCommandIndex]);
        /** @type {?} */
        const next = currentCommandIndex < commands.length - 1 ? commands[currentCommandIndex + 1] : null;
        if (currentPathIndex > 0 && curr === undefined)
            break;
        if (curr && next && (typeof next === 'object') && next.outlets === undefined) {
            if (!compare(curr, next, path))
                return noMatch;
            currentCommandIndex += 2;
        }
        else {
            if (!compare(curr, {}, path))
                return noMatch;
            currentCommandIndex++;
        }
        currentPathIndex++;
    }
    return { match: true, pathIndex: currentPathIndex, commandIndex: currentCommandIndex };
}
/**
 * @param {?} segmentGroup
 * @param {?} startIndex
 * @param {?} commands
 * @return {?}
 */
function createNewSegmentGroup(segmentGroup, startIndex, commands) {
    /** @type {?} */
    const paths = segmentGroup.segments.slice(0, startIndex);
    /** @type {?} */
    let i = 0;
    while (i < commands.length) {
        if (typeof commands[i] === 'object' && commands[i].outlets !== undefined) {
            /** @type {?} */
            const children = createNewSegmentChildren(commands[i].outlets);
            return new UrlSegmentGroup(paths, children);
        }
        // if we start with an object literal, we need to reuse the path part from the segment
        if (i === 0 && isMatrixParams(commands[0])) {
            /** @type {?} */
            const p = segmentGroup.segments[startIndex];
            paths.push(new UrlSegment(p.path, commands[0]));
            i++;
            continue;
        }
        /** @type {?} */
        const curr = getPath(commands[i]);
        /** @type {?} */
        const next = (i < commands.length - 1) ? commands[i + 1] : null;
        if (curr && next && isMatrixParams(next)) {
            paths.push(new UrlSegment(curr, stringify(next)));
            i += 2;
        }
        else {
            paths.push(new UrlSegment(curr, {}));
            i++;
        }
    }
    return new UrlSegmentGroup(paths, {});
}
/**
 * @param {?} outlets
 * @return {?}
 */
function createNewSegmentChildren(outlets) {
    /** @type {?} */
    const children = {};
    forEach(outlets, (/**
     * @param {?} commands
     * @param {?} outlet
     * @return {?}
     */
    (commands, outlet) => {
        if (commands !== null) {
            children[outlet] = createNewSegmentGroup(new UrlSegmentGroup([], {}), 0, commands);
        }
    }));
    return children;
}
/**
 * @param {?} params
 * @return {?}
 */
function stringify(params) {
    /** @type {?} */
    const res = {};
    forEach(params, (/**
     * @param {?} v
     * @param {?} k
     * @return {?}
     */
    (v, k) => res[k] = `${v}`));
    return res;
}
/**
 * @param {?} path
 * @param {?} params
 * @param {?} segment
 * @return {?}
 */
function compare(path, params, segment) {
    return path == segment.path && shallowEqual(params, segment.parameters);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3JlYXRlX3VybF90cmVlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9jcmVhdGVfdXJsX3RyZWUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFTQSxPQUFPLEVBQUMsY0FBYyxFQUFTLE1BQU0sVUFBVSxDQUFDO0FBQ2hELE9BQU8sRUFBQyxVQUFVLEVBQUUsZUFBZSxFQUFFLE9BQU8sRUFBQyxNQUFNLFlBQVksQ0FBQztBQUNoRSxPQUFPLEVBQUMsT0FBTyxFQUFFLElBQUksRUFBRSxZQUFZLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQzs7Ozs7Ozs7O0FBRS9ELE1BQU0sVUFBVSxhQUFhLENBQ3pCLEtBQXFCLEVBQUUsT0FBZ0IsRUFBRSxRQUFlLEVBQUUsV0FBbUIsRUFDN0UsUUFBZ0I7SUFDbEIsSUFBSSxRQUFRLENBQUMsTUFBTSxLQUFLLENBQUMsRUFBRTtRQUN6QixPQUFPLElBQUksQ0FBQyxPQUFPLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxJQUFJLEVBQUUsT0FBTyxFQUFFLFdBQVcsRUFBRSxRQUFRLENBQUMsQ0FBQztLQUN6RTs7VUFFSyxHQUFHLEdBQUcsaUJBQWlCLENBQUMsUUFBUSxDQUFDO0lBRXZDLElBQUksR0FBRyxDQUFDLE1BQU0sRUFBRSxFQUFFO1FBQ2hCLE9BQU8sSUFBSSxDQUFDLE9BQU8sQ0FBQyxJQUFJLEVBQUUsSUFBSSxlQUFlLENBQUMsRUFBRSxFQUFFLEVBQUUsQ0FBQyxFQUFFLE9BQU8sRUFBRSxXQUFXLEVBQUUsUUFBUSxDQUFDLENBQUM7S0FDeEY7O1VBRUssZ0JBQWdCLEdBQUcsb0JBQW9CLENBQUMsR0FBRyxFQUFFLE9BQU8sRUFBRSxLQUFLLENBQUM7O1VBRTVELFlBQVksR0FBRyxnQkFBZ0IsQ0FBQyxlQUFlLENBQUMsQ0FBQztRQUNuRCwwQkFBMEIsQ0FDdEIsZ0JBQWdCLENBQUMsWUFBWSxFQUFFLGdCQUFn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LEdBQUcsR0FBRyxDQUFDLEVBQUUsRUFBQyxDQUFDO0lBQ3hELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQzs7Ozs7OztBQUVELFNBQVMsT0FBTyxDQUFDLElBQVksRUFBRSxNQUE0QixFQUFFLE9BQW1CO0lBQzlFLE9BQU8sSUFBSSxJQUFJLE9BQU8sQ0FBQyxJQUFJLElBQUksWUFBWSxDQUFDLE1BQU0sRUFBRSxPQUFPLENBQUMsVUFBVSxDQUFDLENBQUM7QUFDMUUsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtBY3RpdmF0ZWRSb3V0ZX0gZnJvbSAnLi9yb3V0ZXJfc3RhdGUnO1xuaW1wb3J0IHtQUklNQVJZX09VVExFVCwgUGFyYW1zfSBmcm9tICcuL3NoYXJlZCc7XG5pbXBvcnQge1VybFNlZ21lbnQsIFVybFNlZ21lbnRHcm91cCwgVXJsVHJlZX0gZnJvbSAnLi91cmxfdHJlZSc7XG5pbXBvcnQge2ZvckVhY2gsIGxhc3QsIHNoYWxsb3dFcXVhbH0gZnJvbSAnLi91dGlscy9jb2xsZWN0aW9uJztcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVVybFRyZWUoXG4gICAgcm91dGU6IEFjdGl2YXRlZFJvdXRlLCB1cmxUcmVlOiBVcmxUcmVlLCBjb21tYW5kczogYW55W10sIHF1ZXJ5UGFyYW1zOiBQYXJhbXMsXG4gICAgZnJhZ21lbnQ6IHN0cmluZyk6IFVybFRyZWUge1xuICBpZiAoY29tbWFuZHMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHRyZWUodXJsVHJlZS5yb290LCB1cmxUcmVlLnJvb3QsIHVybFRyZWUsIHF1ZXJ5UGFyYW1zLCBmcmFnbWVudCk7XG4gIH1cblxuICBjb25zdCBuYXYgPSBjb21wdXRlTmF2aWdhdGlvbihjb21tYW5kcyk7XG5cbiAgaWYgKG5hdi50b1Jvb3QoKSkge1xuICAgIHJldHVybiB0cmVlKHVybFRyZWUucm9vdCwgbmV3IFVybFNlZ21lbnRHcm91cChbXSwge30pLCB1cmxUcmVlLCBxdWVyeVBhcmFtcywgZnJhZ21lbnQpO1xuICB9XG5cbiAgY29uc3Qgc3RhcnRpbmdQb3NpdGlvbiA9IGZpbmRTdGFydGluZ1Bvc2l0aW9uKG5hdiwgdXJsVHJlZSwgcm91dGUpO1xuXG4gIGNvbnN0IHNlZ21lbnRHcm91cCA9IHN0YXJ0aW5nUG9zaXRpb24ucHJvY2Vzc0NoaWxkcmVuID9cbiAgICAgIHVwZGF0ZVNlZ21lbnRHcm91cENoaWxkcmVuKFxuICAgICAgICAgIHN0YXJ0aW5nUG9zaXRpb24uc2VnbWVudEdyb3VwLCBzdGFydGluZ1Bvc2l0aW9uLmluZGV4LCBuYXYuY29tbWFuZHMpIDpcbiAgICAgIHVwZGF0ZVNlZ21lbnRHcm91cChzdGFydGluZ1Bvc2l0aW9uLnNlZ21lbnRHcm91cCwgc3RhcnRpbmdQb3NpdGlvbi5pbmRleCwgbmF2LmNvbW1hbmRzKTtcbiAgcmV0dXJuIHRyZWUoc3RhcnRpbmdQb3NpdGlvbi5zZWdtZW50R3JvdXAsIHNlZ21lbnRHcm91cCwgdXJsVHJlZSwgcXVlcnlQYXJhbXMsIGZyYWdtZW50KTtcbn1cblxuZnVuY3Rpb24gaXNNYXRyaXhQYXJhbXMoY29tbWFuZDogYW55KTogYm9vbGVhbiB7XG4gIHJldHVybiB0eXBlb2YgY29tbWFuZCA9PT0gJ29iamVjdCcgJiYgY29tbWFuZCAhPSBudWxsICYmICFjb21tYW5kLm91dGxldHMgJiYgIWNvbW1hbmQuc2VnbWVudFBhdGg7XG59XG5cbmZ1bmN0aW9uIHRyZWUoXG4gICAgb2xkU2VnbWVudEdyb3VwOiBVcmxTZWdtZW50R3JvdXAsIG5ld1NlZ21lbnRHcm91cDogVXJsU2VnbWVudEdyb3VwLCB1cmxUcmVlOiBVcmxUcmVlLFxuICAgIHF1ZXJ5UGFyYW1zOiBQYXJhbXMsIGZyYWdtZW50OiBzdHJpbmcpOiBVcmxUcmVlIHtcbiAgbGV0IHFwOiBhbnkgPSB7fTtcbiAgaWYgKHF1ZXJ5UGFyYW1zKSB7XG4gICAgZm9yRWFjaChxdWVyeVBhcmFtcywgKHZhbHVlOiBhbnksIG5hbWU6IGFueSkgPT4ge1xuICAgICAgcXBbbmFtZV0gPSBBcnJheS5pc0FycmF5KHZhbHVlKSA/IHZhbHVlLm1hcCgodjogYW55KSA9PiBgJHt2fWApIDogYCR7dmFsdWV9YDtcbiAgICB9KTtcbiAgfVxuXG4gIGlmICh1cmxUcmVlLnJvb3QgPT09IG9sZFNlZ21lbnRHcm91cCkge1xuICAgIHJldHVybiBuZXcgVXJsVHJlZShuZXdTZWdtZW50R3JvdXAsIHFwLCBmcmFnbWVudCk7XG4gIH1cblxuICByZXR1cm4gbmV3IFVybFRyZWUocmVwbGFjZVNlZ21lbnQodXJsVHJlZS5yb290LCBvbGRTZWdtZW50R3JvdXAsIG5ld1NlZ21lbnRHcm91cCksIHFwLCBmcmFnbWVudCk7XG59XG5cbmZ1bmN0aW9uIHJlcGxhY2VTZWdtZW50KFxuICAgIGN1cnJlbnQ6IFVybFNlZ21lbnRHcm91cCwgb2xkU2VnbWVudDogVXJsU2VnbWVudEdyb3VwLFxuICAgIG5ld1NlZ21lbnQ6IFVybFNlZ21lbnRHcm91cCk6IFVybFNlZ21lbnRHcm91cCB7XG4gIGNvbnN0IGNoaWxkcmVuOiB7W2tleTogc3RyaW5nXTogVXJsU2VnbWVudEdyb3VwfSA9IHt9O1xuICBmb3JFYWNoKGN1cnJlbnQuY2hpbGRyZW4sIChjOiBVcmxTZWdtZW50R3JvdXAsIG91dGxldE5hbWU6IHN0cmluZykgPT4ge1xuICAgIGlmIChjID09PSBvbGRTZWdtZW50KSB7XG4gICAgICBjaGlsZHJlbltvdXRsZXROYW1lXSA9IG5ld1NlZ21lbnQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNoaWxkcmVuW291dGxldE5hbWVdID0gcmVwbGFjZVNlZ21lbnQoYywgb2xkU2VnbWVudCwgbmV3U2VnbWVudCk7XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIG5ldyBVcmxTZWdtZW50R3JvdXAoY3VycmVudC5zZWdtZW50cywgY2hpbGRyZW4pO1xufVxuXG5jbGFzcyBOYXZpZ2F0aW9uIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgaXNBYnNvbHV0ZTogYm9vbGVhbiwgcHVibGljIG51bWJlck9mRG91YmxlRG90czogbnVtYmVyLCBwdWJsaWMgY29tbWFuZHM6IGFueVtdKSB7XG4gICAgaWYgKGlzQWJzb2x1dGUgJiYgY29tbWFuZHMubGVuZ3RoID4gMCAmJiBpc01hdHJpeFBhcmFtcyhjb21tYW5kc1swXSkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignUm9vdCBzZWdtZW50IGNhbm5vdCBoYXZlIG1hdHJpeCBwYXJhbWV0ZXJzJyk7XG4gICAgfVxuXG4gICAgY29uc3QgY21kV2l0aE91dGxldCA9IGNvbW1hbmRzLmZpbmQoYyA9PiB0eXBlb2YgYyA9PT0gJ29iamVjdCcgJiYgYyAhPSBudWxsICYmIGMub3V0bGV0cyk7XG4gICAgaWYgKGNtZFdpdGhPdXRsZXQgJiYgY21kV2l0aE91dGxldCAhPT0gbGFzdChjb21tYW5kcykpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcigne291dGxldHM6e319IGhhcyB0byBiZSB0aGUgbGFzdCBjb21tYW5kJyk7XG4gICAgfVxuICB9XG5cbiAgcHVibGljIHRvUm9vdCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5pc0Fic29sdXRlICYmIHRoaXMuY29tbWFuZHMubGVuZ3RoID09PSAxICYmIHRoaXMuY29tbWFuZHNbMF0gPT0gJy8nO1xuICB9XG59XG5cbi8qKiBUcmFuc2Zvcm1zIGNvbW1hbmRzIHRvIGEgbm9ybWFsaXplZCBgTmF2aWdhdGlvbmAgKi9cbmZ1bmN0aW9uIGNvbXB1dGVOYXZpZ2F0aW9uKGNvbW1hbmRzOiBhbnlbXSk6IE5hdmlnYXRpb24ge1xuICBpZiAoKHR5cGVvZiBjb21tYW5kc1swXSA9PT0gJ3N0cmluZycpICYmIGNvbW1hbmRzLmxlbmd0aCA9PT0gMSAmJiBjb21tYW5kc1swXSA9PT0gJy8nKSB7XG4gICAgcmV0dXJuIG5ldyBOYXZpZ2F0aW9uKHRydWUsIDAsIGNvbW1hbmRzKTtcbiAgfVxuXG4gIGxldCBudW1iZXJPZkRvdWJsZURvdHMgPSAwO1xuICBsZXQgaXNBYnNvbHV0ZSA9IGZhbHNlO1xuXG4gIGNvbnN0IHJlczogYW55W10gPSBjb21tYW5kcy5yZWR1Y2UoKHJlcywgY21kLCBjbWRJZHgpID0+IHtcbiAgICBpZiAodHlwZW9mIGNtZCA9PT0gJ29iamVjdCcgJiYgY21kICE9IG51bGwpIHtcbiAgICAgIGlmIChjbWQub3V0bGV0cykge1xuICAgICAgICBjb25zdCBvdXRsZXRzOiB7W2s6IHN0cmluZ106IGFueX0gPSB7fTtcbiAgICAgICAgZm9yRWFjaChjbWQub3V0bGV0cywgKGNvbW1hbmRzOiBhbnksIG5hbWU6IHN0cmluZykgPT4ge1xuICAgICAgICAgIG91dGxldHNbbmFtZV0gPSB0eXBlb2YgY29tbWFuZHMgPT09ICdzdHJpbmcnID8gY29tbWFuZHMuc3BsaXQoJy8nKSA6IGNvbW1hbmRzO1xuICAgICAgICB9KTtcbiAgICAgICAgcmV0dXJuIFsuLi5yZXMsIHtvdXRsZXRzfV07XG4gICAgICB9XG5cbiAgICAgIGlmIChjbWQuc2VnbWVudFBhdGgpIHtcbiAgICAgICAgcmV0dXJuIFsuLi5yZXMsIGNtZC5zZWdtZW50UGF0aF07XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKCEodHlwZW9mIGNtZCA9PT0gJ3N0cmluZycpKSB7XG4gICAgICByZXR1cm4gWy4uLnJlcywgY21kXTtcbiAgICB9XG5cbiAgICBpZiAoY21kSWR4ID09PSAwKSB7XG4gICAgICBjbWQuc3BsaXQoJy8nKS5mb3JFYWNoKCh1cmxQYXJ0LCBwYXJ0SW5kZXgpID0+IHtcbiAgICAgICAgaWYgKHBhcnRJbmRleCA9PSAwICYmIHVybFBhcnQgPT09ICcuJykge1xuICAgICAgICAgIC8vIHNraXAgJy4vYSdcbiAgICAgICAgfSBlbHNlIGlmIChwYXJ0SW5kZXggPT0gMCAmJiB1cmxQYXJ0ID09PSAnJykgeyAgLy8gICcvYSdcbiAgICAgICAgICBpc0Fic29sdXRlID0gdHJ1ZTtcbiAgICAgICAgfSBlbHNlIGlmICh1cmxQYXJ0ID09PSAnLi4nKSB7ICAvLyAgJy4uL2EnXG4gICAgICAgICAgbnVtYmVyT2ZEb3VibGVEb3RzKys7XG4gICAgICAgIH0gZWxzZSBpZiAodXJsUGFydCAhPSAnJykge1xuICAgICAgICAgIHJlcy5wdXNoKHVybFBhcnQpO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcmV0dXJuIHJlcztcbiAgICB9XG5cbiAgICByZXR1cm4gWy4uLnJlcywgY21kXTtcbiAgfSwgW10pO1xuXG4gIHJldHVybiBuZXcgTmF2aWdhdGlvbihpc0Fic29sdXRlLCBudW1iZXJPZkRvdWJsZURvdHMsIHJlcyk7XG59XG5cbmNsYXNzIFBvc2l0aW9uIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgc2VnbWVudEdyb3VwOiBVcmxTZWdtZW50R3JvdXAsIHB1YmxpYyBwcm9jZXNzQ2hpbGRyZW46IGJvb2xlYW4sIHB1YmxpYyBpbmRleDogbnVtYmVyKSB7XG4gIH1cbn1cblxuZnVuY3Rpb24gZmluZFN0YXJ0aW5nUG9zaXRpb24obmF2OiBOYXZpZ2F0aW9uLCB0cmVlOiBVcmxUcmVlLCByb3V0ZTogQWN0aXZhdGVkUm91dGUpOiBQb3NpdGlvbiB7XG4gIGlmIChuYXYuaXNBYnNvbHV0ZSkge1xuICAgIHJldHVybiBuZXcgUG9zaXRpb24odHJlZS5yb290LCB0cnVlLCAwKTtcbiAgfVxuXG4gIGlmIChyb3V0ZS5zbmFwc2hvdC5fbGFzdFBhdGhJbmRleCA9PT0gLTEpIHtcbiAgICByZXR1cm4gbmV3IFBvc2l0aW9uKHJvdXRlLnNuYXBzaG90Ll91cmxTZWdtZW50LCB0cnVlLCAwKTtcbiAgfVxuXG4gIGNvbnN0IG1vZGlmaWVyID0gaXNNYXRyaXhQYXJhbXMobmF2LmNvbW1hbmRzWzBdKSA/IDAgOiAxO1xuICBjb25zdCBpbmRleCA9IHJvdXRlLnNuYXBzaG90Ll9sYXN0UGF0aEluZGV4ICsgbW9kaWZpZXI7XG4gIHJldHVybiBjcmVhdGVQb3NpdGlvbkFwcGx5aW5nRG91YmxlRG90cyhcbiAgICAgIHJvdXRlLnNuYXBzaG90Ll91cmxTZWdtZW50LCBpbmRleCwgbmF2Lm51bWJlck9mRG91YmxlRG90cyk7XG59XG5cbmZ1bmN0aW9uIGNyZWF0ZVBvc2l0aW9uQXBwbHlpbmdEb3VibGVEb3RzKFxuICAgIGdyb3VwOiBVcmxTZWdtZW50R3JvdXAsIGluZGV4OiBudW1iZXIsIG51bWJlck9mRG91YmxlRG90czogbnVtYmVyKTogUG9zaXRpb24ge1xuICBsZXQgZyA9IGdyb3VwO1xuICBsZXQgY2kgPSBpbmRleDtcbiAgbGV0IGRkID0gbnVtYmVyT2ZEb3VibGVEb3RzO1xuICB3aGlsZSAoZGQgPiBjaSkge1xuICAgIGRkIC09IGNpO1xuICAgIGcgPSBnLnBhcmVudCAhO1xuICAgIGlmICghZykge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdJbnZhbGlkIG51bWJlciBvZiBcXCcuLi9cXCcnKTtcbiAgICB9XG4gICAgY2kgPSBnLnNlZ21lbnRzLmxlbmd0aDtcbiAgfVxuICByZXR1cm4gbmV3IFBvc2l0aW9uKGcsIGZhbHNlLCBjaSAtIGRkKTtcbn1cblxuZnVuY3Rpb24gZ2V0UGF0aChjb21tYW5kOiBhbnkpOiBhbnkge1xuICBpZiAodHlwZW9mIGNvbW1hbmQgPT09ICdvYmplY3QnICYmIGNvbW1hbmQgIT0gbnVsbCAmJiBjb21tYW5kLm91dGxldHMpIHtcbiAgICByZXR1cm4gY29tbWFuZC5vdXRsZXRzW1BSSU1BUllfT1VUTEVUXTtcbiAgfVxuICByZXR1cm4gYCR7Y29tbWFuZH1gO1xufVxuXG5mdW5jdGlvbiBnZXRPdXRsZXRzKGNvbW1hbmRzOiBhbnlbXSk6IHtbazogc3RyaW5nXTogYW55W119IHtcbiAgaWYgKCEodHlwZW9mIGNvbW1hbmRzWzBdID09PSAnb2JqZWN0JykpIHJldHVybiB7W1BSSU1BUllfT1VUTEVUXTogY29tbWFuZHN9O1xuICBpZiAoY29tbWFuZHNbMF0ub3V0bGV0cyA9PT0gdW5kZWZpbmVkKSByZXR1cm4ge1tQUklNQVJZX09VVExFVF06IGNvbW1hbmRzfTtcbiAgcmV0dXJuIGNvbW1hbmRzWzBdLm91dGxldHM7XG59XG5cbmZ1bmN0aW9uIHVwZGF0ZVNlZ21lbnRHcm91cChcbiAgICBzZWdtZW50R3JvdXA6IFVybFNlZ21lbnRHcm91cCwgc3RhcnRJbmRleDogbnVtYmVyLCBjb21tYW5kczogYW55W10pOiBVcmxTZWdtZW50R3JvdXAge1xuICBpZiAoIXNlZ21lbnRHcm91cCkge1xuICAgIHNlZ21lbnRHcm91cCA9IG5ldyBVcmxTZWdtZW50R3JvdXAoW10sIHt9KTtcbiAgfVxuICBpZiAoc2VnbWVudEdyb3VwLnNlZ21lbnRzLmxlbmd0aCA9PT0gMCAmJiBzZWdtZW50R3JvdXAuaGFzQ2hpbGRyZW4oKSkge1xuICAgIHJldHVybiB1cGRhdGVTZWdtZW50R3JvdXBDaGlsZHJlbihzZWdtZW50R3JvdXAsIHN0YXJ0SW5kZXgsIGNvbW1hbmRzKTtcbiAgfVxuXG4gIGNvbnN0IG0gPSBwcmVmaXhlZFdpdGgoc2VnbWVudEdyb3VwLCBzdGFydEluZGV4LCBjb21tYW5kcyk7XG4gIGNvbnN0IHNsaWNlZENvbW1hbmRzID0gY29tbWFuZHMuc2xpY2UobS5jb21tYW5kSW5kZXgpO1xuICBpZiAobS5tYXRjaCAmJiBtLnBhdGhJbmRleCA8IHNlZ21lbnRHcm91cC5zZWdtZW50cy5sZW5ndGgpIHtcbiAgICBjb25zdCBnID0gbmV3IFVybFNlZ21lbnRHcm91cChzZWdtZW50R3JvdXAuc2VnbWVudHMuc2xpY2UoMCwgbS5wYXRoSW5kZXgpLCB7fSk7XG4gICAgZy5jaGlsZHJlbltQUklNQVJZX09VVExFVF0gPVxuICAgICAgICBuZXcgVXJsU2VnbWVudEdyb3VwKHNlZ21lbnRHcm91cC5zZWdtZW50cy5zbGljZShtLnBhdGhJbmRleCksIHNlZ21lbnRHcm91cC5jaGlsZHJlbik7XG4gICAgcmV0dXJuIHVwZGF0ZVNlZ21lbnRHcm91cENoaWxkcmVuKGcsIDAsIHNsaWNlZENvbW1hbmRzKTtcbiAgfSBlbHNlIGlmIChtLm1hdGNoICYmIHNsaWNlZENvbW1hbmRzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBuZXcgVXJsU2VnbWVudEdyb3VwKHNlZ21lbnRHcm91cC5zZWdtZW50cywge30pO1xuICB9IGVsc2UgaWYgKG0ubWF0Y2ggJiYgIXNlZ21lbnRHcm91cC5oYXNDaGlsZHJlbigpKSB7XG4gICAgcmV0dXJuIGNyZWF0ZU5ld1NlZ21lbnRHcm91cChzZWdtZW50R3JvdXAsIHN0YXJ0SW5kZXgsIGNvbW1hbmRzKTtcbiAgfSBlbHNlIGlmIChtLm1hdGNoKSB7XG4gICAgcmV0dXJuIHVwZGF0ZVNlZ21lbnRHcm91cENoaWxkcmVuKHNlZ21lbnRHcm91cCwgMCwgc2xpY2VkQ29tbWFuZHMpO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBjcmVhdGVOZXdTZWdtZW50R3JvdXAoc2VnbWVudEdyb3VwLCBzdGFydEluZGV4LCBjb21tYW5kcyk7XG4gIH1cbn1cblxuZnVuY3Rpb24gdXBkYXRlU2VnbWVudEdyb3VwQ2hpbGRyZW4oXG4gICAgc2VnbWVudEdyb3VwOiBVcmxTZWdtZW50R3JvdXAsIHN0YXJ0SW5kZXg6IG51bWJlciwgY29tbWFuZHM6IGFueVtdKTogVXJsU2VnbWVudEdyb3VwIHtcbiAgaWYgKGNvbW1hbmRzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBuZXcgVXJsU2VnbWVudEdyb3VwKHNlZ21lbnRHcm91cC5zZWdtZW50cywge30pO1xuICB9IGVsc2Uge1xuICAgIGNvbnN0IG91dGxldHMgPSBnZXRPdXRsZXRzKGNvbW1hbmRzKTtcbiAgICBjb25zdCBjaGlsZHJlbjoge1trZXk6IHN0cmluZ106IFVybFNlZ21lbnRHcm91cH0gPSB7fTtcblxuICAgIGZvckVhY2gob3V0bGV0cywgKGNvbW1hbmRzOiBhbnksIG91dGxldDogc3RyaW5nKSA9PiB7XG4gICAgICBpZiAoY29tbWFuZHMgIT09IG51bGwpIHtcbiAgICAgICAgY2hpbGRyZW5bb3V0bGV0XSA9IHVwZGF0ZVNlZ21lbnRHcm91cChzZWdtZW50R3JvdXAuY2hpbGRyZW5bb3V0bGV0XSwgc3RhcnRJbmRleCwgY29tbWFuZHMpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgZm9yRWFjaChzZWdtZW50R3JvdXAuY2hpbGRyZW4sIChjaGlsZDogVXJsU2VnbWVudEdyb3VwLCBjaGlsZE91dGxldDogc3RyaW5nKSA9PiB7XG4gICAgICBpZiAob3V0bGV0c1tjaGlsZE91dGxldF0gPT09IHVuZGVmaW5lZCkge1xuICAgICAgICBjaGlsZHJlbltjaGlsZE91dGxldF0gPSBjaGlsZDtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gbmV3IFVybFNlZ21lbnRHcm91cChzZWdtZW50R3JvdXAuc2VnbWVudHMsIGNoaWxkcmVuKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBwcmVmaXhlZFdpdGgoc2VnbWVudEdyb3VwOiBVcmxTZWdtZW50R3JvdXAsIHN0YXJ0SW5kZXg6IG51bWJlciwgY29tbWFuZHM6IGFueVtdKSB7XG4gIGxldCBjdXJyZW50Q29tbWFuZEluZGV4ID0gMDtcbiAgbGV0IGN1cnJlbnRQYXRoSW5kZXggPSBzdGFydEluZGV4O1xuXG4gIGNvbnN0IG5vTWF0Y2ggPSB7bWF0Y2g6IGZhbHNlLCBwYXRoSW5kZXg6IDAsIGNvbW1hbmRJbmRleDogMH07XG4gIHdoaWxlIChjdXJyZW50UGF0aEluZGV4IDwgc2VnbWVudEdyb3VwLnNlZ21lbnRzLmxlbmd0aCkge1xuICAgIGlmIChjdXJyZW50Q29tbWFuZEluZGV4ID49IGNvbW1hbmRzLmxlbmd0aCkgcmV0dXJuIG5vTWF0Y2g7XG4gICAgY29uc3QgcGF0aCA9IHNlZ21lbnRHcm91cC5zZWdtZW50c1tjdXJyZW50UGF0aEluZGV4XTtcbiAgICBjb25zdCBjdXJyID0gZ2V0UGF0aChjb21tYW5kc1tjdXJyZW50Q29tbWFuZEluZGV4XSk7XG4gICAgY29uc3QgbmV4dCA9XG4gICAgICAgIGN1cnJlbnRDb21tYW5kSW5kZXggPCBjb21tYW5kcy5sZW5ndGggLSAxID8gY29tbWFuZHNbY3VycmVudENvbW1hbmRJbmRleCArIDFdIDogbnVsbDtcblxuICAgIGlmIChjdXJyZW50UGF0aEluZGV4ID4gMCAmJiBjdXJyID09PSB1bmRlZmluZWQpIGJyZWFrO1xuXG4gICAgaWYgKGN1cnIgJiYgbmV4dCAmJiAodHlwZW9mIG5leHQgPT09ICdvYmplY3QnKSAmJiBuZXh0Lm91dGxldHMgPT09IHVuZGVmaW5lZCkge1xuICAgICAgaWYgKCFjb21wYXJlKGN1cnIsIG5leHQsIHBhdGgpKSByZXR1cm4gbm9NYXRjaDtcbiAgICAgIGN1cnJlbnRDb21tYW5kSW5kZXggKz0gMjtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKCFjb21wYXJlKGN1cnIsIHt9LCBwYXRoKSkgcmV0dXJuIG5vTWF0Y2g7XG4gICAgICBjdXJyZW50Q29tbWFuZEluZGV4Kys7XG4gICAgfVxuICAgIGN1cnJlbnRQYXRoSW5kZXgrKztcbiAgfVxuXG4gIHJldHVybiB7bWF0Y2g6IHRydWUsIHBhdGhJbmRleDogY3VycmVudFBhdGhJbmRleCwgY29tbWFuZEluZGV4OiBjdXJyZW50Q29tbWFuZEluZGV4fTtcbn1cblxuZnVuY3Rpb24gY3JlYXRlTmV3U2VnbWVudEdyb3VwKFxuICAgIHNlZ21lbnRHcm91cDogVXJsU2VnbWVudEdyb3VwLCBzdGFydEluZGV4OiBudW1iZXIsIGNvbW1hbmRzOiBhbnlbXSk6IFVybFNlZ21lbnRHcm91cCB7XG4gIGNvbnN0IHBhdGhzID0gc2VnbWVudEdyb3VwLnNlZ21lbnRzLnNsaWNlKDAsIHN0YXJ0SW5kZXgpO1xuXG4gIGxldCBpID0gMDtcbiAgd2hpbGUgKGkgPCBjb21tYW5kcy5sZW5ndGgpIHtcbiAgICBpZiAodHlwZW9mIGNvbW1hbmRzW2ldID09PSAnb2JqZWN0JyAmJiBjb21tYW5kc1tpXS5vdXRsZXRzICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIGNvbnN0IGNoaWxkcmVuID0gY3JlYXRlTmV3U2VnbWVudENoaWxkcmVuKGNvbW1hbmRzW2ldLm91dGxldHMpO1xuICAgICAgcmV0dXJuIG5ldyBVcmxTZWdtZW50R3JvdXAocGF0aHMsIGNoaWxkcmVuKTtcbiAgICB9XG5cbiAgICAvLyBpZiB3ZSBzdGFydCB3aXRoIGFuIG9iamVjdCBsaXRlcmFsLCB3ZSBuZWVkIHRvIHJldXNlIHRoZSBwYXRoIHBhcnQgZnJvbSB0aGUgc2VnbWVudFxuICAgIGlmIChpID09PSAwICYmIGlzTWF0cml4UGFyYW1zKGNvbW1hbmRzWzBdKSkge1xuICAgICAgY29uc3QgcCA9IHNlZ21lbnRHcm91cC5zZWdtZW50c1tzdGFydEluZGV4XTtcbiAgICAgIHBhdGhzLnB1c2gobmV3IFVybFNlZ21lbnQocC5wYXRoLCBjb21tYW5kc1swXSkpO1xuICAgICAgaSsrO1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgY29uc3QgY3VyciA9IGdldFBhdGgoY29tbWFuZHNbaV0pO1xuICAgIGNvbnN0IG5leHQgPSAoaSA8IGNvbW1hbmRzLmxlbmd0aCAtIDEpID8gY29tbWFuZHNbaSArIDFdIDogbnVsbDtcbiAgICBpZiAoY3VyciAmJiBuZXh0ICYmIGlzTWF0cml4UGFyYW1zKG5leHQpKSB7XG4gICAgICBwYXRocy5wdXNoKG5ldyBVcmxTZWdtZW50KGN1cnIsIHN0cmluZ2lmeShuZXh0KSkpO1xuICAgICAgaSArPSAyO1xuICAgIH0gZWxzZSB7XG4gICAgICBwYXRocy5wdXNoKG5ldyBVcmxTZWdtZW50KGN1cnIsIHt9KSk7XG4gICAgICBpKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBuZXcgVXJsU2VnbWVudEdyb3VwKHBhdGhzLCB7fSk7XG59XG5cbmZ1bmN0aW9uIGNyZWF0ZU5ld1NlZ21lbnRDaGlsZHJlbihvdXRsZXRzOiB7W25hbWU6IHN0cmluZ106IGFueX0pOiBhbnkge1xuICBjb25zdCBjaGlsZHJlbjoge1trZXk6IHN0cmluZ106IFVybFNlZ21lbnRHcm91cH0gPSB7fTtcbiAgZm9yRWFjaChvdXRsZXRzLCAoY29tbWFuZHM6IGFueSwgb3V0bGV0OiBzdHJpbmcpID0+IHtcbiAgICBpZiAoY29tbWFuZHMgIT09IG51bGwpIHtcbiAgICAgIGNoaWxkcmVuW291dGxldF0gPSBjcmVhdGVOZXdTZWdtZW50R3JvdXAobmV3IFVybFNlZ21lbnRHcm91cChbXSwge30pLCAwLCBjb21tYW5kcyk7XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIGNoaWxkcmVuO1xufVxuXG5mdW5jdGlvbiBzdHJpbmdpZnkocGFyYW1zOiB7W2tleTogc3RyaW5nXTogYW55fSk6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9IHtcbiAgY29uc3QgcmVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICBmb3JFYWNoKHBhcmFtcywgKHY6IGFueSwgazogc3RyaW5nKSA9PiByZXNba10gPSBgJHt2fWApO1xuICByZXR1cm4gcmVzO1xufVxuXG5mdW5jdGlvbiBjb21wYXJlKHBhdGg6IHN0cmluZywgcGFyYW1zOiB7W2tleTogc3RyaW5nXTogYW55fSwgc2VnbWVudDogVXJsU2VnbWVudCk6IGJvb2xlYW4ge1xuICByZXR1cm4gcGF0aCA9PSBzZWdtZW50LnBhdGggJiYgc2hhbGxvd0VxdWFsKHBhcmFtcywgc2VnbWVudC5wYXJhbWV0ZXJzKTtcbn1cbiJdfQ==