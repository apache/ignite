/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import {Observable, merge} from 'rxjs';
import {share, distinctUntilChanged, startWith, filter, map, pluck, withLatestFrom, mapTo} from 'rxjs/operators';
import {RejectType, TransitionService} from '@uirouter/angularjs';
import isEqual from 'lodash/isEqual';

configSelectionManager.$inject = ['$transitions'];
export default function configSelectionManager($transitions: TransitionService) {
    /**
     * Determines what items should be marked as selected and if something is being edited at the moment.
     */
    return ({itemID$, selectedItemRows$, visibleRows$, loadedItems$}) => {
        // Aborted transitions happen when form has unsaved changes, user attempts to leave
        // but decides to stay after screen asks for leave confirmation.
        const abortedTransitions$ = Observable.create((observer) => {
            return $transitions.onError({}, (t) => observer.next(t));
        }).pipe(filter((t) => t.error().type === RejectType.ABORTED));

        const firstItemID$ = visibleRows$.pipe(
            withLatestFrom(itemID$, loadedItems$),
            filter(([rows, id, items]) => !id && rows && rows.length === items.length),
            pluck('0', '0', 'entity', '_id')
        );

        const selectedItemRowsIDs$ = selectedItemRows$.pipe(map((rows) => rows.map((r) => r._id)), share());
        const singleSelectionEdit$ = selectedItemRows$.pipe(filter((r) => r && r.length === 1), pluck('0', '_id'));
        const selectedMultipleOrNone$ = selectedItemRows$.pipe(filter((r) => r.length > 1 || r.length === 0));
        const loadedItemIDs$ = loadedItems$.pipe(map((rows) => new Set(rows.map((r) => r._id))), share());
        const currentItemWasRemoved$ = loadedItemIDs$.pipe(
            withLatestFrom(
                itemID$.pipe(filter((v) => v && v !== 'new')),
                /**
                 * Without startWith currentItemWasRemoved$ won't emit in the following scenario:
                 * 1. User opens items page (no item id in location).
                 * 2. Selection manager commands to edit first item.
                 * 3. User removes said item.
                 */
                selectedItemRowsIDs$.pipe(startWith([]))
            ),
            filter(([existingIDs, itemID, selectedIDs]) => !existingIDs.has(itemID)),
            map(([existingIDs, itemID, selectedIDs]) => selectedIDs.filter((id) => id !== itemID)),
            share()
        );

        // Edit first loaded item or when there's only one item selected
        const editGoes$ = merge(firstItemID$, singleSelectionEdit$).pipe(
            // Don't go to non-existing items.
            // Happens when user naviagtes to older history and some items were already removed.
            withLatestFrom(loadedItemIDs$),
            filter(([id, loaded]) => id && loaded.has(id)),
            pluck('0')
        );
        // Stop edit when multiple or none items are selected or when current item was removed
        const editLeaves$ = merge(
            selectedMultipleOrNone$.pipe(mapTo({})),
            currentItemWasRemoved$.pipe(mapTo({location: 'replace', custom: {justIDUpdate: true}}))
        ).pipe(share());

        const selectedItemIDs$ = merge(
            // Select nothing when creating an item or select current item
            itemID$.pipe(filter((id) => id), map((id) => id === 'new' ? [] : [id])),
            // Restore previous item selection when transition gets aborted
            abortedTransitions$.pipe(withLatestFrom(itemID$, (_, id) => [id])),
            // Select all incoming selected rows
            selectedItemRowsIDs$
        ).pipe(
            // If nothing's selected and there are zero rows, ui-grid will behave as if all rows are selected
            startWith([]),
            // Some scenarios cause same item to be selected multiple times in a row,
            // so it makes sense to filter out duplicate entries
            distinctUntilChanged(isEqual),
            share()
        );

        return {selectedItemIDs$, editGoes$, editLeaves$};
    };
}
