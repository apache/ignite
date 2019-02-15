--
--                   GridGain Community Edition Licensing
--                   Copyright 2019 GridGain Systems, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
-- Restriction; you may not use this file except in compliance with the License. You may obtain a
-- copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed under the
-- License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the specific language governing permissions
-- and limitations under the License.
--
-- Commons Clause Restriction
--
-- The Software is provided to you by the Licensor under the License, as defined below, subject to
-- the following condition.
--
-- Without limiting other conditions in the License, the grant of rights under the License will not
-- include, and the License does not grant to you, the right to Sell the Software.
-- For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
-- under the License to provide to third parties, for a fee or other consideration (including without
-- limitation fees for hosting or consulting/ support services related to the Software), a product or
-- service whose value derives, entirely or substantially, from the functionality of the Software.
-- Any license notice or attribution required by the License must also include this Commons Clause
-- License Condition notice.
--
-- For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
-- the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
-- Edition software provided with this notice.
--

select *
from (
  select rootOrderId as custOrderId, co.date, co.orderId, replace(co.alias,'_ALGO','') as alias, op.parentAlgo
  from (
	select  date, orderId, rootOrderId, origOrderId, archSeq, alias 
	from "custord".CustOrder where alias='CUSTOM'

	union all

	select  date, orderId, rootOrderId, refOrderId as origOrderId, archSeq, alias
	from "replord".ReplaceOrder where alias='CUSTOM'
  ) co,
  "ordparam".OrderParams op,
    (
      select origOrderId, date, max(archSeq) maxArchSeq
      from (
          select  date, orderId, rootOrderId, origOrderId, archSeq, alias
          from "custord".CustOrder where alias='CUSTOM'

          union all

          select  date, orderId, rootOrderId, refOrderId as origOrderId, archSeq, alias
          from "replord".ReplaceOrder where alias='CUSTOM'
      )
      group by origOrderId, date
   ) h
  where co.date = op.date and co.orderId = op.orderId
    and h.origOrderId = co.origOrderId and h.date = co.date
    and co.archSeq = h.maxArchSeq
	and co.alias='CUSTOM'
) cop
inner join (
  select e.date, e.rootOrderId as eRootOrderId, e.rootOrderId, sum(e.execShares) as execShares,
	sum(e.execShares*e.price)/sum(e.execShares) as price,
	case when min(e.lastMkt) = max(e.lastMkt) then min(e.lastMkt) else min('XOFF') end as execMeet
  from "exec".Exec e
  group by e.date, e.rootOrderId
) oep on (cop.date = oep.date and cop.custOrderId = oep.eRootOrderId)
left outer join (
  select top 1 refOrderId, date from "cancel".Cancel order by date desc
) cc on (cc.refOrderId = cop.orderId and cc.date = cop.date)
where cop.alias='CUSTOM'
