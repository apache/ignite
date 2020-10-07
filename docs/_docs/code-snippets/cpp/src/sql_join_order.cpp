#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "person.h"

using namespace ignite;
using namespace cache;
using namespace query;

int main()
{
	//tag::sql-join-order[]
	SqlFieldsQuery query = SqlFieldsQuery("SELECT * FROM TABLE_A, TABLE_B USE INDEX(HASH_JOIN_IDX) WHERE TABLE_A.column1 = TABLE_B.column2");
	query.SetEnforceJoinOrder(true);
	//end::sql-join-order[]
}