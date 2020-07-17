package org.apache.ignite.snippets;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Indexes_groups {

	//tag::group-indexes[]
	public class Person implements Serializable {
		/** Indexed in a group index with "salary". */
		@QuerySqlField(orderedGroups = { @QuerySqlField.Group(name = "age_salary_idx", order = 0, descending = true) })
		private int age;

		/** Indexed separately and in a group index with "age". */
		@QuerySqlField(index = true, orderedGroups = { @QuerySqlField.Group(name = "age_salary_idx", order = 3) })
		private double salary;
	}
	//end::group-indexes[]
}
