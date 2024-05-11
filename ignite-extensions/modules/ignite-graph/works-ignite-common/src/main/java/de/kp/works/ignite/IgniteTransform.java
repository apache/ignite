package de.kp.works.ignite;

import java.util.*;
import java.util.stream.Collectors;

import de.kp.works.ignite.query.*;

import de.kp.works.ignite.graph.*;

public final class IgniteTransform {
	public static final IgniteTransform MODULE$ = new IgniteTransform();

	public static List<IgniteResult> transformEdgeEntries(final List<IgniteEdgeEntry> entries) {
		List<IgniteResult> list = new LinkedList<IgniteResult>();

		Map<String, List<IgniteEdgeEntry>> groupByResult = entries.stream()
				.collect(Collectors.groupingBy(entry -> entry.id));

		groupByResult.forEach((k, values) -> {
			IgniteResult igniteResult = new IgniteResult();

			IgniteEdgeEntry head = values.get(0);

			/*
			 * Extract common fields
			 */

			String id = head.id;

			String idType = head.idType;
			String label = head.label;

			String toId = head.toId;
			String toIdType = head.toIdType;

			String fromId = head.fromId;
			String fromIdType = head.fromIdType;

			Long createdAt = head.createdAt;
			Long updatedAt = head.updatedAt;

			/*
			 * Add common fields
			 */

			igniteResult.addColumn(IgniteConstants.ID_COL_NAME, idType, id);

			igniteResult.addColumn(IgniteConstants.LABEL_COL_NAME, ValueType.STRING.name(), label);

			igniteResult.addColumn(IgniteConstants.TO_COL_NAME, toIdType, toId);

			igniteResult.addColumn(IgniteConstants.FROM_COL_NAME, fromIdType, fromId);

			igniteResult.addColumn(IgniteConstants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt);

			igniteResult.addColumn(IgniteConstants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt);
			
			/*
			 * Extract & add properties
			 */
			values.forEach(value -> {

				igniteResult.addColumn(value.propKey, value.propType, value.propValue);

			});

			list.add(igniteResult);

		});

		return list;
	}

	public static List<IgniteResult> transformVertexEntries(final List<IgniteVertexEntry> entries) {
		List<IgniteResult> list = new LinkedList<IgniteResult>();

		Map<String, List<IgniteVertexEntry>> groupByResult = entries.stream()
				.collect(Collectors.groupingBy(entry -> entry.id));

		groupByResult.forEach((k, values) -> {
			IgniteResult igniteResult = new IgniteResult();

			IgniteVertexEntry head = values.get(0);

			/*
			 * Extract common fields
			 */

			String id = head.id;

			String idType = head.idType;
			String label = head.label;

			Long createdAt = head.createdAt;
			Long updatedAt = head.updatedAt;

			/*
			 * Add common fields
			 */

			igniteResult.addColumn(IgniteConstants.ID_COL_NAME, idType, id);

			igniteResult.addColumn(IgniteConstants.LABEL_COL_NAME, ValueType.STRING.name(), label);

			igniteResult.addColumn(IgniteConstants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt);

			igniteResult.addColumn(IgniteConstants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt);
			/*
			 * Extract & add properties
			 */
			values.forEach(value -> {

				igniteResult.addColumn(value.propKey, value.propType, value.propValue);

			});

			list.add(igniteResult);

		});

		return list;
	}

	private IgniteTransform() {

	}

}
