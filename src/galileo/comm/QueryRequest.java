/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import galileo.dataset.Coordinates;
import galileo.event.Event;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Operator;
import galileo.query.Query;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

/**
 * Encapsulates query information submitted by clients to be processed by
 * StorageNodes.
 *
 * @author malensek
 */
public class QueryRequest implements Event {

	private String fsName;
	private Query featureQuery;
	private Query metadataQuery;
	private List<Coordinates> polygon;
	private String time;
	private boolean dryRun;

	private void validate(String fsName) {
		if (fsName == null || fsName.trim().length() == 0 || !fsName.matches("[a-z0-9-]{5,50}"))
			throw new IllegalArgumentException("invalid filesystem name");
		this.fsName = fsName;
	}

	private void validate(Query query) {
		if (query == null || query.getOperations().isEmpty())
			throw new IllegalArgumentException("illegal query. must have at least one operation");
		Operation operation = query.getOperations().get(0);
		if (operation.getExpressions().isEmpty())
			throw new IllegalArgumentException("no expressions found for an operation of the query");
		Expression expression = operation.getExpressions().get(0);
		if (expression.getOperand() == null || expression.getOperand().trim().length() == 0
				|| expression.getOperator() == Operator.UNKNOWN || expression.getValue() == null)
			throw new IllegalArgumentException("illegal expression for an operation of the query");
	}

	public void setFeatureQuery(Query query) {
		validate(query);
		this.featureQuery = query;
	}

	public void setMetdataQuery(Query query) {
		validate(query);
		this.metadataQuery = query;
	}

	public void setPolygon(List<Coordinates> polygon) {
		if (polygon == null)
			throw new IllegalArgumentException("Spatial coordinates cannot be null");
		this.polygon = polygon;
	}

	public void setTime(String time) {
		if (time != null) {
			if (time.length() != 13)
				throw new IllegalArgumentException(
						"time must be of the form yyyy-mm-dd-hh with missing values replaced as x");
			this.time = time;
		}
	}

	public QueryRequest(String fsName, Query featureQuery, Query metadataQuery) {
		validate(fsName);
		if (featureQuery == null && metadataQuery == null)
			throw new IllegalArgumentException("Atleast one of the queries must be present");
		if (featureQuery != null)
			setFeatureQuery(featureQuery);
		if (metadataQuery != null)
			setMetdataQuery(metadataQuery);
	}

	public QueryRequest(String fsName, List<Coordinates> polygon) {
		validate(fsName);
		setPolygon(polygon);
	}
	
	public QueryRequest(String fsName, String time) {
		validate(fsName);
		setTime(time);
	}

	public String getFilesystemName() {
		return this.fsName;
	}

	public Query getFeatureQuery() {
		return this.featureQuery;
	}
	
	public Query getMetadataQuery() {
		return this.featureQuery;
	}

	public String getTime() {
		return this.time;
	}

	public List<Coordinates> getPolygon() {
		if (this.polygon == null)
			return null;
		return Collections.unmodifiableList(this.polygon);
	}

	public boolean isSpatial() {
		return polygon != null;
	}

	public boolean isTemporal() {
		return time != null;
	}

	public boolean hasFeatureQuery() {
		return this.featureQuery != null;
	}

	public String getFeatureQueryString() {
		if (this.featureQuery != null)
			return featureQuery.toString();
		return "";
	}
	
	public boolean hasMetadataQuery() {
		return this.metadataQuery != null;
	}

	public String getMetadataQueryString() {
		if (this.metadataQuery != null)
			return metadataQuery.toString();
		return "";
	}

	public boolean isDryRun() {
		return this.dryRun;
	}

	public void enableDryRun() {
		this.dryRun = true;
	}

	public void disableDryRun() {
		this.dryRun = false;
	}

	@Deserialize
	public QueryRequest(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		boolean isTemporal = in.readBoolean();
		if (isTemporal)
			time = in.readString();
		boolean isSpatial = in.readBoolean();
		if (isSpatial) {
			List<Coordinates> poly = new ArrayList<Coordinates>();
			in.readSerializableCollection(Coordinates.class, poly);
			polygon = poly;
		}
		boolean hasFeatureQuery = in.readBoolean();
		if (hasFeatureQuery)
			this.featureQuery = new Query(in);
		boolean hasMetadataQuery = in.readBoolean();
		if (hasMetadataQuery)
			this.metadataQuery = new Query(in);
		dryRun = in.readBoolean();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeBoolean(isTemporal());
		if (isTemporal())
			out.writeString(time);
		out.writeBoolean(isSpatial());
		if (isSpatial())
			out.writeSerializableCollection(polygon);
		out.writeBoolean(hasFeatureQuery());
		if (hasFeatureQuery())
			out.writeSerializable(this.featureQuery);
		out.writeBoolean(hasMetadataQuery());
		if (hasMetadataQuery())
			out.writeSerializable(this.metadataQuery);
		out.writeBoolean(dryRun);
	}
}
