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
import galileo.dataset.TemporalProperties;
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
	private boolean interactive;
	private Query query;
	private List<Coordinates> polygon;
	private TemporalProperties time;

	private void validate(String fsName) {
		if (fsName == null || fsName.trim().length() == 0 || !fsName.matches("[a-z0-9-]{5,50}"))
			throw new IllegalArgumentException("invalid filesystem name");
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

	private void setQuery(Query query) {
		validate(query);
		this.query = query;
	}

	private void setPolygon(List<Coordinates> polygon) {
		this.polygon = polygon;
	}

	private void setTime(TemporalProperties time) {
		this.time = time;
	}

	private QueryRequest(String fsName) {
		validate(fsName);
		this.fsName = fsName;
	}

	public QueryRequest(String fsName, Query query) {
		this(fsName);
		setQuery(query);
	}

	public QueryRequest(String fsName, Query query, boolean interactive) {
		this(fsName, query);
		this.interactive = interactive;
	}

	public QueryRequest(String fsName, List<Coordinates> polygon) {
		this(fsName);
		setPolygon(polygon);
	}

	public QueryRequest(String fsName, List<Coordinates> polygon, boolean interactive) {
		this(fsName, polygon);
		this.interactive = interactive;
	}

	public QueryRequest(String fsName, List<Coordinates> polygon, Query query) {
		this(fsName, polygon);
		setQuery(query);
	}

	public QueryRequest(String fsName, List<Coordinates> polygon, Query query, boolean interactive) {
		this(fsName, polygon, query);
		this.interactive = interactive;
	}

	public QueryRequest(String fsName, TemporalProperties time) {
		this(fsName);
		setTime(time);
	}

	public QueryRequest(String fsName, TemporalProperties time, boolean interactive) {
		this(fsName, time);
		this.interactive = interactive;
	}

	public QueryRequest(String fsName, TemporalProperties time, Query query) {
		this(fsName, time);
		setQuery(query);
	}

	public QueryRequest(String fsName, TemporalProperties time, Query query, boolean interactive) {
		this(fsName, time, query);
		this.interactive = interactive;
	}

	public QueryRequest(String fsName, TemporalProperties time, List<Coordinates> polygon) {
		this(fsName, time);
		setPolygon(polygon);
	}

	public QueryRequest(String fsName, TemporalProperties time, List<Coordinates> polygon, boolean interactive) {
		this(fsName, time, polygon);
		this.interactive = interactive;
	}

	public QueryRequest(String fsName, TemporalProperties time, List<Coordinates> polygon, Query query) {
		this(fsName, time, polygon);
		setQuery(query);
	}

	public QueryRequest(String fsName, TemporalProperties time, List<Coordinates> polygon, Query query,
			boolean interactive) {
		this(fsName, time, polygon, query);
		this.interactive = interactive;
	}

	public String getFileSystemName() {
		return this.fsName;
	}

	public Query getQuery() {
		return query;
	}

	public TemporalProperties getTemporalProperties() {
		return this.time;
	}

	public List<Coordinates> getPolygon() {
		if (this.polygon == null)
			return null;
		return Collections.unmodifiableList(this.polygon);
	}

	public boolean isInteractive() {
		return this.interactive;
	}

	public boolean isSpatial() {
		return polygon != null;
	}

	public boolean isTemporal() {
		return time != null;
	}

	public boolean hasQuery() {
		return query != null;
	}

	public String getQueryString() {
		if (query != null)
			return query.toString();
		return "";
	}

	@Deserialize
	public QueryRequest(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		boolean isTemporal = in.readBoolean();
		if (isTemporal)
			time = new TemporalProperties(in);
		boolean isSpatial = in.readBoolean();
		if (isSpatial) {
			List<Coordinates> poly = new ArrayList<Coordinates>();
			in.readSerializableCollection(Coordinates.class, poly);
			polygon = poly;
		}
		boolean hasQuery = in.readBoolean();
		if (hasQuery)
			query = new Query(in);
		interactive = in.readBoolean();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeBoolean(isTemporal());
		if (isTemporal())
			out.writeSerializable(time);
		out.writeBoolean(isSpatial());
		if (isSpatial())
			out.writeSerializableCollection(polygon);
		out.writeBoolean(hasQuery());
		if (hasQuery())
			out.writeSerializable(query);
		out.writeBoolean(interactive);
	}
}
