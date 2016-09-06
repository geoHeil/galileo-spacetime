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
 * Contains an internal query between StorageNodes.
 *
 * @author malensek
 */
public class QueryEvent implements Event {
	private String id;
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

	private QueryEvent(String id, String fsName) {
		this.id = id;
		validate(fsName);
		this.fsName = fsName;
	}

	public QueryEvent(String id, String fsName, Query query) {
		this(id, fsName);
		setQuery(query);
	}

	public QueryEvent(String id, String fsName, Query query, boolean interactive) {
		this(id, fsName, query);
		this.interactive = interactive;
	}

	public QueryEvent(String id, String fsName, List<Coordinates> polygon) {
		this(id, fsName);
		setPolygon(polygon);
	}

	public QueryEvent(String id, String fsName, List<Coordinates> polygon, boolean interactive) {
		this(id, fsName, polygon);
		this.interactive = interactive;
	}

	public QueryEvent(String id, String fsName, List<Coordinates> polygon, Query query) {
		this(id, fsName, polygon);
		setQuery(query);
	}

	public QueryEvent(String id, String fsName, List<Coordinates> polygon, Query query, boolean interactive) {
		this(id, fsName, polygon, query);
		this.interactive = interactive;
	}

	public QueryEvent(String id, String fsName, TemporalProperties time) {
		this(id, fsName);
		setTime(time);
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, boolean interactive) {
		this(id, fsName, time);
		this.interactive = interactive;
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, Query query) {
		this(id, fsName, time);
		setQuery(query);
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, Query query, boolean interactive) {
		this(id, fsName, time, query);
		this.interactive = interactive;
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, List<Coordinates> polygon) {
		this(id, fsName, time);
		setPolygon(polygon);
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, List<Coordinates> polygon,
			boolean interactive) {
		this(id, fsName, time, polygon);
		this.interactive = interactive;
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, List<Coordinates> polygon, Query query) {
		this(id, fsName, time, polygon);
		setQuery(query);
	}

	public QueryEvent(String id, String fsName, TemporalProperties time, List<Coordinates> polygon, Query query,
			boolean interactive) {
		this(id, fsName, time, polygon, query);
		this.interactive = interactive;
	}

	public Query getQuery() {
		return query;
	}

	public String getQueryString() {
		if (query != null)
			return query.toString();
		return "";
	}

	public String getQueryId() {
		return id;
	}

	public String getFileSystemName() {
		return this.fsName;
	}
	
	public TemporalProperties getTemporalProperties(){
		return this.time;
	}
	
	public List<Coordinates> getPolygon(){
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

	@Deserialize
	public QueryEvent(SerializationInputStream in) throws IOException, SerializationException {
		id = in.readString();
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
		out.writeString(id);
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
