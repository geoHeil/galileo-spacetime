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
import java.util.List;

import org.json.JSONObject;

import galileo.event.Event;
import galileo.graph.GraphException;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class QueryResponse implements Event {

	private String id;
	private boolean interactive;
	private boolean isDryRun;
	private List<String> resultHeader;
	private List<List<String>> results;
	private JSONObject jsonResults;
	private long elapsedTime;

	public QueryResponse(String id, List<String> header, List<List<String>> results) {
		this.id = id;
		this.resultHeader = header;
		this.results = results;
		this.interactive = true;
		this.jsonResults = new JSONObject();
	}
	
	public QueryResponse(String id, JSONObject results) {
		this.id = id;
		this.jsonResults = results;
		this.resultHeader = new ArrayList<String>();
		this.results = new ArrayList<>();
	}

	public long getElapsedTime() {
		return this.elapsedTime;
	}
	
	public void setElapsedTime(long time) {
		this.elapsedTime = time;
	}

	public boolean isInteractive() {
		return this.interactive;
	}
	
	public void setDryRun(boolean dryRun){
		this.isDryRun = dryRun;
	}
	
	public boolean isDryRun(){
		return this.isDryRun;
	}

	public String getId() {
		return id;
	}
	
	public List<String> getResultHeader(){
		return this.resultHeader;
	}

	public List<List<String>> getResults() {
		return results;
	}

	public JSONObject getJSONResults() {
		return this.jsonResults;
	}

	@Deserialize
	public QueryResponse(SerializationInputStream in) throws IOException, SerializationException, GraphException {
		id = in.readString();
		interactive = in.readBoolean();
		isDryRun = in.readBoolean();
		elapsedTime = in.readLong();
		if (isInteractive()) {
			resultHeader = new ArrayList<String>();
			in.readStringCollection(resultHeader);
			int pathsSize = in.readInt();
			results = new ArrayList<>(3 * pathsSize / 2);
			for (int i = 0; i < pathsSize; ++i) {
				int pathSize = in.readInt();
				List<String> path = new ArrayList<String>(3 * pathSize / 2);
				in.readStringCollection(path);
				results.add(path);
			}
		} else {
			jsonResults = new JSONObject(in.readString());
		}
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(id);
		out.writeBoolean(interactive);
		out.writeBoolean(isDryRun);
		out.writeLong(elapsedTime);
		if (isInteractive()) {
			out.writeStringCollection(resultHeader);
			out.writeInt(results.size());
			for (List<String> path : results) {
				out.writeInt(path.size());
				out.writeStringCollection(path);
			}
		} else {
			out.writeString(jsonResults.toString());
		}

	}
}
