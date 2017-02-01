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
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.dataset.Block;
import galileo.event.Event;
import galileo.net.NetworkDestination;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.util.Pair;

public class QueryResponse implements Event {
	private String id;
	private boolean isDryRun;
	private JSONArray header;
	private JSONObject jsonResults;
	private long elapsedTime;
	private Iterator<Pair<NetworkDestination, String>> blockIterator;
	private String fsName;
	private Connector connector;

	private void buildBlockDestinations() {
		if (this.jsonResults != null && this.jsonResults.has("result")
				&& this.jsonResults.get("result") instanceof JSONArray) {
			List<Pair<NetworkDestination, String>> blockDestinations = new ArrayList<>();
			JSONArray results = this.jsonResults.getJSONArray("result");
			for (int i = 0; i < results.length(); i++) {
				JSONObject hostResult = results.getJSONObject(i);
				if (hostResult.has("fileSize") && hostResult.getLong("fileSize") > 0) {
					JSONArray blocks = hostResult.getJSONArray("filePath");
					NetworkDestination host = new NetworkDestination(hostResult.getString("hostName"),
							hostResult.getInt("hostPort"));
					for (int j = 0; j < blocks.length(); j++)
						blockDestinations.add(new Pair<NetworkDestination, String>(host, blocks.getString(j)));
				}
			}
			if (this.jsonResults.has("filesystem"))
				this.fsName = this.jsonResults.getString("filesystem");
			this.blockIterator = blockDestinations.iterator();
		}
	}

	public QueryResponse(String id, JSONArray header, JSONObject results) {
		this.id = id;
		this.header = header;
		this.jsonResults = results;
	}

	public long getElapsedTime() {
		return this.elapsedTime;
	}

	public void setElapsedTime(long time) {
		this.elapsedTime = time;
	}

	public void setDryRun(boolean dryRun) {
		this.isDryRun = dryRun;
	}

	public boolean isDryRun() {
		return this.isDryRun;
	}

	public String getId() {
		return id;
	}

	public JSONObject getJSONResults() {
		return this.jsonResults;
	}
	
	public int getBlockCount(){
		if(this.jsonResults.has("totalNumPaths")){
			return this.jsonResults.getInt("totalNumPaths");
		}
		return 0;
	}

	public boolean hasBlocks() {
		if (this.blockIterator != null) {
			boolean hasNext = this.blockIterator.hasNext();
			if (!hasNext && this.connector != null){
				this.connector.close();
			}
			return hasNext;
		}
		return false;
	}
	
	public Block getNextBlock() throws IOException, InterruptedException {
		if (hasBlocks()) {
			if (this.connector == null)
				this.connector = new Connector();
			Pair<NetworkDestination, String> pair = this.blockIterator.next();
			BlockResponse response = (BlockResponse) connector.sendMessage(pair.a,
					new BlockRequest(this.fsName, pair.b));
			if (response.getBlocks() != null && response.getBlocks().length > 0)
				return response.getBlocks()[0];
			return null;
		}
		return null;
	}
	
	public List<Block> getNextBlocks(int blockCount) throws IOException, InterruptedException {
		if (hasBlocks()) {
			if (this.connector == null)
				this.connector = new Connector();
			Pair<NetworkDestination, String> pair = this.blockIterator.next();
			NetworkDestination lastND = pair.a;
			List<Pair<NetworkDestination, BlockRequest>> requests = new ArrayList<>();
			BlockRequest br = new BlockRequest(this.fsName, pair.b);
			requests.add(new Pair<NetworkDestination, BlockRequest>(pair.a, br));
			while(this.blockIterator.hasNext() && --blockCount > 0) {
				pair = this.blockIterator.next();
				if(pair.a.equals(lastND)){
					br.addFilePath(pair.b);
				} else {
					br = new BlockRequest(this.fsName, pair.b);
					requests.add(new Pair<NetworkDestination, BlockRequest>(pair.a, br));
				}
				lastND = pair.a;
			}
			List<Block> blocks = new ArrayList<>();
			for(Pair<NetworkDestination, BlockRequest> request : requests){
				BlockResponse response = (BlockResponse) connector.sendMessage(request.a, request.b);
				for(Block block : response.getBlocks())
					blocks.add(block);
			}
			return blocks;
		}
		return null;
	}

	@Deserialize
	public QueryResponse(SerializationInputStream in) throws IOException, SerializationException {
		id = in.readString();
		isDryRun = in.readBoolean();
		elapsedTime = in.readLong();
		header = new JSONArray(in.readString());
		jsonResults = new JSONObject(in.readString());
		buildBlockDestinations();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(id);
		out.writeBoolean(isDryRun);
		out.writeLong(elapsedTime);
		out.writeString(header.toString());
		out.writeString(jsonResults.toString());
	}
}
