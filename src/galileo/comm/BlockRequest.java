package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class BlockRequest implements Event {

	private String fsName;
	private List<String> filePaths;

	public BlockRequest(String fsName, String filePath) {
		if (fsName == null || fsName.trim().length() == 0 || filePath == null || filePath.trim().length() == 0)
			throw new IllegalArgumentException(
					"Filesystem name and filepath on the server is needed to retrieve the block");
		this.fsName = fsName;
		this.filePaths = new ArrayList<String>();
		this.filePaths.add(filePath);
	}
	
	public void addFilePath(String filePath){
		this.filePaths.add(filePath);
	}

	public String getFilesystem() {
		return this.fsName;
	}

	public List<String> getFilePaths() {
		return this.filePaths;
	}

	@Deserialize
	public BlockRequest(SerializationInputStream in) throws IOException, SerializationException {
		this.fsName = in.readString();
		this.filePaths = new ArrayList<String>();
		in.readStringCollection(this.filePaths);
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeStringCollection(filePaths);
	}

}
