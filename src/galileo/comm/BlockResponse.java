package galileo.comm;

import java.io.IOException;

import galileo.dataset.Block;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class BlockResponse implements Event {
	private Block[] blocks;

	public BlockResponse(Block[] blocks) {
		this.blocks = blocks;
	}

	public Block[] getBlocks() {
		return this.blocks;
	}

	@Deserialize
	public BlockResponse(SerializationInputStream in) throws IOException, SerializationException {
		int numBlocks = in.readInt();
		this.blocks = new Block[numBlocks];
		for(int i = 0; i < numBlocks; i++)
			this.blocks[i] = new Block(in);
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeInt(blocks.length);
		for(Block block : blocks)
			block.serialize(out);
	}
}
