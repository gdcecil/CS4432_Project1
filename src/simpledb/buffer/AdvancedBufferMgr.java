package simpledb.buffer;

import simpledb.file.*;
import java.util.LinkedList;
import java.util.HashMap;

public class AdvancedBufferMgr extends BasicBufferMgr {
	private LinkedList<Buffer> emptyBuffers;
	private HashMap<Integer,Buffer> fullBuffers;
	AdvancedBufferMgr(int numbuffs) {
		super(numbuffs);
		for (Buffer buff : bufferpool)
		{
			emptyBuffers.add(buff);
		}
	}
	
	
	synchronized Buffer pin(Block blk) {
		//find the blk if it already in a buffer
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			//if not in memory, pin to a new buffer
			buff = chooseUnpinnedBuffer();
			if (buff == null) {
				return null;
			}
			buff.assignToBlock(blk);
		}
		if (buff.isPinned()) {
			
		}
		buff.pin();
		return buff;
	}
	
	private Buffer findExistingBuffer(Block blk)
	{
		if (blk != null && fullBuffers.containsKey(blk.hashCode())) 
		{
			Buffer b = fullBuffers.get(blk.hashCode());
			return b;
		}
		return null; 
	}
	
	//Returns the next 
	private Buffer chooseUnpinnedBuffer() {
		return emptyBuffers.pop();
	}	

}
