package simpledb.buffer;

import simpledb.file.*;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.ArrayList;


public class AdvancedBufferMgr extends BasicBufferMgr {
	private LinkedList<Buffer> emptyBuffers;
	private LinkedList<Buffer> unpinnedBuffers;
	private HashMap<Integer,Buffer> fullBuffers;
	private ArrayList<Buffer> clock = new ArrayList<Buffer>();
	private int index;
	AdvancedBufferMgr(int numbuffs) {
		super(numbuffs);
		for (Buffer buff : bufferpool)
		{
			emptyBuffers.add(buff);
			clock.add(buff);
		}
		index = 0;
	}
	
	
	synchronized Buffer pin(Block blk) {
		//find the blk if it already in a buffer
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			//if not in memory, look for an empty space
			buff = chooseEmptyBuffer();
			if (buff == null) {
				buff = chooseUnpinnedBuffer();
				if (buff == null) {
					return null;
				}
			}
			buff.assignToBlock(blk);
		}
		buff.pin();
		return buff;
	}
	
	//Uses clock replacement policy to pin a new block
	synchronized Buffer clockPin(Block blk) {
		//Check if block is already in buffer
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			//if not in memory already, look for next empty space
			buff = chooseEmptyBufferClock();
			if (buff == null) {
				//if there are no empty buffers, run replacement policy
				buff = chooseUnpinnedBufferClock();
				if (buff == null) {
					return null;
				}
			}
		}
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
		if (unpinnedBuffers.size() == 0) {
			return null;
		}
		return unpinnedBuffers.remove();
	}	
	
	private Buffer chooseEmptyBuffer() {
		if (emptyBuffers.size() == 0) {
			return null;
		}
		return emptyBuffers.pop(); 
	}
	
	private Buffer chooseUnpinnedBufferClock() {
		int size = clock.size();
		return clock.get(0);
//		while () {
//			
//		}
	}
	
	private Buffer chooseEmptyBufferClock() {
		if (emptyBuffers.size() == 0) {
			return null;
		}
		emptyBuffers.pop();
		Buffer buff = clock.get(index);
		index++;
		if (clock.size() == index) {
			index = 0;
		}
		return buff;
	}

}
