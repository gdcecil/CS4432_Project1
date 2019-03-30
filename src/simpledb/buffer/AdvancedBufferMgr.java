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
		buff.setSecondChance(true);
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
				buff = chooseUnpinnedBuffer();
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
	
	private Buffer chooseEmptyBuffer() {
		if (emptyBuffers.size() == 0) {
			return null;
		}
		return emptyBuffers.pop(); 
	}
	
	
	// Assumption: Blocks are not removed from buffer until replaced by another block
	// This means that once filled, a buffer will never be empty unless being replaced
	/**
	 * find either an empty buffer frame or the next buffer to replace 
	 * according to the clock replacement policy
	 * 
	 * @return an empty buffer in the pool or one that is unpinned, or null if
	 * 		   there are no unpinned buffers in the pool
	 */
	@Override
	protected Buffer chooseUnpinnedBuffer() {
		
		//check the buffer that the clock is currently looking at
		Buffer buff = clock.get(index);
		
		
		// we check if the current buffer frame is empty
		// because of the way the clock replacement algorithm works 
		// either the frame the clock is currently pointing at will be empty
		// or no frame will be empty
		if (clock.get(index).isEmpty()) {
			
			//increment the clock pointer
			index = (index + 1) % clock.size();
			
			return buff;
		}
		
		// if there's no empty frame use the clock replacement policy
		// to find a buffer frame to replace
		int initialIndex = index;
		boolean fullFlag = true;
		boolean endFlag = true;
		
		while (endFlag) {
			
			
			buff = clock.get(index);
			
			//if the buffer isn't pinned, 
			// check if the second chance bit is set 
			// if it is, set it to false and move to the next 
			// buffer in the pool. If it isn't, return this buffer
			if (!buff.isPinned()) {
				fullFlag = false;
				if (buff.hasSecondChance()) {
					buff.setSecondChance(false);
				} else {
					index = (index + 1) % clock.size();
					return buff;
				}
			}	
			
			
			index = (index + 1) % clock.size();
			// if we have gone through all buffers without finding 
			// an unpinned one, return null.
			if (index == initialIndex) {
				if (fullFlag == true) {
					return null;
				}
			}
		}
		
		return null;
	}
	
	// Replaced by clock replacement policy
	private Buffer chooseEmptyBufferClock() {
		Buffer buff = clock.get(index);
		if (clock.get(index).isEmpty()) {
			index++;
			if (clock.size() == index) {
				index = 0;
			}
			return buff;
		}
		return null;
	}

}
