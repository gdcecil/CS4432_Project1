package simpledb.buffer;

import simpledb.file.*;
import java.util.LinkedList;
import java.util.HashMap;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
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
	
	
	// Assumption: Blocks are not removed from buffer until replaced by another block
	// This means that once filled, a buffer will never be empty unless being replaced
	private Buffer chooseUnpinnedBufferClock() {
		Buffer buff = clock.get(index);
		if (clock.get(index).isEmpty()) {
			index++;
			if (clock.size() == index) {
				index = 0;
			}
			return buff;
		}
		int initialIndex = index;
		boolean fullFlag = true;
		boolean endFlag = true;
		while (endFlag) {
			buff = clock.get(index);
			if (!buff.isPinned()) {
				fullFlag = false;
				if (buff.hasSecondChance()) {
					buff.setSecondChance(false);
				} else {
					return buff;
				}
			}	
			index = (index + 1) % clock.size();
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

	@Override
	public String toString() {
		String str = "";
		for (Buffer buff: clock) {
			str += buff.toString();
		}
		try {
			PrintWriter writer = new PrintWriter("buffer-output", "UTF-8");
			writer.println(str);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return str;
	}
}
