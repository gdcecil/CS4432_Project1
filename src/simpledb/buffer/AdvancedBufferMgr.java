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

	private HashMap<Integer,Buffer> fullBuffers = new HashMap<Integer,Buffer>();
	private ArrayList<Buffer> clock = new ArrayList<Buffer>();

	private int index;

	AdvancedBufferMgr(int numbuffs) {
		super(numbuffs);
		for (Buffer buff : bufferpool)
		{
			//emptyBuffers.add(buff);
			clock.add(buff);
		}
		index = 0;
	}

	/**
	 * flush dirty buffers modified by specified transaction, in the 
	 * clock buffer pool 
	 * 
	 * @param txnum transactionid
	 */
	@Override
	synchronized void flushAll(int txnum) 
	{
		for (Buffer buff : clock)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * pin a given block to a frame in the buffer pool 
	 * 
	 * @param Block blk, block to pin 
	 * 
	 * @return buffer where the block was assigned, 
	 * or null if there are no unpinned buffers
	 */
	@Override
	synchronized Buffer pin(Block blk) {
		//find the blk if it already in a buffer
		Buffer buff = findExistingBuffer(blk);

		if (buff == null) {
			// if the block doesn't exist in the buffer, 
			// find an unpinnedbuffer and pin it
			buff = chooseUnpinnedBuffer();
			if (buff == null) {
				// if there are no unpinned buffers return null
				return null;
			}

			// if the buffer is not empty remove its entry from the hash table
			if (! buff.isEmpty()) 
			{
				fullBuffers.remove(buff.block().hashCode());
			}
			// assign this block to the buffer we found
			buff.assignToBlock(blk);
			fullBuffers.put(blk.hashCode(), buff);
		}
		// pin this buffer frame
		if (!buff.isPinned()) numAvailable--;
		buff.pin();
		// set its second chance bit
		buff.setSecondChance(true);

		return buff;
	}
	/**
	 * Override the super pinNew method to be compatible 
	 * with the clock replacement policy 
	 *
	 * @parem filename the name of the file 
	 * @param fmtr formatter object to format the new block 
	 * 
	 * @return the pinned buffer or null if there are no unpinned buffers
	 */
	@Override
	synchronized Buffer pinNew(String filename, PageFormatter fmtr)
	{
		// choose an unpinned buffer 
		Buffer buff = chooseUnpinnedBuffer(); 

		// if there's no unpinned buffer, return 
		if (buff == null)
		{
			return null;
		}

		// if the buffer is not empty remove its entry from the hash table
		if (! buff.isEmpty()) 
		{
			fullBuffers.remove( buff.block().hashCode());
		}
		// assign buffer to a new block
		buff.assignToNew(filename, fmtr);

		numAvailable--;

		//but the buffer in the full buffers hash table 
		fullBuffers.put(buff.block().hashCode(), buff);

		// pin it 
		buff.pin();

		// set its second chance bit
		buff.setSecondChance(true);
		return buff;
	}

	/**
	 * find an existing buffer in the pool that holds the given block
	 * 
	 * @param blk block to check the buffer pool for
	 * 
	 * @return the buffer with the given block or null if it doesn't
	 */
	@Override
	protected Buffer findExistingBuffer(Block blk)
	{
		if (blk != null && fullBuffers.containsKey(blk.hashCode())) 
		{
			Buffer b = fullBuffers.get(blk.hashCode());
			return b;
		}
		return null; 
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

		// if there's no empty frame use the clock replacement policy
		// to find a buffer frame to replace
		if (available() == 0)
			return null;


		// we check if the current buffer frame is empty
		// because of the way the clock replacement algorithm works 
		// either the frame the clock is currently pointing at will be empty
		// or no frame will be empty
		if (clock.get(index).isEmpty()) {

			//increment the clock pointer
			index = (index + 1) % clock.size();

			return buff;
		}


		//boolean fullFlag = true;
		boolean endFlag = true;

		while (endFlag) {


			buff = clock.get(index);

			//if the buffer isn't pinned, 
			// check if the second chance bit is set 
			// if it is, set it to false and move to the next 
			// buffer in the pool. If it isn't, return this buffer
			if (!buff.isPinned()) {
				//fullFlag = false;
				if (buff.hasSecondChance()) {
					buff.setSecondChance(false);
				} else {
					index = (index + 1) % clock.size();
					return buff;
				}
			}	
			index = (index + 1) % clock.size();
		}

		return null;
	}

	@Override
	public String toString() {
		String str = "";
		for (Buffer buff: clock) {
			str += buff.toString();
		}
		System.out.println(str);
		return str;
	}
}
