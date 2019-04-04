package simpledb.buffer;

import simpledb.file.*;
import java.util.HashMap;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public class AdvancedBufferMgr extends BasicBufferMgr {
	/*Hashmap to track the buffers in bufferpool which currently hold a block */
	private HashMap<Integer,Buffer> fullBuffers = new HashMap<Integer,Buffer>();

	private int index = 0;
	
	/**
	 * CS4432-Project1
	 * 
	 * Constructor that takes a number of buffers, and 
	 * then calls the constructor for the superclass, BasicBufferManager, 
	 * with this value.
	 * 
	 * @param numbuffs, number of buffers to manage.
	 */
	AdvancedBufferMgr(int numbuffs) {
		super(numbuffs);
		//for (Buffer buff : bufferpool) clock.add(buff);
	}
	
	/**
	 * CS4432-Project1
	 * 
	 * Alternate constructor used for testing purposes that
	 * calls the usual constructor and then turns off disk interaction 
	 * for each buffer if output is true.
	 * 
	 * @param numbuffs, number of buffers
	 * @param output, if true turn off disk interaction
	 */
	AdvancedBufferMgr(int numbuffs, boolean noDiskInteraction)
	{
		this(numbuffs);
		
		if (noDiskInteraction) 
		{
			for (Buffer b : bufferpool)
				b.setDiskInteraction(false);
		}
	}

	/**
	 * flush dirty buffers modified by specified transaction, in the 
	 * clock buffer pool 
	 * 
	 * @param txnum transactionid
	 */
//	@Override
//	synchronized void flushAll(int txnum) 
//	{
//		for (Buffer buff : clock)
//			if (buff.isModifiedBy(txnum))
//				buff.flush();
//	}

	/**
	 * CS4432-Project1
	 *
	 * Pin a given block to a frame in the buffer pool. This method follows
	 * the same logic as the superclass pin, but here we makes sure to update
	 * the hash table of full buffers, and the second chance bit of the pinned
	 * block. 
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
			// update the hashtable to reflect this change
			fullBuffers.put(blk.hashCode(), buff);
		}
		// pin this buffer frame
		if (!buff.isPinned()) numAvailable--;
		buff.pin();
		System.out.println(buff.id());
		
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

		// put the buffer in the full buffers hash table 
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
		
		// if there's no unpinned frame return null
		if (available() == 0)
			return null;
		
		// if the buffer currently pointed to is empty return it 
		// and leaved index unchanged
		// should only occur the first time this is called.
		
		//if (clock.get(index).isEmpty()) return clock.get(index);
		if (bufferpool[index].isEmpty()) return bufferpool[index];
		
		System.out.println("Replacing buffer");
		// if the next buffer is empty, return it and increment the index
		
//		if (clock.get((index + 1) % clock.size()).isEmpty()) {
//			
//			//increment the clock pointer 
//			index = (index + 1) % clock.size();
//			Buffer buff = clock.get(index);
//			System.out.println("Storing in buffer " + buff.id() + "\n");
//			return buff;
//		}
		
		if (bufferpool[(index+1) % bufferpool.length].isEmpty()) {
			
			//increment the clock pointer 
			index = (index + 1) % bufferpool.length;
			Buffer buff = bufferpool[index];
			System.out.println("Storing in buffer " + buff.id() + "\n");
			return buff;
		}
		
		


		//does nothing
		boolean endFlag = true;

		//while true
		while (endFlag) {

			//look at the current buffer in the clock
			
			//Buffer buff = clock.get(index);
			Buffer buff = bufferpool[index];

			// check that the current buff is not pinned
			if (!buff.isPinned()) 
			{
				// check the second chance bit
				if (buff.hasSecondChance()) 
				{
					// set the second chance bit to false if it was previously true
					buff.setSecondChance(false);
				}
				// if it's unpinned and secondchance = false return the buffer
				else return buff; 
			}	
			//look at the next buffer
			//index = (index + 1) % clock.size();
			index = (index + 1) % bufferpool.length;
		}

		return null;
	}

	@Override
	public String toString() {
		System.out.println("Printing out bufferpool\n");
		System.out.println("Current index " + index + "\n");
		String str = "";
		
//		for (Buffer buff: clock) {
//			str += buff.toString();
//			str += "////////////////////\n";
//			
//		}
		for (Buffer buff: bufferpool) {
			str += buff.toString();
			str += "///////////////////\r\n";
			
		}
		
		System.out.println(str);
		
//		try {
//			writeFile(str);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			
//		}
		
		return str;
	}
	
	
//	public int getIndex()
//	{
//		return index;
//	}

	
//	private String writeFile(String str) throws IOException {
//		FileWriter writer = new FileWriter("C:/Users/Griffin/Desktop/sample.txt");
//		
//		writer.write(str);
//		writer.close();
//		System.out.println(str);
//		System.setOut(new PrintStream(new FileOutputStream("C:/Users/Griffin/Desktop/sample.txt")));
//		System.out.println(str);
//		return str;
//	}
}
