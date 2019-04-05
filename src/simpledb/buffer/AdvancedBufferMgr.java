package simpledb.buffer;
import simpledb.file.*;
import java.util.HashMap;
import simpledb.buffer.Buffer;

/**
 * CS4432-Project1
 * 
 * Our implementation of an improved buffer manager. AdvancedBufferMgr
 * extends BasicBufferMgr, and is able to be used wherever BasicBufferMgr
 * is used (for example, BufferMgr has a reference to a BasicBufferMgr 
 * that references an AdvancedBufferMgr). 
 * 
 * This buffer manager implements a clock replacement policy (the buffer
 * frames are stored in the same array created by the superclass). 
 * 
 * Instead of just scanning the frames, AdvancedBufferMgr implements 
 * a HashMap to keep of which buffers are holding which blocks.
 * 
 * 
 * Griffin Cecil, Michael Warms
 * @author mcwarms, gdcecil
 *
 */
public class AdvancedBufferMgr extends BasicBufferMgr {
	
	//CS4432-Project1: HashMap with Integer keys and Buffer values. A pair (K,V) 
	//in the HashMap means that Buffer V is currently holding a Block B with 
	//B.hashCode() = K (regardless of whether or not the block is pinned in the 
	//buffer).
	private HashMap<Integer,Buffer> fullBuffers = new HashMap<Integer,Buffer>();
	
	
	//CS4432-Project1: This is the "clock hand" used in clock replacement policy. 
	//When we need to replace a buffer the index points to the first buffer we check.
	//The index moves to a buffer, and then replaces it (that is, it points to the 
	//last buffer that was replaced, unless each buffer is empty. 
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
	 * CS4432-Project1
	 *
	 * Overrides BasicBufferMgr.Pin(Block blk)
	 *
	 * Works almost exactly the same way as super.pin)(), adding
	 * compatibility with the fullBuffers HashMap and making 
	 * sure to update the secondChance bit of the pinned block.
	 * 
	 * @param Block blk, block to pin 
	 * 
	 * @return buffer where the block was assigned, 
	 * or null if there are no unpinned buffers
	 */
	@Override
	synchronized Buffer pin(Block blk) {
		
		//Check if this block is already in a buffer
		Buffer buff = findExistingBuffer(blk);
		
		//If buff is null, then the block is not in any buffer
		//in the buffer list.
		if (buff == null) {
			
			//Choose an unpinned buffer to pin the block in
			//this is either an empty buffer, or a buffer 
			//that has been selected for replacement
			buff = chooseUnpinnedBuffer();
			
			//if buff == null, there are no unpinned buffers in the pool
			if (buff == null) {
				//In that case, this function will return null
				return null;
			}

			//Update the HashMap of full buffers. 
			
			//If the buffer we found was empty, we only need
			//to add the entry the hash table, 
			if (! buff.isEmpty()) 
			{
				//if the buffer isn't empty, remove it's entry in the HashMap
				fullBuffers.remove(buff.block().hashCode());
			}
			//Overwrite the buffer with the new block.
			buff.assignToBlock(blk);
			
			//Add the Hashcode of the new block and the buffer 
			//it was placed in to the Hashmap
			fullBuffers.put(blk.hashCode(), buff);
		} 
		
		//If this buffer is not already pinned,
		//Decrement the number of available buffers 
		//(i.e. number of buffers that are not pinned)
		//numAvailable is a super class instance variable
		if (!buff.isPinned()) numAvailable--;
		
		//Either way, increment the pin count of this buffer.
		buff.pin();
		
		//set the second chance bit of this buffer to true, 
		//in accordance with the clock replacement policy.
		buff.setSecondChance(true);

		return buff;
	}
	/**
	 * CS4432-Project1
	 * 
	 * Overrides the pinNew method in BasicBufferMgr. 
	 * 
	 * pinNew works almost exactly the same as super.pinNew method, 
	 * adding compatibility with the fullBuffers HashMap and 
	 * setting the second chance of pinned buffers to true.
	 *
	 * @parem filename the name of the file 
	 * @param fmtr formatter object to format the new block 
	 * 
	 * @return the pinned buffer or null if there are no unpinned buffers
	 */
	@Override
	synchronized Buffer pinNew(String filename, PageFormatter fmtr)
	{
		//Choose an unpinned buffer, using our implementation 
		//of chooseUnpoinnedBuffer()
		Buffer buff = chooseUnpinnedBuffer(); 

		//If chooseUnpinnedBuffer returns null, there are no unpinned
		//buffers; Return null
		if (buff == null)
		{
			return null;
		}
		
		
		//Update the HashMap
		
		// If the buffer returned by chooseUnpinnedBuffer is empty, 
		// we just add a new entry to to the HashMap. If not, 
		if (! buff.isEmpty()) 
		{
			//remove the pair of Buff and the hashcode of the block 
			//we intend to replace.
			fullBuffers.remove( buff.block().hashCode());
		}
		
		// Allocate a block (through the buffer) in the given file
		buff.assignToNew(filename, fmtr);
		
		//Since chooseUnPinnedBuffer returns an unpinned buffer, 
		//we always decrement the number of available unpinned buffers. 
		numAvailable--;

		//Add the pair consisting of the hascode of this block
		//and its buffer to the HashMap
		fullBuffers.put(buff.block().hashCode(), buff);

		//increment the pin count of the buffer. 
		buff.pin();

		//Since this block was pinned, the secondChance bit is set to true.
		buff.setSecondChance(true);
		
		return buff;
	}
	

	/**
	 * CS4432-Project1
	 * 
	 * Overrides BasicBufferMgr.findExistingBuffer(Block Blk).
	 * 
	 * AdvancedBufferMgr uses a HashMap to keep track of which blocks 
	 * are in each Buffer (using Block.hashCode() for the key, so 
	 * findExistingBuffer(Block blk) simply checks that the block 
	 * is not null and if the given Block's hashcode is a key
	 *  in the hashmap. 
	 *  
	 * If this block's hashcode is a key in the HashMap, then
	 * the corresponding Buffer in the map is returned.
	 * 
	 * If the Block is null or its hashcode is not in the Hashmap, 
	 * null is returned. 
	 * 
	 * Because of the usage of the HashMap, this method is a marked
	 * improvement over super.findExistingBuffer, which just scans 
	 * the entire buffer pool.
	 * 
	 * @param blk block to check the buffer pool for
	 * 
	 * @return the buffer with the given block, or null if 
	 * it is not present in the buffer.
	 */
	@Override
	protected Buffer findExistingBuffer(Block blk)
	{
		//Check that the block is not null and if its hashcode is in fullBuffers.
		if (blk != null && fullBuffers.containsKey(blk.hashCode())) 
		{
			//if so, return the Buffer holding this block
			Buffer b = fullBuffers.get(blk.hashCode());
			return b;
		}
		//otherwise return null
		return null; 
	}


	// Assumption: Blocks are not removed from buffer until replaced by another block
	// This means that once filled, a buffer will never be empty unless being replaced
	/**
	 * CS4432-Project1
	 * 
	 * Overrides BasicBufferMgr.chooseUnpinnedBuffer
	 * 
	 * The chooseUnpinnedBuffer method here implements the 'clock' 
	 * replacement policy: when it's necessary to replace the block
	 * in a buffer, starting with with buffer in the pool pointed to by 
	 * index, and 
	 * 		If the buffer is pinned, increment the index (skip it).
	 * 
	 * 		If the buffer is not pinned and the second chance bit is true,
	 * 		set the second chance bit to false, then increment the index.
	 * 
	 * 		If the buff is not pinned its second chance bit is false, 
	 * 		replace the block in this buffer. Do not increment the index.
	 * 
	 * The buffer pool is stored in the array bufferpool constructed in the 
	 * superclass, and the incrementing here is done modulo bufferpool.length.
	 * 
	 * We assume that Blocks will never be removed, only replaced, and that this 
	 * replacement only ever occurs in chooseUnpinnedBuffer as part of the 
	 * clock replacement policy.
	 * 
	 * This implies once each buffer holds a block, there will never be another
	 * empty buffer. 
	 * 
	 * In fact, when there are still empty buffers in the pool, the buffer adjacent
	 * to the index position will always be empty, allowing for O(1) access to an
	 * empty buffer whenever one exists. 
	 * 
	 * If there are no unpinned buffers, null is returned and no other action
	 * occurs. 
	 * 
	 * @return A buffer that is either unpinned or empty, or null in the case that
	 * there are no unpinned buffers available
	 */
	@Override
	protected Buffer chooseUnpinnedBuffer() {

		//If there are no unpinned buffers, return null.
		if (available() == 0)
			return null;

		// If the buffer currently pointed to is empty, return it 
		// and do not change the index. This only occurs the first time 
		// chooseUnpinnedBuffer is called, since the index will always point
		// where the last block was assigned. 
		if (bufferpool[index].isEmpty()) return bufferpool[index];
		
		// If the next buffer is empty, return it and increment the index
		// After the buffer becomes full for the first time, this code
		// is no longer used. 
		if (bufferpool[(index+1) % bufferpool.length].isEmpty()) 
		{
			//increment the clock pointer 
			index = (index + 1) % bufferpool.length;
			
			//return the empty buffer at the new index location
			Buffer buff = bufferpool[index];
			return buff;
		}
		
		
		boolean b = true;
		
		//In the case where there are unpinned buffers and no empty buffers
		//We must replace a block in a buffer according to the clock replacement
		//policy. We loop through the bufferpool, setting second the chance 
		//bits to false until we eventually find an upinned buffer with 
		//second chance bit set to fallse
		
		//This loop must eventually terminate, as if there were no available 
		//buffers then this method would have returned null.
		while (b) {
			
			//Check the buffer pointed to by the index
			Buffer buff = bufferpool[index];

			//if this buffer is pinned, we skip it
			if (!buff.isPinned()) 
			{
				//Check the secondChance bit. Since the buffer is not pinned, 
				//either return the buffer (if secondChance is set to false)
				//or set secondChance to false, and go to the next buffer in 
				//the pool
				if (buff.hasSecondChance()) 
				{
					// set the second chance bit to false
					buff.setSecondChance(false);
				}
				else 
					return buff; //return this buffer since it's second chance is false
			}	
			//increment the index
			//this is not called when the final buffer is returned.
			index = (index + 1) % bufferpool.length;
		}

		return null;
	}

	/**
	 * CS4432-Project1 
	 * 
	 * toString method for Advanced buffer manager. The string has the 
	 * current index (the pointer for the clock replacement policy) and 
	 * the toString() of each buffer in the bufferpool.
	 * 
	 * @return String representation of the buffer pool
	 */
	@Override
	public String toString() {

		//Put the index in the string
		String str = "\r\n\r\nCurrent index: "+ index + "\r\n\r\n"; 
		str += "////////////////////\r\n";
		
		//add Buff.toString() for each buffer in the pool
		for (Buffer buff: bufferpool) {
			str += buff.toString();
			str += "////////////////////\r\n";

		}
		return str;
	}

}
