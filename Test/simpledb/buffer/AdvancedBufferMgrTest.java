package simpledb.buffer;

import simpledb.file.Block;

class AdvancedBufferMgrTest {
	
	static void main(String[] args) {
		// TODO Auto-generated method stub
	}
	
	/**
	 * CS4432-Project1
	 * 
	 * Test the behavior of advanced buffer manager when 
	 * all buffers are pinned.
	 * @return true if tests pass. 
	 */
	boolean testFullBuffer()
	{
		boolean results = true;
		
		//number of buffers to use
		int numBuffs = 8;
		
		AdvancedBufferMgr mgr = new AdvancedBufferMgr(numBuffs, true);
		
		//get a list of buffers to use
		Block[] blockList = generateBlockList(numBuffs);
		
		//use an array to keep track of buffers in the pool,
		//so that we can use unpin, etc.
		Buffer[] bufferList = new Buffer[numBuffs];
		
		//pin each block and add the buffer it was pinned in to bufferList
		for (int i = 0; i < numBuffs; i++)
		{
			bufferList[i] = mgr.pin(blockList[i]);
		}
		
		//check that there are no buffers available
		boolean availableOK1 = mgr.numAvailable==0;
		
		//check that we can't pin a block, as all buffers are full
		boolean pinOK1 = mgr.pin(new Block("file", -1)) == null;
		
		//try unpinning the the last buffer in the pool
		mgr.unpin(bufferList[numBuffs - 1]);
		
		//check that available has been decrepremented
		boolean availableOK2= mgr.available() == 1;
		Buffer b = mgr.pin(new Block("file", -1));
		
		//check that the last buffer (the one we unpinned) has been replaced. 
		boolean pinOK2 = b.id() == bufferList[numBuffs-1].id();
	
		results = availableOK1 && availableOK2 && pinOK1 && pinOK2;
		return results;
	}
	
	/**
	 * CS4432-Project1
	 * 
	 * Test the functionality of pinning the block to a buffer, including
	 * the usage of the clock replacement policy.
	 * 
 	 * @return true if the tests pass
	 */
	boolean testPin()
	{
		boolean results = true;
		
		int numbuffs = 8; 
		
		AdvancedBufferMgr mgr = new AdvancedBufferMgr(numbuffs);
		
		
		return true;
	}
	
	/**
	 * CS4432-Project1
	 * 
	 * Generate a list of n blocks, each with filename "file" and 
	 * with block numbers offset, offset + 1,..., offset + n-1
	 * 
	 * @param n, number of blocks to put in the list
	 * @param offset, integer to start the block numbers at
	 * @return
	 */
	/*ArrayList<Block> */Block[] generateBlockList(int n)
	{
		Block[] blockList = new Block[n];
		
		for (int i = 0; i < n; i++)
			blockList[i] = new Block("file", i);
		return blockList;
	}

}
