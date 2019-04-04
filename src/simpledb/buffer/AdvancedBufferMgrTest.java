package simpledb.buffer;

import simpledb.file.Block;

/**
 * Runs tests on AdvancedBufferMgr, specifically regarding the
 * implementation of the clodck replacement policy.
 * 
 * Note: In the documentation throughout this file when we 
 * refer to the "nth buffer" we mean the location of the buffer
 * in the bufferList array. I.e. the first buffer is BufferList[0],
 * and the last buffer is BufferList[numBuffs-1]. Do not confuse this
 * with the buffer's unique ID, which is printed as part of the toString 
 * method of buffer.
 * 
 * Griffin Cecil, Michael Warms
 * @author mcwarms, gdcecil
 *
 */
class AdvancedBufferMgrTest {

	/**
	 * CS4432-Project1
	 * 
	 * Run the advancedBufferMgr tests, printing whether or not 
	 * the tests succeed. 
	 * @param args, do nothing with this.
	 */
	public static void main(String[] args) {
		
		boolean fullBuffTest = testFullBuffer();
		if (fullBuffTest) System.out.println("\nFull buffer test passed.\n");
		else System.out.println("\nFull buffer test failed.\n");
		
		boolean pinBuffTest = testPin();
		if (pinBuffTest) System.out.println("\nPin test passed.\n");
		else System.out.println("\nPin test failed.\n");
		
		if (fullBuffTest && pinBuffTest)
			System.out.println("All tests passed.");
	}

	/**
	 * CS4432-Project1
	 * 
	 * Test the behavior of advanced buffer manager when 
	 * all buffers are pinned.
	 * @return true if tests pass. 
	 */
	static boolean testFullBuffer()
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


		results = availableOK1 && pinOK1;
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
	static boolean testPin()
	{
		boolean results = true;

		int numBuffs = 8; 

		AdvancedBufferMgr mgr = new AdvancedBufferMgr(numBuffs, true);

		//get a list of buffers to use
		Block[] blockList = generateBlockList(numBuffs);

		//use an array to keep track of buffers in the pool,
		//so that we can use unpin, etc.
		Buffer[] bufferList = new Buffer[numBuffs];

		boolean pinToEmptyOK = true;
		boolean availableDecrementOK = true;
		//pin each block in an empty frame, add the buffer it was pinned in 
		//to bufferList, and then unpin the block.
		for (int i = 0; i < numBuffs; i++)
		{
			bufferList[i] = mgr.pin(blockList[i]);
			
			// available() should decremented and then be incremented
			// each iteration of the loop
			if (mgr.numAvailable != (numBuffs-1)) availableDecrementOK = false;
			
			// check that pin was successful
			if (bufferList[i] == null) pinToEmptyOK = false;
			mgr.unpin(bufferList[i]);
			
		}
		//check that all buffers are available
		boolean allAvailableOK= mgr.available() == numBuffs;
		Buffer b = mgr.pin(new Block("file", -1));

		//check that the last buffer (the one we unpinned)
		//has been replaced witht the correct block
		boolean pinOK1 = b.id() == bufferList[numBuffs-1].id()
				&& b.block().equals(new Block("file", -1));
		
		//check that all of the other buffers have secondChance bit false, 
		//as according to clock replacement policy chooseUnpinnedBuffer should
		//have iterated over each block in the pool before pinning in the last block
		boolean secondChanceOK = true;
		for (int i = 0; i < numBuffs -1; i++)
		{
			if (bufferList[i].hasSecondChance()) secondChanceOK = false;
		}
		
		//set secondchance bit of odd numbered buffers to true
		for (int i = 1; i < numBuffs; i+=2)
		{
			bufferList[i].setSecondChance(true);
		}
		
		//try pinning blocks in the even-numbered buffers (except for the last one,
		//which is already pinned, and confirm that the advanced buffer manager adds them
		//in order to the correct blocks
		boolean pinOK2 = true;
		for (int i = 0; i < numBuffs -1; i+= 2)
		{ 
			//pin a new block
			b = mgr.pin(new Block("file", i+20));
			
			//the state of the pool before the loop is that the last buffer is 
			//pinned, and the odd numbered buffers have their second chance bit 
			//set to true, so we expect that the new blocks are pinned in the locations
			//0,2,...,numBuffs-2, so we check these locations (they are indexed by i)
			if (b.id()!= bufferList[i].id()) pinOK2 = false; 
			
		}
		
		//each pin above should first set an odd numbered buffer's second chance bit to
		//false before moving and pinning in an even number buffer, so we verify that the second 
		//chance bits are set to false in the odd-numbered buffers. 
		boolean secondChanceOK2 = true;
		for (int i = 1; i < numBuffs-1; i+=2)
		{
			if (bufferList[i].hasSecondChance()) secondChanceOK2 = false;
		}

		results = pinToEmptyOK && availableDecrementOK && allAvailableOK && pinOK1
					&& secondChanceOK && pinOK2 && secondChanceOK2;
		return results;
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
	static Block[] generateBlockList(int n)
	{
		Block[] blockList = new Block[n];

		for (int i = 0; i < n; i++)
			blockList[i] = new Block("file", i);
		return blockList;
	}

}
