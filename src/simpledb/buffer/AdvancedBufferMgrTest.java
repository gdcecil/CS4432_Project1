package simpledb.buffer;

import simpledb.file.Block;

/**
 * CS4432-Project1 
 * 
 * Runs tests on AdvancedBufferMgr, specifically regarding the
 * implementation of the clodck replacement policy.
 * 
 * Note0: In the documentation throughout this file when we 
 * refer to the "nth buffer" we mean the location of the buffer
 * in the bufferList array. I.e. the first buffer is BufferList[0],
 * and the last buffer is BufferList[numBuffs-1]. Do not confuse this
 * with the buffer's unique ID, which is printed as part of the toString 
 * method of buffer.
 * 
 * Note1: The method pinNew is not tested, since its logic is exactly
 * the same as in the method pin, differing only in a call to the method
 * assignToNew of a buffer instead of a call to the method assignToBlock
 * 
 * Note2: The methods chooseUnPinnedBuffer and findExistingBuffer are not
 * explicitly tested here. In the original BasicBufferMgr class, these were
 * set to private, but we changed them to protected in order to design 
 * AdvancedBufferMgr as a subclass of BasicBufferMgr. However, these 
 * methods are not intended to be called outside of AdvancedBufferMgr, so
 * rather than testing them we test the behavior of Pin and other methods of 
 * AdvancedBufferMgr that are actually called by other classes.
 * 
 * Note3: AdvancedBufferMgrTest can be run without the simpledb server.
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

		boolean pinBuffTest = testPin();
		if (pinBuffTest) System.out.println("\nPin test passed.\n");
		else System.out.println("\nPin test failed.\n");

		boolean fullBuffTest = testFullBuffer();
		if (fullBuffTest) System.out.println("\nFull buffer test passed.\n");
		else System.out.println("\nFull buffer test failed.\n");

		boolean dupBuffTest = testBlockAlreadyExists();
		if (dupBuffTest) System.out.println("\nDuplicate block pin test passed.\n");
		else System.out.println("\nDuplicate block pin test failed.\n");

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

		//First print out the intitial buffer pool
		System.out.println("\r\n(testFullBuffer) Buffer pool after pinning 8 blocks:" +mgr);


		//check that there are no buffers available
		boolean availableOK1 = mgr.numAvailable==0;

		//check that we can't pin a block, as all buffers are full
		boolean pinOK1 = mgr.pin(new Block("file", -1)) == null;

		//This point needs to print out letting us know that the buffers
		//are still the same
		
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

		System.out.println("\r\n(testPin) Here is our empty buffer pool:"+mgr+"\r\n\r\n");

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

			System.out.println("\r\n(testPin) Buffer pool after pinning and unpinning block "
					+ blockList[i]+":"+mgr+"\r\n\r\n");

		}
		//check that all buffers are available
		boolean allAvailableOK= mgr.available() == numBuffs;

		Buffer b = mgr.pin(new Block("file", -1));

		System.out.println("\r\n(testPin) Buffer pool after pinning block " 
				+ b.block() +":" + mgr + "\r\n\r\n");

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

		System.out.println("\r\n(testPin) Buffer pool after setting the second"
				+" chance bits of the odd numbered buffers to true:\r\n" + 
				"(with respect to their position in the buffer pool array)\r\n"
				+ ":" + mgr + "\r\n\r\n");

		//try pinning blocks in the even-numbered buffers (except for the last one,
		//which is already pinned, and  confirm that the advanced buffer manager adds them
		//in order to the correct blocks
		boolean pinOK2 = true;
		for (int i = 0; i < numBuffs -1; i+= 2)
		{ 
			//pin a new block
			b = mgr.pin(new Block("file", i+20));

			System.out.println("\r\n(testPin) Buffer pool after pinning block " 
					+ b.block() + ":" + mgr + "\r\n\r\n");

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
	 * CS4432-project1
	 * 
	 * test the behavior of the advanced buffer manager when 
	 * trying to pin a block that already exists in the buffer
	 * 
	 * @return true if tests pass.
	 */
	static boolean testBlockAlreadyExists()
	{
		boolean results = true;

		//number of buffers to use
		int numBuffs = 8;

		AdvancedBufferMgr mgr = new AdvancedBufferMgr(numBuffs, true);

		//first test the case when there are more empty buffers
		//when we try to add a block that has already been pinned. 
		Block blk = new Block("file", 0);
		//pin a single block to the buffer
		Buffer buff1 = mgr.pin(blk);

		System.out.println("(testBlockAlreadyExists) Buffer pool after adding the block "
				+ blk +":" + mgr + "\r\n\r\n");

		//now we try adding the same block again
		Buffer buff2 = mgr.pin(blk);

		System.out.println("(testBlockAlreadyExists) Buffer pool after adding the block "
				+ blk +" again:" + mgr + "\r\n\r\n");

		//check that the block that was pinned is the same
		//and that there is still only one pinned buffer
		boolean addedDupEmptyOK = (buff1.id() == buff2.id())
				&& (mgr.available() == numBuffs-1); 

		//print the current buffer pool 
		System.out.println(mgr);

		//now test the case when there are no other empty 
		//spaces in the buffer.
		mgr = new AdvancedBufferMgr(numBuffs, true);

		//get an array of blocks
		Block[] blockList = generateBlockList(numBuffs);
		//use an array to keep track of the buffers

		//use an array to keep track of the buffers in the manager.
		Buffer[] buffList = new Buffer[numBuffs];

		for (int i = 0; i < numBuffs; i++)
		{
			buffList[i] = mgr.pin(blockList[i]);
		}
		System.out.println("(testBlockAlreadyExists) New buffer pool with "+numBuffs 
				+ " blocks pinned:" + mgr+ "\r\n\r\n");

		//the configuration after adding the blocks. each buffer
		//should be full, pinned, and with second chance true
		System.out.println(mgr);

		//now try adding every block in blocklist, verifying that
		//each time pin is called it returns the buffer where the 
		//same block is located in the buffer pool
		boolean addDupFullOK = true;
		for (int i = 0; i < numBuffs; i++) 
		{
			Buffer buff = mgr.pin(blockList[i]);

			//since we are pinning the same blocks in the same
			//order, pin should return the same buffer. Furthermore
			//available() should always return 0, since each block is
			//pinned.
			addDupFullOK = (buff.id() == buffList[i].id())
					&& (mgr.available()== 0);
		}

		System.out.println("(testBlockAlreadyExists) Buffer pool after calling pin on every block" 
				+ " that is in the buffer pool (i.e. each buffer is pinned for a second time:" + mgr+ "\r\n\r\n");

		//since we are pinning each block again without unpinning, the 
		//pin count of each block should be incremented by one.
		//thus when we unpin every buffer in the buffer mgr, there 
		//should still be 0 available buffers.

		for (Buffer b: buffList) mgr.unpin(b);
		System.out.println("(testBlockAlreadyExists) Buffer pool after calling unpin on every buffer:"  + mgr+ "\r\n\r\n");
		boolean multiPinOK = (mgr.available() == 0);

		//now we test the behavior of pinning an existing block
		// with the pin count and the second chance bit


		//first unpin each buffer and set the second chance bit to false
		for (Buffer b: buffList) 
		{
			mgr.unpin(b);
			b.setSecondChance(false);
		}
		System.out.println("(testBlockAlreadyExists) Buffer pool after calling unpin again on every buffer" + 
				"and setting each buffer's second chance bit to false:" + mgr+ "\r\n\r\n");
		//then pin each block
		for (Block b: blockList) mgr.pin(b);
		System.out.println("(testBlockAlreadyExists) Buffer pool after calling pin on each block" + 
				" that's already in the pool:" + mgr+ "\r\n\r\n");

		//now check that the block has been pinned and that the second
		//chance bit has been set to true. Also that available is again 0.
		boolean addDupFlagsOK = true;
		for (int i = 0; i< numBuffs; i++) 
		{
			if (! buffList[i].hasSecondChance() || // if the second chance bit is false,
					! buffList[i].isPinned() || // or the buffer isn't pinned
					! buffList[i].block().equals(blockList[i])) // or if the buffer is holding the wrong block
				addDupFlagsOK = false; // then the test fails
		}
		results = addedDupEmptyOK && addDupFullOK && multiPinOK && addDupFlagsOK;

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
