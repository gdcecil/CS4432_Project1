package simpledb.buffer;

import simpledb.file.*;

/**
 * 
 * C4432-Project1
 * This is the superclass for AdvancedBufferMrg. 
 * 
 * In order to facilitate inheritance and testing, access modifiers on 
 * several functions and variables have been changed from private
 * to protected. 
 * 
 * The subclass-superclass relationship between the old buffer 
 * manager (this) and the new buffer manager (AdvancedBufferMgr)
 * allows us to easily integrate the AdvancedBufferMgr into the database
 * while preserving all the functionality of BasicBufferManager.
 * 
 * 
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	
	//CS4432-Project1: bufferpool changed from private to
	//protected to grant access to AdvancedBufferMgr
	protected Buffer[] bufferpool;
	
	//CS4432-Project1: numAvailable changed from private to
	//protected to grant access to AdvancedBufferMgr
	protected int numAvailable;

	/**
	 * Creates a buffer manager having the specified number 
	 * of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects 
	 * that it gets from the class
	 * {@link simpledb.server.SimpleDB}.
	 * Those objects are created during system initialization.
	 * Thus this constructor cannot be called until 
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
	 * is called first.
	 * @param numbuffs the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. 
	 * If there is already a buffer assigned to that block
	 * then that buffer is used;  
	 * otherwise, an unpinned buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and
	 * pins a buffer to it. 
	 * Returns null (without allocating the block) if 
	 * there are no available buffers.
	 * @param filename the name of the file
	 * @param fmtr a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * @param buff the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	//CS4432-Project1: findExistingBuffer changed from 
	//private to protected to grant access to AdvancedBufferMgr
	protected Buffer findExistingBuffer(Block blk) {
		for (Buffer buff : bufferpool) {
			Block b = buff.block();
			if (b != null && b.equals(blk))
				return buff;
		}
		return null;
	}

	//CS4432-Project1: chooseUnpinnedBuffer changed from 
	//private to protected to grant access to AdvancedBufferMgr
	protected Buffer chooseUnpinnedBuffer() {
		for (Buffer buff : bufferpool)
			if (!buff.isPinned())
				return buff;
		return null;
	}
}
