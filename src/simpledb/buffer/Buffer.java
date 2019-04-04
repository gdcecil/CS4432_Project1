package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 */
public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   private int modifiedBy = -1;  // negative means not modified
   private int logSequenceNumber = -1; // negative means no corresponding log record
   
   //CS4432-Project1: keep track of a secondChance bit for the clock replacement policy
   private boolean secondChance = true;
   
   //CS4432-Project1: holds a unique int ID for the buffer
   private final int buffID;
   
   //CS4432-Project1: when set to true, disk pages are accessed as
   //usual. When false, disk pages are not accessed. this is used 
   //for testing purposes
   private boolean diskInteraction = true;
   
   /*
    *  CS4432-Project1
    * keep track of the number of buffers constructed,
    * used to assign each buffer a unique int id;
    */
   private static int bufferCount = 0;

   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * 
    * CS4432-Project1: assigns the unqiue bufferID, using the bufferCount
    * static variable.
    */
   public Buffer() {
	   //CS4432-Project1: set buffer ID to the buffer count, 
	   //and increment buffer count. Sycnchronized to make sure
	   //no to buffers get the same ID.
	   synchronized(Buffer.class)
	   {
		   buffID = bufferCount;
		   bufferCount++;
	   }
   }
   
   
   /**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * 
    * CS4432-Project1: if diskInteraction is false, instead
    * return 0, and do not access the page.
    * 
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return diskInteraction ? contents.getInt(offset) : 0;
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * 
    * CS4432-Project1: if diskInteraction is false, instead 
    * return 0, and do not access the page.
    * 
    * access the file (for testing purposes).
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return diskInteraction ? contents.getString(offset) : "";
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * 
    * CS4432-Project1: if diskInteraction is false, do not write to page,
    * but still update modified by and logSequenceNumber as appropriate.
    * 
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      if (diskInteraction) contents.setInt(offset, val);
   }

   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * 
    * CS4432-Project1: if diskInteraction is false, do not write to page,
    * but still update modified by and logSequenceNumber as appropriate.
    * 
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      if (diskInteraction) contents.setString(offset, val);
   }

   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
      return blk;
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    * 
    * CS4432-Project1: if diskInteraction is false, do not write to disk
    * and do not flush the log record. Still reset modifiedBy.
    */
   void flush() {
      if (modifiedBy >= 0) {
         if (diskInteraction)
         {
        	 SimpleDB.logMgr().flush(logSequenceNumber);
             contents.write(blk);
         }
         modifiedBy = -1;
      }
   }

   /**
    * Increases the buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decreases the buffer's pin count.
    */
   void unpin() {
      pins--;
      if (pins < 0) {
    	  pins = 0;
      }
   }

   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   boolean isPinned() {
      return pins > 0;
   }

   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * 
    * CS4432-Project1: if diskInteraction is false, 
    * do not read the block into the page. Flush is still
    * called, but that method will not write to disk either.
    * The pin count is still reset, and the block is still set.
    * 
    * @param b a reference to the data block
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      if (diskInteraction) contents.read(blk);
      pins = 0;
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * 
    * CS4432-Project1: if diskInteraction is false,
    * do not assign the new block to this buffer. This method
    * should not be called in situations where diskInteraction is 
    * false (i.e. testing). Flush is still called and the pins
    * are still set to 0. 
    * 
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      if (diskInteraction) 
      {
    	  fmtr.format(contents);
          blk = contents.append(filename);
      }
      pins = 0;
   }
   
   /**
    * CS4432-Project1
    * 
    * Sets the second chance bit of this buffer to the given value.
    * 
    * @param boolean bit, set the second chance bit to this value
    * 
    * @return nothing
    */
   public void setSecondChance(boolean bit) 
   {
	   secondChance = bit;
   }
   /**
    * CS4432-Project1
    * 
    * Return the value of the second chance bit, i.e. if this
    * buffer still has a second chance in the clock replacement policy.
    * 
    * @return the second chance bit.
    */
   public boolean hasSecondChance() 
   {
	   return secondChance;
   }
   
   /**
    * CS4432-Project1
    * 
    * Check if there's a block currently in this buffer
    * @return true if there is no block in this buffer, false otherwise.
    */
   public boolean isEmpty() 
   {
	   return (blk == null);
   }
   
   /**
    * CS4432-Project1
    * 
    * Gets the unique integer ID for this buffer. 
    * 
    * @return Buffer ID
    */
   public int id() 
   {
	   return buffID;
   }
   
   /**
    * CS4432-Project1
    * 
    * sets the boolean disk interaction to the given value. This determines 
    * whether or not this buffer will read/write to the disk. 
    * 
    * Turning off disk interactions is used for testing purposes.
    * 
    * @param access, set diskInteraction to this value
    */
   void setDiskInteraction(boolean access)
   {
	   diskInteraction = access;
   }
   
   /**
    * CS4432-Project1
    * 
    * Implementation of toString method for the buffer class. The
    * string contains the buffer ID, the pin count, pin status
    * (i.e. true if pin count > 0, false otherwise), second 
    * chance bit, and, if there is a block in the buffer, 
    * the toString representation of the block. 
    * 
    * @return String representing the fields of this buffer. 
    */
   @Override
   public String toString()
   {
	   String str = "Buffer ID: " + buffID + "\r\n" +
			   "Pin Count: " + pins + "\r\n";
	   if (blk == null)
		   str += "No block in buffer\r\n";
	   else {
		   str += "Holding Block " + blk.toString() + "\r\n";
	   }
	   str += "Pin status " + isPinned() + "\r\n";
	   str += "Second chance " + hasSecondChance() + "\r\n";
	   return str;
   }
}