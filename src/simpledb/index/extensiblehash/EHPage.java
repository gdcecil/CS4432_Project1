package simpledb.index.extensiblehash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import static simpledb.index.extensiblehash.EHPageFormatter.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;


/**
 * CS4432-Project2
 * 
 * EHPage is an abstract class that is a parent for EHDir and 
 * EHBucket, implementing methods used by both classes to access
 * and manipulate data on disk. 
 * 
 * Classes that extend EHPage are essentially wrappers for disk 
 * pages that were formatted by EHPageFormatter. The methods in this 
 * class and its subclasses get and set data by passing the requests
 * to the transaction tx, which handles requests as per the scheduling
 * policy and then accesses buffers and so on. 
 * 
 * The corresponding page on disk holds some metadata and then records
 * (see EHPageFormatter for details). The TableInfo variable keeps track 
 * of the filename on disk where the blocks are stored and the schema 
 * information for the records.
 * 
 * In general, the way EHPage interacts with the disk imitates the 
 * implementation of BTree. Many of the methods here are either taken 
 * from or modified from records in BTreePage. 
 * 
 * In the page, records are indexed by slot number, which is related to
 * the byte offset through the method slotpos.
 * 
 * 
 * @author mcwarms, gdcecil
 *
 */
public abstract class EHPage 
{
	//Block EHPage will read/write to
	protected Block blk; 
	
	//TableInfo holding the filename 
	protected TableInfo ti;
	
	//Transaction to process reads/writes, requests for size of the file
	//and appends new blocks to file
	protected Transaction tx;
	
	//Stores the size of a record in bytes
	protected int slotsize;
	
	/**
	 * CS4432-Project2
	 * 
	 * Construct an EHPage with the block EHPage is wrapping, the TableInfo
	 * for information about the records in the block, and the transaction 
	 * that will be used to get and set data on disk. 
	 * 
	 * @param currentblk, Block EHPage references in the file 
	 * @param ti, TableInfo containing the schema for records in the block
	 * @param tx, Transaction for getting and setting data
	 */
	public EHPage (Block currentblk, 
							TableInfo ti, 
							Transaction tx) 
	{
		this.blk = currentblk; 
		this.ti = ti;
		this.tx = tx; 
		
		//Use the TableInfo object to get the length of a record in bytes
		slotsize = ti.recordLength();
		
		//Pin the current block, so that it will stay in the buffers 
		//so that we can read/write to it
		tx.pin(currentblk);
	}
	
	
	/**
	 * CS4432-Project2
	 * 
	 * In an extensible hash table, only the D rightmost bits are considered
	 * when computing the corresponding bucket number for a key, where D is depth. 
	 * 
	 * That is, we compute (hashcode) % 2^D.
	 * 
	 * This method is static, as this calculation is used when the depth
	 * does not correspond to the actual depth of the extensible hash index, 
	 * and furthermore, this calculation is independent of any instance of 
	 * this object. 
	 * 
	 * This can be interpreted as the bucket number with respect to depth.
	 * For global depth this is just the bucket number. Locally, in a bucket 
	 * with local depth d, for any key val in the bucket, computeBucketNumber(val, d)
	 * will have the same value.
	 * 
	 * However, this method is also called without referring to the global or 
	 * local depth, in particular it is used in calculations involving splitting
	 * buckets and incrementing the global depth.
	 * 
	 * @param val, val to hash and compute
	 * @param depth, power of two for the modulus 
	 * @return bucket number with respect to depth. For global depth this is just 
	 * the bucket number. In a bucket with local depth d, for any key val in the 
	 * bucket, computeBucketNumber 
	 * 
	 */
	static int computeBucketNumber (Constant val, int depth)
	{
		//Hash val and get its remainder under modulus 2^depth 
		//Here we compute 2^depth by just left shifting 1 depth
		//places. 
		System.out.println("val: " + val + " depth: " + depth);
		return val.hashCode() % (1 << depth);
	}
	
	/**
	 * CS4432-Project2 
	 * 
	 * Returns the maximum number of records that will fit in the block represented 
	 * by this object, i.e. the records that fit in the part of the block not 
	 * occupied by metadata.
	 * 
	 * Note that if the block is full, the zero-indexed position relative to the start 
	 * of the records is given by maxRecordsInBlock()-1
	 * 
	 * @return maxRecordsInBlock()
	 */
	public int maxRecordsInBlock() 
	{
		//Subtract the bytes occupied by metadata, and then divide by slotsize
		//Integer division discards the remaineder, so this will give the maximum
		//number of records that can fit in the block.
		return (BLOCK_SIZE - RECORD_START_OFFSET)/slotsize;
	}
	
	/**
	 * CS4432-Project2
	 * Set the depth (interpreted to be global/local as appropriate) of this page to the 
	 * specified value. This is part of the metadata stored in the block on disk, and so 
	 * a transaction is used to write it.
	 * 
	 * DEPTH_OFFSET, the offset of depth in the block, is a static final variable in 
	 * EHPageFormatter.
	 * 
	 * @param depth, value to set
	 * 
	 */
	protected void setDepth(int depth) 
	{
		//request the int at offset DEPTH_OFFSET from the transaction
		tx.setInt(blk, DEPTH_OFFSET, depth);
	}
	
	/**
	 * CS4432-Project2
	 * 
	 * Get the depth (interpreted to be global/local as appropriate) of this page from 
	 * the block on disk. This is part of the metadata stored in the block on disk, and so 
	 * a transaction is used to read it.
	 * 
	 * DEPTH_OFFSET, the offset of depth in the block, is a static final variable in 
	 * EHPageFormatter.
	 * 
	 * @return depth, depth of this index page as read from disk
	 */
	protected int getDepth() 
	{
		//pass a request to write an int at offset DEPTH_OFFSET to the transaction
		return tx.getInt(blk, DEPTH_OFFSET);
	}
	
	/* CS4432-Project2
	 * 
	 * The following methods are copied from simpledb.index.btree.BTreePage,
	 * and are use to access and manipulate records in the block referenced
	 * by either ExtensiHashBucket or ExtensiHashDir (subclasses of this).
	 * 
	 * Documentation has been elaborated on or added by us where lacking. 
	 * 
	 * If the method has been modified, "CS4432-Project2" is in the javadoc
	 * header. If this is absent, this is the function exactly as appears in 
	 * BTreePage
	 * 
	 */
	
	/*
	 * Start methods taken from BTreepage
	 */
	
	/**
	 * Closes the page by unpinning its buffer. This should only be 
	 * called when we are done using this instance of EHPage (viz. EHDir or EHBucket). 
	 * The behavior of any call to a method that reads or writes to disk after close() 
	 * has been called is undefined.
	 * 
	 * @author Edward Sciore 
	 */
	public void close() {
		if (blk != null)
			tx.unpin(blk);
		blk = null;
	}
	
	/**
	 * Returns true if the block is full (in terms of records, 
	 * i.e. this doesn't look at any metadata).
	 * 
	 * @author Edward Sciore
	 * 
	 * @return true if the block is full
	 */
	public boolean isFull() {
		return slotpos(getNumRecs()+1) >= BLOCK_SIZE;
	}
	
	/**
	 * CS4432-Project2: modified to use the constant RECORD_COUNT_OFFSET 
	 * to access the metadata instead of INT_SIZE. 
	 * 
	 * Returns the number of index records in this page. This value is 
	 * kept up to date by the insert and delete methods. The number of 
	 * records is stored as metadata on the page. 
	 * 
	 * Note: EHDir uses getNumRecs to store all the records in its file, 
	 * as opposed to just those in the page. See EHDir for details. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @return the number of index records in this page (resp. file in the 
	 * case of EHDir)
	 */
	public int getNumRecs() {
		return tx.getInt(blk, RECORD_COUNT_OFFSET);
	}
	
	/**
	 * Deletes the index record at the specified slot, by 
	 * copying the records at greater slot positions all down 
	 * by one slot. This ensures that all records are packed in 
	 * the sense that for any two records at slot i and j, for any 
	 * k with i < k < j, k holds a record.
	 * 
	 * Also decrements the number of records.
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot the slot of the deleted index record
	 */
	public void delete(int slot) {
		for (int i=slot+1; i<getNumRecs(); i++)
			copyRecord(i, i-1);
		setNumRecs(getNumRecs()-1);
		return;
	}
	
	
	/**
	 * Creates space for a new record at the specified slot by
	 * moving all the records at slots greater or equal to the parameter 
	 * slot up by one, and increments getNumRecs. Again this preserves the 
	 * packing of records as in delete. 
	 * 
	 * Note: this does not actually insert a record, rather it makes space for a 
	 * new record. This method should be called any time a new recorded is added 
	 * to the page (or at least care should be taken to preserve packing and 
	 * maintain getNumRecs).
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot the slot where the new record will be inserted
	 */
	protected void insert(int slot) {
		for (int i=getNumRecs(); i>slot; i--)
			copyRecord(i-1, i);
		setNumRecs(getNumRecs()+1);
	}

	/**
	 * Copies a record from slot from to slot to. This is used in insert 
	 * and delete to shift the records. 
	 * 
	 * Caution: this method overwrites the record at to, but does not 
	 * change the record at from. Care need also be taken to maintain 
	 * the packing of the records. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param from, slot to copy record from
	 * @param to, slot to copy record to.
	 */
	protected void copyRecord(int from, int to) {
		Schema sch = ti.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}
	/**
	 * 
	 * Gets the int int the record at slot number slot in field fldname (through a 
	 * transaction). This calls fldpos to compute the byte offset from the given slot number.
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, slot number to read from (used to compute a byte offset to read
	 * from)
	 * @param fldname, name of the field of the record to read the int from 
	 * 
	 * @return the int read from the record at slot in field field name
	 */
	protected int getInt(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return tx.getInt(blk, pos);
	}
	/** 
	 * 
	 * Gets the string in the record at slot number slot in field fldname (through
	 *  a transaction). This calls fldpos to compute the byte offset from the given slot number.
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, slot number to read from (used to compute a byte offset to read
	 * from)
	 * @param fldname, name of the field of the record to read the string from 
	 * 
	 * @return the string read from the record at slot in field field name
	 */
	protected String getString(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return tx.getString(blk, pos);
	}
	
	/**
	 * Get the Constant in field fldname in the record at slot. Constant 
	 * is either String or Int, this checks which and calls the appropriate get method. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, slot number to read from 
	 * @param fldname, field in record to access
	 * @return Constant val at slot in fldname field
	 */
	protected Constant getVal(int slot, String fldname) {
		int type = ti.schema().type(fldname);
		if (type == INTEGER)
			return new IntConstant(getInt(slot, fldname));
		else
			return new StringConstant(getString(slot, fldname));
	}
	
	/**
	 * Set the int in the record at slot in field fldname (using tx). Again calls 
	 * fldpos to compute the byte offset of the field of the record in the 
	 * page. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, slot number to write to
	 * @param fldname, field in record to write to
	 * @param val, int to write
	 */
	protected void setInt(int slot, String fldname, int val) {
		int pos = fldpos(slot, fldname);
		tx.setInt(blk, pos, val);
	}
	
	/**
	 * Set the string in the record at slot in field fldname (using tx). Again calls 
	 * fldpos to compute the byte offset of the field of the record in the 
	 * page. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, slot number to write to
	 * @param fldname, field in the record to write to
	 * @param val, string to write
	 */
	protected void setString(int slot, String fldname, String val) {
		int pos = fldpos(slot, fldname);
		tx.setString(blk, pos, val);
	}
	
	/**
	 * Checks if fldname is int or string and then calls either setInt or setString 
	 * as required. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, slot number to write to
	 * @param fldname, field in record to write to
	 * @param val, Constant to write
	 */
	protected void setVal(int slot, String fldname, Constant val) {
		int type = ti.schema().type(fldname);
		if (type == INTEGER)
			setInt(slot, fldname, (Integer)val.asJavaVal());
		else
			setString(slot, fldname, (String)val.asJavaVal());
	}
	
	
	/**
	 * Sets the integer at byte offset = RECORD_COUNT_OFFSET to the given 
	 * int n. As the name suggests this value is interpreted as the number of 
	 * records in the block. 
	 * 
	 * Note: in EHDir this instead keeps track of all the entries in the 
	 * directory file.
	 * 
	 * @author Edward Sciore
	 * 
	 * @param n, value to set
	 */
	protected void setNumRecs(int n) {
		tx.setInt(blk, RECORD_COUNT_OFFSET, n);
	}
	
	/**
	 * Returns the byte offset of field fldname in the record at the specified
	 * slot, using TableInfo ti and a call to slotpos
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, index of record 
	 * @param fldname, field in record
	 * @return byte offset of the field fldname in the record at slot in the page
	 */
	protected int fldpos(int slot, String fldname) {
		int offset = ti.offset(fldname);
		return slotpos(slot) + offset;
	}
	
	/*
	 * End methods taken from BTreePage
	 */
	
	

	/**
	 * CS4432-Project2
	 * 
	 * Edited from the slotpos method in BTreePage to support the usage of
	 * of a symbolic constant (where the records start) 
	 * 
	 * Computes the byte offset of the record number slot (zero based) in the page. 
	 * 
	 * For example, the byte offset of the record at slot 0 is the byte offset of the 
	 * start of the records in the page, the byte offset of the record at slot 1 is 
	 * RECORD_START_OFFSET + 1*Ti.recordLength(), and so on, so the byte offset of the 
	 * record at slot n is given by RECORD_START_OFFSET + n*Ti.recordLength().
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot, index to convert to a byte offset
	 * @return byte offset of slot in page
	 */
	protected int slotpos(int slot)
	{
		return RECORD_START_OFFSET + (slot * slotsize);
	}

}
