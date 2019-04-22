package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;


/**
 * ExtensiHashPage
 * 
 * 
 * @author mcwarms, gdcecil
 *
 */
public abstract class ExtensiHashPage 
{
	protected Block blk; 
	protected TableInfo ti;
	protected Transaction tx;
	protected int slotsize;
	
	public ExtensiHashPage (Block currentblk, 
							TableInfo ti, 
							Transaction tx) 
	{
		this.blk = currentblk; 
		this.ti = ti;
		this.tx = tx; 
		slotsize = ti.recordLength();
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
	 * This method is implemented statically, as this calculation is used when the depth
	 * does not correspond to the actual depth of the extensible hash index.
	 * 
	 * @param val, val to hash and compute
	 * @param depth, power of two for the modulus 
	 * @return bucket number for the given value, if global depth = depth
	 */
	static int computeBucketNumber (Constant val, int depth)
	{
		return val.hashCode() % (1 << depth);
	}
	
	/**
	 * CS4432-Project2
	 * Set the depth (interpreted to be global/local as appropriate) of this page to the 
	 * specified value.
	 * 
	 * @author mcwarms, gdcecil
	 */
	protected void setDepth(int depth) {
		tx.setInt(blk, EHPageFormatter.DEPTH_OFFSET, depth);
	}
	
	/**
	 * CS4432-Project2
	 * 
	 * Get the depth (interpreted to be global/local as appropriate) of this page
	 * 
	 * @return depth depth of this index page
	 */
	protected int getDepth() 
	{
		return tx.getInt(blk, EHPageFormatter.DEPTH_OFFSET);
	}
	
	/* CS4432-Project2
	 * 
	 * The following methods are copied from simpledb.index.btree.BTreePage,
	 * and are use to access and manipulate records in the block referenced
	 * by either ExtensiHashBucket or ExtensiHashDir (subclasses of this).
	 * 
	 */
	
	/*
	 * Start methods taken from BTreepage
	 */
	
	/**
	 * Closes the page by unpinning its buffer.
	 */
	public void close() {
		if (blk != null)
			tx.unpin(blk);
		blk = null;
	}
	
	/**
	 * Returns true if the block is full.
	 * 
	 * @author Edward Sciore
	 * 
	 * @return true if the block is full
	 */
	public boolean isFull() {
		return slotpos(getNumRecs()+1) >= BLOCK_SIZE;
	}
	
	/**
	 * Returns the number of index records in this page.
	 * 
	 * @author Edward Sciore
	 * 
	 * @return the number of index records in this page
	 */
	public int getNumRecs() {
		return tx.getInt(blk, INT_SIZE);
	}
	
	/**
	 * Deletes the index record at the specified slot.
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
	 * moving all the records at and after 0slot by one slot.
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
	 * 
	 * @author Edward Sciore
	 * 
	 * @param from
	 * @param to
	 */
	protected void copyRecord(int from, int to) {
		Schema sch = ti.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}
	/**
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @return
	 */
	protected int getInt(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return tx.getInt(blk, pos);
	}
	/** 
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @return
	 */
	protected String getString(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return tx.getString(blk, pos);
	}
	
	/**
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @return
	 */
	protected Constant getVal(int slot, String fldname) {
		int type = ti.schema().type(fldname);
		if (type == INTEGER)
			return new IntConstant(getInt(slot, fldname));
		else
			return new StringConstant(getString(slot, fldname));
	}
	
	/**
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @param val
	 */
	protected void setInt(int slot, String fldname, int val) {
		int pos = fldpos(slot, fldname);
		tx.setInt(blk, pos, val);
	}
	/**
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @param val
	 */
	protected void setString(int slot, String fldname, String val) {
		int pos = fldpos(slot, fldname);
		tx.setString(blk, pos, val);
	}
	
	/**
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @param val
	 */
	protected void setVal(int slot, String fldname, Constant val) {
		int type = ti.schema().type(fldname);
		if (type == INTEGER)
			setInt(slot, fldname, (Integer)val.asJavaVal());
		else
			setString(slot, fldname, (String)val.asJavaVal());
	}
	
	
	/**
	 * @author Edward Sciore
	 * 
	 * @param n
	 */
	protected void setNumRecs(int n) {
		tx.setInt(blk, EHPageFormatter.RECORD_COUNT_OFFSET, n);
	}
	
	/**
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @param fldname
	 * @return
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
	 * of a symbolic constant. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param slot
	 * @return 
	 */
	protected int slotpos(int slot)
	{
		return EHPageFormatter.RECORD_START_OFFSET + (slot * slotsize);
	}

}
