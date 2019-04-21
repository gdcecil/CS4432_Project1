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
	protected Block currentblk; 
	protected TableInfo ti;
	protected Transaction tx;
	protected int slotsize;
	protected int depth;
	
	public ExtensiHashPage (Block currentblk, TableInfo ti, Transaction tx, int depth) 
	{
		this.currentblk = currentblk; 
		this.ti = ti;
		this.tx = tx; 
		slotsize = ti.recordLength();
		tx.pin(currentblk);
		this.depth=depth;
	}
	
	/**
	 * CS4432-Project2
	 * Increment the depth (global/local as appropriate) of this page.
	 * 
	 * @author mcwarms, gdcecil
	 */
	protected void incrementDepth() {
		this.depth++;
		tx.setInt(currentblk, 0, depth);
	}
	
	
	/* CS4432-Project2
	 * 
	 * The following methods are copied from simpledb.index.btree.BTreePage,
	 * and are use to access and manipulate records in the block referenced
	 * by either ExtensiHashBucket or ExtensiHashDir (subclasses of this).
	 * 
	 */
	
	/**
	 * Closes the page by unpinning its buffer.
	 */
	public void close() {
		if (currentblk != null)
			tx.unpin(currentblk);
		currentblk = null;
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
		return tx.getInt(currentblk, INT_SIZE);
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
	 * moving all the records at and afterslot by one slot.
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
		return tx.getInt(currentblk, pos);
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
		return tx.getString(currentblk, pos);
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
		tx.setInt(currentblk, pos, val);
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
		tx.setString(currentblk, pos, val);
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
		tx.setInt(currentblk, INT_SIZE, n);
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

	/**
	 * CS4432-Project2
	 * 
	 * Computes the the offset (relative to the start of the block) 
	 * of the record at the specified slot (relative to the start of the 
	 * records). 
	 * 
	 * This depends on the metadata of the page, so its implementation 
	 * is left to subclasses.
	 * 
	 * @param slot
	 * @return 
	 */
	protected abstract int slotpos(int slot);

}
