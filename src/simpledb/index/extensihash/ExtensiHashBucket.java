package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.index.btree.BTPageFormatter;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;

public class ExtensiHashBucket {
	private Block currentblk; 
	private TableInfo ti;
	private Transaction tx;
	private int slotsize;
	private int localDepth;
	private int bucketNum;

	public ExtensiHashBucket (Block currentblk, 
			TableInfo ti, 
			Transaction tx, 
			int localDepth,
			int bucketNum) 
	{
		this.currentblk = currentblk; 
		this.ti = ti;
		this.tx = tx; 
		slotsize = ti.recordLength();
		tx.pin(currentblk);
		this.localDepth=localDepth;
		this.bucketNum = bucketNum;
	}

	/**
	 * Splits this bucket into two, incrementing the local depth in each new
	 * bucket. The index entries are distributed into the new buckets based on the
	 * value of bucket num=(index entry).(datavalue).hashcode() % 2^(new local depth).
	 * 
	 * This method will give two distinct bucket numbers 
	 * (since (index entry).(datavalue).hashcode() for each entry was equivalent 
	 * modulo 2^(old local depth) and this bucket will hold the lower one while the
	 * new bucketw will hold the greater one
	 * 
	 * @return Block of the new split bucket
	 */
	public Block split() 
	{
		int bigKey = bucketNum+(1<< localDepth);
		
		localDepth++;

		Block bigKeyBlk = appendNew(localDepth, bigKey); 
		
		ExtensiHashBucket bigKeyBucket = new ExtensiHashBucket(bigKeyBlk, ti, tx, localDepth, bigKey);
		
		moveRecords(bigKeyBucket, bigKey);
		return bigKeyBlk;
	}


	/** 
	 * Moves records to extensiHashBucket dest if they satisfy 
	 * record.dataval.hashcode() % localdepth == modPredicate.
	 * 
	 * @param dest
	 * @param modPredicate
	 */
	private void moveRecords(ExtensiHashBucket dest, int modPredicate)
	{
		int recLen = ti.recordLength();
		Schema sch = ti.schema();

		int destSlot = 0;
		int pos = 0; 

		while (pos < getNumRecs())
		{
			if (getVal(pos,"dataval").hashCode() % (1<<localDepth) == modPredicate)
			{
				for (String fldname : sch.fields()) 
				{
					dest.insert(destSlot);
					dest.setVal(destSlot, fldname, getVal(pos, fldname));
					delete(pos);
					destSlot++;
				}

			}
			else
			{
				pos+= recLen;
			}
		}
	}

	/**
	 * Appends a new block to the end of the specified B-tree file,
	 * having the specified flag value.
	 * @param flag the initial value of the flag
	 * @return a reference to the newly-created block
	 */
	public Block appendNew(int localdepth, int bucketNum) {
		return tx.append(ti.fileName(), new BucketFormatter(ti, localdepth, bucketNum));
	}


	private void setBucketNum(int newBucketNum)
	{
		tx.setInt(currentblk, BucketFormatter.BUCKET_NUM_OFFSET, newBucketNum);
	}

	/* CS4432-Project2
	 * 
	 * The following methods were copied from simpledb.index.btree. They are 
	 * all I/O methodst that don't have anything in particular to do with the 
	 * B-Tree structure, so we use them here as well.
	 */

	/**
	 * Calculates the position where the first record having
	 * the specified search key should be, then returns
	 * the position before it.
	 * @param searchkey the search key
	 * @return the position before where the search key goes
	 */
	public int findSlotBefore(Constant searchkey) {
		int slot = 0;
		while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
			slot++;
		return slot-1;
	}

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
	 * @return true if the block is full
	 */
	public boolean isFull() {
		return slotpos(getNumRecs()+1) >= BLOCK_SIZE;
	}


	/**
	 * Returns the dataval of the record at the specified slot.
	 * @param slot the integer slot of an index record
	 * @return the dataval of the record at that slot
	 */
	public Constant getDataVal(int slot) {
		return getVal(slot, "dataval");
	}


	/**
	 * Returns the dataRID value stored in the specified leaf index record.
	 * @param slot the slot of the desired index record
	 * @return the dataRID value store at that slot
	 */
	public RID getDataRid(int slot) {
		return new RID(getInt(slot, "block"), getInt(slot, "id"));
	}

	/**
	 * Deletes the index record at the specified slot.
	 * @param slot the slot of the deleted index record
	 */
	public void delete(int slot) {
		for (int i=slot+1; i<getNumRecs(); i++)
			copyRecord(i, i-1);
		setNumRecs(getNumRecs()-1);
		return;
	}

	/**
	 * Returns the number of index records in this page.
	 * @return the number of index records in this page
	 */
	public int getNumRecs() {
		return tx.getInt(currentblk, INT_SIZE);
	}


	private int getInt(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return tx.getInt(currentblk, pos);
	}

	private String getString(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return tx.getString(currentblk, pos);
	}

	private Constant getVal(int slot, String fldname) {
		int type = ti.schema().type(fldname);
		if (type == INTEGER)
			return new IntConstant(getInt(slot, fldname));
		else
			return new StringConstant(getString(slot, fldname));
	}

	private void setInt(int slot, String fldname, int val) {
		int pos = fldpos(slot, fldname);
		tx.setInt(currentblk, pos, val);
	}

	private void setString(int slot, String fldname, String val) {
		int pos = fldpos(slot, fldname);
		tx.setString(currentblk, pos, val);
	}

	private void setVal(int slot, String fldname, Constant val) {
		int type = ti.schema().type(fldname);
		if (type == INTEGER)
			setInt(slot, fldname, (Integer)val.asJavaVal());
		else
			setString(slot, fldname, (String)val.asJavaVal());
	}

	private void setNumRecs(int n) {
		tx.setInt(currentblk, INT_SIZE, n);
	}

	private void insert(int slot) {
		for (int i=getNumRecs(); i>slot; i--)
			copyRecord(i-1, i);
		setNumRecs(getNumRecs()+1);
	}

	private void copyRecord(int from, int to) {
		Schema sch = ti.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}

	private int fldpos(int slot, String fldname) {
		int offset = ti.offset(fldname);
		return slotpos(slot) + offset;
	}

	private int slotpos(int slot) {
		return INT_SIZE + INT_SIZE + (slot * slotsize);
	}

}
