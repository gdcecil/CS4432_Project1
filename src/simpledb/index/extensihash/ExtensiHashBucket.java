package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;

class ExtensiHashBucket extends ExtensiHashPage{
	private int bucketNum;

	ExtensiHashBucket (Block currentblk, 
			TableInfo ti, 
			Transaction tx,
			int bucketNum) 
	{
		super (currentblk, ti, tx);
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
		int bigKey = bucketNum+(1<< depth);
		
		depth++;

		Block bigKeyBlk = appendNew(depth, bigKey); 
		
		//TODO fix this depth
		ExtensiHashBucket bigKeyBucket = new ExtensiHashBucket(bigKeyBlk, ti, tx, bigKey);
		
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
			if (getVal(pos,"dataval").hashCode() % (1<<depth) == modPredicate)
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
		return tx.append(ti.fileName(), new EHPageFormatter(ti, localdepth, bucketNum));
	}


	/*private void setBucketNum(int newBucketNum)
	{
		tx.setInt(currentblk, BucketFormatter.BUCKET_NUM_OFFSET, newBucketNum);
	}*/

	/* CS4432-Project2
	 * 
	 * The following methods were copied from simpledb.index.btree. They are 
	 * all I/O methodst that don't have anything in particular to do with the 
	 * B-Tree structure, so we use them here as well.
	 */

	/*
	 * Calculates the position where the first record having
	 * the specified search key should be, then returns
	 * the position before it.
	 * @param searchkey the search key
	 * @return the position before where the search key goes
	 
	public int findSlotBefore(Constant searchkey) {
		int slot = 0;
		while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
			slot++;
		return slot-1;
	}*/


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

	protected int slotpos(int slot) {
		return INT_SIZE + INT_SIZE + INT_SIZE + (slot * slotsize);
	}

}
