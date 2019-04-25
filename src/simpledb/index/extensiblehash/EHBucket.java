package simpledb.index.extensiblehash;

import static simpledb.index.extensiblehash.EHPageFormatter.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;

/**
 * CS4432-Project2
 * 
 * EHBucket is a subclass of EHPage that serves as a wrapper for a bucket page on 
 * disk. EHBucket has methods for splitting in case of over flow, inserting, deleting, 
 * and accessing records. 
 * 
 * EHBucket also keeps track of a current slot and a search key, which are the current 
 * position and search key of the EHIndex respectively. 
 * 
 * Inserts and deletes on EHBucket maintain the constraint that index entries with 
 * equal datavals are adjacent on disk (i.e. they are in consecutive slot numbers).
 * 
 * @author gdcecil, mcwarms
 *
 */
class EHBucket extends EHPage
{
	//keep track of the current index position and search key 
	//-1 is the default value so that a call of EHIndex.next() will 
	//look at the 0th record.
	private int currentSlot = -1;
	private Constant searchkey = null;

	
	/**
	 * CS4432-Project2 
	 * 
	 * Construct an instance of an EHBucket, wrapping the specified block, with 
	 * TableInfo Ti for the index entry schema and Transaction tx for IOs,
	 *  
	 * This calls the superclass constructor which pins this block in memory.
	 * 
	 * This block can be unpinned by calling close().
	 * 
	 * @param currentblk, block that this EHBucket object is wrapping
	 * @param ti, TableInfo for the index entries
	 * @param tx, Transaction for inputting an outputting data from the underlying disk page
	 */
	EHBucket (Block currentblk, 
			TableInfo ti, 
			Transaction tx) 
	{
		super (currentblk, ti, tx);
	}

	/**
	 * CS4432-Project2
	 * 
	 * Inserts the index entry (consisting of a dataval and an RID) into this bucket page.
	 * 
	 * If there is already an entry with the same dataval, the new index entry is inserted 
	 * directly before that entry. Otherwise, the new entry is inserted at slot 0.
	 * 
	 * Modifies currentSlot and searchkey, but this is never the instance 
	 * of bucket used in ExtensiHashIndex (as this is only called when
	 * insertIndexRecord is called in ExtensiHashDir, which creates an 
	 * local instance of this class for that purpose.
	 * 
	 * @param dataval, dataval for entry to insert
	 * @param rid, contains id and block number for entry to insert
	 */
	void insertIndexRecord(Constant dataval, RID rid) 
	{
		//move before the first entry with dataval searchkey
		//ensures entries with equal datavals are adjacenet in buckets
		moveBeforeValue(dataval);
		
		//if next() (i.e. if there is already an index entry with
		//searchkey as its dataval), insert at currentslot. Otherwise,
		//insert at slot 0.
		int position = next() ? currentSlot : 0;

		//move records over to make room for the new entry, and 
		//update the record count.
		insert(position);
		
		//set the fields of the index entry
		setVal(position, "dataval",dataval);
		setInt(position, "id", rid.id());
		setInt(position, "block", rid.blockNumber());
		
		resetSearchInfo();
		
	}

	/**
	 * CS4432-Project2
	 * 
	 * Get the bucket number of this bucket, stored at offset BUCKET_NUM_OFFSET
	 * of block blk (the block an instance of this class wraps)
	 * 
	 * bucket number will be a number between 0 and 2^(local depth - 1). This 
	 * will correspond the the bucket number of one directory entry (that is, 
	 * the slot number of the directory entry) that references this block.
	 * The other directory entries that reference this block will have 
	 * bucket numbers congruent to getBucketNum(), modulo local depth
	 * 
	 * @return the bucket number for this block
	 */
	int getBucketNum()
	{
		return tx.getInt(blk, BUCKET_NUM_OFFSET);
	}
	
	

	/**
	 * CS4432-Project2 
	 * 
	 * Splits this bucket into two, incrementing the local depth in each new
	 * bucket. The index entries are distributed into the new buckets based on the
	 * value of bucket num=(index entry).(datavalue).hashcode() % 2^(new local depth).
	 * 
	 * This method will give two distinct bucket numbers (since (index entry).(datavalue).hashcode() 
	 * for each entry was equivalent modulo 2^(old local depth)) and this bucket 
	 * will hold the lesser one (i.e. this bucket's bucket number is not changed) 
	 * while the new bucket will hold the greater one (getBucketNum() + 2^(old local depth))
	 * 
	 * @return Block of the new split bucket
	 */
	public Block split() 
	{
		// store old depth in local variable
		int localDepth = getDepth();

		// if bucketnum is congruent k (mod 2^depth), the only values in 0,1,...,(2^(Depth+1))-1
		// that are congurent to k (mod 2^(depth)) are k and k+2^(depth), so the bucket numbers of
		// the split blocks are k and k+2^(depth). 
		int bucketNum = getBucketNum();
		int newBucketNum = bucketNum+(1 << localDepth); //left shift for a power of two

		// increment local depth
		setDepth(localDepth+1);

		// append a new block to the bucket file for the split bucket 
		// this bucket has the same local depth and number bucketNum+2^(old depth)
		Block newBlk = appendNew(getDepth(), newBucketNum); 


		//wrap the new page in an ExtensiHashBucket, and move index entries whose 
		//corresponding bucket number equals newBucketNum to the new page
		EHBucket newBucket = new EHBucket(newBlk, ti, tx);
		moveRecords(newBucket);

		//close the extensiHashBucket object
		newBucket.close();

		//return the block of the new bucket
		return newBlk;
	}


	/** 
	 * CS4432-Project2
	 * 
	 * Moves index records from this bucket to bucket dest if they satisfy 
	 * record.dataval.hashcode() % localdepth == dest.getBucketNum().
	 * 
	 * Used as part of the block splitting process.
	 * 
	 * @param dest
	 * @param modPredicate
	 */
	private void moveRecords(EHBucket dest)
	{
		// store depth, record size, the index schema, and the
		// destination bucket number in local variables
		int depth = getDepth();
		int bucketNum = dest.getBucketNum();

		//iterate through the index entries on this page
		for (int slot = 0; slot < getNumRecs();)
		{
			//if the bucket number for an index entry matches the dest bucket number,  
			//copy the index entry to dest
			Constant value = getDataVal(slot);
			if (computeBucketNumber(value, depth) == bucketNum)
			{
				//insert the index record into dest
				dest.insertIndexRecord(getDataVal(slot), getDataRID(slot));

				//delete the index record from this page. Delete moves the index
				//entries over to fill the empty slot, so no need to increment slot
				delete(slot);
			}
			//only increment slot if we didn't delete an index enttry
			else slot++;
		}
	}

	/**
	 * CS4432-Project2
	 * 
	 * Appends a new block to the end of the specified file, setting the depth
	 * to the specificed value, and the bucketnum to the specified value. 
	 * 
	 * This is the appendNew() method from BTreePage, edited to use the 
	 * EHPageFormatter and pass localDepth and BucketNum as arguments.
	 * 
	 * @param localdepth, number to write in the depth slot of the new block 
	 * @param bucketNum, number to write in the bucketNum slot of the new block
	 * @return a reference to the newly-created block
	 */
	public Block appendNew(int localdepth, int bucketNum) {
		return tx.append(ti.fileName(), new EHPageFormatter(ti, localdepth, bucketNum));
	}

	/*
	 * Start methods taken from BTreePage
	 */

	/**
	 * Returns the dataval of the record at the specified slot.
	 * 
	 * @author Edward Sciore
	 * @param slot the integer slot of an index record
	 * @return the dataval of the record at that slot
	 */
	public Constant getDataVal(int slot) {
		return getVal(slot, "dataval");
	}


	/**
	 * Returns the dataRID value stored in the specified index record.
	 * 
	 * @author Edward Sciore
	 * @param slot the slot of the desired index record
	 * @return the dataRID value store at that slot
	 */
	private RID getDataRID(int slot) {
		return new RID(getInt(slot, "block"), getInt(slot, "id"));
	}
	
	/*
	 * End methods taken from BTreePage
	 */
	
	/**
	 * CS4432-Project2 
	 * 
	 * Get the RID from the current index entry.
	 * 
	 * @return RID of index entry at slot currentSlot
	 */
	public RID getCurrentRID()
	{
		return getDataRID(currentSlot);
	}
	
	/**
	 * CS4432-Project2
	 * 
	 * Delete the index entry at current slot
	 */
	public void deleteCurrentEntry()
	{
		delete(currentSlot);
	}

	/**
	 * CS4432-Project2
	 * 
	 * First resets the currentslot and searchkey. If there is no index entry 
	 * with dataval search key, current info is not changed from its default
	 * value (-1).
	 * 
	 * Sets private variable currentSlot to the slot before the first index 
	 * entry having the specified search key (If the slot with the search key 
	 * is zero, then currentSlot is set to -1).
	 *
	 * Also sets private variable searchkey to the specified searchkey (this 
	 * occurs whether or not the given searchkey was found in the bucket).
	 * 
	 * @param searchkey, value to search for 
	 */
	public void moveBeforeValue(Constant searchkey)
	{
		//reset the currentSlot and searchkey to default values
		resetSearchInfo();
		//set the current searchkey to the specified value
		this.searchkey = searchkey;

		//keep track of whether we have found an index entry with dataval searchkey
		boolean found = false;

		//iterate through the records in this bucket while we haven't found
		//an index entry with dataval searchkey, or until we've checked every record
		for (int pos = 0; (pos < getNumRecs()) && (!found); pos++)
		{
			//check if dataval of index equals searchkey 
			if (getVal(pos, "dataval").equals(searchkey))
			{
				//set currentSlot to the position before the first index
				//entry with dataval searchkey
				currentSlot = pos-1;

				//set found to true (this breaks out of the loop)
				found = true;
			}
		}

	}

	/**
	 * CS4432-Project2
	 * 
	 * Increments currentslot. Then if current slot holds a record and the dataval at the 
	 * new currentSlot is still equal to the search key, this returns true. 
	 * 
	 * If the currentslot is past the records in the bucket or its dataval is not equal to the 
	 * search key, currentSlot is set to -1, searchkey is set to null, and then this returns false;
	 * 
	 * @return true if the next slot has an index entry with dataval == searchkey, and false otherwise.
	 */
	public boolean next()
	{

		boolean hasNext;

		//increment the current slot
		currentSlot++;

		//if the next slot doesn't hold a record, or if the search key is null, 
		//or the dataval of the entry in the next slot is not equal to searchkey,
		//reset the currentSlot and Searchkey to default values and return false.
		if (currentSlot >= getNumRecs() || 
				searchkey == null ||
				! getVal(currentSlot,"dataval").equals(searchkey))
		{
			resetSearchInfo();
			hasNext = false;
		}
		//otherwise, return true false.
		else hasNext = true;

		return hasNext;
	}

	/**
	 * CS4432-Project2
	 * 
	 * Set the currentSlot to -1 and the searchkey to null (default values)
	 */
	private void resetSearchInfo()
	{
		currentSlot = -1; 
		searchkey = null;
	}
	
	/**
	 * CS4432-Project2
	 * 
	 * toString() representation of this bucket, including associated metadata and each 
	 * index entry in the bucket.
	 * 
	 * @return string containing metadata and each index entry
	 */
	@Override
	public String toString()
	{
		//print metadata
		String out = "Bucket number (stored in bucket): " + Integer.toBinaryString(getBucketNum()) + "\n";
		
		out += "Block " + blk.number() + " in file " + ti.fileName() + "\n";
		
		out += "Local Depth: " + getDepth() + "\n";
		
		out += "Max records in bucket block: " + maxRecordsInBlock();
		
		out += "Number of index entries in block: " + getNumRecs() + "\n";
		
		out += "Index entries:\n\n";
		
		//print each index entry
		for (int slot = 0; slot < getNumRecs(); slot++)
		{
			out += "Slot " + slot + ":\n";
			
			Schema sch = ti.schema(); 
			
			for (String fldname : sch.fields())
			{
				out += "\t\t" + fldname + " = " + getVal(slot, fldname).toString() + "\n";
			}
			
		}
		
		return out + "\n";
	}

}
