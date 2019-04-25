package simpledb.index.extensiblehash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;

import simpledb.file.Page;
import simpledb.buffer.PageFormatter;

/**
 * CS4432-Project2
 * ExtensiHashDir (EHD) is a wrapper for the underlying extensible hash directory
 * on disk. EHD stores the global depth of the extensible hash table and records 
 * for each bucket (containing the block number of the bucket in the file.
 * 
 * There are always 2^(global depth) such records, but they may not point to 
 * different buckets (if multiple records reference the same bucket, then the 
 * local depth of that bucket is strictly less than the global depth).
 * 
 * The records in the directory only contain a block number; the records are stored 
 * in order. That is, if the global depth is n, then the kth directory record holds
 * the block that contains index entries whose data value hash to k (mod 2^n). The 
 * ordering of directory records is preserved throughout any actions on the directory.
 * 
 * Note that block 0 contains metadata, but subsequent blocks do not. So a different 
 * number of records will fit in block 0 than in any other block.
 * 
 * EHD extends the abstract class ExtensiHashPage, which contains methods (authored 
 * by E. Sciore) for manipulating the data in the block.
 * 
 * @author mcwarms, gdcecil
 *
 */
class EHDir extends EHPage 
{
	//TableInfo for the buckets, used to construct EHBucket objects as needed
	private TableInfo bucketsTi;
	
	//Store the field name of the directory entries
	static final String DIR_FIELD = "BlockNum";

	/**
	 * Constructor for an EHDir object. This will always refer to the 
	 * first block of ~idxname~dir.tbl, so we evoke the super constructor 
	 * with block 0 of this file. 
	 * 
	 * Passed a TableInfo for the records and a transaction for IOs, 
	 * and then the tableinfo for the buckets file
	 * 
	 * @param ti, tableinfo for the directory  
	 * @param bucketsTi, tableinfo for the buckets
	 * @param tx, tableinfo for the directory
	 */
	EHDir (TableInfo ti, TableInfo bucketsTi, Transaction tx)
	{
		//call the super constructor with the first block of the directory file
		super (new Block(ti.fileName(),0), ti, tx);
		
		this.bucketsTi = bucketsTi;
	}


	/**
	 * CS4432-Project2
	 * 
	 * Increment the global depth, doubling the number of references
	 * to buckets.
	 * 
	 * This method does not double the buckets themselves, so index entries
	 * with bucket numbers congruent modulo 2^(old depth) reference the
	 * same bucket (this method updates the new references).
	 * 
	 */
	private void incrementGlobalDepth() 
	{
		//store the current number of dir entries
		int oldNum = getNumRecs(); 

		/*
		 * When doubling the number of records, there are two cases: 
		 * the first is when there is still enough space in block 0 of 
		 * the directory file. In that case, there's no need to append 
		 * any blocks
		 * 
		 * If 2*NumRecs larger than the number of recs in block 0, we 
		 * must append more blocks to the file to fit the directory entries.
		 * 
		 * If the directory takes up more than one block, it will have to 
		 * double the number of blocks every time thereafter, so it is sufficient
		 * to just see if we have overflowed block 0
		 */
		if (2*getNumRecs() > maxRecordsInBlock())
		{
			int oldFileSize = tx.size(ti.fileName());
			
			//compute the total number of blocks needed to store the directory entries
			//to do this we take the last slot number, 2*getNumRecs()-1, subtract the 
			//number of records that fit in block zero, and then divide by the number 
			//of records that fit in a block of all records
			int blocksRequired = 1+((2*getNumRecs()-1 - maxRecordsInBlock()) / maxRecordsInDirBlock());

			//blocks to add
			int blocksNeeded = blocksRequired - oldFileSize;

			for (int i = 0; i < blocksNeeded; i++)
			{
				//append a number of all-record blocks as needed
				tx.append(ti.fileName(), new RecordBlockFormatter(ti));
			}
		}

		/*
		 * Since no new buckets are created here, doubling the number of directory
		 * entries means that each bucket will have two directory entries referencing it.
		 * Two entries should be the same if their slot numbers are congruent modulo 
		 * oldNum, so for each entry at slot pos, we copy it to slot pos+oldnum. 
		 */
		for (int pos = 0; pos < oldNum; pos++)
		{
			//make space for the new directory entry at pos+oldNum
			insert(pos + oldNum); 
			//copy directory entry at pos to slot pos+oldNum
			copyRecord(pos, pos + oldNum); 
		}

		//increment the global depth
		setDepth(getDepth()+1);
		System.out.println("New Global Depth after split: " + getDepth());
	}
	/**
	 * CS4432-Project2
	 * 
	 * Insert the given index record (consisting of a data value and an
	 * RID) into the appropriate bucket. 
	 * 
	 * This method extends the index as necessary, if the corresponding bucket 
	 * is full, then the bucket is split (incrementing the global depth as needed). 
	 * 
	 * This also updates any directory entries as needed. 
	 * 
	 * For the actual insert, the EHBucket method is called.
	 * 
	 * @param dataval, Constant data value to insert
	 * @param rid, Record id to insert.
	 */
	public void insertIndexRecord (Constant dataval, RID rid)
	{
		//compute the bucket number for the given data value
		int bucketNum = EHPage.computeBucketNumber(dataval, getDepth());
		//get the block of the bucket with given bucket number
		Block blk = getBucketBlock(bucketNum);

		//Open an instance of ExtensiHashBucket to wrap the block containing 
		//the bucket.
		EHBucket bucket = new EHBucket(blk, bucketsTi, tx);
	
		System.out.println("Bucket state before changing");
		System.out.println(bucket.toString());
		//If the bucket is full, extend the hash index by splitting the bucket
		//and incrementing the global depth as needed.
//		System.out.println("while bucket is full");
		while (bucket.isFull())
		{
			//if the bucket is full and its local depth is equal to the global 
			//depth of the hash index, increment the global depth before splitting
			//the bucket.
			if (bucket.getDepth() == this.getDepth())
			{
				System.out.println("Bucket full, splitting bucket");
				//increment the global depth, doubling the number of 
				//references to buckets.
				incrementGlobalDepth();

				//update the bucket number for dataval under the new global depth
				bucketNum = EHPage.computeBucketNumber(dataval, getDepth());
			}
			//Now that the global depth has been updated, we can split the bucket.

			//First store the bucket number for the data value with respect to the local depth of the full bucket.
			int oldBucketNum = EHPage.computeBucketNumber(dataval, bucket.getDepth());

			//Split the bucket 
			Block newblk = bucket.split();

			//Compute the bucket number of the new bucket, given by oldBucketNum + 2^(old local depth).
			int newBucketNum = oldBucketNum + (1 << (bucket.getDepth()-1));

			/*
			 * This bucket might be referenced multiple times in the directory. If it is, its directory entries 
			 * will be at slots 
			 * 
			 * oldBucketNum, oldBucketNum + 1*(2^(old local depth)),...,oldBucketNum+N*(2^(old local depth)),
			 * 
			 * where N is the largest integer such that oldBucketNum+N*(2^(old local depth)) is less than 2^((global depth)).
			 * 
			 * oldBucketNum is necessarily the smallest of these slots, since all the bits of oldBucketNum to the 
			 * left of the (local depth)'th bit are zero.
			 * 
			 * Since we are splitting the bucket, we need to update these index entries to make sure they point to the
			 * correct bucket. The entries that should point to the old bucket won't need to be changed. These entries are at slots
			 * 
			 * oldBucketNum, oldBucketNum + 1*(2^(new local depth)),...,oldBucketNum+N*(2^(new local depth)),
			 * 
			 * where N is the largest integer such that oldBucketNum+N*(2^(new local depth)) is less than 2^((global depth)).
			 * The directory entries that we need to update to reference the new bucket will be at 
			 * 
			 * newBucketNum, newBucketNum + 1*(2^(new local depth)),...,newBucketNum+M*(2^(new local depth)),
			 * 
			 * where M is the largest integer such that newBucketNum+M*(2^(new local depth)) is less than 2^((global depth)).
			 */

			//compute 2^(new local depth), which is used to step through the directory entries as above.
			//Here bucket.getDepth() gets the correct number, since split() updates the local depth of the bucket.
			int stepSize = (1 << bucket.getDepth());

			//iterate through the directory entries that need to be changed, and update them to 
			//reference the new block. Note that if local depth = global depth, stepSize will be equal to 
			//the number of records, and so only one entry will be changed (as expected).
			for (int pos = newBucketNum; pos < getNumRecs(); pos += stepSize) 
			{
				//change the reference for the directory entry at pos to reference the new block 
				updateDirEntry(pos, newblk.number());
			}

			//Set blk to the block with the bucket corresponding to the bucket number for dataval with respect
			//to the new local depth.
			blk = (EHPage.computeBucketNumber(dataval, bucket.getDepth())== oldBucketNum) ?
					blk : newblk;

			//close the current ExtensiHashBucket wrapper
			bucket.close();

			//open a new ExtensiHashBucket wrapper for blk, which holds whichever split bucket that the 
			//index entry for dataval belongs in.
			bucket = new EHBucket(blk, bucketsTi, tx);

			//return to the predicate for the while loop. If the split bucket we want to add the index entry
			//for dataval to is still full, repeat this process. 
		}

		//bucket is not full, so insert the index entry for dataval.
		bucket.insertIndexRecord(dataval, rid );

		//close the ExtensiHashBucket wrapper for the bucket.
		bucket.close();

	}

	/**
	 * CS4432-Project2
	 * 
	 * Update the directory record at slot bucketNum to contain 
	 * the specified block number
	 * 
	 * @param bucketNum, which directory entry to update
	 * @param blockNum, value to update
	 * 
	 */
	public void updateDirEntry (int bucketNum, int blockNum)
	{
		setInt(bucketNum, "BlockNum", blockNum);
	}

	/**
	 * CS4432-Project2
	 * 
	 * Uses the directory to get the block in the buckets file 
	 * where the specified bucket is stored.
	 * 
	 * @param bucketNum, which bucket to get the block for 
	 * @return a block for the buckets file with the block number of the specified bucket
	 */
	public Block getBucketBlock(int bucketNum)
	{
		return new Block (bucketsTi.fileName(), getInt(bucketNum, DIR_FIELD));
	}


	/**
	 * CS4432-Project2
	 * 
	 * Get the maximum number of records that fit in an all-record block, 
	 * that is a block in this file with num >= 1.
	 * 
	 * @return maximum number of records that will fit in a block 
	 */
	private int maxRecordsInDirBlock()
	{
		return BLOCK_SIZE/ti.recordLength();	 		
	}

	/**
	 * CS4432-Project2
	 * Takes an index relative to the whole of the directory file and then computes 
	 * the slot index relative to the block where original index was located. 
	 * 
	 * @param slotNum, index relative to the whole directory file
	 * @return an index relative to the number of records in a single block
	 */
	private int getSlotNumInBlock(int slotNum)
	{
		return slotNum < maxRecordsInBlock() ?
				slotNum : //if the index refers to block zero we can just return it 
					//otherwise we subtract the number of records in block 0 and return 
					//the result modulo the max number of records in an all-record block
					(slotNum - maxRecordsInBlock()) % maxRecordsInDirBlock();
	}

	/**
	 * CS4432-Project2
	 * 
	 * Takes the slot index and computes which block in this file that slot 
	 * is actually located in.
	 * 
	 * @param slot, index relative to the whole of the directory file
	 * @return block where that slot is located.
	 */
	private Block dirBlock(int slot)
	{
		
		int blk = slot < maxRecordsInBlock() ? 
				0//if the slot is in block zero, return block zero. 
				//otherwise subtract the number of records in block 0 
				//and divide by the number of records in an all-record block
				//add one because the first block with only records is 1
				: 1+((slot- maxRecordsInBlock())/maxRecordsInDirBlock());
		return new Block(ti.fileName(), blk);
	}

	/**
	 * CS4432-Project2
	 * 
	 * Given a slot index relative to the whole directory, returns 
	 * the byte offset of that slot relative to the block where it is located.
	 * 
	 * @param slot, index relative to the whole of the directory file
	 * @return byte offset of slot in the block where it is located
	 */
	protected int dirBlockPos(int slot)
	{
		return getSlotNumInBlock(slot) * slotsize;
	}

	
	/**
	 * CS4432-Project2
	 * 
	 * This overrides the get int method to support having many blocks in the directory file. 
	 * If slot is in block 0, the int is retrieved using super getInt as normal. 
	 * 
	 * @param slot, slot relative to the whole file where we want to get an int
	 * @param fldname, field to get the int from in the record
	 * 
	 * @return the int in fldname in the record at the given slot in the dir file
	 */
	@Override
	protected int getInt(int slot, String fldname) 
	{
		int ret;
		
		//if slot is in block 0, retrieve it as usual
		if (slot < maxRecordsInBlock()) 
			ret = super.getInt(slot, fldname);
		else 
		{
			//find the block and offset for slot
			Block dirblk = dirBlock(slot);
			int posInBlock = dirBlockPos(slot);

			//pin the directory block and get the int
			tx.pin(dirblk);
			ret = tx.getInt(dirblk, posInBlock);
			
			//unpin the block
			tx.unpin(dirblk);

		}
		return ret;
	}


	/**
	 * CS4432-Project2
	 * 
	 * This overrides the set int method to support having many blocks in the directory file. 
	 * If slot is in block 0, the int is set using super setInt as normal. 
	 * 
	 * @param slot, slot relative to the whole file where we want to set the int
	 * @param fldname, field to change in record.
	 * @param val, int to write
	 */
	@Override
	protected void setInt(int slot, String fldname, int val) 
	{
		//if slot is in block zero, set it as usual
		if (slot < maxRecordsInBlock()) 
			super.setInt(slot, fldname, val);
		else 
		{
			//find the block and byte offset for slot
			Block dirblk = dirBlock(slot);
			int posInBlock = dirBlockPos(slot);
			
			
			//pin the directory block and set the int
			tx.pin(dirblk);
			tx.setInt(dirblk, posInBlock, val);
			
			//unpin the block
			tx.unpin(dirblk);

		}
	}


	/**
	 * CS4432-Project2
	 * 
	 * Creates a string representation of this directory. This string includes both metadata
	 * and then the string representation for each bucket in the directory. 
	 * 
	 * @return String representation of this object
	 */
	@Override
	public String toString()
	{
		//output metadata
		String out = "Filename: " + ti.fileName() + "\n";

		out += "Size: " + tx.size(ti.fileName()) + " blocks\n";

		out += "Max records in dir block 0: " + maxRecordsInBlock() + "\n";

		out += "Max records in dir block 1+: " + maxRecordsInDirBlock() + "\n";

		out += "Global depth: " + getDepth() + "\n";

		out += "Number of directory entries: " + getNumRecs() + "\n\n";

		//output toString for each bucket
		for (int slot = 0; slot < getNumRecs(); slot++)
		{
			out += "Bucket number in directory: " + Integer.toBinaryString(slot) + ":\n\n";

			Block b = new Block (bucketsTi.fileName(), getInt(slot, DIR_FIELD));

			EHBucket bucket = new EHBucket(b, bucketsTi, tx);

			out += bucket.toString();
			bucket.close();
		}

		return out;
	}


}


/**
 * CS4432-Project2 
 * 
 * Pageformatter for the blocks in the directory after the first one. 
 * 
 * This pageformatter just formats the page to look a like a collection 
 * of empty records with no metadata. 
 * 
 * @author mcwarms, gdcecil
 *
 */
class RecordBlockFormatter implements PageFormatter
{
	//tableinfo for the records
	private TableInfo ti;

	/**
	 * CS4432-Project2: 
	 * 
	 * Constructor. As there are only records in this block we just need a TableInfo object
	 * 
	 * @param ti, table info for the records
	 */
	RecordBlockFormatter (TableInfo ti)
	{
		this.ti = ti;
	}
	
	/**
	 * CS4432-Project2
	 * 
	 * Formats the page by writing default records at each slot  of the page. 
	 * 
	 * @param p, page to format
	 */
	public void format (Page p)
	{
		int recSize = ti.recordLength();
		for (int pos = 0; pos + recSize <= BLOCK_SIZE; pos += recSize)
			makeDefaultRecord(p, pos);

	}

	
	/**
	 * Taken from BTPageFormatter, and unchanged. This 
	 * overwrites a record slot with default values. 
	 * 
	 * @author Edward Sciore
	 * 
	 * @param page, page to format
	 * @param pos, byte offset for record
	 */
	private void makeDefaultRecord(Page page, int pos) {
		for (String fldname : ti.schema().fields()) {
			int offset = ti.offset(fldname);
			if (ti.schema().type(fldname) == INTEGER)
				page.setInt(pos + offset, 0);
			else
				page.setString(pos + offset, "");
		}
	}

}