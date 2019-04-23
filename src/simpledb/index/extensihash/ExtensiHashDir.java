package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;

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
 * EHD extends the abstract class ExtensiHashPage, which contains methods (authored 
 * by E. Sciore) for manipulating the data in the block.
 * 
 * @author mcwarms, gdcecil
 *
 */
class ExtensiHashDir extends ExtensiHashPage 
{
	private TableInfo bucketsTi;
	static final String DIR_FIELD = "BlockNum";

	ExtensiHashDir (Block currentblk, 
			TableInfo ti, 
			TableInfo bucketsTi,
			Transaction tx)
	{
		super (currentblk, ti, tx);
		this.bucketsTi = bucketsTi;
	}

	ExtensiHashDir (TableInfo ti, TableInfo bucketsTi, Transaction tx)
	{
		this(new Block(ti.fileName(),0), ti, bucketsTi, tx);
	}


	/**
	 * CS4432-Project2
	 * 
	 * Increment the global depth, doubling the number of references
	 * to buckets.
	 * 
	 * This method does not double the buckets themselves, so index entries
	 * with bucket numbers congruent modulo 2^(old depth) reference the
	 * same bucket.
	 */
	private void incrementGlobalDepth() 
	{
		//store the current number of dir entries
		int oldNum = getNumRecs(); 
		
		/* 
		 * Increasing the depth by one doubles the number of directory entries, so if
		 * slot 2*oldNum - 1 (subtracting one since slots are zero-based) exceeds the 
		 * block size, print the global depth, the global depth + 1, and the number of blocks in 
		 * bucket table file, and then throw a runtime exception. 
		 * 
		 * TODO: either delete or implement directories that span multiple blocks
		 */
		if (slotpos(2*oldNum -1) >= BLOCK_SIZE)
		{
			System.out.println("Error: Extensible hash index overflow.");
			System.out.println("Attempted to increase depth from " +getDepth()+" to " + (getDepth()+1));
			System.out.println("Number of buckets: " + tx.size(bucketsTi.fileName()));
			throw new RuntimeException();
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

		setDepth(getDepth()+1);
	}
	/**
	 * CS4432-Project2
	 * 
	 * Insert the given index record (consisting of a data value and an
	 * RID) into the appropriate bucket. 
	 * 
	 * This method extends the index as necessary, if the corresponding bucket 
	 * is full, then the bucket is split (incrementing the global depth as needed). 
	 * If the 
	 * @param dataval
	 * @param rid
	 */
	public void insertIndexRecord (Constant dataval, RID rid)
	{
		//compute the bucket number for the given data value
		int bucketNum = ExtensiHashPage.computeBucketNumber(dataval, getDepth());

		//get the block of the bucket with given bucket number
		Block blk = getBucketBlock(bucketNum);

		//Open an instance of ExtensiHashBucket to wrap the block containing 
		//the bucket.
		ExtensiHashBucket bucket = new ExtensiHashBucket(blk, bucketsTi, tx);

		//If the bucket is full, extend the hash index by splitting the bucket
		//and incrementing the global depth as needed.
		while (bucket.isFull())
		{
			//if the bucket is full and its local depth is equal to the global 
			//depth of the hash index, increment the global depth before splitting
			//the bucket.
			if (bucket.getDepth() == this.getDepth())
			{
				//increment the global depth, doubling the number of 
				//references to buckets.
				incrementGlobalDepth();
				
				//update the bucket number for dataval under the new global depth
				bucketNum = ExtensiHashPage.computeBucketNumber(dataval, getDepth());
			}
			//Now that the global depth has been updated, we can split the bucket.
			
			//First store the bucket number for the data value with respect to the local depth of the full bucket.
			int oldBucketNum = ExtensiHashPage.computeBucketNumber(dataval, bucket.getDepth());
			
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
			blk = (ExtensiHashPage.computeBucketNumber(dataval, bucket.getDepth())== oldBucketNum) ?
					blk : newblk;
			
			//close the current ExtensiHashBucket wrapper
			bucket.close();
			
			//open a new ExtensiHashBucket wrapper for blk, which holds whichever split bucket that the 
			//index entry for dataval belongs in.
			bucket = new ExtensiHashBucket(blk, bucketsTi, tx);
			
			//return to the predicate for the while loop. If the split bucket we want to add the index entry
			//for dataval to is still full, repeat this process. 
		}
		
		//bucket is a priori not full, so insert the index entry for dataval.
		bucket.insertIndexRecord(dataval, rid );
		
		//close the ExtensiHashBucket wrapper for the bucket.
		bucket.close();

	}


	public void updateDirEntry (int bucketNum, int block)
	{
		setInt(bucketNum, "BlockNum", block);
	}

	public Block getBucketBlock(int key)
	{
		return new Block (bucketsTi.fileName(), getInt(key, DIR_FIELD));
	}
	

	@Override
	protected int slotpos(int slot) {
		// TODO Auto-generated method stub
		return INT_SIZE+INT_SIZE+INT_SIZE + (slot * slotsize);
	}
	
	@Override
	public String toString()
	{
		String out = "Block " + blk.number() + " in file " + ti.fileName() + "\n";
		out += "Global Depth: " + getDepth() + "\n";
		out += "Number of directory entries: " + getNumRecs() + "\n";
		
		for (int slot = 0; slot < getNumRecs(); slot++)
		{
			out += "Bucket Number " + Integer.toBinaryString(slot) + ":\n";
			
			Block b = new Block (bucketsTi.fileName(), getInt(slot, DIR_FIELD));
			
			ExtensiHashBucket bucket = new ExtensiHashBucket(b, bucketsTi, tx);
			
			out += bucket.toString();
			bucket.close();
		}
		
		return out;
	}

}