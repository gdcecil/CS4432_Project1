package simpledb.index.extensiblehash;

import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.query.Constant;
import simpledb.record.RID;

public class EHIndex implements Index {

	private Transaction tx;

	private TableInfo bucketsTi;
	private TableInfo dirTi;

	private EHDir dir = null;
	private EHBucket bucket = null;
	
	private boolean printing = true;

	public EHIndex(String idxname, Schema sch, Transaction tx)
	{
		this.tx = tx; 
		bucketsTi = new TableInfo(idxname, sch);

		Schema dirSch = new Schema();
		dirSch.addIntField(EHDir.DIR_FIELD);

		String dirName = idxname + "dir";
		dirTi = new TableInfo(dirName, dirSch);



		if (tx.size(dirTi.fileName()) == 0) 
		{
			tx.append(dirTi.fileName(), new EHPageFormatter(dirTi, 0));

			Block firstIdxBlk = tx.append(bucketsTi.fileName(), new EHPageFormatter(bucketsTi, 0, 0));

			dir = new EHDir(dirTi, bucketsTi, this.tx);

			dir.updateDirEntry(0, firstIdxBlk.number());
			
			dir.setNumRecs(1);

			dir.close();
			
			if (printing)
			{
				System.out.println("Created new extensible hash index\nDirectory file: "
						+ dirName + "\nBucket file: " + bucketsTi.fileName() + "\n(global and local depth are 0)");
			}
		}
		

	}

	public void beforeFirst(Constant searchkey) 
	{
		//close previous instances of ExtensiHashDir and ExtensiHashBucket
		close();

		//Wrap the directory in an ExtensiHashDir object
		dir = new EHDir(dirTi, bucketsTi, tx);

		//get the bucket number for this searchkey
		int bucketNum = EHPage.computeBucketNumber(searchkey, dir.getDepth());

		//get the block of the bucket having bucket number bucketNum
		Block blk = dir.getBucketBlock(bucketNum);

		//Wrap the bucket in an ExtensiHashBucket object
		bucket = new EHBucket(blk, bucketsTi, tx);

		//Move the bucket to the slot strictly before the first index entry 
		//that has value
		bucket.moveBeforeValue(searchkey);

	}


	public boolean next() 
	{
		return bucket.next();
	}

	public RID getDataRid() 
	{
		return bucket.getCurrentRID();
	}

	public void insert(Constant dataval, RID datarid) 
	{
		if (printing)
		{
			 System.out.println("State of index before insert of key " + dataval.toString());
			 System.out.println(dir.toString());
		}
		beforeFirst(dataval);
		dir.insertIndexRecord(dataval, datarid);
		if (printing) 
		{
			System.out.println("State of index after insert of key " + dataval.toString());		
			System.out.println("dir.toString()");
		}
	}

	public void delete(Constant dataval, RID datarid)
	{
		if (printing)
		{
			 System.out.println("State of index before delete of key " + dataval.toString());
			 System.out.println(dir.toString());
		}
		beforeFirst(dataval);

		boolean deleted = false;

		while (!deleted && bucket.next())
		{
			if (getDataRid().equals(datarid))
			{
				bucket.deleteCurrentEntry();
				deleted = true;
			}
		}
		
		if (printing && !deleted)
		{
			System.out.println("No index entry with key " + dataval.toString() + 
					" and RID " + datarid.toString());
			System.out.println("No index entry was deleted the index is unchanged:");
			
		} else 
		{ 
			System.out.println("Deleted entry from index with key " + dataval.toString()
			+ " and RID " + datarid.toString()+ " in bucket number " + Integer.toBinaryString(bucket.getBucketNum())); 
		}

	}

	
	public void close() {
		if (dir != null) dir.close();
		if (bucket != null) bucket.close();
	}

	public static int searchCost(int numblocks, int rpb) {
		return numblocks / HashIndex.NUM_BUCKETS;
	}
	
	@Override
	public String toString()
	{
		close();
		
		dir = new EHDir(dirTi, bucketsTi, tx);
		
		String out = dir.toString();
		//String out = dir.dirTableToString();
		dir.close();
		
		return out;
	}

}
