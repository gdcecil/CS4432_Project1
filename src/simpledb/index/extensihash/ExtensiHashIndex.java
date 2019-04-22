package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;

public class ExtensiHashIndex implements Index {

	private Transaction tx;

	private TableInfo bucketsTi;
	private TableInfo dirTi;

	private ExtensiHashDir dir = null;
	private ExtensiHashBucket bucket = null;

	public ExtensiHashIndex(String idxname, Schema sch, Transaction tx)
	{
		this.tx = tx; 
		bucketsTi = new TableInfo(idxname, sch);

		Schema dirSch = new Schema();
		dirSch.addIntField(ExtensiHashDir.DIR_FIELD);

		String dirName = idxname + "dir";
		dirTi = new TableInfo(dirName, dirSch);



		if (tx.size(dirTi.fileName()) == 0) 
		{
			tx.append(dirTi.fileName(), new EHPageFormatter(dirTi, 0));

			Block firstIdxBlk = tx.append(bucketsTi.fileName(), new EHPageFormatter(bucketsTi, 0, 0));

			dir = new ExtensiHashDir(dirTi, bucketsTi, this.tx);

			dir.updateDirEntry(0, firstIdxBlk.number());

			dir.close();
		}

	}

	@Override
	public void beforeFirst(Constant searchkey) 
	{
		//close previous instances of ExtensiHashDir and ExtensiHashBucket
		close();

		//Wrap the directory in an ExtensiHashDir object
		dir = new ExtensiHashDir(dirTi, bucketsTi, tx);

		//get the bucket number for this searchkey
		int bucketNum = ExtensiHashPage.computeBucketNumber(searchkey, dir.getDepth());

		//get the block of the bucket having bucket number bucketNum
		Block blk = dir.getBucketBlock(bucketNum);

		//Wrap the bucket in an ExtensiHashBucket object
		bucket = new ExtensiHashBucket(blk, bucketsTi, tx);

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
		beforeFirst(dataval);
		dir.insertIndexRecord(dataval, datarid);
	}

	public void delete(Constant dataval, RID datarid)
	{
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

	}

	@Override
	public void close() {
		if (dir != null) dir.close();
		if (bucket != null) bucket.close();
	}

	public static int searchCost(int numblocks, int rpb) {
		return numblocks / HashIndex.NUM_BUCKETS;
	}

}
