package simpledb.index.extensiblehash;

import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.query.Constant;
import simpledb.record.RID;


/**
 * CS4432-Project2
 * 
 * Implementation of the extensible hash index. This class implemetnts the index 
 * interface, but most of its methods are passed to the EHDir object. 
 * 
 * This class also creates the index files if they do not already exist.
 * 
 * @author gdcecil, mcwarms
 *
 */
public class EHIndex implements Index {

	//transaction used to read/write data
	private Transaction tx;

	//tableinfo for the buckets
	private TableInfo bucketsTi;
	
	//tableinfo for the directory
	private TableInfo dirTi;

	//current instance of a directory
	private EHDir dir = null;
	
	//current instance of a bucket
	private EHBucket bucket = null;

	//whether or not to print data
	private boolean printing = false;

	
	/**
	 * CS4432-Project2 
	 * 
	 * Constructs a new EHIndex. If files for the directory and 
	 * the buckets do not exist, they are created. 
	 * 
	 * @param idxname, name of the index
	 * @param sch, schema for the directory entry
	 * @param tx, transaction to use
	 */
	public EHIndex(String idxname, Schema sch, Transaction tx)
	{
		this.tx = tx; 
		
		//create a tableinfo for the buckets
		bucketsTi = new TableInfo(idxname, sch);

		//make the directory schema
		Schema dirSch = new Schema();
		dirSch.addIntField(EHDir.DIR_FIELD);

		
		//make directory table info
		String dirName = idxname + "dir";
		dirTi = new TableInfo(dirName, dirSch);


		//if the files don't exist, create them
		if (tx.size(dirTi.fileName()) == 0) 
		{
			//format a new block for the directory and append it to the dir file
			tx.append(dirTi.fileName(), new EHPageFormatter(dirTi, 0));

			//add the first bucket
			Block firstIdxBlk = tx.append(bucketsTi.fileName(), new EHPageFormatter(bucketsTi, 0, 0));

			//update the directory entry for this bucket and set the number of records to 1
			dir = new EHDir(dirTi, bucketsTi, this.tx);

			dir.updateDirEntry(0, firstIdxBlk.number());

			dir.setNumRecs(1);

			dir.close();

			//if printing, print out that a new bucket file and directory file have been created
			if (printing)
			{
				System.out.println("Created new extensible hash index\nDirectory file: "
						+ dirName + "\nBucket file: " + bucketsTi.fileName() + "\n(global and local depth are 0)");
			}
		}


	}
	
	/**
	 * CS4432-Project2
	 * 
	 * Looks up searchkey and moves the index position to before the first index 
	 * entry with a matching data value. 
	 * 
	 * This method closes previous dir and bucket and creates new instances, with 
	 * the bucket the bucket where searchkey belongs.
	 * 
	 * @param searchkey, constant to lookup
	 */
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


	/**
	 * CS4432-Project2
	 * 
	 * Moves the index to the next entry, and if the value of that entry matches the search
	 * key it returns true, and false otherwise. 
	 * 
	 * Calls the next method in the bucket. 
	 * 
	 * Needs to be called after beforeFirst().
	 * 
	 * @return true if the next index entry matches the current searchkey
	 */
	public boolean next() 
	{
		return bucket.next();
	}

	/**
	 * CS4432-Project2
	 * 
	 * Gets the RID of the entry at the current postition by calling the corresponding method
	 * in EHBucket
	 * 
	 * @return RID of index entry at current position.
	 */
	public RID getDataRid() 
	{
		return bucket.getCurrentRID();
	}

	
	/**
	 * CS4432-Project2
	 * 
	 * Inserts the entry consisting of dataval and rid into the index.
	 * 
	 * @param dataval, constant dataval in entry to insert 
	 * @param datarid, rid in entry to insert 
	 */
	public void insert(Constant dataval, RID datarid) 
	{
		//First move to the correct bucket for dataval
		beforeFirst(dataval);
		
		
		//If output is set to on then print info about insert
		if (printing) 
		{
			int gDepth = dir.getDepth();
			int lDepth = bucket.getDepth();
			
			System.out.println("Insert value " + dataval.toString() + ", RID " + datarid.toString() + ":\n");
			System.out.println("\tHashcode modulo 2^(global depth) = " + (dataval.hashCode() % (1 << gDepth)) + "\n");
			System.out.println("\tHashcode modulo 2^(local depth) = "  + (dataval.hashCode() % (1 << lDepth)) + "\n\n");
			
			System.out.println("State of index before insert of key " + dataval.toString() + "\n");		
			System.out.println(this.toString());
		}
		
		//call the insert method in EHDir
		dir.insertIndexRecord(dataval, datarid);
		
		//Print index info after the insert if printing is on
		if (printing) 
		{
			System.out.println("State of index after insert of key " + dataval.toString() + "\n");		
			System.out.println(this.toString());
		}
	}

	/**
	 * CS4432-Project2 
	 * 
	 * Deletes all index entries with dataval and datarid. Does this by looking up 
	 * dataval using the beforeFirst() function, and then while next() delete each matching 
	 * index entry 
	 * 
	 * @param dataval, constant dataval of entry to delete 
	 * @param datarid, RID of entry to delete
	 */
	public void delete(Constant dataval, RID datarid)
	{
		//first look up dataval
		beforeFirst(dataval);
		
		
		//if printing, print data about the index before delete
		if (printing)
		{
			int gDepth = dir.getDepth();
			int lDepth = bucket.getDepth();
			
			System.out.println("Delete value " + dataval.toString() + ", RID " + datarid.toString() + ":\n");
			System.out.println("\tHashcode modulo 2^(global depth) = " + (dataval.hashCode() % (1 << gDepth)) + "\n");
			System.out.println("\tHashcode modulo 2^(local depth) = "  + (dataval.hashCode() % (1 << lDepth)) + "\n\n");
			
			System.out.println("State of index before delete of key " + dataval.toString() + "\n");		
			System.out.println(this.toString());
		}
		
		//check if we have actually deleted something
		boolean deleted = false;

		//while there are index entries with matching search key, check if the RIDs are equal 
		//if they are, delete the current entry.
		while (bucket.next())
		{
			if (getDataRid().equals(datarid))
			{
				deleted = true;
				bucket.deleteCurrentEntry();
			}
		}

		
		//If printing and we haven't deleted anythin, say so
		if (printing && !deleted)
		{
			System.out.println("No index entry with key " + dataval.toString() + 
					" and RID " + datarid.toString());
			System.out.println("No index entry was deleted; the index is unchanged:");

		} else if (printing) //if we deleted something print the index after deletion
		{ 
			System.out.println("Deleted entry from index with key " + dataval.toString()
			+ " and RID " + datarid.toString()+ " in bucket number " + 
					Integer.toBinaryString(bucket.getBucketNum())); 
			System.out.println("State of index after delete of key " + dataval.toString() + "\n");		
			System.out.println(this.toString());
			
		}

	}

	/**
	 * CS4432-Project2
	 * 
	 * Closes the current EHDir object and the current EHBucket object. 
	 */
	public void close() {
		if (dir != null) dir.close();
		if (bucket != null) bucket.close();
	}

	
	//TODO fix this
	public static int searchCost(int numblocks, int rpb) {
		return numblocks / HashIndex.NUM_BUCKETS;
	}

	/**
	 * CS4432-Project2
	 * 
	 * toString method for EHIndex. This just calls toString on EHDir.
	 */
	@Override
	public String toString()
	{
		close();

		dir = new EHDir(dirTi, bucketsTi, tx);

		String out = dir.toString();
		
		dir.close();

		return out;
	}

}
