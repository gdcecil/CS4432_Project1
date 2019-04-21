package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;
import java.lang.Exception;


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
	 * Increment the global depth, doubling the number of references
	 * to buckets.
	 * 
	 * This method does not double the buckets themselves; records 
	 * with bucket numbers equivalent modulo 2^(old depth) reference the
	 * same bucket.
	 */
	private void incrementGlobalDepth() throws BucketOverflowException
	{
		int oldNum = getNumRecs(); 

		if (slotpos(2*oldNum -1) >= BLOCK_SIZE)
		{
			throw new BucketOverflowException (tx.size(bucketsTi.fileName()), depth + 1);
		}

		for (int pos = 0; pos < 2*oldNum; pos+=2)
		{
			insert(pos + 1); //make space for the new bucket record
			copyRecord(pos, pos+1); //copy old record to new record
		}

		setDepth(depth+1);
	}

	public void updateDirEntry (int bucketNum, int block)
	{
		setInt(bucketNum, "BlockNum", block);
	}

	public Block getBucketBlock(int key)
	{
		return new Block (bucketsTi.fileName(), getInt(key, DIR_FIELD));
	}

	public void insertIndexRecord (Constant dataval, RID rid)
	{
		int key = dataval.hashCode() % (1 << depth);

		Block blk = getBucketBlock(key);

		ExtensiHashBucket bucket = new ExtensiHashBucket(blk, bucketsTi, tx, key);
		
		
		try 
		{
			while (bucket.isFull())
			{
				if (bucket.getDepth() == depth)
				{
					incrementGlobalDepth();
					key = dataval.hashCode() % (1<<depth);
				}
				
				int oldNum = 1<< bucket.getDepth();
				int oldKey = dataval.hashCode() % (1<< oldNum);
				int numDuplicates = (1<<depth) / oldNum;
				
				Block newblk = bucket.split();
				for (int i = 1; i< numDuplicates; i+=2) 
				{
					updateDirEntry(oldKey + i*oldNum, newblk.number());
				}
				
				int newKey = dataval.hashCode() %  (1 << bucket.getDepth());
				
				blk = newKey == oldKey ? blk : newblk;
				bucket.close();
				bucket = new ExtensiHashBucket(blk, bucketsTi, tx, key);
 			
			}
			
			bucket.insertIndexRecord(dataval, rid );
			bucket.close();
		}
		catch (BucketOverflowException e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
			throw new RuntimeException();
		}

		bucket.close();
	}


	@Override
	protected int slotpos(int slot) {
		// TODO Auto-generated method stub
		return INT_SIZE+INT_SIZE+INT_SIZE + (slot * slotsize);
	}

}

class BucketOverflowException extends Exception
{
	private int idxBlocks; 
	private int oldDepth;

	BucketOverflowException (int idxBlocks, int oldDepth)
	{
		super ("Directory overflow:\n" +
				idxBlocks+ " Index Blocks\n"
				+ oldDepth + "old depth\n");

		this.idxBlocks = idxBlocks; 
		this.oldDepth = oldDepth;
	}

	int getOldDepth()
	{
		return oldDepth;
	}

	int getNumBlocks()
	{
		return idxBlocks;
	}

}
