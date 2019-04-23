package simpledb.index.extensihash;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

/**
 * EHPageFormatter formats a block for usage as either an EH directory 
 * or an EH bucket. 
 * 
 * Blocks are formatted in the same way in either case: 
 * 
 * At offset 0, an integer depth is stored (interpreted to be global or local
 * as required).
 *  
 * At offset INT_SIZE, an integer bucket number is stored (the directory doesn't
 * use this slot, but since both bucket and directory are formatted in the same
 * way, the slot is still created in a directory block).
 * 
 * At offset 2*INT_SIZE, an integer is used to store the number of records (i.e. 
 * index entries or directory entries) in the block. 
 * 
 * At offset 3*INT_SIZE, records are stored. 
 * 
 * On disk, the block will look like
 *
 * |	Depth	|  BucketNum | Num Records|  Rec0  |  Rec1  | ... |
 * 
 * @author mcwarms, gdcecil
 *
 */
public class EHPageFormatter implements PageFormatter 
{
	//info 
	private TableInfo ti; 
	private int depth = 0;
	private int bucketNum = 0;
	private boolean isBucket = false;
	
	//keep offsets as static constants
	static final int DEPTH_OFFSET = 0;
	static final int BUCKET_NUM_OFFSET = INT_SIZE;
	static final int RECORD_COUNT_OFFSET = INT_SIZE+INT_SIZE;
	static final int RECORD_START_OFFSET = INT_SIZE+INT_SIZE+INT_SIZE;
	
	public EHPageFormatter (TableInfo ti)
	{
		this.ti = ti;
	}
	
	public EHPageFormatter (TableInfo ti, int depth) 
	{
		this(ti);
		this.depth = depth;
	}

	public EHPageFormatter (TableInfo ti, int depth, int bucketNum) 
	{ 
		this(ti, depth);
		this.bucketNum = bucketNum;
		this.isBucket = true;

	}

	/**
	 * CS4432-Project2:
	 * 
	 * Formats the page into a bucket for the extensible hash,
	 * that stores local depth of the bucket as an integer, the number of 
	 * records as an integer, and then each record.
	 * 
	 * | local depth (int) | #records (int) | record1 | record2 | ... |
	 * 
	 */
	public void format(Page p) {
		
		
		p.setInt(DEPTH_OFFSET, depth); //store global/local depth
		
		p.setInt(RECORD_COUNT_OFFSET, 0); //store number of records
		
		p.setInt(BUCKET_NUM_OFFSET, bucketNum);//store bucket num
		
		int recSize = ti.recordLength(); 
		
		for (int pos = RECORD_START_OFFSET; pos + recSize <= BLOCK_SIZE; pos += recSize)
		{
			makeDefaultRecord(p, pos);
		}

	}

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
