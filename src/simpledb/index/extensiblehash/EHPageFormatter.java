package simpledb.index.extensiblehash;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

/**
 * CS4432-Project2
 * 
 * This is essentially the same as BTPageFormatter, modified to format the files 
 * for the extensible hash blocks.
 * 
 * EHPageFormatter formats a block for usage as either an EH directory 
 * or an EH bucket. 
 * 
 * Blocks are formatted in the same way in either case: 
 * 
 * At offset DEPTH_OFFSET=0, an integer depth is stored (interpreted to be global or local
 * as required).
 *  
 * At offset BUCKET_NUM_OFFSET=INT_SIZE, an integer bucket number is stored (the 
 * directory doesn't use this slot, but since both bucket and directory are formatted in the same
 * way, the slot is still created in a directory block).
 * 
 * At offset RECORD_COUNT_OFFSET=2*INT_SIZE, an integer is used to store the number of records (i.e. 
 * index entries or directory entries) in the block. 
 * 
 * At offset RECORD_START_OFFSET=3*INT_SIZE, records are stored. 
 * 
 * On disk, the block will look like
 *
 * |	Depth		| 		 BucketNum 	|		 Num Records	|  Rec0  |  Rec1  | ... |
 * ^DEPTH_OFFSET	|					|						|
 * 					^BUCKET_NUM_OFFSET	|						|
 * 										^RECORD_COUNT_OFFSET	|
 * 																^RECORD_START_OFFSET
 * @author mcwarms, gdcecil		
 *
 */
public class EHPageFormatter implements PageFormatter 
{
	//Table info for information on records
	private TableInfo ti; 
	
	//int to store at DEPTH_OFFSET
	private int depth = 0;
	
	//int to store at BUCKET_NUM_OFFSET
	private int bucketNum = -1;
	
	//keep offsets as static constants
	static final int DEPTH_OFFSET = 0;
	static final int BUCKET_NUM_OFFSET = INT_SIZE;
	static final int RECORD_COUNT_OFFSET = INT_SIZE+INT_SIZE;
	static final int RECORD_START_OFFSET = INT_SIZE+INT_SIZE+INT_SIZE;

	
	/**
	 * CS4432-Project2
	 * 
	 * Construct an EHPageFormatter with depth and tableinfo only. 
	 * In this case, bucketNum is set to -1. This is used to format
	 * the base directory page.
	 * 
	 * @param ti, tableinfo for records
	 * @param depth, integer depth to put at DEPTH_OFFSET
	 */
	public EHPageFormatter (TableInfo ti, int depth) 
	{
		this.ti=ti;
		this.depth = depth;
	}

	/**
	 * CS4432-Project2
	 * 
	 * Construct an EHPageFormatter with depth, tableinfo and bucketNum. 
	 * Used to format buckets.
	 * 
	 * @param ti, tableinfo for records 
	 * @param depth, int to put at DEPTH_OFFSET
	 * @param bucketNum, 
	 */
	public EHPageFormatter (TableInfo ti, int depth, int bucketNum) 
	{ 
		this(ti,depth);
		this.bucketNum = bucketNum;
	}

	/**
	 * CS4432-Project2
	 * 
	 * Formats the page into a bucket for the extensible hash,
	 * that stores local depth of the bucket as an integer, the number
	 * of buckets as an integer, the number of records as an integer, 
	 * and then each record.
	 * 
	 * @param p, page to format
	 */
	public void format(Page p) {
		
		
		p.setInt(DEPTH_OFFSET, depth); //store global/local depth
		
		p.setInt(RECORD_COUNT_OFFSET, 0); //store number of records
		
		p.setInt(BUCKET_NUM_OFFSET, bucketNum);//store data
		
		int recSize = ti.recordLength(); 
		
		for (int pos = RECORD_START_OFFSET; pos + recSize <= BLOCK_SIZE; pos += recSize)
		{
			makeDefaultRecord(p, pos);
		}

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
