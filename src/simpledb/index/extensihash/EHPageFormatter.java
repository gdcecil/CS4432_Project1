package simpledb.index.extensihash;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

public class EHPageFormatter implements PageFormatter {
	private TableInfo ti; 
	private int depth=0;
	private int bucketNum = 0;
	private boolean isBucket = false;
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
