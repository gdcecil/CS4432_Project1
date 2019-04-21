package simpledb.index.extensihash;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

public class EHPageFormatter implements PageFormatter {
	private TableInfo ti; 
	private int depth;
	private int bucketNum = -1;
	private boolean isBucket = false;
	static final int BUCKET_NUM_OFFSET = INT_SIZE;
	static final int RECORD_COUNT_OFFSET = 2*INT_SIZE;
	static final int RECORD_START_OFFSET = 3*INT_SIZE;
	
	public EHPageFormatter (TableInfo ti, int depth) 
	{
		this.ti = ti; 
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
		
		
		p.setInt(0, depth); //store global/local depth
		
		p.setInt(INT_SIZE, 0); //store number of records
		
		int recStart = 2 * INT_SIZE;
		
		if (isBucket) 
		{
			p.setInt(2*INT_SIZE, bucketNum);
			recStart = 3*INT_SIZE;
		}
		
		int recSize = ti.recordLength(); 
		
		for (int pos = recStart; pos + recSize <= BLOCK_SIZE; pos += recSize)
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
