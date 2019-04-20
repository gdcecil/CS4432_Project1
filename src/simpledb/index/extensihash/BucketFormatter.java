package simpledb.index.extensihash;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

public class BucketFormatter implements PageFormatter {
	private TableInfo ti; 
	private int localDepth;
	private int num;
	static final int LOCAL_DEPTH_OFFSET = 0;
	static final int BUCKET_NUM_OFFSET = INT_SIZE;
	static final int RECORD_COUNT_OFFSET = 2*INT_SIZE;
	static final int RECORD_START_OFFSET = 3*INT_SIZE;

	public BucketFormatter (TableInfo ti, int localDepth, int num) { 
		this.ti = ti;
		this.localDepth = localDepth;
		this.num = num;

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
		p.setInt(LOCAL_DEPTH_OFFSET, localDepth);
		p.setInt(BUCKET_NUM_OFFSET, num);
		p.setInt(RECORD_COUNT_OFFSET, 0);
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
