package simpledb.index.extensihash;


import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.TableInfo;

public class BucketFormatter implements PageFormatter {
	private TableInfo ti; 
	private int localDepth;

	public BucketFormatter (TableInfo ti, int localDepth) { 
		this.ti = ti;
		this.localDepth = localDepth;

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
		p.setInt(0, localDepth);
		p.setInt(INT_SIZE, 0);
		int recSize = ti.recordLength(); 
		for (int pos = 2*INT_SIZE; pos + recSize <= BLOCK_SIZE; pos += recSize)
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
