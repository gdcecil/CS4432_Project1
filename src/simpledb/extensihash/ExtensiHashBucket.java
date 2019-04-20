package simpledb.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;

public class ExtensiHashBucket {
	private Block currentblk; 
	private TableInfo ti;
	private Transaction tx;
	private int slotsize;
	
	public ExtensiHashBucket (Block currentblk, TableInfo ti, Transaction tx) 
	{
		this.currentblk = currentblk; 
		this.ti = ti;
		this.tx = tx; 
		slotsize = ti.recordLength();
		tx.pin(currentblk);
	}

}
