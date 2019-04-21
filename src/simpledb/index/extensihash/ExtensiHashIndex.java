package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;

public class ExtensiHashIndex implements Index {
	
	private Transaction tx;
	
	private TableInfo bucketsTi;
	private TableInfo dirTi;
	
	private Block dirBlk;
	private Block idxBlk;
	
	private ExtensiHashDir dir;
	
	private Constant searchkey = null;
	
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
			dirBlk = tx.append(dirTi.fileName(), new EHPageFormatter(dirTi, 0));
			
			tx.append(bucketsTi.fileName(), new EHPageFormatter(bucketsTi, 0, 0));
			
			dir = new ExtensiHashDir(new Block(dirTi.fileName(),0),dirTi, bucketsTi, this.tx);
			
			dir.updateDirEntry(0,idxBlk.number());
			
			dir.close();
		}
		
		
		
		
	}

	@Override
	public void beforeFirst(Constant searchkey) {
		close();
		

	}

	@Override
	public boolean next() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RID getDataRid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insert(Constant dataval, RID datarid) {
		dir.insertIndexRecord(dataval, datarid);
	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		dir.close();

	}
	
	public static int searchCost(int numblocks, int rpb) {
		return numblocks / HashIndex.NUM_BUCKETS;
	}

}
