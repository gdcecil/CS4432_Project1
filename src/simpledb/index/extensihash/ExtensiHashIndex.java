package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import simpledb.file.Block;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.RID;

public class ExtensiHashIndex implements Index {
	private Transaction tx;
	private TableInfo ti;
	private TableInfo dirTi;
	private Schema dirSch;
	private Block dir;
	private Constant searchkey = null;
	
	public ExtensiHashIndex(String idxname, Schema sch, Transaction tx)
	{
		this.tx = tx; 
		ti = new TableInfo(idxname, sch);
		
		dirSch = new Schema();
		dirSch.addIntField("BlockNum");

		if (tx.size(ti.fileName()) == 0)
		{
			tx.append(ti.fileName(), new EHPageFormatter(ti, 0));
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
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
