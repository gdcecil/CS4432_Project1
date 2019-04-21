package simpledb.index.extensihash;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import simpledb.file.Block;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.tx.Transaction;


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
class ExtensiHashDir extends ExtensiHashPage {

	ExtensiHashDir (Block currentblk, TableInfo ti, Transaction tx, int globalDepth)
	{
		super (currentblk, ti, tx, globalDepth);
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
	private void incrementGlobalDepth()
	{
		int oldNum = getNumRecs(); 
		
		for (int pos = 0; pos < 2*oldNum; pos+=2)
		{
			insert(pos + 1); //make space for the new bucket record
			copyRecord(pos, pos+1); //copy old record to new record
		}
		
		setNumRecs(2 * oldNum);
		incrementDepth();
	}
	
	
	@Override
	protected int slotpos(int slot) {
		// TODO Auto-generated method stub
		return INT_SIZE+INT_SIZE+INT_SIZE + (slot * slotsize);
	}

}
