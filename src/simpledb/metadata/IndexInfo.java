package simpledb.metadata;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.BLOCK_SIZE;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex; 
import simpledb.index.btree.BTreeIndex;
import simpledb.index.extensiblehash.EHIndex;


/**
 * The information about an index.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the schema of the index records.
 * Its methods are essentially the same as those of Plan.
 * @author Edward Sciore
 */
public class IndexInfo {
   private String indextype, idxname, fldname;
   private Transaction tx;
   private TableInfo ti;
   private StatInfo si;
   
   /**
    * Creates an IndexInfo object for the specified index.
    * @param idxname the name of the index
    * @param tblname the name of the table
    * @param fldname the name of the indexed field
    * @param tx the calling transaction
    */
   public IndexInfo(String indextype, String idxname, String tblname, String fldname,
                    Transaction tx) {
	  this.indextype = indextype;
      this.idxname = idxname;
      this.fldname = fldname;
      this.tx = tx;
      ti = SimpleDB.mdMgr().getTableInfo(tblname, tx);
      si = SimpleDB.mdMgr().getStatInfo(tblname, ti, tx);
   }
   
   /**
    * Opens the index described by this object.
    * @return the Index object associated with this information
    */
   public Index open() {
       Schema sch = schema();
       System.out.println("Index type: " + indextype + " with name: " + idxname);
       //Create index based on type stored in IndexInfo
       if (indextype.equals("sh")) {
         return new HashIndex(idxname, sch, tx);
       } else if (indextype.equals("bt")) {
         return new BTreeIndex(idxname, sch, tx);
       } else if (indextype.equals("eh")) {
         return new EHIndex(idxname, sch, tx);
       } else {
         //Not supposed to reach this point, should handle error
    	 return new HashIndex(idxname, sch, tx);
       }     
      
   }
   
   /**
    * Estimates the number of block accesses required to
    * find all index records having a particular search key.
    * The method uses the table's metadata to estimate the
    * size of the index file and the number of index records
    * per block.
    * It then passes this information to the traversalCost
    * method of the appropriate index type,
    * which provides the estimate.
    * @return the number of block accesses required to traverse the index
    */
   public int blocksAccessed() {
      TableInfo idxti = new TableInfo("", schema());
      int rpb = BLOCK_SIZE / idxti.recordLength();
      int numblocks = si.recordsOutput() / rpb;
      System.out.println("Not finding an indextype");
      //Call searchcost based on the type of index
      if (indextype.equals("sh")) {
    	System.out.println("cost of static hash");
		System.out.println("Numblocks: " + numblocks + " rpb: " + rpb);
	    return HashIndex.searchCost(numblocks, rpb);
	  } else if (indextype.equals("bt")) {
		System.out.println("cost of BTree");
		System.out.println("Numblocks: " + numblocks + " rpb: " + rpb);
	    return BTreeIndex.searchCost(numblocks, rpb);
	  }
      //commented out until implemented in extensihash
	  else if (indextype.equals("eh")) {
		System.out.println("cost of extensihash");
		System.out.println("Numblocks: " + numblocks + " rpb: " + rpb);
	    return EHIndex.searchCost(numblocks,rpb);
	  } 
	  else {
	  //Not supposed to reach this point, should handle error
	     System.out.println("Not finding an indextype");
	     return -1;
	  }
   }
   
   /**
    * Returns the estimated number of records having a
    * search key.  This value is the same as doing a select
    * query; that is, it is the number of records in the table
    * divided by the number of distinct values of the indexed field.
    * @return the estimated number of records having a search key
    */
   public int recordsOutput() {
      return si.recordsOutput() / si.distinctValues(fldname);
   }
   
   /** 
    * Returns the distinct values for a specified field 
    * in the underlying table, or 1 for the indexed field.
    * @param fname the specified field
    */
   public int distinctValues(String fname) {
      if (fldname.equals(fname))
         return 1;
      else 
         return Math.min(si.distinctValues(fldname), recordsOutput());
   }
   
   /**
    * Returns the schema of the index records.
    * The schema consists of the dataRID (which is
    * represented as two integers, the block number and the
    * record ID) and the dataval (which is the indexed field).
    * Schema information about the indexed field is obtained
    * via the table's metadata.
    * @return the schema of the index records
    */
   private Schema schema() {
      Schema sch = new Schema();
      sch.addIntField("block");
      sch.addIntField("id");
      if (ti.schema().type(fldname) == INTEGER)
         sch.addIntField("dataval");
      else {
         int fldlen = ti.schema().length(fldname);
         sch.addStringField("dataval", fldlen);
      }
      return sch;
   }
}
