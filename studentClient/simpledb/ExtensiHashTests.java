import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import java.time.LocalTime;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.MILLIS;



/**
 * 
 * @author mcwarms
 *
 */
public class ExtensiHashTests {
	
	 public static void main(String[] args) {
		 
		 Connection conn=null;
		 Driver d = new SimpleDriver();
		 String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
		 String url = "jdbc:simpledb://" + host;
		 

		 
		 Statement s=null;

		 try {
			   conn = d.connect(url, null);
			   s=conn.createStatement();
			   
			   s.executeUpdate("Create table test1" +
					     "( a1 int," +
					     "  a2 int"+
					   			")");
			   
			   s.executeUpdate("create eh index idx1 on test1 (a1)");
			   
			   
			   s.executeUpdate( "insert into test1 (a1, a2) values(0,1)");
			   s.executeUpdate( "Insert into test1 (a1, a2) values(0,2)");
			   s.executeUpdate( "Insert into test1 (a1, a2) values(1,1)");
			   s.executeUpdate( "Insert into test1 (a1, a2) values(1,2)");
			   
			   //With bucket size 2, bucket 1 should now split and global
			   //depth should be 2. A new bucket should should also point
			   //to bucket 0 now
			   s.executeUpdate( "Insert into test1 (a1, a2) values(3,3)");
			   
			   //This value should be inserted into the newly split bucket 3
			   s.executeUpdate( "Insert into test1 (a1, a2) values(3,1)");
			   
			   //Bucket 1 should split again increasing global depth to 3 and
			   //making a bunch of new buckets that point to 0, 1, and 3
			   s.executeUpdate( "Insert into test1 (a1, a2) values(5,1)");
			   
			   //This should fill bucket 4
			   s.executeUpdate( "Insert into test1 (a1, a2) values(4,1)");
			   
			   //This should fill bucket 2 because althouh bucket 6 exists,
			   //bucket 2 and 6 have local depth 2 meaning bucket 6 still
			   //points to bucket 2
			   s.executeUpdate( "Insert into test1 (a1, a2) values(6,1)");
			   			   
		 }
	 	 catch (SQLException e) {
		 // TODO Auto-generated catch block
	 		 e.printStackTrace();
		 }
		 finally{
			 try {
				 conn.close();
			 } catch (SQLException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
		 }
	 }
	
}