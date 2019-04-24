import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
import simpledb.tx.Transaction;
import simpledb.index.extensihash.ExtensiHashIndex;
import simpledb.record.Schema;
import java.time.LocalTime;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.MILLIS;


public class ExtensiHashTests {
	
	 public static void main(String[] args) {
		 
		 Connection conn=null;
		 Driver d = new SimpleDriver();
		 String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
		 String url = "jdbc:simpledb://" + host;
		 String query = "Create table test" +
		 "( col1 int," +
		 " col2 int)";
		 Statement s=null;

		 try {
			   conn = d.connect(url, null);
			   s=conn.createStatement();
			   
			   s.executeUpdate(query);
			   
			   s.executeUpdate("create eh index idx1 on test (col1)");

			   
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