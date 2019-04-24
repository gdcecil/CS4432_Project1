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


public class CreateTestTables {
 final static int maxSize=100;
 /**
  * @param args
  */
 public static void main(String[] args) {
  // TODO Auto-generated method stub
	   
  Connection conn=null;
  Driver d = new SimpleDriver();
  String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
  String url = "jdbc:simpledb://" + host;
  String qry="Create table test1" +
  "( a1 int," +
  "  a2 int"+
  ")";
  Random rand=null;
  Statement s=null;
  try {
   conn = d.connect(url, null);
   s=conn.createStatement();
   s.executeUpdate("Create table test1" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test2" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test3" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test4" +
     "( a1 int," +
     "  a2 int"+
   ")");
   s.executeUpdate("Create table test5" +
     "( a3 int," +
     "  a4 int"+
   ")");
	
   
   s.executeUpdate("create sh index idx1 on test2 (a1)");
   s.executeUpdate("create bt index idx2 on test3 (a1)");
   s.executeUpdate("create eh index idx3 on test4 (a1)");

   //Track time to fill relations
   LocalTime time1 = LocalTime.now();
   LocalTime time2 = LocalTime.now();
   LocalTime time3 = LocalTime.now();
   LocalTime time4 = LocalTime.now();
   LocalTime time5 = LocalTime.now();
   LocalTime time6 = LocalTime.now();
   LocalTime time7 = LocalTime.now();
   LocalTime time8 = LocalTime.now();
   LocalTime time9 = LocalTime.now();
   LocalTime time10 = LocalTime.now();
   LocalTime time11 = LocalTime.now();
   LocalTime time12 = LocalTime.now();
   LocalTime time13 = LocalTime.now();
   LocalTime time14 = LocalTime.now();
   LocalTime time15 = LocalTime.now();
   LocalTime time16 = LocalTime.now();

   //Insert values into i<number of tables + 1
   for(int i=1;i<6;i++)
   {
    if(i!=5)
    {
     rand=new Random(1);// ensure every table gets the same data
     for(int j=0;j<maxSize;j++)
     {
        s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+rand.nextInt(1000)+ ")");
     }
    }
    else//case where i=5
    {
     for(int j=0;j<maxSize/2;j++)// insert 50000 records into test5
     {
      s.executeUpdate("insert into test"+i+" (a3,a4) values("+j+","+j+ ")");
     }
    }
   }
   
   time2 = LocalTime.now();
   System.out.println("Insert into tables time: " + time1.until(time2, SECONDS) + " Seconds");
  
   
   
   /* TEST CASES
    * Test 1: Query each table based on the same attribute
    * 	The time of each query is printed as well as the result	.
    * 	This shows the difference in querying between the different
    * 	indices and the control table which has no index
    * 
    * Test 2: Query the join of each table with table5
    *   The time of each query is printed as well as the result.
    */
   
   //Query test1 on a1=1, does not use an index
   time1 = LocalTime.now();
   
   String query = "Select a2 from test1 Where a1=1";
   ResultSet rs = s.executeQuery(query);
   
   time2 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("NO INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time1.until(time2, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a2 = rs.getInt("a2");
		System.out.println(a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test2 based on a1=1, should use idx1 (Hash) w/ fldname a1
   time3 = LocalTime.now();
   
   query = "Select a1, a2 from test2 Where a1=1";
   rs = s.executeQuery(query);
   
   time4 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("HASH INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time3.until(time4, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a2 = rs.getInt("a2");
		System.out.println(a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test3 based on a1=1, should use idx2 (BTree) w/ fldname a2
   time5 = LocalTime.now();
   
   query = "Select a1, a2 from test3 Where a1=1";
   rs = s.executeQuery(query);
   
   time6 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("B-TREE INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time5.until(time6, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a2 = rs.getInt("a2");
		System.out.println(a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test3 based on a1=1, should use idx2 (Extensihash) w/ fldname a2
   time7 = LocalTime.now();
   
   query = "Select a1, a2 from test4 Where a1=1";
   rs = s.executeQuery(query);
   
   time8 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("Extensihash INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time7.until(time8, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a2 = rs.getInt("a2");
		System.out.println(a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test1 joined with test5 based on a1=a1, should use no index)
   time9 = LocalTime.now();
   
   query = "Select a2, a4 from test1, test5 Where a1=a3";
   rs = s.executeQuery(query);
   
   time10 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("JOINED NO INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time9.until(time10, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a1 = rs.getInt("a2");
		int a2 = rs.getInt("a4");
		System.out.println("a2: " + a1 + " a4: " + a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test2 joined with test5 based on a1=a1, should use idx1 (Static)
   time11 = LocalTime.now();
   
   query = "Select a2, a4 from test2, test5 Where a1=a3";
   rs = s.executeQuery(query);
   
   time12 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("JOINED HASH INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time11.until(time12, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a1 = rs.getInt("a2");
		int a2 = rs.getInt("a4");
		System.out.println("a2: " + a1 + " a4: " + a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test3 joined with test5 based on a1=a1, should use idx2 (BTree)
   time13 = LocalTime.now();
   
   query = "Select a2, a4 from test3, test5 Where a1=a3";
   rs = s.executeQuery(query);
   
   time14 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("JOINED B-TREE INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time13.until(time14, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a1 = rs.getInt("a2");
		int a2 = rs.getInt("a4");
		System.out.println("a2: " + a1 + " a4: " + a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   //Query test4 joined with test5 based on a2=1, should use idx3 (Extensihash)
   time15 = LocalTime.now();
   
   query = "Select a2, a4 from test4, test5 Where a1=a3";
   rs = s.executeQuery(query);
   
   time16 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("JOINED EXTENSIHASH INDEX");
   System.out.println("Query: " + query);
   System.out.println("Run time: " + time15.until(time16, MILLIS) + " Milliseconds");
   System.out.println("Query output: ");
   while(rs.next())
	{
		int a1 = rs.getInt("a2");
		int a2 = rs.getInt("a4");
		System.out.println("a2: " + a1 + " a4: " + a2);
	}
   System.out.println("/////////////////////////////");
   
   rs.close();
   
   query = "Delete From test1 Where a1=1";
   
      
   //Print out of all time data collected
   System.out.println("Table 1 (no index) query time: " + time1.until(time2, MILLIS) + " Milliseconds");
   System.out.println("Table 2 (Hash) query time: " + time3.until(time4, MILLIS) + " Milliseconds");
   System.out.println("Table 3 (B-Tree) query time: " + time5.until(time6, MILLIS) + " Milliseconds");
   System.out.println("Table 4 (Extensihash) query time: " + time7.until(time8, MILLIS) + " Milliseconds");
   System.out.println("Table 1 (no index) join time: " + time9.until(time10, MILLIS) + " Milliseconds");
   System.out.println("Table 2 (Hash) join time: " + time11.until(time12, MILLIS) + " Milliseconds");
   System.out.println("Table 3 (B-Tree) join time: " + time13.until(time14, MILLIS) + " Milliseconds");
   System.out.println("Table 4 (Extensihash) join time: " + time15.until(time16, MILLIS) + " Milliseconds");


  } catch (SQLException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
  }finally
  {
   try {
    conn.close();
   } catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
   }
  }
 }
}

