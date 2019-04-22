import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
import java.time.LocalTime;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.MILLIS;

public class CreateTestTables {
 final static int maxSize=10000;
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
   /*
   s.executeUpdate("Create table test4" +
     "( a1 int," +
     "  a2 int"+
   ")");
   */
   s.executeUpdate("Create table test5" +
     "( a1 int," +
     "  a2 int"+
   ")");
	
   
   s.executeUpdate("create sh index idx1 on test2 (a1)");
   s.executeUpdate("create bt index idx2 on test3 (a1)");
   //s.executeUpdate("create eh index idx2 on test3 (a1)");

   //Track time to fill relations
   LocalTime time1 = LocalTime.now();

   //Insert values into i<number of tables + 1
   for(int i=1;i<6;i++)
   {
    if(i!=5)
    {
     //TEMP: skip table 4 - extensihash table
     if (i == 4) {
    	 
     } else {
	     rand=new Random(1);// ensure every table gets the same data
	     for(int j=0;j<maxSize;j++)
	     {
	        s.executeUpdate("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
	     
	     }
     }
    }
    else//case where i=5
    {
     for(int j=0;j<maxSize/2;j++)// insert 50000 records into test5
     {
      s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+j+ ")");
     }
    }
   }
   
   LocalTime time2 = LocalTime.now();
   System.out.println("Insert into test1 time: " + time1.until(time2, SECONDS) + " Seconds");
   
   /* TEST CASES
    * Test 1: Query each table based on the same attribute
    * 	The time of each query is printed as well as the result	
    * 	This shows the difference in querying between the different
    * 	indices and the control table which has no index
    */
   
   //Query test1 on a1=1, does not use an index
   time1 = LocalTime.now();
   
   String query = "Select a1, a2 from test1 Where a1=1";
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
   time1 = LocalTime.now();
   
   query = "Select a1, a2 from test2 Where a1=1";
   rs = s.executeQuery(query);
   
   time2 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("HASH INDEX");
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
   
   //Query test3 based on a2=1, should use idx2 (BTree) w/ fldname a2
   time1 = LocalTime.now();
   
   query = "Select a1, a2 from test3 Where a1=1";
   rs = s.executeQuery(query);
   
   time2 = LocalTime.now();
   
   System.out.println("/////////////////////////////");
   System.out.println("B-TREE INDEX");
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

