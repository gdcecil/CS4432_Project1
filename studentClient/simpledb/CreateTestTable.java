import java.sql.*; 
import simpledb.remote.SimpleDriver;

public class CreateTestTable {

	public static void main(String[] args) {
		Connection conn = null;
		try {


			//connect to the DB
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// create a three-attribute table
			Statement stmt = conn.createStatement();
			String s = "create table TEST(ID int, Name varchar(20), Year int)";
			stmt.executeUpdate(s);
			System.out.println("Table TEST created.");

			// insert some records into TEST
			s = "insert into TEST(ID, Name, Year) values ";
			String[] testVals = {"0, 'rob', 2222)",
					"(1, 'greg', 2018)",
					"(2, 'may', 2017)",
					"(3, 'dan' , 1334)", 
					"(4, 'ted', 1335)", 
					"(5, 'alice', 1999)",
					"(6, 'carl', 2002)",
					"(7, 'peg', 1922)",
					"(8, 'sue', 3000)",
					"(9, 'meg', 2002)"};
			for (int i = 1; i < testVals.length; i++)
				stmt.executeUpdate(s + testVals[i]);
			System.out.println("Table TEST Populated.");

			// create a five attribute table 
			s = "create table TESTMORE(moreID int, Status varchar(8), Cost int)";
			stmt.executeUpdate(s);
			System.out.println("Table TESTMORE created.");


			// insert some records into TESTMORE
			s = "insert into TESTMORE(moreID, Status, Cost) values ";
			String[] testMoreVals = {
					"(1, 'dead', -232)",
					"(3, 'dying' , 100)", 
					"(5, 'dead', 2)",
					"(7, 'alive', 5)",
					"(9, 'alive', 700000)"};
			for (int i = 1; i < testMoreVals.length; i++)
				stmt.executeUpdate(s + testMoreVals[i]);
			System.out.println("Table TESTMORE Populated.");
			
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
