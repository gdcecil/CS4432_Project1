import java.sql.*; 
import simpledb.remote.SimpleDriver;

public class Examples {

	public static void main(String[] args) {
		Connection conn = null;
		try {
			
			//File used to test SimpleDB and our modified version
			//Statements used aim to test:
			// - Creating and inserting into a table
			// - Joining two tables (Querying on a joined table)
			// - Querying with multiple qualifications
			// - Querying with no results
			// - Delete entry in table


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
			String[] testVals = {"(0, 'rob', 2222)",
					"(1, 'greg', 2018)",
					"(2, 'may', 2017)",
					"(3, 'dan' , 1334)", 
					"(4, 'ted', 1335)", 
					"(5, 'alice', 1999)",
					"(6, 'carl', 2002)",
					"(7, 'peg', 1922)",
					"(8, 'sue', 3000)",
					"(9, 'meg', 2002)",
					"(10, 'carl', 3000)"};
			for (int i = 0; i < testVals.length; i++) {
				stmt.executeUpdate(s + testVals[i]);
			}
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
					"(9, 'alive', 700000)",
					"(10, 'alive', 5)"};
			for (int i = 0; i < testMoreVals.length; i++)
				stmt.executeUpdate(s + testMoreVals[i]);
			System.out.println("Table TESTMORE Populated.");
			
//			Print entire table TEST
//			Output from unmodified SimpleDB:
			
//			greg
//			may
//			dan
//			ted
//			alice
//			carl
//			peg
//			sue
//			meg
			
			s = "select Name "
					+ "from TEST";
			ResultSet newrs = stmt.executeQuery(s);
			
			System.out.println(s);
			
			while(newrs.next())
			{
				String id = newrs.getString("Name");
				System.out.println(id);
			}
			newrs.close();
			
//			Print name and status from joining test and testmore
//			Query:
//			  	select Name, Status
//			  	from test, testmore 
//			  	where id = moreid
//			
//			Output from unmodified SimpleDB:
//				
//				dan dying
//				alice dead
//				peg alive
//				meg alive
			
			s = "select Name, Status "
					+ "from test, testmore " 
					+ "where id = moreid";
			
			newrs = stmt.executeQuery(s);
			
			System.out.println(s);
			
			while(newrs.next()) {
				String name = newrs.getString("Name");
				String status = newrs.getString("Status");
				System.out.println(name + " " + status);
			}
			newrs.close();
			
//			Query with multiple conditionals
//			
//			Query:
//				select Name, Cost
//				from TEST, TESTMORE
//				where id = moreid
//				and Year = 3000
//				and Cost = 5
//				
//			Output from unmodified SimpleDB
//			
//				carl 5
			
			
			s = "select Name, Cost "
					+ "from test, testmore "
					+ "where id = moreid "
					+ "and year = 3000 "
					+ "cost = 5";
			
			newrs = stmt.executeQuery(s);
			System.out.println(s);
			System.out.println(5);
			
			while(newrs.next()) {
				String name = newrs.getString("Name");
				int cost = newrs.getInt("Cost");
				System.out.println(name + " " + cost);
			}
			newrs.close();
			
			s = "select Cost "
					+ "from testmore";
			newrs = stmt.executeQuery(s);
			System.out.println(s);
			while(newrs.next()) {
				int cost = newrs.getInt("Cost");
				System.out.println(cost);
			}
			newrs.close();
			
//			Delete a value from a table and query all entries of the table
//			Queries:
//				delete from testmore
//				where moreid = 9
//			
//				select *
//				from testmore
//			
//			Output from unmodified SimpleDB:
//			
//				1 dead -232
//				3 dying 100
//				5 dead 2
//				7 alive 5
//				10 alive 5		
			
			s = "delete from testmore "
					+ "where moreid = 9";
			stmt.executeUpdate(s);
			System.out.println("Deleted entry");
			
			s = "select moreID, status, cost "
					+ "from testmore";
			newrs = stmt.executeQuery(s);
			while(newrs.next()) {
				int id = newrs.getInt("moreid");
				String status = newrs.getString("Status");
				int cost = newrs.getInt("Cost");
				System.out.println(id + " " + status + " " + cost);						
						
			}
			newrs.close();
			
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

//For your convenience, the output from running each statement
//in this file has been printed below.

//Table TEST created.
//Table TEST Populated.
//Table TESTMORE created.
//Table TESTMORE Populated.

//select Name from TEST
//rob
//greg
//may
//dan
//ted
//alice
//carl
//peg
//sue
//meg
//carl

//select Name, Status from test, testmore where id = moreid
//greg dead
//dan dying
//alice dead
//peg alive
//meg alive
//carl alive
//select Name, Cost from test, testmore where id = moreid and year = 3000 cost = 5
//5
//carl 5
//select Cost from testmore
//-232
//100
//2
//5
//700000
//5
//Deleted entry
//1 dead -232
//3 dying 100
//5 dead 2
//7 alive 5
//10 alive 5
