import java.sql.*; 
import simpledb.remote.SimpleDriver;

//File used to test both SimpleDB and our modified version
//  These tests ensure that our database is capable of
//  performing the same actions as simpleDB accurately

//To run this file, first go to CS4432_Project1/src/simpledb.server
//and run startup.java with the argument CS4432
//Once startup is running, run this file without arguments.
//If you'd like to run this file again, stop running startup.java,
//delete the CS4432 file that startup created (likely in the home folder)
//and follow these instructions from the beginning.

//Examples consists of two tables with the following attributes
//	TEST
//			ID	Name	Year

//	TESTMORE
//			moreID	Status	Cost

//Test and TESTMORE share primary keys in ID and moreID

//Statements used in this file test:
// - Creating and inserting into a table
// - Joining two tables (Querying on a joined table)
// - Querying with multiple qualifications
// - Querying with no results
// - Delete entry in table

//The output for each query when run on vanilla SimpleDB is
//given before each statement
//The output when Examples.java is run on CS4432_Project1 is
//given at the end of the file

public class Examples {

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

			// Create new table TESTMORE
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
			for (int i = 1; i < testMoreVals.length; i++)
				stmt.executeUpdate(s + testMoreVals[i]);
			System.out.println("Table TESTMORE Populated.");
			
//			Creating and inserting into a table
//			Output from unmodified SimpleDB:
			
//				greg
//				may
//				dan
//				ted
//				alice
//				carl
//				peg
//				sue
//				meg
			
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
//				select moreID, status, cost
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
			
//			Update a value in the table TEST and query all entries.
//			goodby alice, hello fred
//			Queries:
//				update test
//				set name=fred
//				where id=5
//			
//				select id, name, year
//				from test
//			
//			Output from unmodified SimpleDB:
//			
//				3 dying 100
//				5 dead 2
//				7 alive 5
//				10 alive 5
//				Updated Entry
//				0 rob 2222
//				1 greg 2018
//				2 may 2017
//				3 dan 1334
//				4 ted 1335
//				5 fred 1999
//				6 carl 2002
//				7 peg 1922
//				8 sue 3000
//				9 meg 2002
//				10 carl 3000
			
			
			
			s = "update test "
					+ "set name = 'fred' "
					+ "where id = 5";
			stmt.executeUpdate(s);
			System.out.println("Updated Entry");
			
			s = "select id, name, year "
					+ "from test";
			
			newrs = stmt.executeQuery(s);
			while(newrs.next()) {
				int id = newrs.getInt("id");
				String name = newrs.getString("name");
				int year = newrs.getInt("year");
				System.out.println(id + " " + name + " " + year);
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
//in this file with the database CS4432_Project1 has been printed below.

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

//Updated Entry
//0 rob 2222
//1 greg 2018
//2 may 2017
//3 dan 1334
//4 ted 1335
//5 fred 1999
//6 carl 2002
//7 peg 1922
//8 sue 3000
//9 meg 2002
//10 carl 3000
