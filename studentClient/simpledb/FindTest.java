import java.sql.*;
import simpledb.remote.SimpleDriver;

public class FindTest {
	public static void main(String args[]) {
		
		// THE BUFFER SIZE IS 8 <- found in simpledb.server/SimpleDB.java - BUFFER_SIZE
		
		Connection conn = null;
		try {
			//connect to the DB
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			
			Statement stmt = conn.createStatement();
			
			//execute a query
			String s = "select Name, Status "
					+ "from TEST, TESTMORE "
					+ "where ID =moreID";
			ResultSet rs = stmt.executeQuery(s);
			
			//print the results of the query
			while(rs.next())
			{
				String name = rs.getString("Name");
				String status = rs.getString("Status");
				System.out.println(name + "\t" + status);
			}
			rs.close();
		}
		catch(Exception e) {
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