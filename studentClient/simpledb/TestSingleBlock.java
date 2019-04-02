import java.sql.*;
import simpledb.remote.SimpleDriver;

public class TestSingleBlock {
	public static void main(String args[]) {
		
		// THE BUFFER SIZE IS 8 <- found in simpledb.server/SimpleDB.java - BUFFER_SIZE
		
		Connection conn = null;
		try {
			//connect to the DB
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			
			Statement stmt = conn.createStatement();
			
			//execute a query
			String s = "select Name "
					+ "from TEST "
					+ "where ID = 1";
			ResultSet rs = stmt.executeQuery(s);
			
			//print the results of the query
			while(rs.next())
			{
				String name = rs.getString("Name");
				System.out.println(name);
			}
			
			s = "select Name "
					+ "from TEST "
					+ "where ID = 1";
			
			rs = stmt.executeQuery(s);
			while(rs.next()) {
				String name = rs.getString("Name");
				System.out.println(name);
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