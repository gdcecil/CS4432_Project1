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
			
			String s = "select Name "
					+ "from TEST";
			ResultSet newrs = stmt.executeQuery(s);
			
			while(newrs.next())
			{
				String id = newrs.getString("Name");
				System.out.println(id);			
			}
			newrs.close();
			
			s = "select Name, Status "
					+ "from test, testmore " 
					+ "where id = moreid";
			
			ResultSet otherrs = stmt.executeQuery(s);
			
			while(otherrs.next()) {
				String name = otherrs.getString("Name");
				System.out.println(name);
			}
			otherrs.close();
			
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