import java.sql.*; 
import simpledb.remote.SimpleDriver;

public class CreateTestTable {

	public static void main(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();
			String s = "create table TEST(ID int, Name varchar(20), Year int)";
			stmt.executeUpdate(s);
			System.out.println("Table TEST created.");
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
