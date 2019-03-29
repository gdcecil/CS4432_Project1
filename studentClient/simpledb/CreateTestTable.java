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
			
			String setx = "INSERT INTO TEST(ID, Name, Year) VALUES (10, 'Fred', 2000)";
			stmt.executeUpdate(setx);
			System.out.println("Inserted Fred");
			
			String getx = "SELECT Name FROM TEST WHERE ID = 10";
			ResultSet rs = stmt.executeQuery(getx);
			while (rs.next()) {
				String name = rs.getString("Name");
				System.out.println(name);
			}
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
