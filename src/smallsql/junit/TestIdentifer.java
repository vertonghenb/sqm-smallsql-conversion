package smallsql.junit;
import java.sql.*;
public class TestIdentifer extends BasicTestCase {
	public TestIdentifer(){
		super();
	}
	public TestIdentifer(String arg0) {
		super(arg0);
	}
	public void testQuoteIdentifer() throws Exception{
		Connection con = AllTests.getConnection();
		dropTable(con,"QuoteIdentifer");
		con.createStatement().execute("create table \"QuoteIdentifer\"(\"a\" int default 5)");
		ResultSet rs = con.createStatement().executeQuery("SELECT tbl.* from \"QuoteIdentifer\" tbl");
		assertEquals( "a", rs.getMetaData().getColumnName(1));
		assertEquals( "QuoteIdentifer", rs.getMetaData().getTableName(1));
		while(rs.next()){
		}
		dropTable(con,"QuoteIdentifer");
	}
}