package smallsql.junit;
import junit.framework.*;
import java.sql.*;
import java.util.Properties;
public class AllTests extends TestCase{
    final static String CATALOG = "AllTests";
    final static String JDBC_URL = "jdbc:smallsql:" + CATALOG;
    private static Connection con;
    public static Connection getConnection() throws SQLException{
        if(con == null || con.isClosed()){
            con = createConnection();
        }
        return con;
    }
	public static Connection createConnection() throws SQLException{
		new smallsql.database.SSDriver();
		new sun.jdbc.odbc.JdbcOdbcDriver();
		return DriverManager.getConnection(JDBC_URL + "?create=true;locale=en");
	}
    public static Connection createConnection(String urlAddition, 
    		Properties info) 
    throws SQLException {
		new smallsql.database.SSDriver();
		new sun.jdbc.odbc.JdbcOdbcDriver();
		if (urlAddition == null) urlAddition = "";
		if (info == null) info = new Properties();
		String urlComplete = JDBC_URL + urlAddition;
		return DriverManager.getConnection(urlComplete, info);
    }
    public static void printRS( ResultSet rs ) throws SQLException{
        while(rs.next()){
            for(int i=1; i<=rs.getMetaData().getColumnCount(); i++){
                System.out.print(rs.getObject(i)+"\t");
            }
            System.out.println();
        }
    }
    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("SmallSQL all Tests");
        theSuite.addTestSuite( TestAlterTable.class );
        theSuite.addTestSuite( TestAlterTable2.class );
        theSuite.addTest    ( TestDataTypes.suite() );
        theSuite.addTestSuite(TestDBMetaData.class);
		theSuite.addTestSuite(TestExceptionMethods.class);
		theSuite.addTest     (TestExceptions.suite());
		theSuite.addTestSuite(TestDeleteUpdate.class);
		theSuite.addTest     (TestFunctions.suite() );
		theSuite.addTestSuite(TestGroupBy.class);
		theSuite.addTestSuite(TestIdentifer.class);
		theSuite.addTest     (TestJoins.suite());
        theSuite.addTestSuite(TestLanguage.class);
		theSuite.addTestSuite(TestMoneyRounding.class );
		theSuite.addTest     (TestOperatoren.suite() );
		theSuite.addTestSuite(TestOrderBy.class);
		theSuite.addTestSuite(TestOther.class);
        theSuite.addTestSuite(TestResultSet.class);
		theSuite.addTestSuite(TestScrollable.class);
        theSuite.addTestSuite(TestStatement.class);
        theSuite.addTestSuite(TestThreads.class);
        theSuite.addTestSuite(TestTokenizer.class);
        theSuite.addTestSuite(TestTransactions.class);
        return theSuite;
    }
    public static void main(String[] argv) {
    	try{
    		junit.textui.TestRunner.main(new String[]{AllTests.class.getName()});
    	}catch(Throwable e){
    		e.printStackTrace();
    	}
    }
}