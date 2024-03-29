package smallsql.junit;
import junit.framework.*;
import java.sql.*;
public class TestExceptions extends BasicTestCase {
    private TestValue testValue;
	private static boolean init;
    private static final int SYNTAX = 1;
    private static final int RUNTIME= 2;
    private static final TestValue[] TESTS = new TestValue[]{
        a( "01000",    0, SYNTAX,  "SELECT 23 FROM"), 
	    a( "01000",    0, SYNTAX,  "SELECT c FROM exceptions Group By i"), 
	    a( "01000",    0, SYNTAX,  "SELECT first(c) FROM exceptions Group By i ORDER  by c"), 
	    a( "01000",    0, SYNTAX,  "SELECT 1 ORDER BY substring('qwert', 2, -3)"), 
        a( "01000",    0, RUNTIME, "SELECT abs('abc')"), 
        a( "01000",    0, SYNTAX,  "Create Table anyTable (c char(10)"), 
        a( "01000",    0, SYNTAX,  "SELECT {ts 'abc'}"), 
        a( "01000",    0, RUNTIME, "SELECT cast('abc' as timestamp)"), 
        a( "01000",    0, SYNTAX, "SELECT 0xas"), 
        a( "01000",    0, RUNTIME, "SELECT cast('1234-56as' as uniqueidentifier)"), 
        a( "01000",    0, SYNTAX, "SELECT {ts '2020-04-31 00:00:00.000'}"), 
        a( "01000",    0, SYNTAX, "SELECT {ts '2020-02-30 12:30:15.000'}"), 
        a( "01000",    0, SYNTAX, "SELECT {d '2021-02-29'}"), 
        a( "01000",    0, SYNTAX, "SELECT {d '2021-22-09'}"), 
        a( "01000",    0, SYNTAX, "SELECT {t '24:30:15.000'}"), 
        a( "01000",    0, SYNTAX, "SELECT {t '12:60:15.000'}"), 
        a( "01000",    0, SYNTAX, "SELECT {t '12:30:65.000'}"), 
        a( "01000",    0, SYNTAX,  "SELECT * FROM exceptions JOIN"), 
        a( "01000",    0, SYNTAX,  "select 10/2,"),
    };
    TestExceptions(TestValue testValue){
        super(testValue.sql);
        this.testValue = testValue;
    }
    private void init() throws Exception{
    	if(init) return;
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		dropTable( con, "exceptions");
		st.execute("Create Table exceptions (c varchar(30), i int)");
		init = true;
    }
    public void runTest() throws Exception{
    	init();
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
		ResultSet rs = null;
        try{
            rs = st.executeQuery( testValue.sql );
        }catch(SQLException sqle){
            assertTrue( "There should no syntax error:"+sqle, SYNTAX == testValue.errorType);
            assertSQLException( testValue.sqlstate, testValue.errorCode, sqle );
        }
        if(testValue.errorType == SYNTAX){
            assertNull("There should be a syntax error", rs);
            return;
        }
        try{
            while(rs.next()){
                for(int i=1; i<=rs.getMetaData().getColumnCount(); i++){
                    rs.getObject(i);
                }
            }
            fail("There should be a runtime error");
        }catch(SQLException sqle){
            assertSQLException( testValue.sqlstate, testValue.errorCode, sqle );
        }
    }
    public static Test suite() throws Exception{
        TestSuite theSuite = new TestSuite("Exceptions");
        for(int i=0; i<TESTS.length; i++){
            theSuite.addTest(new TestExceptions( TESTS[i] ) );
        }
        return theSuite;
    }
    private static TestValue a(String sqlstate, int errorCode, int errorType, String sql ){
        TestValue value = new TestValue();
        value.sql       = sql;
        value.sqlstate  = sqlstate;
        value.errorCode = errorCode;
        value.errorType = errorType;
        return value;
    }
    private static class TestValue{
        String sql;
        String sqlstate;
        int errorCode;
        int errorType;
    }
}