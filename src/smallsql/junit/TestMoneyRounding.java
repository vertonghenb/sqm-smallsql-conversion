package smallsql.junit;
import junit.framework.*;
import java.math.BigDecimal;
import java.sql.*;
import smallsql.database.Money;
public class TestMoneyRounding extends TestCase{
    static final String table = "TestMoneyRounding";
    public void setUp() throws SQLException{
        tearDown();
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		st.execute("create table " + table + "(a money, b smallmoney)");
    }
    public void tearDown(){
        try{
            Connection con = AllTests.getConnection();
            Statement st = con.createStatement();
            st.execute("drop table " + table);
            st.close();
        }catch(Throwable e){
        }
    }
    public void testMoney1() throws Exception{
            Connection con = AllTests.getConnection();
            Statement st = con.createStatement();
            int firstValue = -10000;
            for(int i=firstValue; i<10000; i++){
                st.execute("Insert into " + table + "(a,b) values(" + (i/10000.0) + "," +(i/10000.0) +")");
            }
            st.close();
            verify(firstValue);
    }
    private void verify(int firstValue) throws Exception{
		Connection con = AllTests.getConnection();
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("Select * FROM " + table);
		long i = firstValue;
		while(rs.next()){
			Object obj1 = rs.getObject(1);
			Object obj2 = rs.getObject(2);
			if(obj1 instanceof Money){
				Money mon1 = (Money)obj1;
				Money mon2 = (Money)obj2;
				assertEquals("Roundungsfehler money:", i, mon1.unscaledValue());
				assertEquals("Roundungsfehler smallmoney:", i, mon2.unscaledValue());
			}else{
				BigDecimal mon1 = (BigDecimal)obj1;
				BigDecimal mon2 = (BigDecimal)obj2;
				assertEquals("Roundungsfehler money:", i, mon1.unscaledValue().longValue());
				assertEquals("Roundungsfehler smallmoney:", i, mon2.unscaledValue().longValue());
			}
			i++;
		}
		st.close();
    }
	public void testMoney2() throws Exception{
			Connection con = AllTests.getConnection();
			Statement st = con.createStatement();
			int firstValue = -10000;
			for(int i=firstValue; i<10000; i++){
				st.execute("Insert into " + table + "(a,b) values( (" + i + "/10000.0), (" + i + "/10000.0) )");
			}
			st.close();
			verify(firstValue);
	}
}