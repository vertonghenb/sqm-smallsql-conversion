package smallsql.junit;
import java.sql.*;
import java.util.ArrayList;
public class TestThreads extends BasicTestCase{
    volatile Throwable throwable;
    public void testConcurrentRead() throws Throwable{
        ArrayList threadList = new ArrayList();
        throwable = null;
        final String sql = "Select * From table_OrderBy1";
        final Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery("Select * From table_OrderBy1");
        int count = 0;
        while(rs.next()){
            count++;
        }
        final int rowCount = count;
        for(int i = 0; i < 200; i++){
            Thread thread = new Thread(new Runnable(){
                public void run(){
                    try{
                        assertRowCount(rowCount, sql);
                    }catch(Throwable ex){
                        throwable = ex;
                    }
                }
            });
            threadList.add(thread);
            thread.start();
        }
        for(int i = 0; i < threadList.size(); i++){
            Thread thread = (Thread)threadList.get(i);
            thread.join(5000);
        }
        if(throwable != null){
            throw throwable;
        }
    }
    public void testConcurrentThreadWrite() throws Throwable{
        ArrayList threadList = new ArrayList();
        throwable = null;
        final Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        try{
            st.execute("CREATE TABLE ConcurrentWrite( value int)");
            st.execute("INSERT INTO ConcurrentWrite(value) Values(0)");
            for(int i = 0; i < 200; i++){
                Thread thread = new Thread(new Runnable(){
                    public void run(){
                        try{
                            Statement st2 = con.createStatement();
                            int count = st2.executeUpdate("UPDATE ConcurrentWrite SET value = value + 1");
                            assertEquals("Update Count", 1, count);
                        }catch(Throwable ex){
                            throwable = ex;
                        }
                    }
                });
                threadList.add(thread);
                thread.start();
            }
            for(int i = 0; i < threadList.size(); i++){
                Thread thread = (Thread)threadList.get(i);
                thread.join(5000);
            }
            if(throwable != null){
                throw throwable;
            }
            assertEqualsRsValue(new Integer(200), "SELECT value FROM ConcurrentWrite");
        }finally{
            dropTable(con, "ConcurrentWrite");
        }
    }
    public void testConcurrentConnectionWrite() throws Throwable{
        ArrayList threadList = new ArrayList();
        throwable = null;
        Connection con = AllTests.getConnection();
        Statement st = con.createStatement();
        try{
            st.execute("CREATE TABLE ConcurrentWrite( value int)");
            st.execute("INSERT INTO ConcurrentWrite(value) Values(0)");
            for(int i = 0; i < 200; i++){
                Thread thread = new Thread(new Runnable(){
                    public void run(){
                        try{
                            Connection con2 = AllTests.createConnection();
                            Statement st2 = con2.createStatement();
                            int count = st2.executeUpdate("UPDATE ConcurrentWrite SET value = value + 1");
                            assertEquals("Update Count", 1, count);
                            con2.close();
                        }catch(Throwable ex){
                            throwable = ex;
                        }
                    }
                });
                threadList.add(thread);
                thread.start();
            }
            for(int i = 0; i < threadList.size(); i++){
                Thread thread = (Thread)threadList.get(i);
                thread.join(5000);
            }
            if(throwable != null){
                throw throwable;
            }
            assertEqualsRsValue(new Integer(200), "SELECT value FROM ConcurrentWrite");
        }finally{
            dropTable(con, "ConcurrentWrite");
        }
    }
}