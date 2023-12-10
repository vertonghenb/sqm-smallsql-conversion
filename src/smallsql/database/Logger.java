package smallsql.database;
import java.io.PrintWriter;
import java.sql.*;
class Logger {
	boolean isLogging(){
		return DriverManager.getLogWriter() != null;
	}
	void println(String msg){
		PrintWriter log = DriverManager.getLogWriter();
		if(log != null){
			synchronized(log){
				log.print("[Small SQL]");
				log.println(msg);
				log.flush();
			}
		}
	}
}