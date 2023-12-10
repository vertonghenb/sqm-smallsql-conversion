package smallsql.database;
import java.sql.*;
class SSSavepoint implements Savepoint {
	private final int id;
	private final String name;
	long transactionTime;
	SSSavepoint(int id, String name, long transactionTime){
		this.id = id;
		this.name = name;
		this.transactionTime = transactionTime;
	}
	public int getSavepointId(){
		return id;
	}
	public String getSavepointName(){
		return name;
	}
}