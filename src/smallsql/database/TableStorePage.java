package smallsql.database;
import java.sql.*;
public class TableStorePage extends StorePage{
    final Table table;
	int lockType;
	SSConnection con;
	TableStorePage nextLock;
	TableStorePage(SSConnection con, Table table, int lockType, long fileOffset){
		super( null, 0, table.raFile, fileOffset );
		this.con 	= con;
		this.table = table;
		this.lockType 	= lockType;
	}
    byte[] getData(){
    	return page;
    }
    long commit() throws SQLException{
		if(nextLock != null){
			fileOffset = nextLock.commit();
			nextLock = null;
			rollback();
			return fileOffset;
		}
    	if(lockType == TableView.LOCK_READ)
    		return fileOffset;
    	return super.commit();
    }
    final void freeLock(){
    	table.freeLock(this);
    }
}