package smallsql.database;
import java.sql.*;
class TableStorePageInsert extends TableStorePage {
	final private StorePageLink link = new StorePageLink();
	TableStorePageInsert(SSConnection con, Table table, int lockType){
		super( con, table, lockType, -1);
		link.page = this;
		link.filePos = fileOffset;
	}
	final long commit() throws SQLException{
		long result = super.commit();
		link.filePos = fileOffset;
		link.page = null;
		return result;
	}
	final StorePageLink getLink(){
		return link;
	}
}