package smallsql.database;
import java.sql.*;
class ForeignKey {
	final String pkTable;
	final String fkTable;
	final IndexDescription pk;
	final IndexDescription fk;
	final int updateRule = DatabaseMetaData.importedKeyNoAction;
	final int deleteRule = DatabaseMetaData.importedKeyNoAction;
	ForeignKey(String pkTable, IndexDescription pk, String fkTable, IndexDescription fk){
		this.pkTable = pkTable;
		this.fkTable = fkTable;
		this.pk = pk;
		this.fk = fk;
	}
}