package smallsql.database;
import java.io.File;
class Lobs extends Table {
	Lobs(Table table) throws Exception{
		super(table.database, table.name);
		raFile = Utils.openRaFile( getFile(database), database.isReadOnly() );
	}
	@Override
    File getFile(Database database){
		return new File( Utils.createLobFileName( database, name ) );
	}
}