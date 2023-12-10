package smallsql.database;
import java.io.*;
import smallsql.database.language.Language;
public class CommandDrop extends Command {
    CommandDrop( Logger log, String catalog, String name, int type ){
		super(log);
        this.type 		= type;
        this.catalog 	= catalog;
        this.name 		= name;
    }
    void executeImpl(SSConnection con, SSStatement st) throws Exception {
        switch(type){
            case SQLTokenizer.DATABASE:
                if(name.startsWith("file:"))
                    name = name.substring(5);
                File dir = new File( name );
                if(!dir.isDirectory() || 
                   !new File( dir, Utils.MASTER_FILENAME ).exists())
               			throw SmallSQLException.create(Language.DB_NONEXISTENT, name);
                File files[] = dir.listFiles();
                if(files != null)
	                for(int i=0; i<files.length; i++){
	                    files[i].delete();
	                }
                dir.delete();
                break;
            case SQLTokenizer.TABLE:
                Database.dropTable( con, catalog, name );
                break;
            case SQLTokenizer.VIEW:
				Database.dropView( con, catalog, name );
				break;
            case SQLTokenizer.INDEX:
            case SQLTokenizer.PROCEDURE:
                throw new java.lang.UnsupportedOperationException();
            default:
                throw new Error();
        }
    }
}