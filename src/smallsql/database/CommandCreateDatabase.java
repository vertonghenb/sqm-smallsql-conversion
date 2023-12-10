package smallsql.database;
import java.io.*;
import smallsql.database.language.Language;
public class CommandCreateDatabase extends Command{
    CommandCreateDatabase( Logger log, String name ){
    	super(log);
        this.type = SQLTokenizer.DATABASE;
        if(name.startsWith("file:"))
            name = name.substring(5);
        this.name = name;
    }
    @Override
    void executeImpl(SSConnection con, SSStatement st) throws Exception{
        if( con.isReadOnly() ){
            throw SmallSQLException.create(Language.DB_READONLY);
        }
        File dir = new File( name );
        dir.mkdirs();
        if(!new File(dir, Utils.MASTER_FILENAME).createNewFile()){
        	throw SmallSQLException.create(Language.DB_EXISTENT, name);
        }
    }
}