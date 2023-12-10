package smallsql.database;
import java.io.File;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import smallsql.database.language.Language;
public class CreateFile extends TransactionStep{
    private final File file;
    private final SSConnection con;
    private final Database database;
    CreateFile(File file, FileChannel raFile,SSConnection con, Database database){
        super(raFile);
        this.file = file;
        this.con = con;
        this.database = database;
    }
    @Override
    long commit(){
        raFile = null;
        return -1;
    }
    @Override
    void rollback() throws SQLException{
        FileChannel currentRaFile = raFile;
        if(raFile == null){
            return;
        }
        raFile = null;
        try{
            currentRaFile.close();
        }catch(Throwable ex){
        }
        con.rollbackFile(currentRaFile);
        if(!file.delete()){
            file.deleteOnExit();
            throw SmallSQLException.create(Language.FILE_CANT_DELETE, file.getPath());
        }
        String name = file.getName();
        name = name.substring(0, name.lastIndexOf('.'));
        database.removeTableView(name);
    }
}