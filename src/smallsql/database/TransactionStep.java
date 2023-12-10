package smallsql.database;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
abstract class TransactionStep{
    FileChannel raFile;
    TransactionStep(FileChannel raFile){
        this.raFile = raFile;
    }
    abstract long commit() throws SQLException;
    abstract void rollback() throws SQLException;
    void freeLock(){}
}