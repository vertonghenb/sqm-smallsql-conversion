package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
class StorePage extends TransactionStep{
	byte[] page; 
	int pageSize;
	long fileOffset; 
	StorePage(byte[] page, int pageSize, FileChannel raFile, long fileOffset){
	    super(raFile);
		this.page = page;
		this.pageSize = pageSize;
		this.fileOffset = fileOffset;
	}
	final void setPageData(byte[] data, int size){
		page = data;
		pageSize = size;
	}
	@Override
    long commit() throws SQLException{
		try{
			if(raFile != null && page != null){
			    ByteBuffer buffer = ByteBuffer.wrap( page, 0, pageSize );
			    synchronized(raFile){
    				if(fileOffset < 0){
    					fileOffset = raFile.size();
    				}
				    raFile.position(fileOffset);
				    raFile.write(buffer);
				}
			}
			return fileOffset;
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
	}
	@Override
    final void rollback(){
		raFile = null;
	}
}