package smallsql.database;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
public class FileIndexNode extends IndexNode {
	private final FileChannel file;
	private long fileOffset;
	FileIndexNode(boolean unique, char digit, FileChannel file){
		super(unique, digit);
		this.file = file;
        fileOffset = -1;
	}
    @Override
    protected IndexNode createIndexNode(boolean unique, char digit){
        return new FileIndexNode(unique, digit, file);
    }
	void save() throws SQLException{
        StorePage storePage = new StorePage( null, -1, file, fileOffset);
        StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.INSERT, fileOffset);
		save(store);
        fileOffset = store.writeFinsh(null);
	}
	@Override
    void saveRef(StoreImpl output) throws SQLException{
        if(fileOffset < 0){
            save();
        }
		output.writeLong(fileOffset);
	}
    @Override
    IndexNode loadRef( long offset ) throws SQLException{
        StorePage storePage = new StorePage( null, -1, file, offset);
        StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.INSERT, fileOffset);
        MemoryStream input = new MemoryStream();
		FileIndexNode node = new FileIndexNode( getUnique(), (char)input.readShort(), file );
		node.fileOffset = offset;
        node.load( store );
		return node;	
	}
    static FileIndexNode loadRootNode(boolean unique, FileChannel file, long offset) throws Exception{
        StorePage storePage = new StorePage( null, -1, file, offset);
        StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.SELECT, offset);
        FileIndexNode node = new FileIndexNode( unique, (char)store.readShort(), file );
        node.fileOffset = offset;
        node.load( store );
        return node;    
    }
}