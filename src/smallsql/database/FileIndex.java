package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
class FileIndex extends Index {
static void print(Index index, Expressions expressions){
    IndexScrollStatus scroll = index.createScrollStatus(expressions);
    long l;
    while((l= scroll.getRowOffset(true)) >=0){
        System.out.println(l);
    }
    System.out.println("============================");
}
    private final FileChannel raFile;
    FileIndex( boolean unique, FileChannel raFile ) {
        this(new FileIndexNode( unique, (char)-1, raFile), raFile);
    }
    FileIndex( FileIndexNode root, FileChannel raFile ) {
        super(root);
        this.raFile = raFile;
    }
    static FileIndex load( FileChannel raFile ) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(1);
        raFile.read(buffer);
        buffer.position(0);
        boolean unique = buffer.get() != 0;
        FileIndexNode root = FileIndexNode.loadRootNode( unique, raFile, raFile.position() );
        return new FileIndex( root, raFile );
    }
    void save() throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(rootPage.getUnique() ? (byte)1 : (byte)0 );
        buffer.position(0);
        raFile.write( buffer );
        ((FileIndexNode)rootPage).save();
    }
    void close() throws IOException{
        raFile.close();
    }
}