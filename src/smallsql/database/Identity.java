package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
public class Identity extends Number implements Mutable{
	final private long filePos;
	final private FileChannel raFile;
	final private byte[] page = new byte[8];
	private long value;
	public Identity(FileChannel raFile, long filePos) throws IOException{
	    ByteBuffer buffer = ByteBuffer.wrap(page);
		synchronized(raFile){
			raFile.position(filePos);
			raFile.read(buffer);
		}
		value = ((long)(page[ 0 ]) << 56) |
				((long)(page[ 1 ] & 0xFF) << 48) |
				((long)(page[ 2 ] & 0xFF) << 40) |
				((long)(page[ 3 ] & 0xFF) << 32) |
				((long)(page[ 4 ] & 0xFF) << 24) |
				((page[ 5 ] & 0xFF) << 16) |
				((page[ 6 ] & 0xFF) << 8) |
				((page[ 7 ] & 0xFF));
		this.raFile  = raFile;
		this.filePos = filePos;
	}
	private StorePage createStorePage(){
		page[ 0 ] = (byte)(value >> 56);
		page[ 1 ] = (byte)(value >> 48);
		page[ 2 ] = (byte)(value >> 40);
		page[ 3 ] = (byte)(value >> 32);
		page[ 4 ] = (byte)(value >> 24);
		page[ 5 ] = (byte)(value >> 16);
		page[ 6 ] = (byte)(value >> 8);
		page[ 7 ] = (byte)(value);
		return new StorePage( page, 8, raFile, filePos);
	}
	void createNextValue(SSConnection con) throws SQLException{
		value++;
		con.add( createStorePage() );
	}
	void setNextValue(Expression expr) throws Exception{
		long newValue = expr.getLong();
		if(newValue > value){
			value = newValue;
			createStorePage().commit();
		}
	}
	@Override
    public float floatValue() {
		return value;
	}
	@Override
    public double doubleValue() {
		return value;
	}
	@Override
    public int intValue() {
		return (int)value;
	}
	@Override
    public long longValue() {
		return value;
	}
	@Override
    public String toString(){
	    return String.valueOf(value);
	}
    public Object getImmutableObject(){
        return new Long(value);
    }
}