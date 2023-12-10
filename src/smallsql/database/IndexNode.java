package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class IndexNode {
	final private boolean unique;
	final private char digit; 
	static final private IndexNode[] EMPTY_NODES = new IndexNode[0];
	private IndexNode[] nodes = EMPTY_NODES;
	private char[] remainderKey;
	private Object value;
	protected IndexNode(boolean unique, char digit){
		this.unique = unique;
		this.digit  = digit;
	}
    protected IndexNode createIndexNode(boolean unique, char digit){
        return new IndexNode(unique, digit);
    }
	final char getDigit(){
		return digit;
	}
	final boolean getUnique(){
		return unique;
	}
	final boolean isEmpty(){
		return nodes == EMPTY_NODES && value == null;
	}
	final void clear(){
		nodes = EMPTY_NODES;
		value = null;
		remainderKey = null;
	}
	final void clearValue(){
		value = null;
	}
	final Object getValue(){
		return value;
	}
	final IndexNode[] getChildNodes(){
		return nodes;
	}
	final IndexNode getChildNode(char digit){
		int pos = findNodePos(digit);
		if(pos >=0) return nodes[pos];
		return null;
	}
	final char[] getRemainderValue(){
		return remainderKey;
	}
	final IndexNode addNode(char digit) throws SQLException{
		if(remainderKey != null) moveRemainderValue();
		int pos = findNodePos( digit );
		if(pos == -1){
			IndexNode node = createIndexNode(unique, digit);
			saveNode( node );
			return node;
		}else{
			return nodes[pos];
		}
	}
	final void removeNode(char digit){
		int pos = findNodePos( digit );
		if(pos != -1){
			int length = nodes.length-1;
			IndexNode[] temp = new IndexNode[length];
			System.arraycopy(nodes, 0, temp, 0, pos);
			System.arraycopy(nodes, pos+1, temp, pos, length-pos);
			nodes = temp;
		}
	}
	final void addNode(char digit, long rowOffset) throws SQLException{
		IndexNode node = addNode(digit);
		if(node.remainderKey != null) node.moveRemainderValue();
		node.saveValue(rowOffset);
	}
	final void saveValue(long rowOffset) throws SQLException{
		if(unique){
			if(value != null) throw SmallSQLException.create(Language.KEY_DUPLICATE);
			value = new Long(rowOffset);
		}else{
			LongTreeList list = (LongTreeList)value;
			if(list == null){
				value = list = new LongTreeList();
			}
			list.add(rowOffset);
		}
	}
	final void addRemainderKey(long rowOffset, long remainderValue, int charCount) throws SQLException{
		saveRemainderValue(remainderValue, charCount);
		value = (unique) ? (Object)new Long(rowOffset) : new LongTreeList(rowOffset);
	}
	final void addRemainderKey(long rowOffset, char[] remainderValue, int offset) throws SQLException{
		saveRemainderValue(remainderValue, offset);
		value = (unique) ? (Object)new Long(rowOffset) : new LongTreeList(rowOffset);
	}
	final IndexNode addRoot(char digit) throws SQLException{
		IndexNode node = addNode(digit);
		if(node.remainderKey != null) node.moveRemainderValue();
		return node.addRoot();
	}
	final IndexNode addRootValue(char[] remainderValue, int offset) throws SQLException{
		saveRemainderValue(remainderValue, offset);
		return addRoot();
	}
	final IndexNode addRootValue( long remainderValue, int digitCount) throws SQLException{
		saveRemainderValue(remainderValue, digitCount);
		return addRoot();
	}
	private final void moveRemainderValue() throws SQLException{
		Object rowOffset = value;
		char[] puffer = remainderKey;
		value = null;
		remainderKey = null;
		IndexNode newNode = addNode(puffer[0]);
		if(puffer.length == 1){
			newNode.value  = rowOffset;
		}else{
			newNode.moveRemainderValueSub( rowOffset, puffer);
		}
	}
	private final void moveRemainderValueSub( Object rowOffset, char[] remainderValue){
		int length = remainderValue.length-1;
		this.remainderKey = new char[length];
		value = rowOffset;
		System.arraycopy( remainderValue, 1, this.remainderKey, 0, length);
	}
	private final void saveRemainderValue(char[] remainderValue, int offset){
		int length = remainderValue.length-offset;
		this.remainderKey = new char[length];
		System.arraycopy( remainderValue, offset, this.remainderKey, 0, length);
	}
	private final void saveRemainderValue( long remainderValue, int charCount){
		this.remainderKey = new char[charCount];
		for(int i=charCount-1, d=0; i>=0; i--){
			this.remainderKey[d++] = (char)(remainderValue >> (i<<4));
		}
	}
	final IndexNode addRoot() throws SQLException{
		IndexNode root = (IndexNode)value;
		if(root == null){
			value = root = createIndexNode(unique, (char)-1);
		}
		return root;
	}
	private final void saveNode(IndexNode node){
		int length = nodes.length;
		IndexNode[] temp = new IndexNode[length+1];
		if(length == 0){
			temp[0] = node;
		}else{
			int pos = findNodeInsertPos( node.digit, 0, length);
			System.arraycopy(nodes, 0, temp, 0, pos);
			System.arraycopy(nodes, pos, temp, pos+1, length-pos);
			temp[pos] = node;
		}
		nodes = temp;
	}
	private final int findNodeInsertPos(char digit, int start, int end){
		if(start == end) return start;
		int mid = start + (end - start)/2;
		char nodeDigit = nodes[mid].digit;
		if(nodeDigit == digit) return mid;
		if(nodeDigit < digit){
			return findNodeInsertPos( digit, mid+1, end );
		}else{
			if(start == mid) return start;
			return findNodeInsertPos( digit, start, mid );
		}
	}
	private final int findNodePos(char digit){
		return findNodePos(digit, 0, nodes.length);
	}
	private final int findNodePos(char digit, int start, int end){
		if(start == nodes.length) return -1;
		int mid = start + (end - start)/2;
		char nodeDigit = nodes[mid].digit;
		if(nodeDigit == digit) return mid;
		if(nodeDigit < digit){
			return findNodePos( digit, mid+1, end );
		}else{
			if(start == mid) return -1;
			return findNodePos( digit, start, mid-1 );
		}
	}
	void save(StoreImpl output) throws SQLException{
		output.writeShort(digit);
		int length = remainderKey == null ? 0 : remainderKey.length;
		output.writeInt(length);
		if(length>0) output.writeChars(remainderKey);
		if(value == null){
			output.writeByte(0);
		}else
		if(value instanceof Long){
			output.writeByte(1);
			output.writeLong( ((Long)value).longValue() );
		}else
		if(value instanceof LongTreeList){
			output.writeByte(2);
			((LongTreeList)value).save(output);
		}else
		if(value instanceof IndexNode){
			output.writeByte(3);
			((IndexNode)value).saveRef(output);
		}
        output.writeShort(nodes.length);
        for(int i=0; i<nodes.length; i++){
            nodes[i].saveRef( output );
        }
	}
	void saveRef(StoreImpl output) throws SQLException{
	}
	IndexNode loadRef( long offset ) throws SQLException{
		throw new Error();
	}
	void load(StoreImpl input) throws SQLException{
		int length = input.readInt();
		remainderKey = (length>0) ? input.readChars(length) : null;
		int valueType = input.readByte();
		switch(valueType){
			case 0:
				value = null;
				break;
			case 1:
				value = new Long(input.readLong());
				break;
			case 2:
				value = new LongTreeList(input);
				break;
			case 3:
				value = loadRef( input.readLong());
				break;
			default: 
				throw SmallSQLException.create(Language.INDEX_CORRUPT, String.valueOf(valueType));
		}
        nodes = new IndexNode[input.readShort()];
        for(int i=0; i<nodes.length; i++){
            nodes[i] = loadRef( input.readLong() );
        }
	}
}