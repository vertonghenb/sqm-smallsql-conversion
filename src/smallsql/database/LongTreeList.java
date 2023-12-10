package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
final class LongTreeList {
	private byte[] data;
	private int size;
	private int offset;
	static final private int pointerSize = 3; 
	LongTreeList(){
		data = new byte[25];
	}
	LongTreeList(long value) throws SQLException{
		this();
		add(value);
	}
	LongTreeList(StoreImpl input){
		int readSize = input.readInt();
		data     = input.readBytes(readSize);
	}
	final void save(StoreImpl output){
		output.writeInt(size);
		output.writeBytes(data, 0, size);
	}
	final void add(long value) throws SQLException{
		offset = 0;
		if(size == 0){
			writeShort( (int)(value >> 48) );
			writePointer ( offset+pointerSize+2 );
			writeShort( 0 );
			writeShort( (int)(value >> 32) );
			writePointer ( offset+pointerSize+2 );
			writeShort( 0 );
			writeShort( (int)(value >> 16) );
			writePointer ( offset+pointerSize+2 );
			writeShort( 0 );
			writeShort( (int)(value) );
			writeShort( 0 );
			size = offset;
			return;
		}
		int shift = 48;
		boolean firstNode = (size > 2); 
		while(shift>=0){
			int octet = (int)(value >> shift) & 0xFFFF;
			while(true){
				int nextEntry = getUnsignedShort();
				if(nextEntry == octet){
					if(shift == 0) return; 
					offset = getPointer();
					firstNode = true;
					break;
				}
				if((nextEntry == 0 && !firstNode) || nextEntry > octet){
					offset -= 2;
					while(true){
						if(shift != 0){
							offset = insertNode(octet);						
						}else{
							insertNodeLastLevel(octet);	
							return;
						}
						shift -= 16;
						octet = (int)(value >> shift) & 0xFFFF;
					}
				}
				firstNode = false;
				if(shift != 0) offset += pointerSize;
			}
			shift -= 16;
		}
	}
	final void remove(long value) throws SQLException{
		if(size == 0) return;
		int offset1 = 0;
		int offset2 = 0;
		int offset3 = 0;
		offset = 0;
		int shift = 48;
		boolean firstNode = true; 
		boolean firstNode1 = true;
		boolean firstNode2 = true;
		boolean firstNode3 = true;
		while(shift>=0){
			int octet = (int)(value >> shift) & 0xFFFF;
			while(true){
				int nextEntry = getUnsignedShort();
				if(nextEntry == octet){
					if(shift == 0){
						offset -= 2;
						removeNodeLastLevel();
						while(firstNode && getUnsignedShort() == 0){
							offset -= 2;
							removeNodeLastLevel(); 
							if(shift >= 3) 
								break;
							offset = offset1;
							offset1 = offset2;
							offset2 = offset3;
							firstNode = firstNode1;
							firstNode1 = firstNode2;
							firstNode2 = firstNode3;
							removeNode();
							shift++;
						}
						return;
					}
					offset3 = offset2;
					offset2 = offset1;
					offset1 = offset -2;
					offset = getPointer();
					firstNode3 = firstNode2;
					firstNode2 = firstNode1;
					firstNode1 = firstNode;
					firstNode = true;
					break;
				}
				if((nextEntry == 0 && !firstNode) || nextEntry > octet){
					return;
				}
				firstNode = false;
				if(shift != 0) offset += pointerSize;
			}
			shift -= 16;
		}
	}
	final long getNext(LongTreeListEnum listEnum){
		int shift = (3-listEnum.stack) << 4;
		if(shift >= 64) return -1; 
		offset 		= listEnum.offsetStack[listEnum.stack];
		long result = listEnum.resultStack[listEnum.stack];
		boolean firstNode = (offset == 0); 
		while(true){
			int nextEntry = getUnsignedShort();
			if(nextEntry != 0 || firstNode){
				result |= (((long)nextEntry) << shift);
				if(listEnum.stack>=3){
					listEnum.offsetStack[listEnum.stack] = offset;
					return result;
				}
				listEnum.offsetStack[listEnum.stack] = offset+pointerSize;
				offset = getPointer();
				shift -= 16;
				listEnum.stack++;
				listEnum.resultStack[listEnum.stack] = result;
				firstNode = true;
			}else{
				shift += 16;
				listEnum.stack--;
				if(listEnum.stack<0) return -1; 
				result = listEnum.resultStack[listEnum.stack];
				offset = listEnum.offsetStack[listEnum.stack];
				firstNode = false;
			}
		}
	}
	final long getPrevious(LongTreeListEnum listEnum){
		int shift = (3-listEnum.stack) << 4;
		if(shift >= 64){ 
			shift = 48;
			offset = 0;
			listEnum.stack = 0;
			listEnum.offsetStack[0] = 2 + pointerSize;
			loopToEndOfNode(listEnum);
		}else{
			setPreviousOffset(listEnum);
		}
		long result = listEnum.resultStack[listEnum.stack];
		while(true){
			int nextEntry = (offset < 0) ? -1 : getUnsignedShort();
			if(nextEntry >= 0){
				result |= (((long)nextEntry) << shift);
				if(listEnum.stack>=3){
					listEnum.offsetStack[listEnum.stack] = offset;
					return result;
				}
				listEnum.offsetStack[listEnum.stack] = offset+pointerSize;
				offset = getPointer();
				shift -= 16;
				listEnum.stack++;
				listEnum.resultStack[listEnum.stack] = result;
				loopToEndOfNode(listEnum);
			}else{
				shift += 16;
				listEnum.stack--;
				if(listEnum.stack<0) return -1; 
				result = listEnum.resultStack[listEnum.stack];
				setPreviousOffset(listEnum);
			}
		}
	}
	final private void setPreviousOffset(LongTreeListEnum listEnum){
		int previousOffset = listEnum.offsetStack[listEnum.stack] - 2*(2 + (listEnum.stack>=3 ? 0 : pointerSize));
		if(listEnum.stack == 0){
			offset = previousOffset;
			return;
		}
		offset = listEnum.offsetStack[listEnum.stack-1] - pointerSize;
		int pointer = getPointer();
		if(pointer <= previousOffset){
			offset = previousOffset;
			return;
		}
		offset = -1;
	}
	final private void loopToEndOfNode(LongTreeListEnum listEnum){
		int nextEntry;
		int nextOffset1, nextOffset2;
		nextOffset1 = offset;
		offset += 2;
		if(listEnum.stack<3)
			offset += pointerSize;
		do{
			nextOffset2 = nextOffset1;
			nextOffset1 = offset;
			nextEntry = getUnsignedShort();
			if(listEnum.stack<3)
				offset += pointerSize;
		}while(nextEntry != 0);
		offset = nextOffset2;
	}
	final private int insertNode(int octet) throws SQLException{
		int oldOffset = offset;
		if(data.length < size + 4 + pointerSize) resize();
		System.arraycopy(data, oldOffset, data, oldOffset + 2+pointerSize, size-oldOffset);
		size += 2+pointerSize;
		writeShort( octet );
		writePointer( size );
		correctPointers( 0, oldOffset, 2+pointerSize, 0 );
		data[size++] = (byte)0;
		data[size++] = (byte)0;
		return size-2;
	}
	final private void insertNodeLastLevel(int octet) throws SQLException{
		int oldOffset = offset;
		if(data.length < size + 2) resize();
		System.arraycopy(data, offset, data, offset + 2, size-offset);
		size += 2;
		writeShort( octet );
		correctPointers( 0, oldOffset, 2, 0 );	
	}
	final private void removeNode() throws SQLException{
		int oldOffset = offset;
		correctPointers( 0, oldOffset, -(2+pointerSize), 0 );
		size -= 2+pointerSize;
		System.arraycopy(data, oldOffset + 2+pointerSize, data, oldOffset, size-oldOffset);
		offset = oldOffset;
	}
	final private void removeNodeLastLevel() throws SQLException{
		int oldOffset = offset;
		correctPointers( 0, oldOffset, -2, 0 );
		size -= 2;
		System.arraycopy(data, oldOffset + 2, data, oldOffset, size-oldOffset);
		offset = oldOffset;
	}
	final private void correctPointers(int startOffset, int oldOffset, int diff, int level){
		offset = startOffset;
		boolean firstNode = true;
		while(offset < size){
			if(offset == oldOffset){
				int absDiff = Math.abs(diff);
				if(absDiff == 2) return;
				offset += absDiff;
				firstNode = false;
				continue;
			}
			int value = getUnsignedShort();
			if(value != 0 || firstNode){
				int pointer = getPointer();
				if(pointer > oldOffset){
					offset  -= pointerSize;
					writePointer( pointer + diff );
					if(diff > 0) pointer += diff;
				}				
				if(level < 2){
					startOffset = offset;
					correctPointers( pointer, oldOffset, diff, level+1);
					offset = startOffset;
				}
				firstNode = false;
			}else{
				return;
			}
		}
	}
	final private int getPointer(){
		int value = 0;
		for(int i=0; i<pointerSize; i++){
			value <<= 8;
			value += (data[offset++] & 0xFF);
		}
		return value;
	}
	final private void writePointer(int value){
		for(int i=pointerSize-1; i>=0; i--){
			data[offset++] = (byte)(value >> (i*8));
		}
	}
	final private int getUnsignedShort(){
		return ((data[ offset++ ] & 0xFF) << 8) | (data[ offset++ ] & 0xFF);
	}
	final private void writeShort(int value){
		data[offset++] = (byte)(value >> 8);
		data[offset++] = (byte)(value);
	}
	private final void resize() throws SQLException{
		int newsize = data.length << 1;
		if(newsize > 0xFFFFFF){ 
			newsize = 0xFFFFFF;
			if(newsize == data.length) throw SmallSQLException.create(Language.INDEX_TOOMANY_EQUALS);
		}
		byte[] temp = new byte[newsize];
		System.arraycopy(data, 0, temp, 0, data.length);
		data = temp;
	}
	final int getSize() {
		return size;
	}
}