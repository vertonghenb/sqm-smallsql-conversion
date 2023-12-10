package smallsql.database;
import java.sql.SQLException;
import java.util.ArrayList;
class Index{
	final IndexNode rootPage;
	Index(boolean unique){
		rootPage = new IndexNode(unique, (char)-1);
	}
    Index(IndexNode rootPage){
        this.rootPage = rootPage;
    }
	IndexScrollStatus createScrollStatus(Expressions expressions){
		return new IndexScrollStatus(rootPage, expressions);
	}
	final Object findRows(Expressions expressions, boolean searchNullValues, ArrayList nodeList) throws Exception{
        IndexNode page = rootPage;
        int count = expressions.size();
        for(int i = 0; i < count; i++){
            page = findRows(page, expressions.get(i), searchNullValues, nodeList);
            if(page == null)
                return null;
            if(i + 1 == count)
                return page.getValue();
            else
                page = (IndexNode)page.getValue();
        }
        throw new Error();
    }
    final Object findRows(Expression[] expressions, boolean searchNullValues, ArrayList nodeList) throws Exception{
        IndexNode page = rootPage;
        int count = expressions.length;
        for(int i = 0; i < count; i++){
            page = findRows(page, expressions[i], searchNullValues, nodeList);
            if(page == null)
                return null;
            if(i + 1 == count)
                return page.getValue();
            else
                page = (IndexNode)page.getValue();
        }
        throw new Error();
    }
	final private IndexNode findRows(IndexNode page, Expression expr, boolean searchNullValues, ArrayList nodeList) throws Exception{
			if(expr.isNull()){
                if(!searchNullValues){
                    return null;
                }
				page = findNull(page);
			}else{
				switch(expr.getDataType()){
					case SQLTokenizer.REAL:
						page = find( page, floatToBinarySortOrder( expr.getFloat()), 2, nodeList );
						break;
					case SQLTokenizer.DOUBLE:
					case SQLTokenizer.FLOAT:
						page = find( page, doubleToBinarySortOrder( expr.getDouble()), 4, nodeList );
						break;
					case SQLTokenizer.TINYINT:
						page = find( page, expr.getInt(), 1, nodeList );
						break;
					case SQLTokenizer.SMALLINT:
						page = find( page, shortToBinarySortOrder( expr.getInt()), 1, nodeList );
						break;
					case SQLTokenizer.INT:
						page = find( page, intToBinarySortOrder( expr.getInt()), 2, nodeList );
						break;
					case SQLTokenizer.BIGINT:
					case SQLTokenizer.DATE:
					case SQLTokenizer.TIME:
					case SQLTokenizer.TIMESTAMP:
					case SQLTokenizer.SMALLDATETIME:
					case SQLTokenizer.MONEY:
					case SQLTokenizer.SMALLMONEY:
						page = find( page, longToBinarySortOrder( expr.getLong()), 4, nodeList );
						break;
					case SQLTokenizer.VARCHAR:
					case SQLTokenizer.NVARCHAR:
					case SQLTokenizer.LONGVARCHAR:
					case SQLTokenizer.LONGNVARCHAR:
					case SQLTokenizer.CLOB:
						page = find( page, stringToBinarySortOrder( expr.getString(), false ), nodeList );
						break;
					case SQLTokenizer.NCHAR:
					case SQLTokenizer.CHAR:
						page = find( page, stringToBinarySortOrder( expr.getString(), true ), nodeList );
						break;
					case SQLTokenizer.VARBINARY:
					case SQLTokenizer.BINARY:
					case SQLTokenizer.LONGVARBINARY:
					case SQLTokenizer.BLOB:
					case SQLTokenizer.UNIQUEIDENTIFIER:
						page = find( page, bytesToBinarySortOrder( expr.getBytes()), nodeList );
						break;
					case SQLTokenizer.BIT:
					case SQLTokenizer.BOOLEAN:
						page = find( page, expr.getBoolean() ? 2 : 1, 1, nodeList );
						break;
					case SQLTokenizer.NUMERIC:
					case SQLTokenizer.DECIMAL:						
						page = find( page, numericToBinarySortOrder( expr.getNumeric() ), nodeList );
						break;
					default: 
						throw new Error(String.valueOf(expr.getDataType()));
				}
			}
			return page;
	}
	final void addValues( long rowOffset, Expressions expressions ) throws Exception{
		IndexNode page = this.rootPage;
		int count = expressions.size();
		for(int i=0; i<count; i++){
			Expression expr = expressions.get(i);
			boolean isLastValues = (i == count-1);
			if(expr.isNull()){
				page = addNull(page, rowOffset, isLastValues);
			}else{
				switch(expr.getDataType()){
					case SQLTokenizer.REAL:
						page = add( page, rowOffset, floatToBinarySortOrder( expr.getFloat()), isLastValues, 2 );
						break;
					case SQLTokenizer.DOUBLE:
					case SQLTokenizer.FLOAT:
						page = add( page, rowOffset, doubleToBinarySortOrder( expr.getDouble()), isLastValues, 4 );
						break;
					case SQLTokenizer.TINYINT:
						page = add( page, rowOffset, expr.getInt(), isLastValues, 1 );
						break;
					case SQLTokenizer.SMALLINT:
						page = add( page, rowOffset, shortToBinarySortOrder( expr.getInt()), isLastValues, 1 );
						break;
					case SQLTokenizer.INT:
						page = add( page, rowOffset, intToBinarySortOrder( expr.getInt()), isLastValues, 2 );
						break;
					case SQLTokenizer.BIGINT:
					case SQLTokenizer.DATE:
					case SQLTokenizer.TIME:
					case SQLTokenizer.TIMESTAMP:
					case SQLTokenizer.SMALLDATETIME:
					case SQLTokenizer.MONEY:
					case SQLTokenizer.SMALLMONEY:
						page = add( page, rowOffset, longToBinarySortOrder( expr.getLong()), isLastValues, 4 );
						break;
					case SQLTokenizer.VARCHAR:
					case SQLTokenizer.NVARCHAR:
					case SQLTokenizer.LONGVARCHAR:
					case SQLTokenizer.LONGNVARCHAR:
						page = add( page, rowOffset, stringToBinarySortOrder( expr.getString(), false ), isLastValues );
						break;
					case SQLTokenizer.NCHAR:
					case SQLTokenizer.CHAR:
						page = add( page, rowOffset, stringToBinarySortOrder( expr.getString(), true ), isLastValues );
						break;
					case SQLTokenizer.VARBINARY:
					case SQLTokenizer.BINARY:
					case SQLTokenizer.LONGVARBINARY:
					case SQLTokenizer.BLOB:
					case SQLTokenizer.UNIQUEIDENTIFIER:
						page = add( page, rowOffset, bytesToBinarySortOrder( expr.getBytes()), isLastValues );
						break;
					case SQLTokenizer.BIT:
					case SQLTokenizer.BOOLEAN:
						page = add( page, rowOffset, expr.getBoolean() ? 2 : 1, isLastValues, 1 );
						break;
					case SQLTokenizer.NUMERIC:
					case SQLTokenizer.DECIMAL:
						page = add( page, rowOffset, numericToBinarySortOrder( expr.getNumeric()), isLastValues );
						break;
					default: 
						throw new Error(String.valueOf(expr.getDataType()));
				}
			}
		}		
	}
	final void removeValue( long rowOffset, Expressions expressions ) throws Exception{
		ArrayList nodeList = new ArrayList();
		Object obj = findRows(expressions, true, nodeList);
		if(!rootPage.getUnique()){
			LongTreeList list = (LongTreeList)obj;
			list.remove(rowOffset);
			if(list.getSize() > 0) return;
		}
		IndexNode node = (IndexNode)nodeList.get(nodeList.size()-1);
		node.clearValue();
		for(int i = nodeList.size()-2; i >= 0; i--){
			if(!node.isEmpty())
				break;
			IndexNode parent = (IndexNode)nodeList.get(i);
			parent.removeNode( node.getDigit() );
			node = parent;
		}
	}
	final private IndexNode findNull(IndexNode page){
		return page.getChildNode( (char)0 );
	}
	final private IndexNode addNull(IndexNode page, long rowOffset, boolean isLastValue) throws SQLException{
		if(isLastValue){
			page.addNode( (char)0, rowOffset );
			return null;
		}else
			return page.addRoot((char)0);
	}
	final private IndexNode find(IndexNode node, long key, int digitCount, ArrayList nodeList){
		for(int i=digitCount-1; i>=0; i--){
			char digit = (char)(key >> (i<<4));
			node = node.getChildNode(digit);
			if(node == null) return null;
			if(nodeList != null) nodeList.add(node);
			if(equals(node.getRemainderValue(), key, i)){
				return node;
			}
		}
		return node;
	}
	final private IndexNode add(IndexNode node, long rowOffset, long key, boolean isLastValue, int digitCount) throws SQLException{
		for(int i=digitCount-1; i>=0; i--){
			char digit = (char)(key >> (i<<4));
			if(i == 0){
				if(isLastValue){
					node.addNode( digit, rowOffset );
					return null;
				}
				return node.addRoot(digit);
			}
			node = node.addNode(digit);
			if(node.isEmpty()){
				if(isLastValue){
					node.addRemainderKey( rowOffset, key, i );
					return null;
				}
				return node.addRootValue( key, i);
			}else
			if(equals(node.getRemainderValue(), key, i)){
				if(isLastValue){
					node.saveValue( rowOffset);
					return null;
				}
				return node.addRoot();
			}		
		}
		throw new Error();
	}
	final private IndexNode find(IndexNode node, char[] key, ArrayList nodeList){
		int length = key.length;
		int i=-1;
		while(true){
			char digit = (i<0) ? (length == 0 ? (char)1 : 2)
							  : (key[i]);
			node = node.getChildNode(digit);
			if(node == null) return null;
			if(nodeList != null) nodeList.add(node);
			if(++i == length){
				return node;
			}
			if(equals(node.getRemainderValue(), key, i)){
				return node;
			}
		}
	}
	final private IndexNode add(IndexNode node, long rowOffset, char[] key, boolean isLast) throws SQLException{
		int length = key.length;
		int i=-1;
		while(true){
			char digit = (i<0) ? (length == 0 ? (char)1 : 2)
							  : (key[i]);
			if(++i == length){
				if(isLast){
					node.addNode( digit, rowOffset );
					return null;
				}
				return node.addRoot(digit);
			}
			node = node.addNode(digit);
			if(node.isEmpty()){
				if(isLast){
					node.addRemainderKey( rowOffset, key, i );
					return null;
				}
				return node.addRootValue( key, i );
			}else
			if(equals(node.getRemainderValue(), key, i)){
				if(isLast){
					node.saveValue(rowOffset);
					return null;
				}
				return node.addRoot();
			}
		}
	}
	final void clear(){
		rootPage.clear();
	}
	final static private int floatToBinarySortOrder(float value){
		int intValue = Float.floatToIntBits(value);
		return (intValue<0) ?
			~intValue :
			intValue ^ 0x80000000;			
	}
	final static private long doubleToBinarySortOrder(double value){
		long intValue = Double.doubleToLongBits(value);
		return (intValue<0) ?
			~intValue :
			intValue ^ 0x8000000000000000L;			
	}
	final static private int shortToBinarySortOrder(int value){
		return value ^ 0x8000;
	}
	final static private int intToBinarySortOrder(int value){
		return value ^ 0x80000000;
	}
	final static private long longToBinarySortOrder(long value){
		return value ^ 0x8000000000000000L;
	}
	final static private char[] stringToBinarySortOrder(String value, boolean needTrim){
		int length = value.length();
		if(needTrim){
			while(length > 0 && value.charAt(length-1) == ' ') length--;
		}
		char[] puffer = new char[length];
		for(int i=0; i<length; i++){
			puffer[i] = Character.toLowerCase(Character.toUpperCase( value.charAt(i) ));
		}
		return puffer;
	}
	final static private char[] bytesToBinarySortOrder(byte[] value){
		int length = value.length;
		char[] puffer = new char[length];
		for(int i=0; i<length; i++){
			puffer[i] = (char)(value[i] & 0xFF);
		}
		return puffer;
	}
	final static private char[] numericToBinarySortOrder(MutableNumeric numeric){
		int[] value = numeric.getInternalValue();
		int count = 1;
		int i;
		for(i=0; i<value.length; i++){
			if(value[i] != 0){
				count = 2*(value.length - i)+1;
				break;
			}
		}
		char[] puffer = new char[count];
		puffer[0] = (char)count;
		for(int c=1; c<count;){
			puffer[c++] = (char)(value[i] >> 16);
			puffer[c++] = (char)value[i++];
		}
		return puffer;
	}
	private final boolean equals(char[] src1, char[] src2, int offset2){
		if(src1 == null) return false;
		int length = src1.length;
		if(length != src2.length - offset2) return false;
		for(int i=0; i<length; i++){
			if(src1[i] != src2[i+offset2]) return false;
		}
		return true;
	}
	private final boolean equals(char[] src1, long src2, int charCount){
		if(src1 == null) return false;
		int length = src1.length;
		if(length != charCount) return false;
		for(int i=0, d = charCount-1; i<length; i++){
			if(src1[i] != (char)((src2 >> (d-- << 4)))) return false;
		}
		return true;
	}
}