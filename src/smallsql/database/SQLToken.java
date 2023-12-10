package smallsql.database;
class SQLToken{
	int value;
	int offset;  
	int length;  
	String name;
	SQLToken (int value, int tokenStart, int tokenEnd){
		this.value  = value;
		this.offset = tokenStart;
		this.length = tokenEnd-tokenStart;
	}
	SQLToken (String name, int value, int tokenStart, int tokenEnd){
		this.value  = value;
		this.offset = tokenStart;
		this.length = tokenEnd-tokenStart;
		this.name   = name;
	}
	String getName(char[] sql){
		if(name != null) return name;
		return new String( sql, offset, length );
	}
}