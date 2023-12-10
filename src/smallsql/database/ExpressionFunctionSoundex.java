package smallsql.database;
public class ExpressionFunctionSoundex extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.SOUNDEX;
	}
	final boolean isNull() throws Exception {
		return param1.isNull();
	}
	final byte[] getBytes() throws Exception{
        throw createUnspportedConversion(SQLTokenizer.BINARY);
	}
	final String getString() throws Exception {
		if(isNull()) return null;
        String input = param1.getString();
        return getString(input);
    }
    static String getString(String input){
        char[] output = new char[4];
        int idx = 0;
        input = input.toUpperCase();
        if(input.length()>0){
            output[idx++] = input.charAt(0);
        }
        char last = '0';
        for(int i=1; idx<4 && i<input.length(); i++){
            char c = input.charAt(i);
            switch(c){
            case 'B':
            case 'F':
            case 'P':
            case 'V':
                c = '1';
                break;
            case 'C':
            case 'G':
            case 'J':
            case 'K':
            case 'Q':
            case 'S':
            case 'X':
            case 'Z':
                c = '2';
                break;
            case 'D':
            case 'T':
                c = '3';
                break;
            case 'L':
                c = '4';
                break;
            case 'M':
            case 'N':
                c = '5';
                break;
            case 'R':
                c = '6';
                break;
            default:
                c = '0';
                break;
            }
            if(c > '0' && last != c){
                output[idx++] = c;
            }
            last = c;
        }
        for(; idx<4;){
            output[idx++] = '0';
        }
		return new String(output);
	}
    int getPrecision(){
        return 4;
    }
}