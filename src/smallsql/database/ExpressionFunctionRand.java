package smallsql.database;
import java.util.Random;
final class ExpressionFunctionRand extends ExpressionFunctionReturnFloat {
	final static private Random random = new Random(); 
    final int getFunction(){ return SQLTokenizer.RAND; }
    boolean isNull() throws Exception{
        return getParams().length == 1 && param1.isNull();
    }
    final double getDouble() throws Exception{
		if(getParams().length == 0)
			return random.nextDouble();
		if(isNull()) return 0;
		return new Random(param1.getLong()).nextDouble(); 
    }
}