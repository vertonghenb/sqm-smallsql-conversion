package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
abstract class ExpressionFunction extends Expression {
    Expression param1;
    Expression param2;
    Expression param3;
    Expression param4;
	ExpressionFunction(){
		super(FUNCTION);
	}
    abstract int getFunction();
    byte[] getBytes() throws Exception{
        return ExpressionValue.getBytes(getObject(), getDataType());
    }
    void setParams( Expression[] params ){
        super.setParams( params );
        if(params.length >0) param1 = params[0] ;
        if(params.length >1) param2 = params[1] ;
        if(params.length >2) param3 = params[2] ;
        if(params.length >3) param4 = params[3] ;
    }
	final void setParamAt( Expression param, int idx){
		switch(idx){
			case 0:
				param1 = param;
				break;
			case 1:
				param2 = param;
				break;
			case 2:
				param3 = param;
				break;
			case 3:
				param4 = param;
				break;
		}
		super.setParamAt( param, idx );
	}
	public boolean equals(Object expr){
		if(!super.equals(expr)) return false;
		if(!(expr instanceof ExpressionFunction)) return false;
		return ((ExpressionFunction)expr).getFunction() == getFunction();
	}
	SQLException createUnspportedDataType( int dataType ){
		Object[] params = {
				SQLTokenizer.getKeyWord(dataType),
				SQLTokenizer.getKeyWord(getFunction())
		};
        return SmallSQLException.create(Language.UNSUPPORTED_DATATYPE_FUNC, params);
    }
    SQLException createUnspportedConversion( int dataType ){
    	Object[] params = {
    			SQLTokenizer.getKeyWord(dataType),
    			SQLTokenizer.getKeyWord(getFunction())
    	};
        return SmallSQLException.create(Language.UNSUPPORTED_CONVERSION_FUNC, params);
    }
}