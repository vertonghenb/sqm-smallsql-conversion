package smallsql.database;
class JoinScroll{
    private final Expression condition; 
    final int type;
    final RowSource left; 
    final RowSource right;
    private boolean isBeforeFirst = true;
    private boolean isOuterValid = true;
    private boolean[] isFullNotValid;
    private int fullRightRowCounter;
    private int fullRowCount;
    private int fullReturnCounter = -1;
    JoinScroll( int type, RowSource left, RowSource right, Expression condition ){
        this.type = type;
        this.condition = condition;
        this.left = left;
        this.right = right;
        if(type == Join.FULL_JOIN){
            isFullNotValid = new boolean[10];
        }
    }
    void beforeFirst() throws Exception{
        left.beforeFirst();
        right.beforeFirst();
        isBeforeFirst = true;
        fullRightRowCounter = 0;
        fullRowCount        = 0;
        fullReturnCounter   = -1;
    }
    boolean next() throws Exception{
        boolean result;
        if(fullReturnCounter >=0){
            do{
                if(fullReturnCounter >= fullRowCount){
                    return false; 
                }
                right.next();
            }while(isFullNotValid[fullReturnCounter++]);
            return true;
        }
        do{
            if(isBeforeFirst){               
                result = left.next();
                if(result){ 
                    result = right.first();
                    if(!result){
                        switch(type){
                            case Join.LEFT_JOIN:
                            case Join.FULL_JOIN:
                                isOuterValid = false;
                                isBeforeFirst = false;
                                right.nullRow();
                                return true;
                        }
                    }else fullRightRowCounter++;
                }else{
                    if(type == Join.FULL_JOIN){
                        while(right.next()){
                            fullRightRowCounter++;
                        }
                        fullRowCount = fullRightRowCounter;
                    }
                }
            }else{
                result = right.next();              
                if(!result){
                    switch(type){
                        case Join.LEFT_JOIN:
                        case Join.FULL_JOIN:
                            if(isOuterValid){
                                isOuterValid = false;
                                right.nullRow();
                                return true;
                            }
                            fullRowCount = Math.max( fullRowCount, fullRightRowCounter);
                            fullRightRowCounter = 0;
                    }
                    isOuterValid = true;
                    result = left.next();
                    if(result){ 
                        result = right.first();
                        if(!result){
                            switch(type){
                                case Join.LEFT_JOIN:
                                case Join.FULL_JOIN:
                                    isOuterValid = false;
                                    right.nullRow();
                                    return true;
                            }
                        }else fullRightRowCounter++;
                    }
                }else fullRightRowCounter++;
            }
            isBeforeFirst = false;
        }while(result && !getBoolean());
        isOuterValid = false;
        if(type == Join.FULL_JOIN){
            if(fullRightRowCounter >= isFullNotValid.length){
                boolean[] temp = new boolean[fullRightRowCounter << 1];
                System.arraycopy( isFullNotValid, 0, temp, 0, fullRightRowCounter);
                isFullNotValid = temp;
            }
            if(!result){
                if(fullRowCount == 0){
                    return false; 
                }
                if(fullReturnCounter<0) {
                    fullReturnCounter = 0;
                    right.first();
                    left.nullRow();
                }
                while(isFullNotValid[fullReturnCounter++]){
                    if(fullReturnCounter >= fullRowCount){
                       return false; 
                    }
                    right.next();
                }
                return true;
            }else
                isFullNotValid[fullRightRowCounter-1] = result;
        }
        return result;
    }
    private boolean getBoolean() throws Exception{
        return type == Join.CROSS_JOIN || condition.getBoolean();
    }
}