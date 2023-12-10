package smallsql.database;
public class LongTreeListEnum {
	long[] resultStack = new long[4];
	int[]  offsetStack = new int[4];
	int stack;
	final void reset(){
		stack = 0;
		offsetStack[0] = 0;
	}
}