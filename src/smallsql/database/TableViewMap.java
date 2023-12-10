package smallsql.database;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
final class TableViewMap{
    private final HashMap map = new HashMap();
    private Object getUniqueKey(String name){
        return name.toUpperCase(Locale.US); 
    }
    TableView get(String name){
        return (TableView)map.get(getUniqueKey(name));
    }
    void put(String name, TableView tableView){
        map.put(getUniqueKey(name), tableView);
    }
    TableView remove(String name){
        return (TableView)map.remove(getUniqueKey(name));
    }
    Collection values(){
        return map.values();
    }
}