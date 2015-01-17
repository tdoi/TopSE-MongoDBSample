package jp.topse.nosql.mongodb;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Hello world!
 *
 */
public class App {
    
    public static String DB_SERVER = "157.1.206.2";
    public static String DB_NAME = "mydb";
    public static String COLLECTION_NAME = "testData";
    
    public static void main(String[] args) {
        MongoClient client;
        try {
            client = new MongoClient(DB_SERVER, 27017);
            DB db = client.getDB(DB_NAME);
            DBCollection collection = db.getCollection(COLLECTION_NAME);
            
            // Collection内のドキュメントを全部消す
//            clearCollection(collection);

            // {name: 'xyz', inner: {innder-key: 'innver-value'}}というドキュメントを追加する
            BasicDBObject doc = new BasicDBObject("name", "xyz");
            doc.append("inner", new BasicDBObject("inner-key", "inner-value"));
            collection.insert(doc);
            
            DBCursor cursor = collection.find();
            while (cursor.hasNext()) {
                DBObject obj = cursor.next();
                Set<String> keys = obj.keySet();
                for (String key : keys) {
                    System.out.println(key + ":" + obj.get(key).toString());
                }
            }
            
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    public static void clearCollection(DBCollection collection) {
        collection.remove(new BasicDBObject());
    }
}
