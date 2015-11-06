package jp.topse.nosql.mongodb;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
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
    
    public static final String DB_SERVER = "localhost";
    public static final String DB_NAME = "topseXX";
    public static final String COLLECTION_NAME = "AccessLog";
    public static final String ACCESS_LOG = "src/main/resources/access.log";

    public static void main(String[] args) {
        MongoClient client;
        try {
            client = new MongoClient(DB_SERVER, 27017);
            DB db = client.getDB(DB_NAME);
            DBCollection collection = db.getCollection(COLLECTION_NAME);
                        
//            insertSample(collection);
//            readSample(collection);

            // clear collection
            clearCollection(collection);

            // Problem 1 (and 5)
            insertAccessLog(collection);

            // Problem 2 and 3
            countData(collection);
            
            // Problem 4
            appendWarning(collection);
            
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    public static void insertSample(DBCollection collection) {
        // {name: 'xyz', inner: {innder-key: 'innver-value'}}というドキュメントを追加する
        BasicDBObject doc = new BasicDBObject("name", "xyz");
        doc.append("inner", new BasicDBObject("inner-key", "inner-value"));
        collection.insert(doc);
    }
    
    public static void readSample(DBCollection collection) {
        DBCursor cursor = collection.find();
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            Set<String> keys = obj.keySet();
            for (String key : keys) {
                System.out.println(key + ":" + obj.get(key).toString());
            }
        }
    }
    
    public static void clearCollection(DBCollection collection) {
        collection.remove(new BasicDBObject());
    }

    public static void insertAccessLog(DBCollection collection) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(ACCESS_LOG))));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                String[] logItems = line.split(",");
                String target = logItems[0];
                Map<String, String> params = new HashMap<String, String>();
                String referer = logItems.length > 1 ? logItems[1] : null;
                String[] targetItems = target.split("\\?");
                if (targetItems.length == 2) {
                	target = targetItems[0];
                	String[] paramItems = targetItems[1].split("&");
                	for (int i = 0; i < paramItems.length; ++i) {
                		String[] items = paramItems[i].split("=");
                		params.put(items[0],  items[1]);
                	}
                }
                
                System.out.println("*****************");
                System.out.println(line);
                System.out.println("Target: "  + target);
                System.out.println("Referer: " + referer);
                System.out.println("Params: "  + params.size());
                for (Map.Entry<String, String> entry : params.entrySet()) {
                	System.out.println("\t" + entry.getKey() + "=" + entry.getValue());
                }
                
                // PLEASE IMPLEMENT HERE

                		
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    public static void countData(DBCollection collection) {
        // PLEASE IMPLEMENT HERE

    }
    
    public static void appendWarning(DBCollection collection) {
        // PLEASE IMPLEMENT HERE
    	
    }

}
