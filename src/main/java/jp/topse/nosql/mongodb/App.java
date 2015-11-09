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
import com.mongodb.WriteResult;

/**
 * Hello world!
 *
 */
public class App {
    
    public static final String DB_SERVER = "192.168.99.100";
    public static final String DB_NAME = "topse";
    public static final String COLLECTION_NAME = "AccessLog";
    public static final String COLLECTION_PAGES = "pages";
    public static final String COLLECTION_LOGS = "logs";
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
            
            // Problem 5
            DBCollection pages = db.getCollection(COLLECTION_PAGES);
            DBCollection logs = db.getCollection(COLLECTION_LOGS);
            clearCollection(pages);
            clearCollection(logs);
            insertAccessLog2(pages, logs);
            countData2(pages, logs);
            appendWarning2(pages, logs);

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
                DBObject entry = makeLogDocument(target, referer, params);
                collection.insert(entry);

//                DBObject entry2 = makeLogDocumentWithMap(target, referer, params);
//                collection.insert(entry2);                
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

    public static void insertAccessLog2(DBCollection pages, DBCollection logs) {
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

                WriteResult writeResult = pages.update(new BasicDBObject("target", target), new BasicDBObject("target", target), true, false);
                Object pageId = writeResult.getUpsertedId();
                if (pageId == null) {
                	DBObject page = pages.findOne(new BasicDBObject("target", target));
                	pageId = page.get("_id");
                }
                BasicDBObject log = new BasicDBObject("page_id", pageId);
                log.append("referer", referer);
                log.append("params", new BasicDBObject(params));
                logs.insert(log);
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

    private static DBObject makeLogDocument(String target, String referer, Map<String, String> params) {
    	BasicDBObject doc = new BasicDBObject();
    	doc.append("target", target);
    	if (referer != null) {
    		doc.append("referer", referer);
    	}
    	if (!params.isEmpty()) {
    		BasicDBObject paramsDoc = new BasicDBObject();
    		for (Map.Entry<String, String> entry: params.entrySet()) {
    			paramsDoc.append(entry.getKey(), entry.getValue());
    		}
    		doc.append("params", paramsDoc);
    	}

    	return doc;
    }
    
    private static DBObject makeLogDocumentWithMap(String target, String referer, Map<String, String> params) {
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put("target", target);
    	if (referer != null) {
    		map.put("referer", referer);
    	}
    	if (params.size() > 0) {
    		map.put("params", params);
    	}
    	
    	return new BasicDBObject(map);
    }

    
    public static void countData(DBCollection collection) {
        // PLEASE IMPLEMENT HERE
    	{
    		long count = collection.count(new BasicDBObject("target", "index.html"));
    		System.out.println("MongoDB contains " + count + " records ( target = index.html )");
    	}
    	{
    		BasicDBObject query = new BasicDBObject();
    		query.append("target", "index.html");
    		query.append("params.x", "1");
    		long count = collection.count(query);
    		System.out.println("MongoDB contains " + count + " records ( target = index.html and x = 1 )");
    	} 
    }
    
    public static void appendWarning(DBCollection collection) {
        // PLEASE IMPLEMENT HERE
		BasicDBObject query = new BasicDBObject("referer", "http://badsite.com");
		BasicDBObject doc = new BasicDBObject("$set", new BasicDBObject("warning", "1"));
		collection.update(query, doc, false, true);
    }

    public static void countData2(DBCollection pages, DBCollection logs) {
        // PLEASE IMPLEMENT HERE
    	{
    		DBObject page = pages.findOne(new BasicDBObject("target", "index.html"));
    		BasicDBObject query = new BasicDBObject();
    		query.append("page_id", page.get("_id"));
       		long count = logs.count(query);
       		System.out.println("MongoDB contains " + count + " records ( target = index.html )");    			
    	}
    	{
    		DBObject page = pages.findOne(new BasicDBObject("target", "index.html"));
    		BasicDBObject query = new BasicDBObject();
    		query.append("page_id", page.get("_id"));
    		query.append("params.x", "1");
       		long count = logs.count(query);
       		System.out.println("MongoDB contains " + count + " records ( target = index.html and x = 1 )");    			
    	} 
    }
    
    public static void appendWarning2(DBCollection pages, DBCollection logs) {
        // PLEASE IMPLEMENT HERE
		BasicDBObject query = new BasicDBObject("referer", "http://badsite.com");
		BasicDBObject doc = new BasicDBObject("$set", new BasicDBObject("warning", "1"));
		logs.update(query, doc, false, true);
    }

}
