/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package DBLPCoauthorsTimeSlice;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author zhenjun.wang
 */
public class GetMostConnectedUsers {
    private static final String DB_NAME = "DBLP_Citation";
    private static final String COLLECTION_NAME = "activeCoauthors20yearSlice";
    private static final String NEW_COLLECTION_NAME = "mostConnectedCoauthors20years";
    
    public static void main(String args[]) throws Exception{
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        DBCursor cursor = collection.find().sort(new BasicDBObject("idByName", 1));
        DBCollection coll = db.createCollection(NEW_COLLECTION_NAME, null);
        
        String line = null;
        BufferedReader reader = null;
        ArrayList<Integer> mostConnectedUserIdsList = new ArrayList();
        try {
            reader = new BufferedReader(new FileReader("most_connected_users_id.txt"));
            while((line = reader.readLine()) != null) {
                int id = Integer.parseInt(line);
                mostConnectedUserIdsList.add(id);
            }
        } catch(IOException e) {
            System.out.println(e.getMessage());
        } finally {
            reader.close();
        }
        int idx = 0;
        while(cursor.hasNext()) {
            DBObject obj = cursor.next();
            double currentUserID = (Double)obj.get("idByName");
            if(mostConnectedUserIdsList.contains((int)currentUserID)) {
                obj.put("mostConnectedID", idx);
                coll.insert(obj);
                idx++;
            }
        }
    }
    
}
