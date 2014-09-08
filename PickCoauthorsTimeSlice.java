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
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

/**
 *
 * @author zhenjun.wang
 */
public class PickCoauthorsTimeSlice {
    
    private static final String DB_NAME = "DBLP_Citation";
    private static final String COLLECTION_NAME = "mostConnectedCoauthors20years";
//    private static final String NEW_COLLECTION = "activeAuthors15yearSliceTopicDis";
    private static final int START_YEAR = 1993;
    private static final int END_YEAR = 2012;
    
    public static void main(String args[]) throws Exception {
        // connect to the DB
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        DBCursor cursor = collection.find().sort(new BasicDBObject("mostConnectedID", 1));
        while(cursor.hasNext()) {
            DBObject current = cursor.next();
//            int authorid = Integer.parseInt((String)current.get("_id"));
            int mostConnectedID = (Integer) current.get("mostConnectedID");
            for (int i = START_YEAR; i <= END_YEAR; i++) {
                
                StringBuilder sb = null;
                BufferedWriter writer = null;
                try {
                    writer = new BufferedWriter(new FileWriter("mostConnectedCoauthors_20_" + i + "_SPARSE.txt", true));
                    sb = new StringBuilder();
                    BasicDBObject year = (BasicDBObject) current.get(String.valueOf(i));
                    Set<String> keys = year.keySet();
                    for(String coauthorid: keys ) {
                        BasicDBObject query = new BasicDBObject("_id", coauthorid);
                        DBCursor cur = collection.find(query);
                        int id = 0;
                        while(cur.hasNext()) {
                            id = (Integer)cur.next().get("mostConnectedID");
                        }
                        double weight = (Double) year.get(coauthorid);
                        sb.append(mostConnectedID).append("\t").append(id).append("\t").append(weight).append("\n");
                    }
                    if (sb != null) {
                        writer.write(sb.toString());
                        writer.flush();
                    }
                    
                } catch(IOException e) {
                    System.out.println(e.getMessage());
                } finally {
                    writer.close();
                }               
            }
        }
    }
}
