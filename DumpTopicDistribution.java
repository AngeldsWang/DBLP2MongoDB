/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package DBLPTopicModel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author zhenjun.wang
 */
public class DumpTopicDistribution {
    
    private static final String DB_NAME = "DBLP_Citation";
    private static final String COLLECTION_NAME = "mostConnectedAuthorsTopicDis20years";
    
    private static final int START_YEAR = 1993;
    private static final int END_YEAR = 2012;
    
    public static void main(String args[]) throws Exception{
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        DBCursor cursor = collection.find().sort(new BasicDBObject("mostConnectedID", 1));
        while (cursor.hasNext()) {
            DBObject current = cursor.next();
            int mostConnectedID = (Integer) current.get("mostConnectedID");
            System.out.println(mostConnectedID);
            for (int i = START_YEAR; i <= END_YEAR; i++) {
                
                StringBuilder sb = null;
                BufferedWriter writer = null;
                try {
                    writer = new BufferedWriter(new FileWriter("mostConnectedAuthorsTopicDis_20_" + i + ".txt", true));
                    sb = new StringBuilder();
                    BasicDBList yearTopicDis = (BasicDBList) current.get(String.valueOf(i));
                    Iterator iter = yearTopicDis.iterator();
                    while (iter.hasNext()) {
                        sb.append(iter.next()).append("\t");
                    }
                    sb.append("\n");
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
