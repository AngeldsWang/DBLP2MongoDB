/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package DBLPTopicModel;

/**
 *
 * @author zhenjun.wang
 */

import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.topics.TopicInferencer;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

public class calcAuthorTopicDis {
    
    private static final String DB_NAME = "DBLP_Citation";
    private static final String COLLECTION_NAME = "activeAuthors15yearSliceInfo";
    private static final String NEW_COLLECTION = "activeAuthors15yearSliceTopicDis";
    private static final int START_YEAR = 1998;
    private static final int END_YEAR = 2012;
    
    public static void main(String args[]) throws Exception{
        
        ParallelTopicModel model_trained = null;
        InstanceList instances_trained = null;
        // load model (test)
        try {
            ObjectInputStream inputModel = new ObjectInputStream(new FileInputStream("model"));
            ObjectInputStream inputInstances = new ObjectInputStream(new FileInputStream("instances"));
            model_trained = (ParallelTopicModel) inputModel.readObject();
            instances_trained = (InstanceList) inputInstances.readObject();
            inputModel.close();
            inputInstances.close();
        } catch(IOException e) {
            e.printStackTrace(System.out);
        }
        
        System.out.println("load model OK!");
        // init rhe inferencer
        InstanceList testing = new InstanceList(instances_trained.getPipe());
        TopicInferencer inferencer = model_trained.getInferencer();
        
        // connect to the DB
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        DBCollection coll = db.createCollection(NEW_COLLECTION, null);
        DBCursor cursor = collection.find().sort(new BasicDBObject("idByName", 1));
        int docNum = 0;
//        double id = 0;
        try{
            while(cursor.hasNext()) {
                
                DBObject current = cursor.next();
                
                BasicDBObject doc = new BasicDBObject("idByName", current.get("idByName"));
//                id =(Double) current.get("idByName");
                
                BasicDBObject obj = (BasicDBObject)((BasicDBObject)current.get("value")).get("yearslices");
                
                for(int i = START_YEAR; i <= END_YEAR; i++) {
                    StringBuilder Text = new StringBuilder();
                    
                    BasicDBObject year = (BasicDBObject)obj.get(String.valueOf(i));
                    
                    BasicDBList objTitles = (BasicDBList) year.get("paperTitles");
                    Iterator iterTitle = objTitles.iterator();
                    while (iterTitle.hasNext()) {
                        String oneTitle = (String)iterTitle.next();
                        if (oneTitle != null) {
                            String text = TextUtils.GetCleanText(oneTitle);
                            Text.append(text).append("\n");
                        }
                    }      
                    // add abstracts
                    BasicDBList objAbstracts = (BasicDBList) year.get("paperAbstracts");
                    Iterator iterAbstract = objAbstracts.iterator();
                    while (iterAbstract.hasNext()) {
                        String oneAbstract = (String)iterAbstract.next();
                        if (oneAbstract != null) {
                            String text = TextUtils.GetCleanText(oneAbstract);
                            Text.append(text).append("\n")  ;
                        }
                    }
                    testing.addThruPipe(new Instance(Text.toString(), null, "test every year", null));
                    double[] testProbDis = inferencer.getSampledDistribution(testing.get(docNum), 200, 20, 20);
                    docNum++;
                    doc.put(String.valueOf(i), testProbDis);
                }
                // insert new doc
                coll.insert(doc);
            }
        } finally {
            cursor.close();
        }
    }
}
