/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package DBLPTopicModel;

import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.CharSequenceLowercase;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.TokenSequenceRemoveStopwords;
import cc.mallet.pipe.iterator.StringArrayIterator;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.Alphabet;
import cc.mallet.types.IDSorter;
import cc.mallet.types.InstanceList;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 *
 * @author zhenjun.wang
 */
public class trainDBLPTopicModel {

    private static final String STOP_WORDS = "en.txt";
    private static final String DB_NAME = "DBLP_Citation";
    private static final String COLLECTION_NAME = "AuthorsInfo";
    private static final String COLLECTION_TIME_SLICE = "AuthorsTimeSliceInfo";

    private static final int ITERATIONS = 1000;
    private static final int THREADS = 32;
    private static final int NUM_TOPICS = 30;
    private static final int NOM_WORDS_TO_ANALYZE = 25;
    
    public static void main(String args[]) throws Exception {
        ArrayList<Pipe> pipeList = new ArrayList<>();
        File stopwords = new File(STOP_WORDS);
        pipeList.add((Pipe) new CharSequenceLowercase());
        pipeList.add((Pipe) new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")));
        pipeList.add((Pipe) new TokenSequenceRemoveStopwords(stopwords, "UTF-8", false, false, false));
        pipeList.add((Pipe) new PorterStemmer());
        pipeList.add((Pipe) new TokenSequence2FeatureSequence());
        
        InstanceList instances = new InstanceList(new SerialPipes(pipeList));
        
        LinkedList<String> textList = new LinkedList<>();
        
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        DBCursor cursor = collection.find();
        try {
            while (cursor.hasNext()) {
                // add titles
                BasicDBList objTitles = (BasicDBList) ((BasicDBObject)cursor.next().get("value")).get("paperTiltes");
                Iterator iterTitle = objTitles.iterator();
                while (iterTitle.hasNext()) {
                    String oneTitle = (String)iterTitle.next();
                    if (oneTitle != null) {
                        String text = TextUtils.GetCleanText(oneTitle);
                        textList.add(text);
                    }
                }      
                // add abstracts
                BasicDBList objAbstracts = (BasicDBList) ((BasicDBObject)cursor.next().get("value")).get("paperAbstracts");
                Iterator iterAbstract = objAbstracts.iterator();
                while (iterAbstract.hasNext()) {
                    String oneAbstract = (String)iterAbstract.next();
                    if (oneAbstract != null) {
                        String text = TextUtils.GetCleanText(oneAbstract);
                        textList.add(text);
                    }
                }
            }
        } finally {
            cursor.close();
        }
        
        instances.addThruPipe(new StringArrayIterator(textList.toArray(new String[textList.size()])));

        ParallelTopicModel model = new ParallelTopicModel(NUM_TOPICS);
        model.addInstances(instances);
        model.setNumThreads(THREADS);
        model.setNumIterations(ITERATIONS);
        model.estimate();
        
        // save model
        try {
            ObjectOutputStream outputModel = new ObjectOutputStream(new FileOutputStream("model_30"));
            ObjectOutputStream outputInstances = new ObjectOutputStream(new FileOutputStream("instances_30"));
            outputModel.writeObject(model);
            outputInstances.writeObject(instances);
            outputModel.close();
            outputInstances.close();
        } catch(IOException e) {
            e.printStackTrace(System.out);
        }
        
        ParallelTopicModel model_trained = null;
        InstanceList instances_trained = null;
        // load model (test)
        try {
            ObjectInputStream inputModel = new ObjectInputStream(new FileInputStream("model_30"));
            ObjectInputStream inputInstances = new ObjectInputStream(new FileInputStream("instances_30"));
            model_trained = (ParallelTopicModel) inputModel.readObject();
            instances_trained = (InstanceList) inputInstances.readObject();
            inputModel.close();
            inputInstances.close();
        } catch(IOException e) {
            e.printStackTrace(System.out);
        }
        
        System.out.println("load model OK!");
        
        
        // save topic to file for analysis
        Alphabet dataAlphabet = instances.getDataAlphabet();

        int topicIdx = 0;
        StringBuilder sb;
        
        BufferedWriter writer = null;      
        try {
            writer = new BufferedWriter(new FileWriter("topicmodel_30"));
            
            for (TreeSet<IDSorter> set : model_trained.getSortedWords()) {
                sb = new StringBuilder().append(topicIdx);
                sb.append(" - ");
                int j = 0;
                double sum = 0.0;
                for (IDSorter s : set) {
                    sum += s.getWeight();
                }
                for (IDSorter s : set) {
                    sb.append(dataAlphabet.lookupObject(s.getID())).append(":").append(s.getWeight() / sum).append(", ");
                    if (++j >= NOM_WORDS_TO_ANALYZE) {
                        break;
                    }
                }
//                System.out.println(sb.append("\n").toString());
                writer.write(sb.append("\n").toString());
                writer.flush();
                topicIdx++;
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                writer.close();
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
        
//        DBCollection collTimeSlice = db.getCollection(COLLECTION_TIME_SLICE);
 
    }
}
