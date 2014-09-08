/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package builddblpmongdb;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author zhenjun.wang
 */
public class BuildDBLPMongDB {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        String filename = "acm_output_10000";
        String DBName = "DBLPtest";
        String CollectionName = "first_10000";
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DBName);
        DBCollection collection;
        if (db.collectionExists(CollectionName)) {
            collection = db.getCollection(CollectionName);
        } else {
            DBObject options = BasicDBObjectBuilder.start().add("capped", false).add("size", 2000000000l).get();
            collection = db.createCollection(CollectionName, options);
        }
        
//         parse dblp data 
//         format:
// *****************************************************************************************************************       
//        #* --- paperTitle
//        #@ --- Authors
//        #year ---- Year
//        #conf --- publication venue
//        #citation --- citation number (both -1 and 0 means none)
//        #index ---- index id of this paper
//        #arnetid ---- pid in arnetminer database
//        #% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
//        #! --- Abstract
// *****************************************************************************************************************       
        BufferedReader buffread = null;
        try{
            buffread = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            String line = null;
            int count = 0;
            int recordNum = 0;
            Integer authorID = 0;
            HashMap<String, Integer> authorsNameIDs = new HashMap<>();
            // init a doc
            String paperTitle = null;
            BasicDBObject Authors = new BasicDBObject();
            String Year = null;
            String confName = null;
            int citationNum = -1;
            int index = -1;
            int arnetid = -1;
            ArrayList<Integer> refIDs = new ArrayList<>();
            String Abstract = null;
            while((line = buffread.readLine()) != null) {
                
                // first line is the num of records
                if (count == 0) {
                    recordNum = Integer.parseInt(line);
                    count++;
                    continue;
                }
                
                
                if (line.isEmpty()) {
                    BasicDBObject doc = new BasicDBObject("paperTitle", paperTitle).
                              append("Authors", Authors).
                              append("Year", Year).
                              append("confName", confName).
                              append("citationNum", citationNum).
                              append("index", index).
                              append("arnetid", arnetid).
                              append("refIDs", refIDs.toArray()).
                              append("Abstract", Abstract);
                    collection.insert(doc);
                    
                    System.out.println("save record " + count);
                    count++;
                    
                    // clear the data
                    paperTitle = null;
                    Authors = new BasicDBObject();
                    Year = null;
                    confName = null;
                    citationNum = -1;
                    index = -1;
                    arnetid = -1;
                    refIDs.clear();
                    Abstract = null;
                    
                    continue;
                    
                }
                
                
                if (line.length() >= 2 && "#*".equals(line.substring(0, 2))) {
                    paperTitle = line.substring(2);
                    continue;
                }
                if (line.length() >= 2 && "#@".equals(line.substring(0, 2))) {
                    String[] AuthorNames = line.substring(2).split(",");
                    // calc the author ids
                    for(String one: AuthorNames) {
                        if (authorsNameIDs.containsKey(one)) {
                            Authors.append(authorsNameIDs.get(one).toString(), one);
                            
                        } else {
                            authorsNameIDs.put(one, authorID);
                            Authors.append(authorID.toString(), one);
                            authorID++;
                        }
                    }
                    continue;
                }
                if (line.length() >= 5 && "#year".equals(line.substring(0, 5))) {
                    Year = line.substring(5);
                    continue;
                }
                if (line.length() >= 5 && "#conf".equals(line.substring(0, 5))) {
                    confName = line.substring(5);
                    continue;
                }
                if (line.length() >= 9 && "#citation".equals(line.substring(0, 9))) {
                    citationNum = Integer.parseInt(line.substring(9));
                    continue;
                }
                if (line.length() >= 6 && "#index".equals(line.substring(0, 6))) {
                    index = Integer.parseInt(line.substring(6));
                    continue;
                }
                if (line.length() >= 8 && "#arnetid".equals(line.substring(0, 8))) {
                    arnetid = Integer.parseInt(line.substring(8));
                    continue;
                }
                if (line.length() >= 2 && "#%".equals(line.substring(0, 2))) {
                    refIDs.add(Integer.parseInt(line.substring(2)));
                    continue;
                }
                if (line.length() >= 2 && "#!".equals(line.substring(0, 2))) {
                    Abstract = line.substring(2);
                    continue;
                }
                
                // save the last record if there is no empty line at the end of this file
                if (count == recordNum) {
                    BasicDBObject doc = new BasicDBObject("paperTitle", paperTitle).
                              append("Authors", Authors).
                              append("Year", Year).
                              append("confName", confName).
                              append("citationNum", citationNum).
                              append("index", index).
                              append("arnetid", arnetid).
                              append("refIDs", refIDs.toArray()).
                              append("Abstract", Abstract);
                    collection.insert(doc);
                    System.out.println("save record " + count);
                    break;
                }
                
            }
            
            
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            buffread.close();
        }
    }
}
