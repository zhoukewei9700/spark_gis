package org.zju.zkw.DBConnection;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.Serializable;

public class MongoUtil implements Serializable {
    MongoDatabase mongoDatabase;
    public MongoUtil(){
        MongoClient mongoClient = new MongoClient("221.228.10.124",27017);
        this.mongoDatabase = mongoClient.getDatabase("mytile");
    }

    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }
}
