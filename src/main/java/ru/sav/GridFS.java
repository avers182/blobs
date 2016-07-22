package ru.sav;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Random;

public class GridFS {
    private static final Integer size = new Double(10 * Math.pow(2, 20)).intValue();
    private static byte[] buffer = ByteBuffer.allocate(size).array();

    // 4m56s for 1000 rows, 12.5GB for 1000*10MB. MMAP storage.
    private static void fill(GridFSBucket gridFSBucket) {
        Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            random.nextBytes(buffer);
            String filename = Long.toString(random.nextLong());
            gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream(buffer));
            System.out.printf("%s inserted %s\t%d\n", new java.util.Date().toString(), filename, i);
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello, World!");

        try {
            MongoClient client = new MongoClient();
            MongoDatabase database = client.getDatabase("mediastore");
            GridFSBucket gridFSBucket = GridFSBuckets.create(database);

            FindIterable<Document> result = database.getCollection("fs.files").find();
            result.forEach(new Block<Document>() {
                @Override
                public void apply(Document document) {
                    System.out.println(document);
                }
            });

//            fill(gridFSBucket);

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
