package ru.sav;

import com.mysql.jdbc.*;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

public class MyLOB {
    private static final Integer size = new Double(10 * Math.pow(2, 20)).intValue();
    private static byte[] buffer = ByteBuffer.allocate(size).array();

    private static void fill(Connection connection) {
        try {
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement("insert into blob_files(filename, bytes) values(?, ?)");
            Random random = new Random();

            // 27m25s!!!! 1000 rows 10gb disk space compact row format innodb_compression_level=6 (default) zlib so slow
            // 26m56s dynamic row format
            // nothing changes with innodb_compression_level = 0. mysql - crap.
            for (int i = 0; i < 1000; i++) {
                random.nextBytes(buffer);

                String filename = Long.toString(random.nextLong());
                statement.setString(1, filename);
                statement.setBlob(2, new ByteArrayInputStream(buffer));
                statement.executeUpdate();

                connection.commit();

                System.out.printf("%s inserted %s\t%d\n", new java.util.Date().toString(), filename, i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello, World!");

        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/sav");

            ResultSet resultSet = connection.createStatement().executeQuery("select filename from blob_files");
            while (resultSet.next()) {
                System.out.printf("name: %s; oid: %s\n", resultSet.getString(1),
                        resultSet.getObject(2) != null ? resultSet.getObject(2).toString() : "NULL");
            }
            resultSet.close();

            fill(connection);

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
