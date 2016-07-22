package ru.sav;

import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;

public class PgLOB {
    private static final Integer size = new Double(10 * Math.pow(2, 20)).intValue();
    private static byte[] buffer = ByteBuffer.allocate(size).array();

    private static void fill(Connection connection) {
        try {
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement("insert into public.lo_files(name, oid) values(?, ?)");
            LargeObjectManager largeObjectManager = connection.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
            Random random = new Random();

            // about 44sec. random fill buffer about 4 sec for 100
            // 9m59s for 1000 rows 13.6GB diskspace for 1000 * 10MB
            for (int i = 0; i < 1000; i++) {
                random.nextBytes(buffer);
                Long oid = largeObjectManager.createLO();
                LargeObject object = largeObjectManager.open(oid);
                object.write(buffer);
                object.close();

                String filename = Long.toString(random.nextLong());
                statement.setString(1, filename);
                statement.setLong(2, oid);
                statement.executeUpdate();

                connection.commit();

                System.out.printf("%s inserted %s\n", new java.util.Date().toString(), filename);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void fillBytea(Connection connection) {
        try {
            connection.setAutoCommit(false);
            PreparedStatement statement = connection.prepareStatement("insert into public.bytea_files(name, bytes) values(?, ?)");
            Random random = new Random();

            //about 35 sec -> 59sec Oo -> 33sec for 100
            // 6m23s for 1000 rows 10.5GB diskspace for 1000 * 10MB
            for (int i = 0; i < 1000; i++) {
                random.nextBytes(buffer);

                String filename = Long.toString(random.nextLong());
                statement.setString(1, filename);
                statement.setBytes(2, buffer);
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
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/sav?user=sav&password=savpw");

            ResultSet resultSet = connection.createStatement().executeQuery("select * from public.lo_files");
            while (resultSet.next()) {
                System.out.printf("name: %s; oid: %s\n", resultSet.getString(1),
                        resultSet.getObject(2) != null ? resultSet.getObject(2).toString() : "NULL");
            }
            resultSet.close();

            fillBytea(connection);

            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
