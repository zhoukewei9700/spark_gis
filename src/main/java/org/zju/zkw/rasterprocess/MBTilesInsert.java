package org.zju.zkw.rasterprocess;

import java.io.*;
import java.sql.*;

public class MBTilesInsert {


    public static void Insert2DB(String db,int z,int x, int y, String png) {
        //jdbc connect db
        Connection conn=null;
        byte[] b=null;
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (db == null || !new File(db).exists()) {
            throw new RuntimeException("No database");
        }
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + db);
            InputStream inputStream = new FileInputStream(new File(png));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            int ch;
            while((ch=inputStream.read())!=-1){
                byteArrayOutputStream.write(ch);
            }
            b=byteArrayOutputStream.toByteArray();
            byteArrayOutputStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try{
            ByteArrayInputStream c = new ByteArrayInputStream(b);
            PreparedStatement pstmt = null;
            String sql = "INSERT INTO tiles (zoom_level,tile_column,tile_row,tile_data) " +
                    "VALUES (%d,%d,%d,%s)";
            PreparedStatement ps = conn.prepareStatement("INSERT INTO tiles (zoom_level,tile_column,tile_row,tile_data)" +
                    "VALUES ({z},{x},{y})");
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

}
