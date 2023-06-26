package org.zju.zkw.DBConnection;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class JdbcUtil {
    private static String driver;
    private static String url; //jdbc:mysql://localhost:7077/mbtiles?useUnicode=true&characterEncoding=UTF-8
    private static String user;
    private static String password;

    //静态块
    static {
        try{
            //1.新建属性集对象
            Properties properties = new Properties();
            //2通过反射，新建字符输入流，读取db.properties文件
            InputStream input = JdbcUtil.class.getClassLoader().getResourceAsStream("resources/db.properties");
            //3.将输入流中读取到的属性，加载到properties属性集对象中
            properties.load(input);
            //4.根据键，获取properties中对应的值
            driver = properties.getProperty("driver");
            url = properties.getProperty("url");
            user = properties.getProperty("user");
            password = properties.getProperty("password");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        try{
            //注册数据库的驱动
            Class.forName(driver);
            //获取数据库连接
            Connection connection = DriverManager.getConnection(url,user,password);
            return connection;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
