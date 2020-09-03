package com.yeexun;

import com.mysql.jdbc.StringUtils;
import org.apache.commons.io.input.BOMInputStream;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Java执行sql脚本
 */
public class MysqlInit {

    /**
     * 从环境变量中获取文件
     * @param args 1=ip:port ; 2=username ; 3=password ; 4=database name ; 5=sql path
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

//        args = new String[]{"10.221.121.6:32769","root","123456","datatools","C:\\Users\\yinwa\\Desktop\\sql-tmp\\datatools_0326.sql"};
//        args = new String[]{"10.221.121.6:32769","root","123456","shiro","C:\\Users\\yinwa\\Desktop\\sql-tmp\\auth2019-07-24.sql"};
//        args = new String[]{"10.221.121.6:32769","root","123456","dataxdb","C:\\Users\\yinwa\\Desktop\\sql-tmp\\dataxdb.sql"};
//        args = new String[]{"10.221.121.6:3119","root","123456","datagov","C:\\Users\\yinwa\\Desktop\\sql-tmp\\datagov.sql"};
//        args = new String[]{"10.221.121.6:3119","root","123456","share","C:\\Users\\yinwa\\Documents\\WeChat Files\\WZ15591963697\\FileStorage\\File\\2019-09\\datagov (2)(1).sql"};
        MysqlInit mysqlInit = new MysqlInit();
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://"+args[0]+"/?useUnicode=true&characterEncoding=utf-8&tinyint1isBit=false",args[1],args[2]);
        Statement statement = null;

        /**
         * 判断数据库中是否有库
         *      没有库则直接新建库
         *      NIT_DATABASES作为初始化数据库的信号。
         *      有库获取INIT_DATABASES环境变量：INIT_DATABASES=0 不执行操作，INIT_DATABASES=1重新初始化库
         */
        if(!mysqlInit.isExistDB(connection,args[3])){
            String createDB = "CREATE DATABASE IF NOT EXISTS "+args[3] + "CHARACTER SET utf8 COLLATE utf8_general_ci";
            statement = connection.createStatement();
            statement.execute(createDB);
            mysqlInit.execsql(connection,args[4] );
            System.out.println("create and initate database: "+args[3]);
        }
        /**
         * 对已经存在的库，在INIT_DATABASES=1的时候判断是否有初始化数据库的标志，如果有则不初始化数据库。
         */
        else{
            String isInit = System.getenv("INIT_DATABASES");
//            isInit = "1";

            if(null != isInit && isInit.equals("1") ){
                Connection connectionWithDB =  DriverManager.getConnection("jdbc:mysql://"+args[0]+"/"+args[3]+"?useUnicode=true&characterEncoding=utf-8&tinyint1isBit=false",args[1],args[2]);
                boolean init =  mysqlInit.existInitBD(args[3],connectionWithDB);
                if(!init){
                    String dropDB = "DROP DATABASE "+args[3];
                    String createDB = "CREATE DATABASE IF NOT EXISTS "+args[3];
                    statement = connectionWithDB.createStatement();
                    statement.execute(dropDB);
                    System.out.println("DROPED DATABASE "+args[3]);

                    statement.execute(createDB);
                    mysqlInit.execsql(connection,args[4] );
                    System.out.println("CREATE and reinitiate DATABASE:"+args[3]);
                }
                if (statement != null)
                    statement.close();
                if (connectionWithDB != null)
                    connectionWithDB.close();
            }
        }
        if (statement != null)
            statement.close();
        if (connection != null)
            connection.close();

    }

    /**
     * 执行建表语句
     * @param connection
     * @param sqlPath
     */
    private void execsql(Connection connection ,String sqlPath) throws Exception{
        Statement statement=null;
        try{
            InputStream in = new FileInputStream(sqlPath);
            //检查sql文件是否有BOM头，有则去掉
            BOMInputStream bomIn = new BOMInputStream(in);
            InputStreamReader inputStreamReader  =new InputStreamReader(bomIn, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            StringBuilder sqls = new StringBuilder();
            String sql;
            while((sql=bufferedReader.readLine()) != null ){
                    sqls.append(sql+"\n");
            }
//            System.out.println(sqls);
            statement = connection.createStatement();
            String[] sqla  = sqls.toString().split(";");
            for (String item : sqla){
                if(!StringUtils.isEmptyOrWhitespaceOnly(item))
                    statement.addBatch(item);
            }
            statement.executeBatch();
            inputStreamReader.close();
            bufferedReader.close();
            bomIn.close();
            in.close();
        }catch(Exception e){
            e.printStackTrace();
            throw e;
        }finally {
            try{
                if(statement != null)
                    statement.close();
            }catch (Exception e){
                if(statement != null)
                    statement.close();
            }

        }
    }

    /**
     * 判断数据库中是否有该表
     * @param connection
     * @param name 表名
     * @return 有表:true  无表:false
     * @throws Exception
     */
    private boolean isExistDB(Connection connection ,String name) throws Exception {
        String dbname = "";
        try{
            DatabaseMetaData dbMeta = connection.getMetaData();
            ResultSet resultSet = dbMeta.getCatalogs();
            while(resultSet.next()){
                dbname = resultSet.getString("TABLE_CAT");
                if(dbname.equals(name)){
                    return true;
                }
            }
            if(resultSet != null)
                resultSet.close();
            return false;
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("error databases is "+dbname);
            throw e;
        }

    }

    /**
     * 判断是否已经初始化数据库的标志，如果没有，则往数据库生成已经初始化数据库的标志；如果有则退出
     * @param dbName
     * @param connection
     * @return false:没有重新初始化过密码；true:已经重新初始化密码
     */
    public static boolean existInitBD(String dbName ,Connection connection) throws Exception{
//      判断是否存在表INIT_DATABSAE  存在且表里面有数据
        boolean existInitBD = true;
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet tables;
        Set<String> tablesName = new HashSet();
        Statement statement = connection.createStatement();;
        tables = dbMeta.getTables(dbName, null, null, new String[]{"TABLE"});
        while (tables.next()){
            tablesName.add(tables.getString("TABLE_NAME"));
        }
        if(!tablesName.contains("INIT_DATABSAE")){
            existInitBD = false;
        }
        if(!existInitBD){
            statement.addBatch("CREATE TABLE "+dbName+".`INIT_DATABSAE` (  `init` int(11) NOT NULL DEFAULT '1') ;");
            statement.addBatch("INSERT INTO "+dbName+".`INIT_DATABSAE` VALUES(1);");
            statement.executeBatch();
        }

        if (null != tables)
            tables.close();
        if(null != connection)
            connection.close();
        if (null != statement )
            statement.close();

        return existInitBD;
    }


}
