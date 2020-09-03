package com.yeexun;

import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.sql.*;

public class MysqlInsert {

    /**
     * 从环境变量中获取文件
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

//        args = new String[]{"10.221.121.6:32769","root","123456","datatools","C:\\Users\\yinwa\\Desktop\\sql-tmp\\datatools_0326.sql"};
//        args = new String[]{"10.221.121.6:32769","root","123456","shiro","C:\\Users\\yinwa\\Desktop\\sql-tmp\\auth2019-07-24.sql"};
//        args = new String[]{"10.221.121.6:32769","root","123456","dataxdb","C:\\Users\\yinwa\\Desktop\\sql-tmp\\dataxdb.sql"};
//        args = new String[]{"10.221.121.6:32769","root","123456","datagov","C:\\Users\\yinwa\\Desktop\\sql-tmp\\datagov.sql"};
        args = new String[]{"10.221.121.6:3111","root","123456","datagov","C:\\Users\\yinwa\\Documents\\WeChat Files\\WZ15591963697\\FileStorage\\File\\2019-09\\datagov (2)(1).sql"};

        MysqlInsert mysqlInit = new MysqlInsert();
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://"+args[0]+"/?useUnicode=true&characterEncoding=utf-8&tinyint1isBit=false",args[1],args[2]);
        Statement statement = null;
        /**
         * 判断数据库中是否有库
         *      没有库则直接新建库
         *      有库获取INIT_DATABASES环境变量：INIT_DATABASES=0 不执行操作，INIT_DATABASES=1重新初始化库
         */
        if(!mysqlInit.isExistDB(connection,args[3])){
//            String createDB = "CREATE DATABASE IF NOT EXISTS "+args[3];
//            statement = connection.createStatement();
//            statement.execute(createDB);
            mysqlInit.execsql(connection,args[4] );
//            System.out.println("create database:"+args[3]);
        }else{
//            String isInit = System.getenv("INIT_DATABASES");
            String isInit = "1";
            if(null != isInit && isInit.equals("1") ){
//                String dropDB = "DROP DATABASE "+args[3];
//                String createDB = "CREATE DATABASE IF NOT EXISTS "+args[3];
//                statement = connection.createStatement();
//                statement.execute(dropDB);
//                System.out.println("DROPED DATABASE "+args[3]);
//
//                statement.execute(createDB);
                mysqlInit.execsql(connection,args[4] );
//                System.out.println("CREATE DATABASE:"+args[3]);

            }
        }
        if(statement != null)
            statement.close();
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
            File file = new File("C:\\Users\\yinwa\\Desktop\\sql\\datagov\\datagov_insert.sql");
            InputStream in = new FileInputStream(sqlPath);
            BOMInputStream bomIn = new BOMInputStream(in);
            InputStreamReader inputStreamReader  =new InputStreamReader(bomIn, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            StringBuilder sqls = new StringBuilder();
            String sql;
            while((sql=bufferedReader.readLine()) != null ){
                if (sql.startsWith("INSERT")) {
                    sqls.append(sql + "\n");
                }
            }
//            System.out.println(sqls);
            statement = connection.createStatement();
            String[] sqla  = sqls.toString().split(";");
            FileWriter fileWriter = new FileWriter(file,true);
            fileWriter.write(sqls.toString());
//            for (int i = 0; i < sqla.length; i++) {
//            }
            fileWriter.flush();
            fileWriter.close();
//            for (String item : sqla){
//                if(!StringUtils.isEmptyOrWhitespaceOnly(item))
//                    statement.addBatch(item);
//            }
//            statement.executeBatch();
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
        String dbname = new String();
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
}
