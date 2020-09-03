package com.yeexun;

import com.mysql.jdbc.StringUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 初始化一个目录下的脚本
 */
public class RunSql {
    public static final String[] sqlNames = {
            "1_schema_init.sql",
            "3_data_init.sql",
            "2_schema_modify.sql",
            "4_data_modify.sql",
            "sys_data_standard.sql"
    };
    private static Logger logger = LoggerFactory.getLogger(RunSql.class);
    private static String url = "jdbc:mysql://192.168.32.132:3306/?useUnicode=true&characterEncoding=utf-8&tinyint1isBit=false";
    private static String user = "root";
    private static String passwd = "123qwe";
    private static String sqlDir = "C:\\Code\\gitYEEXUN\\governtools\\sql";
    private static String createDB = "CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8 COLLATE utf8_general_ci";
    private static String[] dbName = {"datagovern", "dataxdb"};


    /**
     * 从环境变量中获取文件
     *
     * @param args 1=ip:port ; 2=username ; 3=password ; 4=database name ; 5=sql path
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        try (
                Connection connection = DriverManager.getConnection(url, user, passwd);
        ) {
//            if (needInit(connection, dbName)) {
            if (true) {
                logger.info("初始化数据库");
                execsql(connection, dbName, sqlDir);
                logger.info("初始化sql完毕");
            } else {
                String msg = " 不用初始化";
                logger.warn(msg);
            }
        }catch (Exception e){
            throw e;
        }
    }

    /**
     * 判断数据库是否有传入的数据库名
     *
     * @param connection 连接
     * @param dbName     数据库名
     * @return
     */
    public static boolean needInit(Connection connection, String[] dbName) throws SQLException {
        boolean needinit = false;
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet schemas = dbMeta.getCatalogs();
        Set<String> catlogNames = new HashSet<>();
        while (schemas.next()) {
            String catlogName = schemas.getString("TABLE_CAT");
            catlogNames.add(catlogName.toLowerCase()) ;
        }
        if (!Arrays.stream(dbName).anyMatch((e) -> {
            return catlogNames.contains(e);
        })) {
            needinit = true;
        }
        return needinit;
    }

    /**
     * 执行建表语句
     *
     * @param connection
     * @param sqlPath
     */
    private static void execsql(Connection connection, String[] dbname, String sqlDirPath) throws Exception {
        connection.setAutoCommit(false);
        //创建数据库
        try (
                Statement st = connection.createStatement();
        ) {
            for (String db : dbName) {
                st.execute(String.format(createDB,db));
            }
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        //执行sql
        File sqlDir = new File(sqlDirPath);

        File[] sqlFiles = sqlDir.listFiles((file) -> {
            return Arrays.stream(sqlNames).anyMatch((sqlName) -> {
                return sqlName.equals(file.getName());
            });
        });
        if (sqlFiles != null && sqlNames.length == sqlFiles.length) {
            Map<String, File> sqlFileMap = Arrays.stream(sqlFiles).collect(Collectors.toMap(File::getName, file -> {
                return file;
            }));

            for (String sqlName : sqlNames) {
                File sqlFile = sqlFileMap.get(sqlName);
                try (
                        Statement statement = connection.createStatement();
                        InputStream in = new FileInputStream(sqlFile);
                        //检查sql文件是否有BOM头，有则去掉
                        BOMInputStream bomIn = new BOMInputStream(in);
                        InputStreamReader inputStreamReader = new InputStreamReader(bomIn, StandardCharsets.UTF_8);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                ) {
                    StringBuilder sqls = new StringBuilder();
                    String sql;
                    int i = 0;
                    int batchSize = 30000;

                    while ((sql = bufferedReader.readLine()) != null) {
                        sqls.append(sql+"\n");
                    }
                    logger.info("读取文件完毕：{}",sqlName);
                    /*System.out.println(sqls.toString());
                    statement.execute(sql);*/
                    logger.info("开始执行：{}",sqlName);
                    String[] sqla = sqls.toString().split(";\n");
                    for (String item : sqla) {
                        if (!StringUtils.isEmptyOrWhitespaceOnly(item)) {
                            statement.addBatch(item);
                            i++;
                        }
                        if (1 > 0 && i % batchSize == 0) {
                            logger.debug("批量执行sql");
                            statement.executeBatch();
                            logger.debug("批量执行完毕");
                            logger.info("已执行{}条sql",i);
                            logger.debug("sql---{}",item);
                        }
                    }
                    if (i % batchSize > 0) {
                        logger.debug("批量执行sql");
                        statement.executeBatch();
                        logger.debug("批量执行完毕");
                        logger.info("已执行{}条sql",i);
                        connection.commit();
                        logger.debug("批量执行完毕");
                    }
                    logger.info("执行完sql文件：{}",sqlName);
                }catch (Exception e){
                    logger.error("执行{}文件报错",sqlName);
                    connection.rollback();
                    logger.warn("数据库回滚");
                    throw e;
                }
            }
        }
    }

    /**
     * 判断数据库中是否有该表
     *
     * @param connection
     * @param name       表名
     * @return 有表:true  无表:false
     * @throws Exception
     */
    private static boolean isExistDB(Connection connection, String name) throws Exception {
        String dbname = "";
        try {
            DatabaseMetaData dbMeta = connection.getMetaData();
            ResultSet resultSet = dbMeta.getCatalogs();
            while (resultSet.next()) {
                dbname = resultSet.getString("TABLE_CAT");
                if (dbname.equals(name)) {
                    return true;
                }
            }
            if (resultSet != null)
                resultSet.close();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("error databases is {}", dbname);
            throw e;
        }

    }

    /**
     * 判断是否已经初始化数据库的标志，如果没有，则往数据库生成已经初始化数据库的标志；如果有则退出
     *
     * @param dbName
     * @param connection
     * @return false:没有重新初始化过密码；true:已经重新初始化密码
     */
    public static boolean existInitBD(String dbName, Connection connection) throws Exception {
//      判断是否存在表INIT_DATABSAE  存在且表里面有数据
        boolean existInitBD = true;
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet tables;
        Set<String> tablesName = new HashSet();
        Statement statement = connection.createStatement();
        tables = dbMeta.getTables(dbName, null, null, new String[]{"TABLE"});
        while (tables.next()) {
            tablesName.add(tables.getString("TABLE_NAME"));
        }
        if (!tablesName.contains("INIT_DATABSAE")) {
            existInitBD = false;
        }
        if (!existInitBD) {
            statement.addBatch("CREATE TABLE " + dbName + ".`INIT_DATABSAE` (  `init` int(11) NOT NULL DEFAULT '1') ;");
            statement.addBatch("INSERT INTO " + dbName + ".`INIT_DATABSAE` VALUES(1);");
            statement.executeBatch();
        }

        if (null != tables)
            tables.close();
        if (null != connection)
            connection.close();
        if (null != statement)
            statement.close();
        return existInitBD;
    }
}
