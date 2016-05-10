package com.arlsura.cargamasivabd.hikaricp;

import com.zaxxer.hikari.HikariDataSource;

public class BDConfig {

    private static final String MYSQL_DB = "MySQL";
    private static final String MYSQL_DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String ORACLE_DB = "Oracle";
    private static final String ORACLE_DB_DRIVER = "oracle.jdbc.OracleDriver";

    public static HikariDataSource dataSource(String server, String port,
            String instance, String user, String pass, String db) {

        HikariDataSource ds = new HikariDataSource();
        ds.setPoolName("CargaMasivaBDHikariCP");

        String url = armarURLDB(server, port, instance, db);
        ds.setJdbcUrl(url);

        String driver = obtenerDBDriver(db);
        ds.setDriverClassName(driver);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setAutoCommit(true);
        ds.setMaximumPoolSize(150);
        ds.setConnectionTimeout(600000000);
        ds.setIdleTimeout(6000000);

        return ds;
    }

    private static String obtenerDBDriver(String database) {
        if (MYSQL_DB.equalsIgnoreCase(database.trim())) {
            return MYSQL_DB_DRIVER;
        } else if (ORACLE_DB.equalsIgnoreCase(database.trim())) {
            return ORACLE_DB_DRIVER;
        } else {
            return null;
        }
    }

    private static String armarURLDB(String server, String port,
            String instance, String database) {
        if (MYSQL_DB.equalsIgnoreCase(database.trim())) {
            return armarURLMySQL(server, port, instance);
        } else if (ORACLE_DB.equalsIgnoreCase(database.trim())) {
            return armarURLOracle(server, port, instance);
        } else {
            return null;
        }
    }

    private static String armarURLMySQL(String server, String port,
            String instance) {
        StringBuilder url = new StringBuilder("jdbc:mysql://");
        url.append(server);
        url.append(":");
        url.append(port);
        url.append("/");
        url.append(instance);

        return url.toString();
    }

    private static String armarURLOracle(String server, String port,
            String instance) {
        StringBuilder url = new StringBuilder("jdbc:oracle:thin:@");
        url.append(server);
        url.append(":");
        url.append(port);
        url.append("/");
        url.append(instance);

        return url.toString();
    }
}
