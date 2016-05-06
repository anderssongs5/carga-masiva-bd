package com.arlsura.cargamasivabd.hikaricp;

import com.arlsura.cargamasivabd.util.Ambiente;
import com.arlsura.cargamasivabd.util.Util;
import com.zaxxer.hikari.HikariDataSource;

public class AplicacionBDITConfig {

    private static final String configuracion = "hikaricpconfig.properties";

    public static HikariDataSource dataSource(String server, String port, String instance, 
            String user, String pass) {

        Util util = new Util();
        util.cargarConfiguraciones(configuracion);

        HikariDataSource ds = new HikariDataSource();
        ds.setPoolName("CargaMasivaBDHikariCP");
        ds.setJdbcUrl(armarURLOracle(server, port, instance));
        ds.setDriverClassName(util.obtenerValorPropiedad(Ambiente.DRIVER_PROP));
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setAutoCommit(true);
        ds.setMaximumPoolSize(75);
        ds.setConnectionTimeout(600000000);
        ds.setIdleTimeout(6000000);

        return ds;
    }

    private static String armarURLOracle(String server, String port, String instance) {
        StringBuilder url = new StringBuilder("jdbc:oracle:thin:@");
        url.append(server);
        url.append(":");
        url.append(port);
        url.append("/");
        url.append(instance);

        return url.toString();
    }
}
