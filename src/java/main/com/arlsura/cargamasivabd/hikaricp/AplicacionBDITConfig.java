package com.arlsura.cargamasivabd.hikaricp;

import com.arlsura.cargamasivabd.util.Ambiente;
import com.arlsura.cargamasivabd.util.Util;
import com.zaxxer.hikari.HikariDataSource;

public class AplicacionBDITConfig {

    private static final String configuracion = "hikaricpconfig.properties";

    public static HikariDataSource dataSource() {

        Util util = new Util();
        util.cargarConfiguraciones(configuracion);

        HikariDataSource ds = new HikariDataSource();
        ds.setPoolName("CargaMasivaBDHikariCP");
        ds.setJdbcUrl(util.obtenerValorPropiedad(Ambiente.URL_PROP));
        ds.setDriverClassName(util.obtenerValorPropiedad(Ambiente.DRIVER_PROP));
        ds.setUsername(util.obtenerValorPropiedad(Ambiente.USER_PROP));
        ds.setPassword(util.obtenerValorPropiedad(Ambiente.PASSWORD_PROP));
        ds.setAutoCommit(true);
        ds.setMaximumPoolSize(50);
        ds.setConnectionTimeout(600000000);
        ds.setIdleTimeout(60000);

        return ds;
    }
}
