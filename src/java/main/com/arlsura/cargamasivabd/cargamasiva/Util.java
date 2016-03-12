package com.arlsura.cargamasivabd.cargamasiva;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Util {

    public static Properties configuracion;
    private static final Logger LOG = Logger.getLogger(Util.class);

    public static void cargarConfiguraciones() {
        configuracion = new Properties();
        InputStream inputStream = null;

        inputStream = Util.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            configuracion.load(inputStream);
        } catch (IOException e) {
            LOG.error("Error cargando archivo de propiedades.", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.error("Error cerrando InputStream.", e);
                }
            }
        }
    }

    public static String obtenerValorConf(String propiedad) {
        return configuracion.getProperty(propiedad);
    }
}
