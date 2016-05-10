package com.arlsura.cargamasivabd.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

public class Util {

    private static Properties propiedades;
    private static final Logger LOG = Logger.getLogger(Util.class);

    public Util() {
        super();
    }

    public void cargarConfiguraciones(String archivoPropiedades) {
        propiedades = new Properties();
        InputStream inputStream = null;

        inputStream = Util.class.getClassLoader().getResourceAsStream(
                archivoPropiedades);
        try {
            propiedades.load(inputStream);
        } catch (IOException e) {
            LOG.error("Error cargando archivo de propiedades."
                    + archivoPropiedades, e);
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

    public String obtenerValorPropiedad(String propiedad) {
        return propiedades.getProperty(propiedad);
    }

    public List<String> leerArchivo(File archivo) throws IOException {
        return Files
                .readAllLines(archivo.toPath(), StandardCharsets.ISO_8859_1);
    }

    public Stream<String> leerLineasAsStream(String directorio)
            throws IOException {
        return Files.lines(Paths.get(directorio), StandardCharsets.ISO_8859_1);
    }
}
