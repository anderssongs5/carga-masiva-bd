package com.arlsura.cargamasivabd;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

public class Aplicacion {

    private static final List<String> motoresBD = Arrays.asList("MySQL",
            "Oracle");
    private static final Logger LOG = Logger.getLogger(Aplicacion.class);

    @SuppressWarnings("rawtypes")
    public static void main(String args[]) {
        try {
            String user = args[0];
            String pass = args[1];
            String server = args[2];
            String port = args[3];
            String instance = args[4];
            String db = args[5];
            String opcion = args[6];
            String ruta = args[7];

            String mensaje;
            if (user == null || user.trim().isEmpty() || pass == null
                    || pass.trim().isEmpty() || server == null
                    || server.trim().isEmpty() || port == null
                    || port.trim().isEmpty() || instance == null
                    || instance.trim().isEmpty() || db == null
                    || db.trim().isEmpty() || opcion == null
                    || opcion.trim().isEmpty() || ruta == null
                    || ruta.trim().isEmpty()) {
                mensaje = "Los parámetros de la aplicación no tienen valores:\n"
                        + "Usuario: " + user + "\nClave: " + pass
                        + "\nServidor: " + server + "\nPuerto: " + port
                        + "\nInstancia: " + instance + "\nMotro DB: " + db
                        + "Opción: " + opcion + "\nRuta: " + ruta;
                LOG.error(mensaje);
                throw new Exception();
            }

            if (!motoresBD.contains(db.trim())) {
                mensaje = "El motor de base de datos no es válido: " + db;
                LOG.error(mensaje);
                throw new Exception();
            }

            int op = Integer.parseInt(opcion.trim());
            if (op < 1 || op > 3) {
                mensaje = "La opción no es válida: " + opcion;
                LOG.error(mensaje);
                throw new Exception();
            }

            File directorio = new File(ruta);
            File[] files = directorio.listFiles();

            for (File f : files) {
                CargaMasivaObservable cargaMasivaObservable = new CargaMasivaObservable(
                        user, pass, server, port, instance, db);

                cargaMasivaObservable.cargar(op, f.getAbsolutePath());
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Error obteniendo parámetros de la aplicación", e);
        } catch (IOException e) {
            LOG.error("Error leyendo directorio", e);
        } catch (NumberFormatException e) {
            LOG.error("Error convirtiendo la opción a un número", e);
        } catch (Exception e) {
            LOG.error("Error no controlado", e);
        }
    }
}
