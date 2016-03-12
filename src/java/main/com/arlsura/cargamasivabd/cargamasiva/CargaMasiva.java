package com.arlsura.cargamasivabd.cargamasiva;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class CargaMasiva {

    private final String DRIVER_PROP = "dbdriver";
    private final String URL_PROP = "dburl";
    private final String USER_PROP = "dbuser";
    private final String PASSWORD_PROP = "dbpassword";
    private final String TABLE_PROP = "table";
    private final String FIELD_SEPARATOR_PROP = "fieldSeparator";
    private final String LINE_SEPARATOR_PROP = "lineSeparator";
    private final String FIELDS_PROP = "fields";
    private final String NULL_FIELDS_PROP = "nullFields";
    private final String FILE_FULL_PATH_PROP = "fileFullPaht";
    private final String COMMA = ",";
    private final String AT = "@";
    private static final Logger LOG = Logger.getLogger(CargaMasiva.class);
    private String query;
    private String driver;
    private String url;
    private String user;
    private String password;
    private String filePath;
    private String table;
    private String fieldSeparator;
    private String fields;
    private String lineSeparator;
    private String nullFields;
    private Connection connection;
    private Statement statement;

    public void cargarArchivoABd() {
        LOG.info("Inició! - " + (new Date()).toString());

        this.cargarConfiguracion();

        try {
            Class.forName(driver);

            LOG.info("Conectándose a la base de datos");
            this.connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();

            this.query = this.construirQuery();
            LOG.info("Query: " + this.query);
//            String sql = "LOAD DATA LOCAL INFILE '" + this.filePath
//                    + "' INTO TABLE TAFI_RUAF_CARGA_MAESTRO FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' "
//                    + "(DSTIPO_ARCHIVO, DSTIPO_ID_AFILIADO, DSNUMERO_ID_AFILIADO, DSGENERO, FENACIMIENTO, "
//                    + "DSPRIMER_APELLIDO, @DSSEGUNDO_APELLIDO, DSPRIMER_NOMBRE, @DSSEGUNDO_NOMBRE, FEAFILIACION, "
//                    + "CDADMINISTRADORA, CDTIPO_COTIZANTE, CDACTIVIDAD_ECONOMICA, DSTIPO_ID_APORTANTE, "
//                    + "DSNUMERO_ID_APORTANTE, DSDIGITO_VER_APORTANTE, DSRAZON_SOCIAL_APORTANTE, CDCLASE_APORTANTE, "
//                    + "CDOCUPACION_AFILIADO, CDDEPARTAMENTO, CDMUNICIPIO, CDALDIA, @CDSUBTIPO_COTIZANTE, @CDMODALIDAD) "
//                    + "SET " + "DSSEGUNDO_APELLIDO = NULLIF(@DSSEGUNDO_APELLIDO, ''), "
//                    + "DSSEGUNDO_NOMBRE = NULLIF(@DSSEGUNDO_NOMBRE, ''), "
//                    + "CDSUBTIPO_COTIZANTE = NULLIF(@CDSUBTIPO_COTIZANTE, ''), "
//                    + "CDMODALIDAD = NULLIF(@CDMODALIDAD,'')";
            // this.query = "LOAD DATA LOCAL INFILE '" + this.filePath
            // + "' INTO TABLE TAFI_RUAF_CARGA_RETIROS FIELDS TERMINATED BY ','
            // LINES TERMINATED BY '\n' "
            // + "(DSTIPO_ARCHIVO, CDADMINISTRADORA, DSTIPO_ID_AFILIADO,
            // DSNUMERO_ID_AFILIADO, DSPRIMER_APELLIDO, "
            // + "@DSSEGUNDO_APELLIDO, DSPRIMER_NOMBRE, @DSSEGUNDO_NOMBRE,
            // DSNOVEDAD, DSTIPO_ID_APORTANTE, "
            // + "DSNUMERO_ID_APORTANTE, DSDIGITO_VER_APORTANTE,
            // FEDESVINCULACION, FERETIRO, CDCAUSA_RETIRO, "
            // + "@FERECONOCIMIENTO, @FEFALLECIMIENTO, @CAMPO1, @CAMPO2,
            // @CAMPO3, @CAMPO4, @CAMPO5, @CAMPO6, @CAMPO7) "
            // + "SET " + "DSSEGUNDO_APELLIDO = NULLIF(@DSSEGUNDO_APELLIDO,''),"
            // + "DSSEGUNDO_NOMBRE = NULLIF(@DSSEGUNDO_NOMBRE,''),"
            // + "FERECONOCIMIENTO = NULLIF(@FERECONOCIMIENTO,''),"
            // + "FEFALLECIMIENTO = NULLIF(@FEFALLECIMIENTO,'')," + "CAMPO1 =
            // NULLIF(@CAMPO1,''),"
            // + "CAMPO2 = NULLIF(@CAMPO2,'')," + "CAMPO3 = NULLIF(@CAMPO3,''),"
            // + "CAMPO4 = NULLIF(@CAMPO4,''),"
            // + "CAMPO5 = NULLIF(@CAMPO5,'')," + "CAMPO6 = NULLIF(@CAMPO6,''),"
            // + "CAMPO7 = NULLIF(@CAMPO7,'')";

            LOG.info("Inicio de ejecución de carga masiva: " + (new Date()).toString());
             statement.executeUpdate(query);
            LOG.info("Fin de ejecución de carga masiva: " + (new Date()).toString());

        } catch (ClassNotFoundException e) {
            LOG.error("Error inicializando driver.", e);
             } catch (SQLException e) {
             LOG.error("Error SQL.", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.error("Error cerrando Statement", e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error("Error cerrando Connection", e);
                }
            }
        }

        LOG.info("Fin! - " + (new Date()).toString());
    }

    private void cargarConfiguracion() {
        LOG.info("Cargando parámetros de configuración de base de datos");
        Util.cargarConfiguraciones();

        this.driver = Util.obtenerValorConf(DRIVER_PROP);
        this.url = Util.obtenerValorConf(URL_PROP);
        this.user = Util.obtenerValorConf(USER_PROP);
        this.password = Util.obtenerValorConf(PASSWORD_PROP);

        this.table = Util.obtenerValorConf(TABLE_PROP);
        this.fieldSeparator = Util.obtenerValorConf(FIELD_SEPARATOR_PROP);
        this.lineSeparator = Util.obtenerValorConf(LINE_SEPARATOR_PROP);
        this.fields = Util.obtenerValorConf(FIELDS_PROP);
        this.nullFields = Util.obtenerValorConf(NULL_FIELDS_PROP);
        this.filePath = Util.obtenerValorConf(FILE_FULL_PATH_PROP);
    }

    private String construirQuery() {
        StringBuilder stringBuilder = new StringBuilder("LOAD DATA LOCAL INFILE '");
        stringBuilder.append(this.filePath);
        stringBuilder.append("' INTO TABLE ");
        stringBuilder.append(this.table);
        stringBuilder.append(" FIELDS TERMINATED BY '");
        stringBuilder.append(this.fieldSeparator);
        stringBuilder.append("' LINES TERMINATED BY '");
        stringBuilder.append(lineSeparator);
        stringBuilder.append("' ");

        boolean hasFields = this.fields != null && !this.fields.trim().isEmpty();
        if (hasFields) {
            stringBuilder.append("(");
            stringBuilder.append(this.fields);
            stringBuilder.append(") ");
        }
        String query = stringBuilder.toString();

        boolean hasNullFields = this.nullFields != null && !this.nullFields.isEmpty();
        if (hasFields && hasNullFields) {
            query = query.concat("SET ");
            StringTokenizer token = new StringTokenizer(this.nullFields.trim(), COMMA);
            while (token.hasMoreElements()) {
                String nullField = (String) token.nextElement();
                nullField = nullField.trim();
                query = query.replace(nullField, AT + nullField);
                query = query.concat(nullField).concat(" = NULLIF(").concat(AT).concat(nullField).
                        concat(COMMA).concat(" ''), ");
            }
        }

        return (hasFields && hasNullFields) ? query.substring(0, query.length() - 2)
                : query.substring(0, query.length() - 1);
    }
}
