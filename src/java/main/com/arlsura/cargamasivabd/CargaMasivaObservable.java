package com.arlsura.cargamasivabd;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import rx.Observable;
import rx.schedulers.Schedulers;

import com.arlsura.cargamasivabd.hikaricp.BDConfig;
import com.arlsura.cargamasivabd.modelo.TafiAfiliadosRuaf;
import com.arlsura.cargamasivabd.modelo.TafiRuafCargaMaestro;
import com.arlsura.cargamasivabd.modelo.TafiRuafCargaRetiros;
import com.arlsura.cargamasivabd.util.Util;
import com.zaxxer.hikari.HikariDataSource;

public class CargaMasivaObservable<T> {

    private static final int MAXIMO_PROCESO = 100000;
    private static final int TAFI_RUAF_CARGA_MAESTRO = 1;
    private static final int TAFI_RUAF_CARGA_RETIROS = 2;
    private static final int TAFI_AFILIADOS_RUAF = 3;
    private static final Logger LOG = Logger
            .getLogger(CargaMasivaObservable.class);
    private static int empezar = 0;
    private static int procesados = 0;
    private static int cantidadProcesar;
    private CountDownLatch latch;
    private HikariDataSource hikariDataSource;
    private final Util util = new Util();
    private List<TafiRuafCargaMaestro> lineasInsertarTafiRuafCargaMaestro;
    private List<TafiRuafCargaRetiros> lineasInsertarTafiRuafCargaRetiros;
    private List<TafiAfiliadosRuaf> lineasInsertarTafiAfiliadosRuaf;
    private Executor executor;

    private String user;
    private String pass;
    private String server;
    private String port;
    private String instance;
    private String database;
    private Stream<String> stream;

    public CargaMasivaObservable(String user, String pass, String server,
            String port, String instance, String database) {
        super();

        this.user = user;
        this.pass = pass;
        this.server = server;
        this.port = port;
        this.instance = instance;
        this.database = database;

        executor = Executors.newFixedThreadPool(150, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public void cargar(int opcionProcesa, String rutaArchivo)
            throws IOException {
        LOG.info("Inicio! - " + (new Date()).toString());
        empezar = 0;
        procesados = 0;

        switch (opcionProcesa) {
        case TAFI_RUAF_CARGA_MAESTRO:
            LOG.info("Procesando líneas para TAFI_RUAF_CARGA_MAESTRO");

            LOG.info("Inicio lectura y mapeo de carga maestro");
            stream = this.util.leerLineasAsStream(rutaArchivo);
            lineasInsertarTafiRuafCargaMaestro = stream.map(
                    this::mapearCargaMaestro).collect(Collectors.toList());
            cantidadProcesar = lineasInsertarTafiRuafCargaMaestro.size();
            LOG.info("Fin lectura y mapeo de carga maestro");

            LOG.info("Inicio de procesar Carga Maestro con archivo: "
                    + rutaArchivo);
            this.procesarMaestro();
            LOG.info("Fin de procesar Carga Maestro con archivo: "
                    + rutaArchivo);

            break;
        case TAFI_RUAF_CARGA_RETIROS:
            LOG.info("Procesando líneas para TAFI_RUAF_CARGA_RETIROS");

            LOG.info("Inicio lectura y mapeo de carga retiro");
            stream = this.util.leerLineasAsStream(rutaArchivo);
            lineasInsertarTafiRuafCargaRetiros = stream.map(
                    this::mapearCargaRetiro).collect(Collectors.toList());
            cantidadProcesar = lineasInsertarTafiRuafCargaRetiros.size();
            LOG.info("Fin lectura y mapeo de carga retiro");

            LOG.info("Inicio de procesar Carga Retiro con archivo: "
                    + rutaArchivo);
            this.procesarRetiros();
            LOG.info("Fin de procesar Carga Retiro con archivo: " + rutaArchivo);

            break;
        case TAFI_AFILIADOS_RUAF:
            LOG.info("Procesando líneas para TAFI_AFILIADOS_RUAF");

            LOG.info("Inicio lectura y mapeo de afiliados");
            stream = this.util.leerLineasAsStream(rutaArchivo);
            lineasInsertarTafiAfiliadosRuaf = stream.map(this::mapearAfiliado)
                    .collect(Collectors.toList());
            cantidadProcesar = lineasInsertarTafiAfiliadosRuaf.size();
            LOG.info("Fin lectura y mapeo de afiliados");

            LOG.info("Inicio de procesar Afiliados con archivo: " + rutaArchivo);
            this.procesarAfiliados();
            LOG.info("Fin de procesar Afiliados con archivo: " + rutaArchivo);

            break;
        default:
            LOG.info("No existe nada para procesar");
            break;
        }
    }

    private TafiAfiliadosRuaf mapearAfiliado(String linea) {
        TafiAfiliadosRuaf a = new TafiAfiliadosRuaf();
        String[] campos = linea.split(",", -1);

        String cdTipoDocumentoAfiliado = campos[0];
        a.setCdtipo_documento_afiliado(cdTipoDocumentoAfiliado.trim());

        String dniAfiliado = campos[1];
        a.setDni_afiliado(dniAfiliado.trim());

        String dsSexo = campos[2];
        a.setDssexo(dsSexo.trim());

        String fechaNacimiento = campos[3];
        a.setFecha_nacimiento(fechaNacimiento.trim());

        String dsPrimerApellido = campos[4];
        a.setDsapellido1(dsPrimerApellido.trim());

        String dsSegundoApellido = campos[5];
        a.setDsapellido2(!dsSegundoApellido.trim().isEmpty() ? dsSegundoApellido
                .trim() : null);

        String dsPrimerNombre = campos[6];
        a.setDsnombre1(dsPrimerNombre.trim());

        String dsSegundoNombre = campos[7];
        a.setDsnombre2(!dsSegundoNombre.trim().isEmpty() ? dsSegundoNombre
                .trim() : null);

        String fechaAfiliacion = campos[8];
        a.setFecha_afiliacion(fechaAfiliacion.trim());

        String cdEntidad = campos[9];
        a.setCdentidad(cdEntidad.trim());

        String dsEntidad = campos[10];
        a.setDsentidad(dsEntidad.trim());

        String cdTipoCotizante = campos[11];
        a.setCdtipo_cotizante(!cdTipoCotizante.trim().isEmpty() ? cdTipoCotizante
                .trim() : null);

        String nmEstado = campos[12];
        a.setNmestado(!nmEstado.trim().isEmpty() ? nmEstado.trim() : null);

        String cdDepartamento = campos[13];
        a.setCddepartamento(!cdDepartamento.trim().isEmpty() ? cdDepartamento
                .trim() : null);

        String cdMunicipio = campos[14];
        a.setCdmunicipio(!cdMunicipio.trim().isEmpty() ? cdMunicipio.trim()
                : null);

        String cdTipoDocumentoEmpleador = campos[15];
        a.setCdtipo_documento_empleador(!cdTipoDocumentoEmpleador.trim()
                .isEmpty() ? cdTipoDocumentoEmpleador.trim() : null);

        String dniEmpleador = campos[16];
        a.setDni_empleador(!dniEmpleador.trim().isEmpty() ? dniEmpleador.trim()
                : null);

        String nmDigitoVerificacion = campos[17];
        a.setNmdigito_verificacion(!nmDigitoVerificacion.trim().isEmpty() ? nmDigitoVerificacion
                .trim() : null);

        String nmDatosBasicos = campos[18];
        a.setNmdatos_basicos(!nmDatosBasicos.trim().isEmpty() ? nmDatosBasicos
                .trim() : null);

        String nmAldia = campos[19];
        a.setNmal_dia(!nmAldia.trim().isEmpty() ? nmAldia.trim() : null);

        a.setCdnovedad(null);

        a.setDsregistro(null);

        a.setSnprocesado("N");

        return a;
    }

    private void procesarRetiros() {
        hikariDataSource = BDConfig.dataSource(server, port, instance, user,
                pass, database);

        int p = this.procesarRetirosObservable(
                lineasInsertarTafiRuafCargaRetiros, empezar);
        procesados = procesados + p;
        empezar = procesados;
        cantidadProcesar = cantidadProcesar - p;

        hikariDataSource.close();

        LOG.info("Procesados: "
                + (lineasInsertarTafiRuafCargaRetiros.size() - cantidadProcesar));

        if (cantidadProcesar > 0) {
            procesarRetiros();
        } else {
            LOG.info("Fin! - " + (new Date()).toString());
        }
    }

    private int procesarRetirosObservable(List<TafiRuafCargaRetiros> retiros,
            int desde) {
        int hasta = desde + MAXIMO_PROCESO;
        if (hasta > retiros.size()) {
            hasta = retiros.size();
        }

        int procesados = hasta - desde;

        List<TafiRuafCargaRetiros> porProcesar = new ArrayList<>();
        for (int i = desde; i < hasta; i++) {
            porProcesar.add(retiros.get(i));
        }

        latch = new CountDownLatch(porProcesar.size());
        Observable<TafiRuafCargaRetiros> observable = Observable
                .from(porProcesar);
        ;

        observable.doOnNext(
                t -> Observable.just(t).observeOn(Schedulers.from(executor))
                        .doOnNext(this::insertarTafiRuafCargaRetiros)
                        .doOnCompleted(() -> latch.countDown()).subscribe())
                .subscribe();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error(
                    "Error esperando a que asíncronamente termine el procesamiento: procesarRetirosObservable",
                    e);
        }

        return procesados;
    }

    private void procesarMaestro() {
        hikariDataSource = BDConfig.dataSource(server, port, instance, user,
                pass, database);

        int p = this.procesarMaestroObservable(
                lineasInsertarTafiRuafCargaMaestro, empezar);
        procesados = procesados + p;
        empezar = procesados;
        cantidadProcesar = cantidadProcesar - p;

        hikariDataSource.close();

        LOG.info("Procesados: "
                + (lineasInsertarTafiRuafCargaMaestro.size() - cantidadProcesar));

        if (cantidadProcesar > 0) {
            procesarMaestro();
        } else {
            LOG.info("Fin! - " + (new Date()).toString());
        }
    }

    private int procesarMaestroObservable(List<TafiRuafCargaMaestro> maestro,
            int desde) {
        int hasta = desde + MAXIMO_PROCESO;
        if (hasta > maestro.size()) {
            hasta = maestro.size();
        }

        int procesados = hasta - desde;

        List<TafiRuafCargaMaestro> porProcesar = new ArrayList<>();
        for (int i = desde; i < hasta; i++) {
            porProcesar.add(maestro.get(i));
        }

        latch = new CountDownLatch(porProcesar.size());
        Observable<TafiRuafCargaMaestro> observable = Observable
                .from(porProcesar);
        ;

        observable.doOnNext(
                t -> Observable.just(t).observeOn(Schedulers.from(executor))
                        .doOnNext(this::insertarTafiRuafCargaMaestro)
                        .doOnCompleted(() -> latch.countDown()).subscribe())
                .subscribe();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error(
                    "Error esperando a que asíncronamente termine el procesamiento: procesarMaestro",
                    e);
        }

        return procesados;
    }

    private void procesarAfiliados() {
        hikariDataSource = BDConfig.dataSource(server, port, instance, user,
                pass, database);

        int p = this.procesarAfiliadosObservable(
                lineasInsertarTafiAfiliadosRuaf, empezar);
        procesados = procesados + p;
        empezar = procesados;
        cantidadProcesar = cantidadProcesar - p;

        hikariDataSource.close();

        // LOG.info("Procesados: "
        //        + (lineasInsertarTafiAfiliadosRuaf.size() - cantidadProcesar));

        if (cantidadProcesar > 0) {
            procesarAfiliados();
        } else {
            LOG.info("Fin! - " + (new Date()).toString());
        }
    }

    private int procesarAfiliadosObservable(List<TafiAfiliadosRuaf> afiliados,
            int desde) {
        int hasta = desde + MAXIMO_PROCESO;
        if (hasta > afiliados.size()) {
            hasta = afiliados.size();
        }

        int procesados = hasta - desde;

        List<TafiAfiliadosRuaf> porProcesar = new ArrayList<>();
        for (int i = desde; i < hasta; i++) {
            porProcesar.add(afiliados.get(i));
        }

        latch = new CountDownLatch(porProcesar.size());
        Observable<TafiAfiliadosRuaf> observable = Observable.from(porProcesar);
        ;

        observable.doOnNext(
                t -> Observable.just(t).observeOn(Schedulers.from(executor))
                        .doOnNext(this::insertarTafiAfiliadosRuaf)
                        .doOnCompleted(() -> latch.countDown()).subscribe())
                .subscribe();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error(
                    "Error esperando a que asíncronamente termine el procesamiento: procesarAfiliados",
                    e);
        }

        return procesados;
    }

    private TafiRuafCargaMaestro mapearCargaMaestro(String linea) {
        TafiRuafCargaMaestro maestro = new TafiRuafCargaMaestro();
        String[] campos = linea.split(",", -1);
        maestro.setLineaCompleta(linea);

        String dsTipoArchivo = campos[0];
        maestro.setDstipo_archivo(dsTipoArchivo.trim());

        String dsTipoIdAfiliado = campos[1];
        maestro.setDstipo_id_afiliado(dsTipoIdAfiliado.trim());

        String dsNumeroIdAfiliado = campos[2];
        maestro.setDsnumero_id_afiliado(dsNumeroIdAfiliado.trim());

        String dsGenero = campos[3];
        maestro.setDsgenero(dsGenero.trim());

        String fenacimiento = campos[4];
        maestro.setFenacimiento(!fenacimiento.trim().isEmpty() ? fenacimiento
                .trim() : null);

        String dsPrimerApellido = campos[5];
        maestro.setDsprimer_apellido(dsPrimerApellido.trim());

        String dsSegundoApellido = campos[6];
        maestro.setDssegundo_apellido(!dsSegundoApellido.trim().isEmpty() ? dsSegundoApellido
                : null);

        String dsPrimerNombre = campos[7];
        maestro.setDsprimer_nombre(dsPrimerNombre.trim());

        String dsSegundoNombre = campos[8];
        maestro.setDssegundo_nombre(!dsSegundoNombre.trim().isEmpty() ? dsSegundoApellido
                .trim() : null);

        String feafiliacion = campos[9];
        maestro.setFeafiliacion(feafiliacion.trim());

        String cdAdministradora = campos[10];
        maestro.setCdadministradora(cdAdministradora.trim());

        String cdTipoCotizante = campos[11];
        maestro.setCdtipo_cotizante(!cdTipoCotizante.trim().isEmpty() ? cdTipoCotizante
                .trim() : null);

        String cdActividadEconomica = campos[12];
        maestro.setCdactividad_economica(!cdActividadEconomica.trim().isEmpty() ? cdActividadEconomica
                .trim() : null);

        String dsTipoIdAportante = campos[13];
        maestro.setDstipo_id_aportante(!dsTipoIdAportante.trim().isEmpty() ? dsTipoIdAportante
                .trim() : null);

        String dsNumeroIdAportante = campos[14];
        maestro.setDsnumero_id_aportante(!dsNumeroIdAportante.trim().isEmpty() ? dsNumeroIdAportante
                .trim() : null);

        String dsDigitoVerAportante = campos[15];
        maestro.setDsdigito_ver_aportante(!dsDigitoVerAportante.trim()
                .isEmpty() ? dsDigitoVerAportante.trim() : null);

        String dsRazonSocialAportante = campos[16];
        maestro.setDsrazon_social_aportante(!dsRazonSocialAportante.trim()
                .isEmpty() ? dsRazonSocialAportante.trim() : null);

        String cdClaseAportante = campos[17];
        maestro.setCdclase_aportante(!cdClaseAportante.trim().isEmpty() ? cdClaseAportante
                .trim() : null);

        String cdOcupacionAfiliado = campos[18];
        maestro.setCdocupacion_afiliado(!cdOcupacionAfiliado.trim().isEmpty() ? cdOcupacionAfiliado
                .trim() : null);

        String cdDepartamento = campos[19];
        maestro.setCddepartamento(cdDepartamento);

        String cdMunicipio = campos[20];
        maestro.setCdmunicipio(cdMunicipio);

        String cdAldia = campos[21];
        maestro.setCdaldia(cdAldia);

        String cdSubtipoCotizante = campos[22];
        maestro.setCdsubtipo_cotizante(!cdSubtipoCotizante.trim().isEmpty() ? cdSubtipoCotizante
                .trim() : null);

        String cdModalidad = campos[23];
        maestro.setCdmodalidad(!cdModalidad.trim().isEmpty() ? cdModalidad
                .trim() : null);

        return maestro;
    }

    private TafiRuafCargaRetiros mapearCargaRetiro(String linea) {
        TafiRuafCargaRetiros retiro = new TafiRuafCargaRetiros();
        String[] campos = linea.split(",", -1);

        retiro.setLineaCompleta(linea);

        String dsTipoArchivo = campos[0];
        retiro.setDstipo_archivo(dsTipoArchivo.trim());

        String cdAdministradora = campos[1];
        retiro.setCdadministradora(cdAdministradora.trim());

        String dsTipoIdAfiliado = campos[2];
        retiro.setDstipo_id_afiliado(dsTipoIdAfiliado.trim());

        String dsNumeroIdAfiliado = campos[3];
        retiro.setDsnumero_id_afiliado(dsNumeroIdAfiliado.trim());

        String dsPrimerApellido = campos[4];
        retiro.setDsprimer_apellido(dsPrimerApellido.trim());

        String dsSegundoApellido = campos[5];
        retiro.setDssegundo_apellido(!dsSegundoApellido.trim().isEmpty() ? dsSegundoApellido
                .trim() : null);

        String dsPrimerNombre = campos[6];
        retiro.setDsprimer_nombre(dsPrimerNombre.trim());

        String dsSegundoNombre = campos[7];
        retiro.setDssegundo_nombre(!dsSegundoNombre.trim().isEmpty() ? dsSegundoNombre
                .trim() : null);

        String dsNovedad = campos[8];
        retiro.setDsnovedad(dsNovedad.trim());

        String dsTipoIdAportante = campos[9];
        retiro.setDstipo_id_aportante(dsTipoIdAportante.trim());

        String dsNumeroIdAportante = campos[10];
        retiro.setDsnumero_id_aportante(dsNumeroIdAportante.trim());

        String dsDigitoVerAportante = campos[11];
        retiro.setDsdigito_ver_aportante(!dsDigitoVerAportante.trim().isEmpty() ? dsDigitoVerAportante
                .trim() : null);

        String fedesvinculacion = campos[12];
        retiro.setFedesvinculacion(!fedesvinculacion.trim().isEmpty() ? fedesvinculacion
                .trim() : null);

        String feretiro = campos[13];
        retiro.setFeretiro(!feretiro.trim().isEmpty() ? feretiro.trim() : null);

        String cdCausaRetiro = campos[14];
        retiro.setCdcausa_retiro(!cdCausaRetiro.trim().isEmpty() ? cdCausaRetiro
                .trim() : null);

        String fereconocimiento = campos[15];
        retiro.setFereconocimiento(!fereconocimiento.trim().isEmpty() ? fereconocimiento
                .trim() : null);

        String fefallecimiento = campos[16];
        retiro.setFefallecimiento(!fefallecimiento.trim().isEmpty() ? fefallecimiento
                .trim() : null);

        String campo1 = campos[17];
        retiro.setCampo1(!campo1.trim().isEmpty() ? campo1.trim() : null);

        String campo2 = campos[18];
        retiro.setCampo2(!campo2.trim().isEmpty() ? campo2.trim() : null);

        String campo3 = campos[19];
        retiro.setCampo3(!campo3.trim().isEmpty() ? campo3.trim() : null);

        String campo4 = campos[20];
        retiro.setCampo4(!campo4.trim().isEmpty() ? campo4.trim() : null);

        String campo5 = campos[21];
        retiro.setCampo5(!campo5.trim().isEmpty() ? campo5.trim() : null);

        String campo6 = campos[22];
        retiro.setCampo6(!campo6.trim().isEmpty() ? campo6.trim() : null);

        String campo7 = campos[23];
        retiro.setCampo7(!campo7.trim().isEmpty() ? campo7.trim() : null);

        return retiro;
    }

    private void insertarTafiRuafCargaMaestro(TafiRuafCargaMaestro maestro) {
        try {
            Connection connection = hikariDataSource.getConnection();
            PreparedStatement statement = connection
                    .prepareStatement("INSERT INTO TAFI_RUAF_CARGA_MAESTRO "
                            + "(DSTIPO_ARCHIVO, DSTIPO_ID_AFILIADO, DSNUMERO_ID_AFILIADO, DSGENERO, FENACIMIENTO, "
                            + "DSPRIMER_APELLIDO, DSSEGUNDO_APELLIDO, DSPRIMER_NOMBRE, DSSEGUNDO_NOMBRE, FEAFILIACION, "
                            + "CDADMINISTRADORA, CDTIPO_COTIZANTE, CDACTIVIDAD_ECONOMICA, DSTIPO_ID_APORTANTE, "
                            + "DSNUMERO_ID_APORTANTE, DSDIGITO_VER_APORTANTE, DSRAZON_SOCIAL_APORTANTE, CDCLASE_APORTANTE, "
                            + "CDOCUPACION_AFILIADO, CDDEPARTAMENTO, CDMUNICIPIO, CDALDIA, CDSUBTIPO_COTIZANTE, CDMODALIDAD) "
                            + "VALUES "
                            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            statement.setString(1, maestro.getDstipo_archivo());
            statement.setString(2, maestro.getDstipo_id_afiliado());
            statement.setString(3, maestro.getDsnumero_id_afiliado());
            statement.setString(4, maestro.getDsgenero());
            statement.setString(5, maestro.getFenacimiento());
            statement.setString(6, maestro.getDsprimer_apellido());
            statement.setString(7, maestro.getDssegundo_apellido());
            statement.setString(8, maestro.getDsprimer_nombre());
            statement.setString(9, maestro.getDssegundo_nombre());
            statement.setString(10, maestro.getFeafiliacion());
            statement.setString(11, maestro.getCdadministradora());
            statement.setString(12, maestro.getCdtipo_cotizante());
            statement.setString(13, maestro.getCdactividad_economica());
            statement.setString(14, maestro.getDstipo_id_aportante());
            statement.setString(15, maestro.getDsnumero_id_aportante());
            statement.setString(16, maestro.getDsdigito_ver_aportante());
            statement.setString(17, maestro.getDsrazon_social_aportante());
            statement.setString(18, maestro.getCdclase_aportante());
            statement.setString(19, maestro.getCdocupacion_afiliado());
            statement.setString(20, maestro.getCddepartamento());
            statement.setString(21, maestro.getCdmunicipio());
            statement.setString(22, maestro.getCdaldia());
            statement.setString(23, maestro.getCdsubtipo_cotizante());
            statement.setString(24, maestro.getCdmodalidad());

            statement.executeUpdate();

            statement.close();
            connection.close();
        } catch (SQLException e) {
            LOG.error("No se ha podido insertar el registro"
                    + maestro.getLineaCompleta());
            LOG.error("Error insertando registro", e);
        }
        // System.out.println(Thread.currentThread().getName());
    }

    private void insertarTafiRuafCargaRetiros(TafiRuafCargaRetiros retiro) {
        try {
            Connection connection = hikariDataSource.getConnection();
            PreparedStatement statement = connection
                    .prepareStatement("INSERT INTO TAFI_RUAF_CARGA_RETIROS "
                            + "(DSTIPO_ARCHIVO, CDADMINISTRADORA, DSTIPO_ID_AFILIADO, DSNUMERO_ID_AFILIADO, "
                            + "DSPRIMER_APELLIDO, DSSEGUNDO_APELLIDO, DSPRIMER_NOMBRE, DSSEGUNDO_NOMBRE, DSNOVEDAD, "
                            + "DSTIPO_ID_APORTANTE, DSNUMERO_ID_APORTANTE, DSDIGITO_VER_APORTANTE, FEDESVINCULACION, "
                            + "FERETIRO, CDCAUSA_RETIRO, FERECONOCIMIENTO, FEFALLECIMIENTO, CAMPO1, CAMPO2, CAMPO3, "
                            + "CAMPO4, CAMPO5, CAMPO6, CAMPO7) "
                            + "VALUES "
                            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            statement.setString(1, retiro.getDstipo_archivo());
            statement.setString(2, retiro.getCdadministradora());
            statement.setString(3, retiro.getDstipo_id_afiliado());
            statement.setString(4, retiro.getDsnumero_id_afiliado());
            statement.setString(5, retiro.getDsprimer_apellido());
            statement.setString(6, retiro.getDssegundo_apellido());
            statement.setString(7, retiro.getDsprimer_nombre());
            statement.setString(8, retiro.getDssegundo_nombre());
            statement.setString(9, retiro.getDsnovedad());
            statement.setString(10, retiro.getDstipo_id_aportante());
            statement.setString(11, retiro.getDsnumero_id_aportante());
            statement.setString(12, retiro.getDsdigito_ver_aportante());
            statement.setString(13, retiro.getFedesvinculacion());
            statement.setString(14, retiro.getFeretiro());
            statement.setString(15, retiro.getCdcausa_retiro());
            statement.setString(16, retiro.getFereconocimiento());
            statement.setString(17, retiro.getFefallecimiento());
            statement.setString(18, retiro.getCampo1());
            statement.setString(19, retiro.getCampo2());
            statement.setString(20, retiro.getCampo3());
            statement.setString(21, retiro.getCampo4());
            statement.setString(22, retiro.getCampo5());
            statement.setString(23, retiro.getCampo6());
            statement.setString(24, retiro.getCampo7());

            statement.executeUpdate();

            statement.close();
            connection.close();
        } catch (SQLException e) {
            LOG.error("No se ha podido insertar el registro"
                    + retiro.getLineaCompleta());
            LOG.error("Error insertando registro", e);
        }
        // System.out.println(Thread.currentThread().getName());
    }

    private void insertarTafiAfiliadosRuaf(TafiAfiliadosRuaf afiliado) {
        try {
            Connection connection = hikariDataSource.getConnection();
            PreparedStatement statement = connection
                    .prepareStatement("INSERT INTO TAFI_AFILIADOS_RUAF "
                            + "(CDTIPO_DOCUMENTO_AFILIADO, DNI_AFILIADO, DSSEXO, FECHA_NACIMIENTO, DSAPELLIDO1, "
                            + "DSAPELLIDO2, DSNOMBRE1, DSNOMBRE2, FECHA_AFILIACION, CDENTIDAD, DSENTIDAD, "
                            + "CDTIPO_COTIZANTE, NMESTADO, CDDEPARTAMENTO, CDMUNICIPIO, CDTIPO_DOCUMENTO_EMPLEADOR, "
                            + "DNI_EMPLEADOR, NMDIGITO_VERIFICACION, NMDATOS_BASICOS, NMAL_DIA, CDNOVEDAD, "
                            + "DSREGISTRO, SNPROCESADO) "
                            + "VALUES "
                            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            statement.setString(1, afiliado.getCdtipo_documento_afiliado());
            statement.setString(2, afiliado.getDni_afiliado());
            statement.setString(3, afiliado.getDssexo());
            statement.setString(4, afiliado.getFecha_nacimiento());
            statement.setString(5, afiliado.getDsapellido1());
            statement.setString(6, afiliado.getDsapellido2());
            statement.setString(7, afiliado.getDsnombre1());
            statement.setString(8, afiliado.getDsnombre2());
            statement.setString(9, afiliado.getFecha_afiliacion());
            statement.setString(10, afiliado.getCdentidad());
            statement.setString(11, afiliado.getDsentidad());
            statement.setString(12, afiliado.getCdtipo_cotizante());
            statement.setString(13, afiliado.getNmestado());
            statement.setString(14, afiliado.getCddepartamento());
            statement.setString(15, afiliado.getCdmunicipio());
            statement.setString(16, afiliado.getCdtipo_documento_empleador());
            statement.setString(17, afiliado.getDni_empleador());
            statement.setString(18, afiliado.getNmdigito_verificacion());
            statement.setString(19, afiliado.getNmdatos_basicos());
            statement.setString(20, afiliado.getNmal_dia());
            statement.setString(21, afiliado.getCdnovedad());
            statement.setString(22, afiliado.getDsregistro());
            statement.setString(23, afiliado.getSnprocesado());

            statement.executeUpdate();

            statement.close();
            connection.close();
        } catch (SQLException e) {
            LOG.error("No se ha podido insertar el registro"
                    + afiliado.getLineaCompleta());
            LOG.error("Error insertando registro", e);
        }
        // System.out.println(Thread.currentThread().getName());
    }
}
