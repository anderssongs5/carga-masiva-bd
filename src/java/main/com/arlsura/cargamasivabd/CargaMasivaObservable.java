package com.arlsura.cargamasivabd;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.arlsura.cargamasivabd.hikaricp.AplicacionBDITConfig;
import com.arlsura.cargamasivabd.modelo.TafiRuafCargaMaestro;
import com.arlsura.cargamasivabd.modelo.TafiRuafCargaRetiros;
import com.arlsura.cargamasivabd.util.Util;
import com.zaxxer.hikari.HikariDataSource;

import rx.Observable;
import rx.schedulers.Schedulers;

public class CargaMasivaObservable<T> {

    private static final int MAXIMO_PROCESO = 3000;
    private static final int TAFI_RUAF_CARGA_MAESTRO = 1;
    private static final int TAFI_RUAF_CARGA_RETIROS = 2;
    private static final Logger LOG = Logger.getLogger(CargaMasivaObservable.class);
    private static int empezar = 0;
    private static int procesados = 0;
    private static int cantidadProcesar;
    private CountDownLatch latch;
    private HikariDataSource hikariDataSource;
    private List<String> lineas;
    private Util util = new Util();
    private List<TafiRuafCargaMaestro> lineasInsertarTafiRuafCargaMaestro;
    private List<TafiRuafCargaRetiros> lineasInsertarTafiRuafCargaRetiros;

    public CargaMasivaObservable() {
        super();
    }

    public void cargar(int opcionProcesa, String rutaArchivo) throws IOException {
        LOG.info("Inicio! - " + (new Date()).toString());
        empezar = 0;
        procesados = 0;
        LOG.info("Inicio lectura archivo: " + (new Date()).toString());
        lineas = util.leerArchivo(new File(rutaArchivo));
        LOG.info("Fin lectura archivo: " + (new Date()).toString());

        switch (opcionProcesa) {
        case TAFI_RUAF_CARGA_MAESTRO:
            LOG.info("Procesando líneas para TAFI_RUAF_CARGA_MAESTRO");
            lineasInsertarTafiRuafCargaMaestro = this.obtenerListaTafiRuafCargaMaestro();
            cantidadProcesar = lineasInsertarTafiRuafCargaMaestro.size();
            this.procesarMaestro();
            break;
        case TAFI_RUAF_CARGA_RETIROS:
            LOG.info("Procesando líneas para TAFI_RUAF_CARGA_RETIROS");
            lineasInsertarTafiRuafCargaRetiros = this.obtenerListaTafiRuafCargaRetiros();
            cantidadProcesar = lineasInsertarTafiRuafCargaRetiros.size();
            this.procesarRetiros();
            break;
        default:
            LOG.info("No existe nada para procesar");
            break;
        }
    }

    private void procesarRetiros() {
        hikariDataSource = AplicacionBDITConfig.dataSource();

        int p = this.procesarRetirosObservable(lineasInsertarTafiRuafCargaRetiros, empezar);
        procesados = procesados + p;
        empezar = procesados;
        cantidadProcesar = cantidadProcesar - p;

        hikariDataSource.close();

        LOG.info("Procesados: " + (lineasInsertarTafiRuafCargaRetiros.size() - cantidadProcesar));

        if (cantidadProcesar > 0) {
            procesarRetiros();
        } else {
            LOG.info("Fin! - " + (new Date()).toString());
        }
    }

    private int procesarRetirosObservable(List<TafiRuafCargaRetiros> retiros, int desde) {
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
        Observable<TafiRuafCargaRetiros> observable = Observable.from(porProcesar).subscribeOn(Schedulers.io());
        ;

        observable.doOnNext(t -> Observable.just(t).observeOn(Schedulers.io())
                .doOnNext(this::insertarTafiRuafCargaRetiros).doOnCompleted(() -> latch.countDown()).subscribe())
                .subscribe();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Error esperando a que asíncronamente termine el procesamiento: procesarRetirosObservable", e);
        }

        return procesados;
    }

    private void procesarMaestro() {
        hikariDataSource = AplicacionBDITConfig.dataSource();

        int p = this.procesarMaestroObservable(lineasInsertarTafiRuafCargaMaestro, empezar);
        procesados = procesados + p;
        empezar = procesados;
        cantidadProcesar = cantidadProcesar - p;

        hikariDataSource.close();

        LOG.info("Procesados: " + (lineasInsertarTafiRuafCargaMaestro.size() - cantidadProcesar));

        if (cantidadProcesar > 0) {
            procesarMaestro();
        } else {
            LOG.info("Fin! - " + (new Date()).toString());
        }
    }

    private int procesarMaestroObservable(List<TafiRuafCargaMaestro> maestro, int desde) {
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
        Observable<TafiRuafCargaMaestro> observable = Observable.from(porProcesar).subscribeOn(Schedulers.io());
        ;

        observable.doOnNext(t -> Observable.just(t).observeOn(Schedulers.io())
                .doOnNext(this::insertarTafiRuafCargaMaestro).doOnCompleted(() -> latch.countDown()).subscribe())
                .subscribe();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Error esperando a que asíncronamente termine el procesamiento: procesarMaestro", e);
        }

        return procesados;
    }

    private List<TafiRuafCargaMaestro> obtenerListaTafiRuafCargaMaestro() {
        LOG.info("Inicio armado lista TafiRuafCargaMaestro " + (new Date()).toString());
        List<TafiRuafCargaMaestro> maestro = new ArrayList<>();
        for (int i = 0; i < lineas.size(); i++) {
            TafiRuafCargaMaestro m = new TafiRuafCargaMaestro();
            String[] campos = lineas.get(i).split(",", -1);

            m.setLineaCompleta(lineas.get(i));

            String dsTipoArchivo = campos[0];
            m.setDstipo_archivo(dsTipoArchivo.trim());

            String dsTipoIdAfiliado = campos[1];
            m.setDstipo_id_afiliado(dsTipoIdAfiliado.trim());

            String dsNumeroIdAfiliado = campos[2];
            m.setDsnumero_id_afiliado(dsNumeroIdAfiliado.trim());

            String dsGenero = campos[3];
            m.setDsgenero(dsGenero.trim());

            String fenacimiento = campos[4];
            m.setFenacimiento(!fenacimiento.trim().isEmpty() ? fenacimiento.trim() : null);

            String dsPrimerApellido = campos[5];
            m.setDsprimer_apellido(dsPrimerApellido.trim());

            String dsSegundoApellido = campos[6];
            m.setDssegundo_apellido(!dsSegundoApellido.trim().isEmpty() ? dsSegundoApellido : null);

            String dsPrimerNombre = campos[7];
            m.setDsprimer_nombre(dsPrimerNombre.trim());

            String dsSegundoNombre = campos[8];
            m.setDssegundo_nombre(!dsSegundoNombre.trim().isEmpty() ? dsSegundoApellido.trim() : null);

            String feafiliacion = campos[9];
            m.setFeafiliacion(feafiliacion.trim());

            String cdAdministradora = campos[10];
            m.setCdadministradora(cdAdministradora.trim());

            String cdTipoCotizante = campos[11];
            m.setCdtipo_cotizante(!cdTipoCotizante.trim().isEmpty() ? cdTipoCotizante.trim() : null);

            String cdActividadEconomica = campos[12];
            m.setCdactividad_economica(!cdActividadEconomica.trim().isEmpty() ? cdActividadEconomica.trim() : null);

            String dsTipoIdAportante = campos[13];
            m.setDstipo_id_aportante(!dsTipoIdAportante.trim().isEmpty() ? dsTipoIdAportante.trim() : null);

            String dsNumeroIdAportante = campos[14];
            m.setDsnumero_id_aportante(!dsNumeroIdAportante.trim().isEmpty() ? dsNumeroIdAportante.trim() : null);

            String dsDigitoVerAportante = campos[15];
            m.setDsdigito_ver_aportante(!dsDigitoVerAportante.trim().isEmpty() ? dsDigitoVerAportante.trim() : null);

            String dsRazonSocialAportante = campos[16];
            m.setDsrazon_social_aportante(
                    !dsRazonSocialAportante.trim().isEmpty() ? dsRazonSocialAportante.trim() : null);

            String cdClaseAportante = campos[17];
            m.setCdclase_aportante(!cdClaseAportante.trim().isEmpty() ? cdClaseAportante.trim() : null);

            String cdOcupacionAfiliado = campos[18];
            m.setCdocupacion_afiliado(!cdOcupacionAfiliado.trim().isEmpty() ? cdOcupacionAfiliado.trim() : null);

            String cdDepartamento = campos[19];
            m.setCddepartamento(cdDepartamento);

            String cdMunicipio = campos[20];
            m.setCdmunicipio(cdMunicipio);

            String cdAldia = campos[21];
            m.setCdaldia(cdAldia);

            String cdSubtipoCotizante = campos[22];
            m.setCdsubtipo_cotizante(!cdSubtipoCotizante.trim().isEmpty() ? cdSubtipoCotizante.trim() : null);

            String cdModalidad = campos[23];
            m.setCdmodalidad(!cdModalidad.trim().isEmpty() ? cdModalidad.trim() : null);

            maestro.add(m);
        }
        LOG.info("Fin armado lista TafiRuafCargaMaestro " + (new Date()).toString());

        return maestro;
    }

    private List<TafiRuafCargaRetiros> obtenerListaTafiRuafCargaRetiros() {
        LOG.info("Inicio armado lista TafiRuafCargaRetiros " + (new Date()).toString());
        List<TafiRuafCargaRetiros> retiros = new ArrayList<>();
        for (int i = 0; i < lineas.size(); i++) {
            TafiRuafCargaRetiros r = new TafiRuafCargaRetiros();
            String[] campos = lineas.get(i).split(",", -1);

            r.setLineaCompleta(lineas.get(i));

            String dsTipoArchivo = campos[0];
            r.setDstipo_archivo(dsTipoArchivo.trim());

            String cdAdministradora = campos[1];
            r.setCdadministradora(cdAdministradora.trim());

            String dsTipoIdAfiliado = campos[2];
            r.setDstipo_id_afiliado(dsTipoIdAfiliado.trim());

            String dsNumeroIdAfiliado = campos[3];
            r.setDsnumero_id_afiliado(dsNumeroIdAfiliado.trim());

            String dsPrimerApellido = campos[4];
            r.setDsprimer_apellido(dsPrimerApellido.trim());

            String dsSegundoApellido = campos[5];
            r.setDssegundo_apellido(!dsSegundoApellido.trim().isEmpty() ? dsSegundoApellido.trim() : null);

            String dsPrimerNombre = campos[6];
            r.setDsprimer_nombre(dsPrimerNombre.trim());

            String dsSegundoNombre = campos[7];
            r.setDssegundo_nombre(!dsSegundoNombre.trim().isEmpty() ? dsSegundoNombre.trim() : null);

            String dsNovedad = campos[8];
            r.setDsnovedad(dsNovedad.trim());

            String dsTipoIdAportante = campos[9];
            r.setDstipo_id_aportante(dsTipoIdAportante.trim());

            String dsNumeroIdAportante = campos[10];
            r.setDsnumero_id_aportante(dsNumeroIdAportante.trim());

            String dsDigitoVerAportante = campos[11];
            r.setDsdigito_ver_aportante(!dsDigitoVerAportante.trim().isEmpty() ? dsDigitoVerAportante.trim() : null);

            String fedesvinculacion = campos[12];
            r.setFedesvinculacion(!fedesvinculacion.trim().isEmpty() ? fedesvinculacion.trim() : null);

            String feretiro = campos[13];
            r.setFeretiro(!feretiro.trim().isEmpty() ? feretiro.trim() : null);

            String cdCausaRetiro = campos[14];
            r.setCdcausa_retiro(!cdCausaRetiro.trim().isEmpty() ? cdCausaRetiro.trim() : null);

            String fereconocimiento = campos[15];
            r.setFereconocimiento(!fereconocimiento.trim().isEmpty() ? fereconocimiento.trim() : null);

            String fefallecimiento = campos[16];
            r.setFefallecimiento(!fefallecimiento.trim().isEmpty() ? fefallecimiento.trim() : null);

            String campo1 = campos[17];
            r.setCampo1(!campo1.trim().isEmpty() ? campo1.trim() : null);

            String campo2 = campos[18];
            r.setCampo2(!campo2.trim().isEmpty() ? campo2.trim() : null);

            String campo3 = campos[19];
            r.setCampo3(!campo3.trim().isEmpty() ? campo3.trim() : null);

            String campo4 = campos[20];
            r.setCampo4(!campo4.trim().isEmpty() ? campo4.trim() : null);

            String campo5 = campos[21];
            r.setCampo5(!campo5.trim().isEmpty() ? campo5.trim() : null);

            String campo6 = campos[22];
            r.setCampo6(!campo6.trim().isEmpty() ? campo6.trim() : null);

            String campo7 = campos[23];
            r.setCampo7(!campo7.trim().isEmpty() ? campo7.trim() : null);

            retiros.add(r);
        }
        LOG.info("Fin armado lista TafiRuafCargaRetiros " + (new Date()).toString());

        return retiros;
    }

    private void insertarTafiRuafCargaMaestro(TafiRuafCargaMaestro maestro) {
        try {
            Connection connection = hikariDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement("INSERT INTO TAFI_RUAF_CARGA_MAESTRO "
                    + "(DSTIPO_ARCHIVO, DSTIPO_ID_AFILIADO, DSNUMERO_ID_AFILIADO, DSGENERO, FENACIMIENTO, "
                    + "DSPRIMER_APELLIDO, DSSEGUNDO_APELLIDO, DSPRIMER_NOMBRE, DSSEGUNDO_NOMBRE, FEAFILIACION, "
                    + "CDADMINISTRADORA, CDTIPO_COTIZANTE, CDACTIVIDAD_ECONOMICA, DSTIPO_ID_APORTANTE, "
                    + "DSNUMERO_ID_APORTANTE, DSDIGITO_VER_APORTANTE, DSRAZON_SOCIAL_APORTANTE, CDCLASE_APORTANTE, "
                    + "CDOCUPACION_AFILIADO, CDDEPARTAMENTO, CDMUNICIPIO, CDALDIA, CDSUBTIPO_COTIZANTE, CDMODALIDAD) "
                    + "VALUES " + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

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
            LOG.error("No se ha podido insertar el registro" + maestro.getLineaCompleta());
            LOG.error("Error insertando registro", e);
        }
        // System.out.println(Thread.currentThread().getName() + " : " +
        // i.toString());
    }

    private void insertarTafiRuafCargaRetiros(TafiRuafCargaRetiros retiro) {
        try {
            Connection connection = hikariDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement("INSERT INTO TAFI_RUAF_CARGA_RETIROS "
                    + "(DSTIPO_ARCHIVO, CDADMINISTRADORA, DSTIPO_ID_AFILIADO, DSNUMERO_ID_AFILIADO, "
                    + "DSPRIMER_APELLIDO, DSSEGUNDO_APELLIDO, DSPRIMER_NOMBRE, DSSEGUNDO_NOMBRE, DSNOVEDAD, "
                    + "DSTIPO_ID_APORTANTE, DSNUMERO_ID_APORTANTE, DSDIGITO_VER_APORTANTE, FEDESVINCULACION, "
                    + "FERETIRO, CDCAUSA_RETIRO, FERECONOCIMIENTO, FEFALLECIMIENTO, CAMPO1, CAMPO2, CAMPO3, "
                    + "CAMPO4, CAMPO5, CAMPO6, CAMPO7) " + "VALUES "
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
            LOG.error("No se ha podido insertar el registro" + retiro.getLineaCompleta());
            LOG.error("Error insertando registro", e);
        }
        // System.out.println(Thread.currentThread().getName() + " : " +
        // i.toString());
    }
}
