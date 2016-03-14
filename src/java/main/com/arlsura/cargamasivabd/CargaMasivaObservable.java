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

    public CargaMasivaObservable() {
        super();
    }

    public void cargar(int opcionProcesa, String rutaArchivo) throws IOException {
        LOG.info("Inicio! - " + (new Date()).toString());
        lineas = util.leerArchivo(new File(rutaArchivo));

        switch (opcionProcesa) {
        case TAFI_RUAF_CARGA_MAESTRO:
            LOG.info("Procesando líneas para TAFI_RUAF_CARGA_MAESTRO");
            lineasInsertarTafiRuafCargaMaestro = this.obtenerListaTafiRuafCargaMaestro();
            cantidadProcesar = lineasInsertarTafiRuafCargaMaestro.size();
            this.procesarMaestro();
            break;
        case TAFI_RUAF_CARGA_RETIROS:
            LOG.info("Procesando líneas para TAFI_RUAF_CARGA_RETIROS");
            break;
        default:
            LOG.info("No existe nada para procesar");
            break;
        }
    }

    private void procesarMaestro() {
        hikariDataSource = AplicacionBDITConfig.dataSource();

        int p = this.procesarMaestro(lineasInsertarTafiRuafCargaMaestro, empezar);
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

    private int procesarMaestro(List<TafiRuafCargaMaestro> maestro, int desde) {
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
        List<TafiRuafCargaMaestro> maestro = new ArrayList<>();
        for (int i = 0; i < lineas.size(); i++) {
            TafiRuafCargaMaestro m = new TafiRuafCargaMaestro();
            String[] campos = lineas.get(i).split(",", -1);

            String dsTipoArchivo = campos[0];
            m.setDstipo_archivo(dsTipoArchivo.trim());

            String dsTipoIdAfiliado = campos[1];
            m.setDstipo_id_afiliado(dsTipoIdAfiliado.trim());

            String dsNumeroIdAfiliado = campos[2];
            m.setDsnumero_id_afiliado(dsNumeroIdAfiliado.trim());

            String dsGenero = campos[3];
            m.setDsgenero(dsGenero.trim());

            String fenacimiento = campos[4];
            m.setFenacimiento(!fenacimiento.isEmpty() ? fenacimiento.trim() : null);

            String dsPrimerApellido = campos[5];
            m.setDsprimer_apellido(dsPrimerApellido.trim());

            String dsSegundoApellido = campos[6];
            m.setDssegundo_apellido(!dsSegundoApellido.isEmpty() ? dsSegundoApellido : null);

            String dsPrimerNombre = campos[7];
            m.setDsprimer_nombre(dsPrimerNombre.trim());

            String dsSegundoNombre = campos[8];
            m.setDssegundo_nombre(!dsSegundoNombre.isEmpty() ? dsSegundoApellido.trim() : null);

            String feafiliacion = campos[9];
            m.setFeafiliacion(feafiliacion.trim());

            String cdAdministradora = campos[10];
            m.setCdadministradora(cdAdministradora.trim());

            String cdTipoCotizante = campos[11];
            m.setCdtipo_cotizante(!cdTipoCotizante.isEmpty() ? cdTipoCotizante.trim() : null);

            String cdActividadEconomica = campos[12];
            m.setCdactividad_economica(!cdActividadEconomica.isEmpty() ? cdActividadEconomica.trim() : null);

            String dsTipoIdAportante = campos[13];
            m.setDstipo_id_aportante(!dsTipoIdAportante.isEmpty() ? dsTipoIdAportante.trim() : null);

            String dsNumeroIdAportante = campos[14];
            m.setDsnumero_id_aportante(!dsNumeroIdAportante.isEmpty() ? dsNumeroIdAportante.trim() : null);

            String dsDigitoVerAportante = campos[15];
            m.setDsdigito_ver_aportante(!dsDigitoVerAportante.isEmpty() ? dsDigitoVerAportante.trim() : null);

            String dsRazonSocialAportante = campos[16];
            m.setDsrazon_social_aportante(!dsRazonSocialAportante.isEmpty() ? dsRazonSocialAportante.trim() : null);

            String cdClaseAportante = campos[17];
            m.setCdclase_aportante(!cdClaseAportante.isEmpty() ? cdClaseAportante.trim() : null);

            String cdOcupacionAfiliado = campos[18];
            m.setCdocupacion_afiliado(!cdOcupacionAfiliado.isEmpty() ? cdOcupacionAfiliado.trim() : null);

            String cdDepartamento = campos[19];
            m.setCddepartamento(cdDepartamento);

            String cdMunicipio = campos[20];
            m.setCdmunicipio(cdMunicipio);

            String cdAldia = campos[21];
            m.setCdaldia(cdAldia);

            String cdSubtipoCotizante = campos[22];
            m.setCdsubtipo_cotizante(!cdSubtipoCotizante.isEmpty() ? cdSubtipoCotizante.trim() : null);

            String cdModalidad = campos[23];
            m.setCdmodalidad(!cdModalidad.isEmpty() ? cdModalidad.trim() : null);

            maestro.add(m);
        }

        return maestro;
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
            LOG.error("Error insertando registro", e);
        }
        // System.out.println(Thread.currentThread().getName() + " : " +
        // i.toString());
    }
}
