package com.arlsura.cargamasivabd;

import java.io.IOException;

public class Aplicacion {

    @SuppressWarnings("rawtypes")
    public static void main(String args[]) throws IOException {
        CargaMasivaObservable cargaMasivaObservable = new CargaMasivaObservable();

        // RUA250RMRP20160121NI000800256161CO014-28.TXT
        // CargaMaestro.txt
        // cargaMasivaObservable.cargar(1,
        // "C:/Users/Andersson/Desktop/Temporales/RUA250RMRP20160121NI000800256161CO014-28.TXT");

        // RUA250RNRA20160121NI000800256161CO014-28.TXT
        // CargaRetiros.txt
        cargaMasivaObservable.cargar(2, "C:/Users/Andersson/Desktop/Temporales/RUA250RNRA20160121NI000800256161CO014-28.TXT");
    }
}
