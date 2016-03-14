package com.arlsura.cargamasivabd;

import java.io.IOException;

public class Aplicacion {

    @SuppressWarnings("rawtypes")
    private static CargaMasivaObservable cargaMasivaObservable;

    @SuppressWarnings("rawtypes")
    public static void main(String args[]) throws IOException {
        cargaMasivaObservable = new CargaMasivaObservable();

        // RUA250RMRP20160121NI000800256161CO014-28.TXT
        // CargaMaestro.txt
        cargaMasivaObservable.cargar(1,
                "C:/Users/Andersson/Desktop/Temporales/RUA250RMRP20160121NI000800256161CO014-28.TXT");
    }
}
