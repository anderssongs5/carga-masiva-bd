package com.arlsura.cargamasivabd;

import com.arlsura.cargamasivabd.cargamasiva.CargaMasiva;

public class Aplicacion {

    public static void main(String args[]) {
        CargaMasiva cargaMasiva = new CargaMasiva();

        cargaMasiva.cargarArchivoABd();
    }
}
