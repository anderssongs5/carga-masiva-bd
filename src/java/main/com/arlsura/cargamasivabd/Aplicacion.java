package com.arlsura.cargamasivabd;

import java.io.IOException;

public class Aplicacion {

    @SuppressWarnings("rawtypes")
    public static void main(String args[]) throws IOException {
        String user = args[0];
        String pass = args[1];
        String server = args[2];
        String port = args[3];
        String instance = args[4];
        
        CargaMasivaObservable cargaMasivaObservable = new CargaMasivaObservable(user, pass, 
                server, port, instance);

        cargaMasivaObservable.cargar(3, "D:/RUAF/AfiliadosRUAF.txt");
    }
}
