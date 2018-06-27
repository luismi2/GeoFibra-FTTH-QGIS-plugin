# -*- coding: utf-8 -*-
"""
/***************************************************************************
 GeoFibraDockWidget
                                 A QGIS plugin
 Asistente GIS para el diseño de Redes FTTH
                             -------------------
        begin                : 2017-12-28
        git sha              : $Format:%H$
        copyright            : (C) 2017 by Luis Miguel Royo Perez
        email                : admin@inisig.com / luis.miguel.royo@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import os

from PyQt4 import QtGui, uic
from PyQt4.QtCore import pyqtSignal

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'geofibra_dockwidget_base.ui'))


class GeoFibraDockWidget(QtGui.QDockWidget, FORM_CLASS):

    closingPlugin = pyqtSignal()
    creaNPsignal = pyqtSignal(str,str,str,str)
    signalArchivoCAT = pyqtSignal(str,str)
    sennalArchivoCatastro = pyqtSignal(str,str)
    sennalCartociudad = pyqtSignal(str, str,str)
    sennalAnalisisUUII = pyqtSignal(bool,bool,bool,bool,bool,bool,bool,bool,bool,bool,bool,bool,bool,bool,bool,bool, int, str)
    sennalCluster = pyqtSignal(int,str)
    sennalSp1n = pyqtSignal(str, str,str)
    sennalCTO = pyqtSignal(int, int, int, int, int, str, str, str, str)
    sennalCTOCerrado = pyqtSignal(int, str, str, str, str)
    sennalCableDist = pyqtSignal( bool, bool, bool,bool, bool, int, str, str)
    sennalError = pyqtSignal(str)
    sennalError2 = pyqtSignal(str)
    sennalRevLin = pyqtSignal(str)
    sennalCableTroncal = pyqtSignal( bool, bool, bool,bool, bool, int, str, str)
    sennalModeloCable = pyqtSignal(str,str,str,str,str,str)
    sennalDistancias= pyqtSignal(bool,bool,str)
    sennalExporta= pyqtSignal(str)
    sennalImporta= pyqtSignal(str)    
    sennalCto_plus = pyqtSignal(int, int,str,str,str,str)
    sennalCto_rem= pyqtSignal(str)
    def __init__(self, parent=None):
        """Constructor."""
        super(GeoFibraDockWidget, self).__init__(parent)
        # Set up the user interface from Designer.
        # After setupUI you can access any designer object by doing
        # self.<objectname>, and you can use autoconnect slots - see
        # http://qt-project.org/doc/qt-4.8/designer-using-a-ui-file.html
        # #widgets-and-dialogs-with-auto-connect
        self.setupUi(self)
        self.pushButtonCreaProyecto.pressed.connect(self.creaProyecto)
        self.botonImportarCat.pressed.connect(self.ImportaCAT)
        self.botonImportarCapas.pressed.connect(self.ImportaCatastro)
        self.pushButton_cartociudad.pressed.connect(self.ImportaCartoCiudad)
        self.boton_uuii.pressed.connect(self.analizaUUII)
        self.botonAsignaCluster.pressed.connect(self.enviaCluster)
        self.botonCreaSplitters.pressed.connect(self.creaSp1n)
        self.crea_ctos.pressed.connect(self.creaCTO)
        self.crea_ctos_cerrado.pressed.connect(self.creaCTO2)
        self.pushButton_rd.pressed.connect(self.rd)
        self.pushButton_errorAd.pressed.connect(self.errorMas)
        self.pushButton_errorAt.pressed.connect(self.errorMenos)
        self.pushButton_RevLin.pressed.connect(self.revLin)
        self.pushButton_tr.pressed.connect(self.rt)
        self.pushButton_annadeModelo.pressed.connect(self.addModel)
        self.pushButtonDistancias.pressed.connect(self.calculaDistancias)
        self.exportar.pressed.connect(self.exporta_dpkg)
        self.importar.pressed.connect(self.importa_dpkg)        
        self.add_ont.pressed.connect(self.ctoP)
        self.remove_ont.pressed.connect(self.cto_remove)


    def closeEvent(self, event):
        self.closingPlugin.emit()
        event.accept()

    def creaProyecto(self):
        host = str(self.lineEditHost.text())
        nombreBBDD = str(self.lineEditNbbdd.text())
        usuario = str(self.lineEditUsuario.text())
        password = str(self.lineEditPassword.text())
        self.creaNPsignal.emit( nombreBBDD, usuario, host, password)

    def ImportaCAT(self):
        rutaArchivoCAT = unicode(str(self.archivoCAT.filePath()))
        conexion=str(self.comboBox.currentText())
        self.signalArchivoCAT.emit(rutaArchivoCAT, conexion)

    def ImportaCatastro(self):
        rutaArchivo = str(self.archivoCatastro.filePath())
        conexion=str(self.comboBox.currentText())
        self.sennalArchivoCatastro.emit(rutaArchivo, conexion)
    
    def ImportaCartoCiudad(self):
        conexion=str(self.comboBox.currentText())
        municipios = str(self.mMapLayerComboBox.currentText())
        cartociudad = str(self.mQgsFileWidget.filePath())
        self.sennalCartociudad.emit(conexion,municipios,cartociudad)
    
    def analizaUUII(self):
        almacen = self.checkBox_Almacen.checkState()
        residencial = self.checkBox_Residencial.checkState()
        industrial = self.checkBox_Industrial.checkState()
        oficinas = self.checkBox_Oficinas.checkState()
        comercial = self.checkBox_Comercial.checkState()
        deportivo = self.checkBox_Deportivo.checkState()
        espectaculos = self.checkBox_Espectaculos.checkState()
        ocio_hosteleria = self.checkBox_Ocio_Hosteleria.checkState()
        sanidad_beneficiencia = self.checkBox_Sanidad_Beneficiencia.checkState()
        cultural = self.checkBox_Cultural.checkState()
        religioso = self.checkBox_Religioso.checkState()
        suelo_sin_edificar = self.checkBox_Suelos_sin.checkState()
        edificio_singular = self.checkBox_Edificio_Singular.checkState()
        almacen_agrario = self.checkBox_Almacen_agrario.checkState()
        industrial_agrario = self.checkBox_Industrial_Agrario.checkState()
        agrario = self.checkBox_Agrario.checkState()
        numeroUUII = self.spinBoxUUII.value()
        conexion=str(self.comboBox.currentText())
        self.sennalAnalisisUUII.emit(almacen,residencial,industrial,oficinas,comercial,deportivo,espectaculos,ocio_hosteleria, sanidad_beneficiencia,cultural,religioso,suelo_sin_edificar,edificio_singular,almacen_agrario, industrial_agrario, agrario, numeroUUII,conexion)

    def enviaCluster(self):
        cluster = self.numeroCluster.value()
        conexion=str(self.comboBox.currentText())
        self.sennalCluster.emit(cluster,conexion)

    def creaSp1n(self):
        conexion=str(self.comboBox.currentText())
        acron_pob = self.lineEdit.text()
        ratioSpliteo = self.lineEdit_rat_split_tr.text()
        self.sennalSp1n.emit(conexion,acron_pob, ratioSpliteo)

    def creaCTO(self):
        tasaPen = self.spinBox_tasa_pen.value()
        ratioSp = self.spinBox_Ratio_Spliteo.value()
        spCTO = self.spinBox_splitters_cto.value()
        num_min_man = self.spinBox_num_min_uuii_man.value()
        num_min_ict = self.spinBoxUUII.value()
        caja_ext = self.lineEdit_caja_ext.text()
        caja_int = self.lineEdit_caja_int.text()
        acron_pob = self.lineEdit.text()
        conexion = str(self.comboBox.currentText())
        self.sennalCTO.emit(tasaPen,ratioSp, spCTO,num_min_man,num_min_ict,caja_ext,caja_int,acron_pob,conexion)

    def creaCTO2(self):
        tasaPen = self.spinBox_tasa_pen.value()
        ratioSp = str(self.spinBox_Ratio_Spliteo.value())
        spCTO = str(self.spinBox_splitters_cto.value())        
        acron_pob = self.lineEdit.text()
        conexion = str(self.comboBox.currentText())
        self.sennalCTOCerrado.emit(tasaPen, ratioSp, spCTO, acron_pob, conexion)

    def rd(self):
        
        checkSD = self.checkBox_sinDer.checkState()        
        checkCartaEmp = self.checkBox_cartaEm.checkState()
        checkEtiq = self.checkBox_etiq.checkState()
        checkNombrado = self.checkBox_nombrado.checkState()
        checkConteo = self.checkBox_conteo.checkState()
        
        conexion = self.comboBox.currentText()
        acron_pob = self.lineEdit.text()
        porceRes = self.spinBoxFibRes.value()
        self.sennalCableDist.emit(checkNombrado, checkConteo, checkSD,checkCartaEmp,checkEtiq, porceRes, acron_pob,conexion)

    def errorMas(self):
        valorEtiqueta = self.label_idError.text()
        self.sennalError.emit(valorEtiqueta)
    
    def errorMenos(self):
        valorEtiqueta = self.label_idError.text()
        self.sennalError2.emit(valorEtiqueta)

    def revLin(self):
        conexion = str(self.comboBox.currentText())
        self.sennalRevLin.emit(conexion)

    def rt(self):
        
        checkSD = self.checkBox_sinDer_tr.checkState()        
        checkCartaEmp = self.checkBox_cartaEm_tr.checkState()
        checkEtiq = self.checkBox_etiq_tr.checkState()
        checkNombrado = self.checkBox_nomb_tr.checkState()
        checkConteo = self.checkBox_conteo_tr.checkState()
        
        conexion = self.comboBox.currentText()
        acron_pob = self.lineEdit.text()
        porceRes = self.spinBoxFibRes_tr.value()
        
        self.sennalCableTroncal.emit(checkNombrado, checkConteo, checkSD,checkCartaEmp,checkEtiq, porceRes, acron_pob,conexion)

    def addModel(self):
        marca = self.lineEdit_marca.text()
        modelo = self.lineEdit_modelo.text()
        fibrasTot = self.lineEdit_fibTot.text()
        tubos = self.lineEdit_tubos.text()
        fibTubos = self.lineEdit_fibTubos.text()
        conexion = str(self.comboBox.currentText())
        self.sennalModeloCable.emit(marca, modelo, fibrasTot, tubos, fibTubos, conexion)

    def calculaDistancias(self):
        checkTr = self.checkBox_troncal.checkState()
        checkDs = self.checkBox_distribucion.checkState()        
        conexion = self.comboBox.currentText()
        self.sennalDistancias.emit(checkTr, checkDs, conexion)

    def exporta_dpkg(self):               
        conexion = self.comboBox.currentText()
        self.sennalExporta.emit(conexion)

    def importa_dpkg(self):               
        conexion = self.comboBox.currentText()
        self.sennalImporta.emit(conexion)

    def ctoP(self):
        conexion = self.comboBox.currentText()               
        ratioSp = self.spinBox_Ratio_Spliteo.value()
        spCTO = self.spinBox_splitters_cto.value()
        caja_ext = self.lineEdit_caja_ext.text()
        caja_int = self.lineEdit_caja_int.text()
        acron_pob = self.lineEdit.text()
        self.sennalCto_plus.emit(ratioSp,spCTO,caja_ext,caja_int,conexion,acron_pob)

    def cto_remove(self):
        conexion = self.comboBox.currentText()
        self.sennalCto_rem.emit(conexion)
