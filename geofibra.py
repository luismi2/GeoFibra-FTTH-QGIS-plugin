# -*- coding: utf-8 -*-
"""
/***************************************************************************
 GeoFibra
                                 A QGIS plugin
 Asistente de Diseño de Redes FTTH.
                              -------------------
        begin                : 2017-12-27
        git sha              : $Format:%H$
        copyright            : (C) 2017 by Luis Miguel Royo Perez
        email                : admin@geofibra.com; luis.miguel.royo@gmail.com
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
from PyQt4.QtCore import QSettings, QTranslator, qVersion, QCoreApplication, Qt
from PyQt4.QtGui import QAction, QIcon
# Initialize Qt resources from file resources.py
import resources

# Import the code for the DockWidget
from geofibra_dockwidget import GeoFibraDockWidget
import os.path

#Mis import
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from os.path import expanduser
import qgis.core
from qgis.core import *
from PyQt4.QtSql import *
from qgis.utils import *
from PyQt4.QtGui import *
from qgis.gui import *
import io
import platform
import ntpath
import os
import processing

#Variables Globales
home = expanduser("~")
host =''
nombreBBDD = ''
usuario = ''
password= ''

canvas = iface.mapCanvas()
nombreRed = ''
nombreCapaCableado = ''


#Mis funciones
#Funcion Actualiza Conexiones
def actConex(self):
    qs = QSettings()
    listaConexiones=[]
    for k in sorted(qs.allKeys()):
        parts = k.split("/") # Will raise exception if too many options
        parts += [None] * (5 - len(parts)) # Assume we can have max. 5 items. Fill in missing entries with None.
        value1, value2, value3, value4, value5 = parts
        if value1 == 'PostgreSQL'and value4!=None:
            listaConexiones.append(value3)
    conUn = sorted(set(listaConexiones))
    self.dockwidget.comboBox.clear()
    for c in conUn:
        self.dockwidget.comboBox.addItem(c)
#Funcion que carga ShapeFiles
def cargaShape(rutaShape,nombreCapa,rutaEstilo):
    root = QgsProject.instance().layerTreeRoot()
    layerShp = QgsVectorLayer(rutaShape, nombreCapa, "ogr")
    mygroup = root.findGroup("BASE")
    QgsMapLayerRegistry.instance().addMapLayer(layerShp, False)
    layerShp.loadNamedStyle(rutaEstilo)
    mygroup.addLayer(layerShp)
    canvas = iface.mapCanvas()
    extent = layerShp.extent()
    canvas.setExtent(extent)
    iface.mapCanvas().refresh()

#Funcion que carga capas WMS
def cargawms(urlWithParams,nombreCapaWMS):
    root = QgsProject.instance().layerTreeRoot()
    rlayer = QgsRasterLayer(urlWithParams, nombreCapaWMS, 'wms')
    mygroup = root.findGroup("BASE")
    QgsMapLayerRegistry.instance().addMapLayer(rlayer, False)
    mygroup.addLayer(rlayer)

#Funcion que carga capas PostGIS
def capas(nombreGrupo,host, nomBBDD,usuario, passw, nombreTabla, nombreTOC, rutaQML):
    root = QgsProject.instance().layerTreeRoot()
    canvas = qgis.utils.iface.mapCanvas()
    grupoTroncal = root.findGroup(nombreGrupo )
    sql=''
    uri = QgsDataSourceURI()
    uri.setConnection(host, '5432', nomBBDD, usuario, passw)
    uri.setDataSource("public",nombreTabla, "geom",sql)
    vlayer = QgsVectorLayer(uri.uri(), nombreTOC, "postgres")
    if not vlayer.isValid():
        print "not valid"
    for lyr in QgsMapLayerRegistry.instance().mapLayers().values():
        if lyr.name() == nombreTOC:
            QgsMapLayerRegistry.instance().removeMapLayer(lyr.id())
    QgsMapLayerRegistry.instance().addMapLayer(vlayer, False)
    grupoTroncal.addLayer(vlayer)
    vlayer.loadNamedStyle(rutaQML)
    vlayer.triggerRepaint()
    extent = vlayer.extent()
    canvas.setExtent(extent)
    canvas.refresh()

#Funcion que  carga capas No espaciales. Tablas simples.
def capasNSP(nombreGrupo,host, nomBBDD,usuario, passw, nombreTabla, nombreTOC):
    root = QgsProject.instance().layerTreeRoot()
    canvas = qgis.utils.iface.mapCanvas()
    grupoTroncal = root.findGroup(nombreGrupo )
    sql=''
    uri = QgsDataSourceURI()
    uri.setConnection(host, '5432', nomBBDD, usuario, passw)
    uri.setDataSource("public",nombreTabla, None,sql)
    vlayer = QgsVectorLayer(uri.uri(), nombreTOC, "postgres")
    if not vlayer.isValid():
        print "not valid"
    for lyr in QgsMapLayerRegistry.instance().mapLayers().values():
        if lyr.name() == nombreTOC:
            QgsMapLayerRegistry.instance().removeMapLayer(lyr.id())
    QgsMapLayerRegistry.instance().addMapLayer(vlayer, False)
    grupoTroncal.addLayer(vlayer)
    canvas.refresh()

#Funcion para quitar gurpo
def removeGroup(name):
    root = QgsProject.instance().layerTreeRoot()
    group = root.findGroup(name)
    if not group is None:
        for child in group.children():
            dump = child.dump()
            id = dump.split("=")[-1].strip()
            QgsMapLayerRegistry.instance().removeMapLayer(id)
        root.removeChildNode(group)
#Funcion para obtener Credenciales
def credenciales(nombreConexion):
    qs = QSettings()
    global usuario
    usuario= qs.value("PostgreSQL/connections/"+nombreConexion+"/username")
    global password
    password= qs.value("PostgreSQL/connections/"+nombreConexion+"/password")
    global nombreBBDD
    nombreBBDD= qs.value("PostgreSQL/connections/"+nombreConexion+"/database")
    global host
    host= qs.value("PostgreSQL/connections/"+nombreConexion+"/host")

#Funcion que ejecuta las consultas SQL
def ejecutaSQL(baseDatosNombre, host, usuario,password,sql):
    conn = psycopg2.connect("dbname="+baseDatosNombre+" user="+usuario+" host="+host+" password="+password)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

#Funcion para tomar el nombre de la capa Shape
def tomaNombre(capaCarto):
    osyst= platform.system()
    if osyst=='Windows':
        head,tail = ntpath.split(capaCarto)
        corte2=tail.rsplit('.',1)[0]
        global nombrePostGIS
        nombrePostGIS = corte2
    elif osyst=='Linux':
        corte1=capaCarto.rsplit("/",1)[1]
        corte2=corte1.rsplit('.',1)[0]
        nombrePostGIS = corte2
#Funcion para relacionar tablas con alguna Capa
def relaciones(nombreCapaTOC1_noSP, nombreCapaTOC2_SP, campoCapaTOC1, campoCapaTOC2, idRelacion,nombreRelacion):
    indiceSp=''
    indiceCTO=''
    for f in QgsMapLayerRegistry.instance().mapLayers().values():
        if f.name() == nombreCapaTOC1_noSP:
            indiceSp=str(f.id())
        elif f.name()== nombreCapaTOC2_SP:
            indiceCTO=str(f.id())
    rel = QgsRelation()
    rel.setReferencingLayer( indiceSp )
    rel.setReferencedLayer( indiceCTO )
    rel.addFieldPair( campoCapaTOC1, campoCapaTOC2 )
    rel.setRelationId( idRelacion )
    rel.setRelationName( nombreRelacion )
    rel.isValid() # It will only be added if it is valid. If not, check the ids and field names
    QgsProject.instance().relationManager().addRelation( rel )

#Funcion que crea los splitters de la RD
def creSplittersDist(campoNombre,campoSplitters, campoRatioSpliteo):
    uri = QgsDataSourceURI()
    uri.setConnection(host, "5432",nombreBBDD, usuario, password)    
    uri.setDataSource ("public", 'cto', "geom")   
    layer=QgsVectorLayer (uri .uri() ,'Splitters',"postgres")
    for feature in layer.getFeatures():
        idx = layer.fieldNameIndex(campoNombre)
        idSp = layer.fieldNameIndex(campoSplitters)
        idRatSp = layer.fieldNameIndex(campoRatioSpliteo)
        for counter1 in range (1,int(feature.attributes()[idSp]+1.0)):
            for counter in range (1,int(feature.attributes()[idRatSp]+1.0)):
                sql= """insert into splitters(nombre_cto, n_splitter,pat_splitter) values ('%s','%s','%s');"""% (feature.attributes()[idx],str(counter1),str(counter))
                ejecutaSQL(nombreBBDD,host, usuario,password,sql)

#Funcion que nombra los cables primarios
def nombresPrimarios(nombre_base_de_datos, host,  usuario,password, cableado, selectNombre, origenCables, letraNombre):
    sqlNull="""UPDATE
                    """+cableado+"""
                SET
                    rank=NULL,
                    nombre=NULL"""
    ejecutaSQL(nombre_base_de_datos, host,  usuario,password,sqlNull)
    sqlPrim=""" UPDATE
                    """+cableado+""" cbl
                SET
                    rank=a.rank,
                    nombre=a.nombre
                FROM
                    (select
                        1 as rank,
                        """+selectNombre+"""
                        t.id
                    from
                        """+cableado+""" t,
                        """+origenCables+""" c
                    where
                        st_dwithin(c.geom, t.geom,0.1)
                    ) a
                WHERE
                    a.id=cbl.id;"""

    ejecutaSQL(nombre_base_de_datos, host,  usuario,password,sqlPrim)

#Funcion que nombra los calbes Secundarios o derivados de los primarios
def nombresSecundarios(nombre_base_de_datos, host,  usuario,password, layerQgisVector, cableado ):
    nulo = 0
    features = layerQgisVector.getFeatures()
    for feature in features:
        if feature["rank"] == qgis.core.NULL:
            nulo=nulo+1
    rank=1
    while nulo!=0:
        sqlUp="UPDATE "+cableado+" SET rank=a.rank , nombre=a.nombre FROM (select distinct a.id , "+str(rank+1)+" as rank, (b.nombre||'-0'||ROW_NUMBER () OVER (partition by b.nombre order by a.id )) nombre   from "+cableado+" a, (select * from "+cableado+" where "+cableado+".rank="+str(rank)+") b where st_dwithin(a.geom, st_endpoint(b.geom),2) and a.id!= b.id ) a WHERE a.id="+cableado+".id;"
        ejecutaSQL(nombre_base_de_datos, host,  usuario,password,sqlUp)
        rank=rank+1
        nulo=0
        for feature in layerQgisVector.getFeatures():
            if feature["rank"] == qgis.core.NULL:
                nulo=nulo+1
#Funcion que actualiza las cajas de empalme. Campo nombre.
def cajas_empalme(nomCapaCe, nombreTablaCables, acron_pob):
    sqlRankCE="""UPDATE """+nomCapaCe+"""
    SET
        rank=a.rank,
        nombre=a.nombre
    FROM (
        select
            """+nomCapaCe+""".id,
            """+nombreTablaCables+""".rank rank,
            'CD-"""+acron_pob+"""-'||"""+nomCapaCe+""".id nombre
        from
            """+nombreTablaCables+""",
            """+nomCapaCe+"""
        where
            st_within(st_endpoint("""+nombreTablaCables+""".geom), """+nomCapaCe+""".geom)
        group by
            """+nomCapaCe+""".id,
            """+nombreTablaCables+""".rank
        order by
            """+nomCapaCe+""".id,
            """+nomCapaCe+""".rank
        ) a
    where
        """+nomCapaCe+""".id=a.id """
    ejecutaSQL(nombreBBDD, host,  usuario,password,sqlRankCE)

#Funcion que realiza el conteo de splitters por cable.
def conteoCables(nombre_base_de_datos,host,usuario,password, cableado, nombreCableadoTOC, cifraIter, cajas, porc_res):
    layer = QgsMapLayerRegistry.instance().mapLayersByName( nombreCableadoTOC )[0]
    # #Averiguar iteraciones
    listaLongitudes= []#Lista para introducir los valores de longitudes de campo
    sqlComp="""create table fcl_"""+cableado+"""_acumulado as """#sql a completar para las fibras claras
    expression = QgsExpression("length(nombre)")#Expresion para calcular las longitudes de campo
    expression.prepare(layer.pendingFields())
    for feature in layer.getFeatures():#Bucle para tomar las longitudes y ponerlas en la lista
        value = expression.evaluate(feature)
        listaLongitudes.append(int(value))
    maxVal= max(listaLongitudes)#maximo valor de la lista
    iterac=(int(maxVal)-cifraIter)/3#Veces que hay que iterar para el calculo de fibras claras || cifraIter es 9 para troncales, 13 para distribucion

    ##Resetear campos
    listaColumnas=['fib_cl', 'fib_res', 'fib_total', 'marca','modelo','fib_totales_cable', 'tubos','fib_tubos' ]
    for column in listaColumnas:
        sqlNull="""UPDATE
                        """+cableado+"""
                    SET
                        """+column+"""=NULL;"""
        ejecutaSQL(nombre_base_de_datos, host,  usuario,password,sqlNull)

    for i in range (0,iterac+1):#iteracion y composicion de la sentencia sql de las fibras claras
        sql= """
        select distinct
            sum(ft.fib_cl)::numeric fib_cl,
            substring(ft.nombre,1,"""+str(i*3+cifraIter)+""") nombre_cable
        from
            fcl_"""+cableado+""" ft
        where
            rank>="""+str(i+1)+"""
        group by
            substring(ft.nombre,1,"""+str(i*3+cifraIter)+""")
        union """    ### CifraNombre: 9 para Troncal; 13 para distribucion
        sqlComp = sqlComp+sql

    sqlFclInd=""" create table fcl_"""+cableado+""" as
                    select
                        nombre,
                        sum(fcl_cab.splitters)::numeric fib_cl,
                        rank
                    from
                        (select distinct
                            d.nombre,
                            c.geom,
                            c.nombre peana,
                            c.splitters,
                            d.rank

                        from
                            """+cajas+""" c,
                            """+cableado+""" d
                        where
                            st_dwithin(d.geom,c.geom, 0.01) and not
                            st_equals(ST_snap(c.geom, d.geom, 0.1), st_startpoint(d.geom))
                        ) fcl_cab
                    group by
                        fcl_cab.nombre,
                        fcl_cab.rank
                    order by
                        fcl_cab.nombre;"""
    if sqlComp[-6:]=='union ': #Quitamos el union final de la sql
        sqlComp = sqlComp[:-6]
    sqlComp = sqlComp+';'    #annadimos un ;
    sqlDrop1="""drop table if exists fcl_"""+cableado+""";"""
    sqlDrop2='drop table if exists fcl_'+cableado+'_acumulado;'

    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlDrop1)
    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlFclInd)

    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlDrop2)
    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlComp)

    sqlUp1='update '+ cableado+' set fib_cl =a.fb_cl FROM (select '+ cableado+'.nombre,fcl_'+cableado+'_acumulado.fib_cl fb_cl FROM public.fcl_'+cableado+'_acumulado, public.'+ cableado+' WHERE fcl_'+cableado+'_acumulado.nombre_cable='+ cableado+'.nombre) a where '+ cableado+'.nombre=a.nombre ;'
    sqlUp2 ='update '+ cableado+' set fib_res = ceil((fib_cl*'+str(porc_res)+')::numeric/100);'
    sqlUp3 = 'update '+ cableado+' set fib_total = fib_cl+fib_res;'

    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlUp1)
    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlUp2)
    ejecutaSQL(nombre_base_de_datos,host,usuario,password,sqlUp3)

    ###Asignado Modelos Cables


    tablaModelos = QgsMapLayerRegistry.instance().mapLayersByName( 'Modelos Cables' )[0]
    nombreTablaModelos= 'modelos_cable'#Tomamos el nombre

    listFib=[]#Lista vacia de fibras totales de la tabla de modelos y marcas de cables
    for f in tablaModelos.getFeatures():
        listFib.append( f['fib_totales'])#Se meten todos los valores en la lista
    listFibOr= sorted(listFib)#Ordenamos la lista de menor a mayor
    sqlCond ='when '+cableado+'.fib_total <= '+str(listFibOr[0])+' then '+nombreTablaModelos+'.fib_totales ='+str(listFibOr[0]) #SQL de condicionales case... when...

    for i in range(0, len(listFibOr)-1):#Segun la longitud de la lista, repite la composicion
        r=i
        sqlCond=sqlCond+'when '+cableado+'.fib_total >'+str(listFibOr[i])+' and '+cableado+'.fib_total <='+str(listFibOr[r+1])+'  then '+nombreTablaModelos+'.fib_totales ='+str(listFibOr[r+1])#Meter todos los condicionales de SQL en uno mismo.
    sqlGen='select '+cableado+'.id,'+cableado+'.fib_total fibras_minimas, '+nombreTablaModelos+'.marca, '+nombreTablaModelos+'.modelo, '+nombreTablaModelos+'.fib_totales, '+nombreTablaModelos+'.tubos, '+nombreTablaModelos+'.fib_tubos from '+ cableado+','+nombreTablaModelos+' where case '+sqlCond+' end order by '+cableado+'.nombre'#Enchufar los condicionales con la consulta general


    sqlUpd= "UPDATE "+ cableado+" set marca=ba.marca, modelo=ba.modelo, fib_totales_cable=ba.fib_totales, tubos=ba.tubos,fib_tubos=ba.fib_tubos FROM ("+sqlGen+") ba where ba.id="+ cableado+".id "#SQL final junto con todo

    ejecutaSQL(nombre_base_de_datos,host, usuario,password,sqlUpd)#Ejecucion final de la consulta SQL

#Funcion para la creacion de las Cartas de Empalme
def carta_empalmes(nombreBBDD, host, usuario,password,nombreCapaTOC, cableado, nombreTablaAux, cajasSplitters, campoDerivador, AbreviaturaRed):
    layer = QgsMapLayerRegistry.instance().mapLayersByName( nombreCapaTOC )[0]
    sqlFib = ' DROP TABLE IF EXISTS carta_empalmes_'+cableado+'; CREATE TABLE carta_empalmes_'+cableado+' (id serial, nombre varchar(40) NOT NULL, tubo_numero integer NOT NULL, fibra integer NOT NULL, destino varchar(255),id_fibra integer, PRIMARY KEY(id));'#SQL de creacion de tabla vacia de fibras_cables
    ejecutaSQL(nombreBBDD, host,usuario,password,sqlFib)#Ejecutamos la creacion
    idx = layer.fieldNameIndex('nombre')#Indice campo Nombre
    idTb = layer.fieldNameIndex('tubos')#Indice campo Tubos
    idFtb = layer.fieldNameIndex('fib_tubos')#Indice campo Fibras por tubo
    for feature in layer.getFeatures(): #3 for encadenados que nos van a servir para crear en cada cable, los tubos y dentro de los tubos las fibras pertinentes.
        for counter1 in range (1,int(feature.attributes()[idTb]+1.0)):
            for counter in range (1,int(feature.attributes()[idFtb]+1.0)):
                sqlConst = "INSERT INTO carta_empalmes_"+cableado+" (nombre, tubo_numero, fibra, destino) VALUES ('%s', '%s', '%s','LIBRE'); "  % (str(feature.attributes()[idx]),str(counter1),str(counter) )#SQL que nos inserta los registros en la tabla previamente creada
                ejecutaSQL(nombreBBDD, host, usuario,password,sqlConst)#Ejecutamos el SQL creado.
    sqlIdfibra="""UPDATE carta_empalmes_"""+cableado+"""
    SET id_fibra=a.id_fibra
        FROM (select fo.* , ROW_NUMBER () OVER (PARTITION BY nombre order by nombre) id_fibra from

        (select carta_empalmes_"""+cableado+""".id,nombre,fibra, carta_empalmes_"""+cableado+""".tubo_numero from carta_empalmes_"""+cableado+""" order by nombre, carta_empalmes_"""+cableado+""".tubo_numero,fibra) fo) a
    where
        carta_empalmes_"""+cableado+""".id=a.id """
    ejecutaSQL(nombreBBDD, host,  usuario,password,sqlIdfibra)
    #SQL que relaciona las Cajas/Splitters con su cable.
    sqlCablesCTO = """ DROP TABLE IF EXISTS """+nombreTablaAux+""";
                        CREATE TABLE """+nombreTablaAux+""" as
                                     SELECT
                                            ROW_NUMBER () OVER () id,
                                            ff.*
                                    FROM
                                        (SELECT
                                            d.nombre nombre_cable,
                                            c.nombre nombre_cto,
                                            c.splitters
                                        FROM
                                            """+cableado+""" d,
                                            """+cajasSplitters+""" c
                                        WHERE
                                             ST_dwithin(d.geom,st_snap(c.geom,d.geom,1),0.1) and not
                                             st_dwithin(st_startpoint(d.geom), c.geom,0.1)
                                        ORDER BY
                                            nombre_cable,
                                            ST_LineLocatePoint(d.geom,c.geom)
                                        )ff;
                                            """
    ejecutaSQL(nombreBBDD, host,  usuario,password,sqlCablesCTO)#Ejecutamos la creacion
    sqlEnlacesPrimarios = """
                        UPDATE carta_empalmes_"""+cableado+"""
                        SET destino=a.destino
                            FROM (
                             select
                                bb.*,
                                ROW_NUMBER () OVER (PARTITION BY nombre order by id, split_part(destino, '.', 1),split_part(destino, '.', 2)::integer ) id_fibra
                            from
                                (select
                                    row_number() over() id,
                                    nombre_cable nombre,
                                    concat(nombre_cto,'.',generate_series) destino
                                from
                                    """+nombreTablaAux+""",
                                    generate_series(1,"""+nombreTablaAux+""".splitters::integer )
                                order by
                                    """+nombreTablaAux+""".id
                                    )bb ) a
                        where
                        carta_empalmes_"""+cableado+""".id_fibra=a.id_fibra and
                        carta_empalmes_"""+cableado+""".nombre=a.nombre;
                        """

    ejecutaSQL(nombreBBDD, host,  usuario,password,sqlEnlacesPrimarios)#Ejecutamos la creacion
    sqlUnique = """ CREATE UNIQUE INDEX fibra_unica ON carta_empalmes_"""+cableado+""" (destino) WHERE (destino !='LIBRE' and destino !='RESERVA'); """#Constraint que impedira que las fibras puedan asignarse dos veces.
    ejecutaSQL(nombreBBDD, host,  usuario,password,sqlUnique)#Ejecutamos la creacion
    rankLista=[]
    qsql="(select row_number() over() as id, * from (select distinct a.geom, a.rank from cajas_empalme_"+cableado+" a union select distinct  b.geom,c.rank from "+cajasSplitters+" b, "+cableado+" c where b."+campoDerivador+"='S' and st_dwithin(st_endpoint(c.geom),b.geom, 0.1)) cc)"
    uri = QgsDataSourceURI()
    uri.setConnection(host, '5432', nombreBBDD, usuario, password)
    uri.setDataSource("",qsql, "geom","","id")
    derivadoreslayer = QgsVectorLayer(uri.uri(), "carga_derivadores", "postgres")

    for feature in derivadoreslayer.getFeatures():
        rankLista.append(feature["rank"])
    listaRankUnique= set(rankLista)

    rankInverse=sorted(listaRankUnique, reverse=True)
    for i in rankInverse:

        sqlEnlacesSecundarios= """UPDATE carta_empalmes_"""+cableado+"""
        SET destino=a.destino
        FROM (
        select
        fibras_raices.id_fibra,
        fibras_raices.nombre,
        fibras_raices.tubo_numero,
        fibras_raices.fibra,
        concat(fibras_filiales.nombre,', T',fibras_filiales.tubo_numero::text,', F',fibras_filiales.fibra::text) destino
            from
                (select
            fr.*,
            ROW_NUMBER () OVER (PARTITION BY fr.nombre )id_fibra_2
        from (
            select
                carem.nombre,
                carem.id_fibra,
                carem.tubo_numero,
                carem.fibra,
                carem.destino,
                raices.idce
            from
                carta_empalmes_"""+cableado+""" carem,
                (select
                    d.nombre,
                    d.rank ,
                    ce.id idce,
                    ce.rank rankce
                from
                    """+cableado+""" d,
                    (select
                        ce1.id::integer,
                        ce1.rank,
                        ce1.geom
                    from
                        cajas_empalme_"""+cableado+""" ce1
                    union
                    select distinct
                        ct1.id::integer,
                        d.rank,
                        ct1.geom
                    from
                        """+cajasSplitters+""" ct1,
                        """+cableado+""" d
                    where
                        ct1."""+campoDerivador+"""='S' and
                        st_equals(st_snap(ct1.geom,st_endpoint(d.geom), 0.01), st_endpoint(d.geom))
                    ) ce
                where
                    st_dwithin(st_endpoint(d.geom),ce.geom, 0.005)
                and
                    ce.rank ="""+str(i)+"""
                order by d.nombre
                    )raices
            where
                raices.nombre=carem.nombre and
                destino='LIBRE'
            order by
                nombre,
                id_fibra
            ) fr
        ) fibras_raices,
        (select
            fl.*,
            ROW_NUMBER () OVER (PARTITION BY fl.idce order by nombre, id_fibra  ) id_fibra_2
        from (
            select
                cartem.nombre,
                cartem.id_fibra,
                cartem.tubo_numero,
                cartem.fibra,
                cartem.destino,
                filiales.idce
            from
                carta_empalmes_"""+cableado+""" cartem,
                (
                select
                    d.nombre,
                    d.rank ,
                    ce.id idce,
                    ce.rank rankce
                from
                    """+cableado+""" d,
                    (select
                        ce1.id::integer,
                        ce1.rank,
                        ce1.geom
                    from
                        cajas_empalme_"""+cableado+""" ce1
                    union
                    select distinct
                        ct1.id::integer,
                        d1.rank,
                        ct1.geom
                    from
                        """+cajasSplitters+""" ct1,
                        """+cableado+""" d1
                    where
                        ct1."""+campoDerivador+"""='S' and
                        st_equals(st_snap(ct1.geom,st_endpoint(d1.geom), 0.01), st_endpoint(d1.geom))
                    ) ce
                where
                    st_dwithin(st_startpoint(d.geom),ce.geom,0.005)
                and
                    ce.rank ="""+str(i)+"""
                ) filiales
            where
                filiales.nombre=cartem.nombre
            and
                cartem.destino!='LIBRE'
            ) fl
        )fibras_filiales

    where
        fibras_raices.id_fibra_2=fibras_filiales.id_fibra_2 and
        fibras_raices.idce=fibras_filiales.idce
    order by
        nombre,
        id_fibra
            ) a

       where
           carta_empalmes_"""+cableado+""".id_fibra=a.id_fibra and carta_empalmes_"""+cableado+""".nombre=a.nombre;"""

        ejecutaSQL(nombreBBDD, host,  usuario,password,sqlEnlacesSecundarios)
    sqlReservas = """ UPDATE carta_empalmes_"""+cableado+"""
                        SET destino=a.destino
                        FROM (

                        select
                            libres.nombre,
                            libres.id_fibra,
                            reservas.destino
                        FROM
                            (SELECT
                                carta_empalmes_"""+cableado+""".*,
                                ROW_NUMBER () OVER (PARTITION BY carta_empalmes_"""+cableado+""".nombre order by nombre, id_fibra )id_fibra_2
                            from
                                carta_empalmes_"""+cableado+"""
                            where
                                destino='LIBRE' ) libres,
                            (SELECT
                                """+cableado+""".nombre,
                                'RESERVA' destino,
                                ROW_NUMBER () OVER (PARTITION BY """+cableado+""".nombre )id_fibra_2
                            FROM
                                """+cableado+""",
                                generate_series(1,"""+cableado+""".fib_res::integer)
                            order by
                                nombre) reservas
                        WHERE
                            libres.nombre=reservas.nombre and
                            libres.id_fibra_2=reservas.id_fibra_2
                        ORDER BY
                            nombre, id_fibra) a

                    where
                        carta_empalmes_"""+cableado+""".id_fibra=a.id_fibra and
                        carta_empalmes_"""+cableado+""".nombre=a.nombre """
    ejecutaSQL(nombreBBDD, host, usuario,password,sqlReservas)
    sqlOrd = """create table carta_empalmes_"""+cableado+"""2 as select * from carta_empalmes_"""+cableado+""" order by nombre, id_fibra asc;
                ALTER TABLE carta_empalmes_"""+cableado+""" RENAME TO "table_old";
                ALTER TABLE carta_empalmes_"""+cableado+"""2 RENAME TO carta_empalmes_"""+cableado+""";
                DROP TABLE "table_old";"""

    ejecutaSQL(nombreBBDD, host, usuario,password,sqlOrd)
    capasNSP('VARIOS',host, nombreBBDD,usuario, password, 'carta_empalmes_'+cableado, AbreviaturaRed+'-Carta de Empalmes')

#Funcion para la creacion de las capas de Etiquetado.
def etiquetado(letra, nombreCampo,nombreTablaCables,nombreTablaCajas,nombreTablaCRE,nombreTablaCE, abrevRed):
    sqlEtiquetado=""" drop table if exists etiquetado_fibras_"""+nombreTablaCables+"""; create table etiquetado_fibras_"""+nombreTablaCables+""" as select
    dd.geom,
    string_agg(dd.fusion, '/ ' order by id, nt, t, f) etiqueta,
    st_x(dd.geom) x,
    st_y(dd.geom) y
from (
    select
        row_number() over() id,
        geom,
        concat(origen,'>',destino) fusion,
        split_part(origen::text, ',',1) nt,
        SUBSTRING(split_part(origen::text, ',',2) FROM '.{1}$')::integer t,
        SUBSTRING(split_part(origen::text, ',',3) FROM '.{1}$')::integer f
     from
        (
            select
                cto.id::integer,
                cto.geom,
                concat (ced.nombre,', T',ced.tubo_numero,', F',ced.fibra ) origen,
                ced.destino destino
            from
                """+nombreTablaCajas+""" cto,
                """+nombreTablaCRE+""" ced
            where
                split_part(ced.destino::text, '.',1) =cto.nombre
            union all
            select
                ce.id::integer,
                ce.geom,
                concat(ced.nombre,', T', ced.tubo_numero,', F',ced.fibra) origen,
                ced.destino

            from
                (select
                    ce1.id::integer,
                    ce1.rank,
                    ce1.geom
                from
                    """+nombreTablaCE+"""   ce1
                union
                select
                    ct1.id::integer,
                    '0' as rank,
                    ct1.geom
                from
                    """+nombreTablaCajas+""" ct1
                where
                    ct1."""+nombreCampo+"""='S'
                ) ce,
                """+nombreTablaCRE+""" ced,
                """+nombreTablaCables+""" dist
            where
                ced.destino like '"""+letra+"""-%' and
                st_dwithin(ce.geom, st_startpoint(dist.geom), 1) and
                split_part(ced.destino::text, ',',1)= dist.nombre



        ) cc
            order by nt, t, f asc
    )dd
        group by  dd.geom;
    """

    sqlID= """alter table etiquetado_fibras_"""+nombreTablaCables+""" add column id bigserial; ALTER TABLE etiquetado_fibras_"""+nombreTablaCables+""" ADD PRIMARY KEY (id); """

    ejecutaSQL(nombreBBDD, host, usuario,password,sqlEtiquetado)
    ejecutaSQL(nombreBBDD, host, usuario,password,sqlID)
    qmlEtiquetado = home+"""/.qgis2/python/plugins/GeoFibra/estilos/etiquetado_distribucion.qml"""
    capas(nombreTablaCables.upper(),host,nombreBBDD, usuario, password, 'etiquetado_fibras_'+nombreTablaCables, abrevRed+'-Etiquetado Fusiones', qmlEtiquetado)

#Funcion para el modulo de errores
def seleccionZoomError(self, expresion, vlayer ):
    expr = QgsExpression( expresion )
    it = vlayer.getFeatures( QgsFeatureRequest( expr ) )
    ids = [i.id() for i in it]
    selected = vlayer.setSelectedFeatures( ids )
    for f in vlayer.getFeatures(QgsFeatureRequest( expr )):
        self.dockwidget.label_tipo.setText(str(f['tipo']))
        self.dockwidget.label_idError.setText(str( f['id']))
    erroresTot = str(vlayer.featureCount())
    self.dockwidget.label_erroresTotales.setText(erroresTot)
    #

    box = vlayer.boundingBoxOfSelected()
    iface.mapCanvas().setExtent(box)
    global canvas
    canvas.zoomScale(500)
    iface.mapCanvas().refresh()

#Algoritmo para el hallazgo de errores en las redes de cableado.
def errores_topologia(self, abrevRed,cajasSplitters, cableado, peanas_cpd_origen,nombreCapaTOC, checkCE, checkDer, campoDerivador):
    sqlDrop = """Drop table if exists errores_"""+abrevRed+"""; """
    sqlErrores=""" create table errores_"""+abrevRed+""" as select distinct row_number() over() id, the_union.* from (
        select distinct
            st_endpoint(d.geom) geom,
            'Caja derivadora sin indicar' as tipo
        from
            """+cajasSplitters+""" c,
            """+cableado+""" d,
            """+cableado+""" e
        where
            st_dwithin(st_endpoint(d.geom), st_startpoint(e.geom),0.1) and
            st_dwithin(st_endpoint(d.geom), c.geom,0.1) and
            c."""+campoDerivador+""" = 'N'
        union
        select
            a.geom geom,
                'Cable cortado' as tipo
        from(
            select distinct
                row_number() over() id,
                st_endpoint(d.geom) geom,
                c.geom ctogeom
            from
                """+cableado+""" d
            left join
                (select a.geom from """+cajasSplitters+""" a  union select b.geom from cajas_empalme_"""+abrevRed+""" b ) c
            on
                st_dwithin(c.geom,st_endpoint(d.geom), 0.1)
        ) a
        where
            a.ctogeom is null
        union
        select distinct
            st_endpoint(e.geom) geom,
            'Linea Invertida' as tipo
        from
            """+cableado+""" d,
            """+cableado+""" e,
            """+peanas_cpd_origen+""" p
        where
            st_endpoint(d.geom)=st_endpoint(e.geom) and
            d.id!=e.id
        union
        select distinct
            st_endpoint(d.geom) geom ,
            'Linea Invertida' as tipo
        from
            """+cableado+""" d,
            """+peanas_cpd_origen+""" p
        where
            st_dwithin(p.geom,st_endpoint(d.geom), 0.05)
        union
        select
            st_startpoint(d.geom) geom,
            'Necesita Cortar' as tipo
        from
            """+cableado+""" d,
            """+cableado+""" e
        where
            st_within(st_startpoint(d.geom), ST_LineSubstring(e.geom, 0.01, 0.99 ))
    ---------------------
        union
        select
            z.egeom geom,
            'Desconexion' as tipo
            from
            (select distinct
                e.geom  egeom,
                f.geom fgeom
            from
                (
                %s
                ) e
            left JOIN

                (select distinct
                    st_endpoint(a.geom) geom
                from
                    """+cableado+""" a ,
                    """+cableado+""" b
                where
                    st_endpoint(a.geom)=st_startpoint(b.geom)
                and a.id !=b.id) f
                on
                    st_dwithin(e.geom, f.geom, 0.001)
                where
                    f.geom is  null
            union
            select distinct
                st_startpoint(e.geom)  egeom,
                f.geom fgeom
            from
                """+cableado+""" e
            left JOIN
                (select distinct

                    st_startpoint(a.geom) geom
                from
                    """+cableado+""" a,
                    %s
                    """+peanas_cpd_origen+""" c,
                    """+cajasSplitters+""" d
                where
                    %s
                    ST_dwithin(st_startpoint(a.geom), c.geom,0.01) or
                    ST_dwithin(st_endpoint(a.geom), d.geom,0.01) ) f
                on
                    st_dwithin(e.geom, f.geom, 0.001)
                where
                    f.geom is  null) z
            union
            select
                d.geom,
                'Caja desconectada' as tipo
            from (
                select
                    c2.geom,
                    c.id
                from
                    """+cajasSplitters+""" c2
            LEFT JOIN
                (select distinct
                    c.*
                from
                    """+cajasSplitters+""" c,
                    """+cableado+""" d
                where
                    st_dwithin(d.geom, c.geom, 0.05)
                ) c ON c.id = c2.id
                ) d
            where
                d.id is null
        ) the_union;
        ALTER TABLE errores_"""+abrevRed+""" ADD PRIMARY KEY (id);
        ALTER TABLE errores_"""+abrevRed+""" ALTER COLUMN geom type geometry(Point, 25830); """

    sqlDerCE=""" select distinct a.geom from cajas_empalme_"""+abrevRed+""" a union select distinct  b.geom from """+cajasSplitters+""" b where b."""+campoDerivador+"""='S' """
    sqlDer="""  select distinct  b.geom from """+cajasSplitters+""" b where b."""+campoDerivador+"""='S' """

    if checkCE:
        tabCE= """ """
        ondCE= """ """
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlDrop)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlErrores%(sqlDer,tabCE,ondCE))
    else:
        tabCE= """cajas_empalme_"""+abrevRed+""" b,"""
        condCE= """st_dwithin(st_startpoint(a.geom), b.geom, 0.01) or """
        sqlCompleto=sqlErrores%(sqlDerCE,tabCE,condCE)

        ejecutaSQL(nombreBBDD,host, usuario,password,sqlDrop)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlCompleto)

    canvas = qgis.utils.iface.mapCanvas()
    sql=''
    uri = QgsDataSourceURI()
    uri.setConnection(host, '5432', nombreBBDD, usuario, password)
    uri.setDataSource("public",'errores_'+abrevRed, "geom",sql)
    vlayer = QgsVectorLayer(uri.uri(), nombreCapaTOC, "postgres")
    qmlErrores = home+"""/.qgis2/python/plugins/GeoFibra/estilos/errores.qml"""
    nc = abrevRed+u'-Errores Topología'
    if vlayer.featureCount()>1:
        self.dockwidget.pushButton_errorAd.setEnabled(True)
        self.dockwidget.pushButton_errorAt.setEnabled(False)
        self.dockwidget.pushButton_RevLin.setEnabled(True)
        capas(nombreRed.upper(),host,nombreBBDD, usuario, password, 'errores_'+abrevRed, nc, qmlErrores)
        expresion = '"id"=1'
        seleccionZoomError(self, expresion, vlayer )

    elif vlayer.featureCount() ==1:
        self.dockwidget.pushButton_errorAd.setEnabled(False)
        self.dockwidget.pushButton_errorAt.setEnabled(False)
        self.dockwidget.pushButton_RevLin.setEnabled(True)
        capas(nombreRed.upper(),host,nombreBBDD, usuario, password, 'errores_'+abrevRed,  nc, qmlErrores)
        expresion = '"id"=1'
        seleccionZoomError(self, expresion, vlayer )

    else:
        for lyr in QgsMapLayerRegistry.instance().mapLayers().values():
            if lyr.name() == nc:
                QgsMapLayerRegistry.instance().removeMapLayer(lyr.id())
        self.dockwidget.pushButton_errorAd.setEnabled(False)
        self.dockwidget.pushButton_errorAt.setEnabled(False)
        self.dockwidget.pushButton_RevLin.setEnabled(False)
        self.dockwidget.label_tipo.setText('')
        self.dockwidget.label_idError.setText('0')
        self.dockwidget.label_erroresTotales.setText('0')

#Funcion para obtener las longitudes de cable entre Cajas.
def cortaCables(nombreCapaS,nombreTablaCables,nombreTablaCajas, nombreTOC):
    sqlDrop="""DROP TABLE IF EXISTS """ +nombreCapaS
    sqlCortaCables = """
    Create table """ +nombreCapaS+""" AS
        select
            row_number() over() as id,
            splitcbl.* e,
            st_length(splitcbl.geom) as longitud_teorica
        from (
            SELECT distinct
                a.nombre,
                (ST_Dump(ST_Split(st_snap(a.geom, multi.geom,0.05),multi.geom))).geom AS geom
            from
                """+nombreTablaCables+""" a,
                (SELECT distinct
                    st_union(b."geom") geom
                FROM
                    """+nombreTablaCajas+""" b
                ) multi
            ) splitcbl;
    ALTER TABLE """ +nombreCapaS+""" ADD PRIMARY KEY (id);
    ALTER TABLE """ +nombreCapaS+""" ALTER COLUMN geom type geometry(Linestring, 25830); """

    ejecutaSQL(nombreBBDD,host,usuario,password,sqlCortaCables)
    qmlDistancias = home+"""/.qgis2/python/plugins/GeoFibra/estilos/distancia_cbl_cajas.qml"""
    capas(nombreTablaCables.upper(),host,nombreBBDD, usuario, password, nombreCapaS, nombreTOC, qmlDistancias)

#Funcion para actualizar el nombre de las CTO's en funcion del puerto GPON
def actualizaNombre(baseDatosNombre, host,  usuario,password):
    it1 = 0
    it2 = 0
    listaRanks=[]
    sqlDrop = """DROP TABLE IF EXISTS  cpd_gpon;"""
    sqlDrop2 = """DROP TABLE IF EXISTS  peana_fibra_raiz;"""

    sqlCreate = """ CREATE TABLE cpd_gpon (
                                            id          serial,
                                            n_tarjeta  integer NOT NULL,
                                            n_puerto  integer NOT NULL,
                                            fibra_troncal varchar(50),
                                            gid integer,
                                            PRIMARY KEY (id)
                                        );"""
    sqlCreate2 = """ CREATE TABLE peana_fibra_raiz (
                                            id          serial,
                                            fibra_raiz  varchar(50),
                                            splitter  varchar(50),
                                            PRIMARY KEY (id)
                                        );"""

    sqlUpdate = """ UPDATE
                        cpd_gpon cpd
                    SET
                        fibra_troncal = a.fib_troncal
                    FROM
                        (select
                            p_gpon.id,
                            p_gpon.n_tarjeta,
                            p_gpon.n_puerto,
                            concat(fibras_uso_id.n,', T',fibras_uso_id.tn,', F',fibras_uso_id.f)  fib_troncal
                        from
                            (select
                                id,
                                n_tarjeta,
                                n_puerto
                            from
                                cpd_gpon
                            ) p_gpon,
                            (select
                                row_number() over() id,
                                *
                            from
                                (select
                                    cde.nombre n,
                                    cde.tubo_numero tn,
                                    cde.fibra f
                                from
                                    carta_empalmes_troncal cde,
                                    troncal cbl
                                where
                                    cbl.nombre=cde.nombre and
                                    cbl.rank = 1 and
                                    cde.destino != 'LIBRE' and
                                    cde.destino != 'RESERVA'
                                order by
                                    n, tn, f
                                ) fibras_uso
                            ) fibras_uso_id
                        where
                            fibras_uso_id.id=p_gpon.id
                        ) a
                    where
                        cpd.id=a.id;
                UPDATE
                    cpd_gpon cpd
                SET
                    gid = 1; """
    ejecutaSQL(baseDatosNombre, host, usuario,password,sqlDrop )
    ejecutaSQL(baseDatosNombre, host, usuario,password,sqlDrop2 )
    ejecutaSQL(baseDatosNombre, host, usuario,password,sqlCreate )
    ejecutaSQL(baseDatosNombre, host, usuario,password,sqlCreate2 )
    uri = QgsDataSourceURI()
    uri.setConnection(host, "5432", baseDatosNombre,usuario, password)
    uri.setDataSource("public", "cpd", "geom", "","id")
    layerCPD = QgsVectorLayer(uri.uri(), "cpd", "postgres")

    uri = QgsDataSourceURI()
    uri.setConnection(host, "5432", baseDatosNombre,usuario, password)
    uri.setDataSource("public", "troncal", "geom", "","id")
    troncalLayer = QgsVectorLayer(uri.uri(), "troncal", "postgres")

    for feature in layerCPD.getFeatures():
        it1 =  int(feature["n_tarjeta"])
        it2 = int(feature["n_puertos"])+1

    for i in range(it1):
        for ii in range(1,it2):
            sqlInsert = """INSERT INTO cpd_gpon (n_tarjeta, n_puerto)values (""" +str(i)+""", """+str(ii)+""")"""
            ejecutaSQL(baseDatosNombre, host,  usuario,password,sqlInsert )
    ejecutaSQL(baseDatosNombre, host,  usuario,password,sqlUpdate )
    for f in troncalLayer.getFeatures():
        listaRanks.append(f["rank"])
    rankMax= max(listaRanks)

    for i in range(1,rankMax+1):
        sqlCore="""select
                    a.destino,
                    concat(a.nombre,', T',a.tubo_numero,', F',a.fibra) f
                from
                    carta_empalmes_troncal a
                where
                    a.destino not like 'LIBRE' and
                    a.destino not like 'RESERVA' and
                    a.destino not like 'T-%' and
                    length(a.nombre)="""+str(6+(i*3))
        sqlUpdate2="""insert into peana_fibra_raiz (splitter, fibra_raiz) %s ;"""
        if i == 1:
            ejecutaSQL(baseDatosNombre, host,  usuario,password,sqlUpdate2%sqlCore)
        else:
            for ii in range(1,i):
                aliasSQL=string.ascii_lowercase[ii]
                sqlAnid=""" select
                        origen"""+str(ii)+""".destino,
                        concat("""+aliasSQL+""".nombre,', T',"""+aliasSQL+""".tubo_numero,', F',"""+aliasSQL+""".fibra) f
                    from
                        (%s) origen"""+str(ii)+""",
                        carta_empalmes_troncal """+aliasSQL+"""
                    where
                        origen"""+str(ii)+""".f="""+aliasSQL+""".destino """
                sql1=sqlAnid % sqlCore
                for iii in range(1,ii):
                    aliasSQL2=string.ascii_lowercase[ii+1]
                    sqlAnid2=""" select
                        origen"""+str(iii)+""".destino,
                        concat("""+aliasSQL2+""".nombre,', T',"""+aliasSQL2+""".tubo_numero,', F',"""+aliasSQL2+""".fibra) f
                    from
                        (%s) origen"""+str(iii)+""",
                        carta_empalmes_troncal """+aliasSQL2+"""
                    where
                        origen"""+str(iii)+""".f="""+aliasSQL2+""".destino"""
                    sql1=sqlAnid2 % sql1
            ejecutaSQL(baseDatosNombre, host,  usuario,password, sqlUpdate2 % sql1)

    sqlAct= """UPDATE cto SET nombre_alternativo = a.n_alt from (
        select distinct
            split_part(cto_peana.cto, '.',1) cto,
            fr_gpon.n_tarjeta::integer,
            fr_gpon.n_puerto::integer,
            fr_gpon.fibra_troncal,
            concat(split_part(cto_peana.cto, '.',1),'-',fr_gpon.n_tarjeta,'-',fr_gpon.n_puerto) n_alt
        from
            (select
                sp.cto,
                concat(sp.nombre_peana,'.',sp.splitter_num ) sp_peana
            from
                splitters_peanas sp
            where
                sp.cto != 'NO_CTO'
            ) cto_peana,

            (select
                fibra_raiz,
                splitter
            from
                peana_fibra_raiz
            ) peana_fr,

            (select
                *
            from
                cpd_gpon
            ) fr_gpon
        where
            cto_peana.sp_peana=peana_fr.splitter and
            peana_fr.fibra_raiz=fr_gpon.fibra_troncal
        order by
            n_tarjeta, n_puerto, fibra_troncal) a
    where
        a.cto=cto.nombre;"""
    ejecutaSQL(baseDatosNombre, host, usuario,password, sqlAct)



class GeoFibra:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        self.iface = iface

        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        # initialize locale
        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            self.plugin_dir,
            'i18n',
            'GeoFibra_{}.qm'.format(locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)

            if qVersion() > '4.3.3':
                QCoreApplication.installTranslator(self.translator)

        # Declare instance attributes
        self.actions = []
        self.menu = self.tr(u'&GeoFibra')
        # TODO: We are going to let the user set this up in a future iteration
        self.toolbar = self.iface.addToolBar(u'GeoFibra')
        self.toolbar.setObjectName(u'GeoFibra')

        #print "** INITIALIZING GeoFibra"

        self.pluginIsActive = False
        self.dockwidget = None


    # noinspection PyMethodMayBeStatic
    def tr(self, message):
        """Get the translation for a string using Qt translation API.

        We implement this ourselves since we do not inherit QObject.

        :param message: String for translation.
        :type message: str, QString

        :returns: Translated version of message.
        :rtype: QString
        """
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate('GeoFibra', message)


    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):
        """Add a toolbar icon to the toolbar.

        :param icon_path: Path to the icon for this action. Can be a resource
            path (e.g. ':/plugins/foo/bar.png') or a normal file system path.
        :type icon_path: str

        :param text: Text that should be shown in menu items for this action.
        :type text: str

        :param callback: Function to be called when the action is triggered.
        :type callback: function

        :param enabled_flag: A flag indicating if the action should be enabled
            by default. Defaults to True.
        :type enabled_flag: bool

        :param add_to_menu: Flag indicating whether the action should also
            be added to the menu. Defaults to True.
        :type add_to_menu: bool

        :param add_to_toolbar: Flag indicating whether the action should also
            be added to the toolbar. Defaults to True.
        :type add_to_toolbar: bool

        :param status_tip: Optional text to show in a popup when mouse pointer
            hovers over the action.
        :type status_tip: str

        :param parent: Parent widget for the new action. Defaults None.
        :type parent: QWidget

        :param whats_this: Optional text to show in the status bar when the
            mouse pointer hovers over the action.

        :returns: The action that was created. Note that the action is also
            added to self.actions list.
        :rtype: QAction
        """

        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            self.toolbar.addAction(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action


    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""
        self.dockwidget = GeoFibraDockWidget()
        icon_path = ':/plugins/GeoFibra/icon.png'
        self.add_action(
            icon_path,
            text=self.tr(u'GeoFibra'),
            callback=self.run,
            parent=self.iface.mainWindow())
        self.dockwidget.creaNPsignal.connect(self.creaProyecto)
        self.dockwidget.signalArchivoCAT.connect(self.ImportaCAT)
        self.dockwidget.sennalArchivoCatastro.connect(self.ImportaCatastro)
        self.dockwidget.sennalCartociudad.connect(self.ImportaCartoCiudad)
        self.dockwidget.sennalAnalisisUUII.connect(self.analizaUUII)
        self.dockwidget.sennalCluster.connect(self.asignaCluster)
        self.dockwidget.sennalSp1n.connect(self.creaSp1n)
        self.dockwidget.sennalCTO.connect(self.creaCTO)
        self.dockwidget.sennalCTOCerrado.connect(self.creaCTO2)
        self.dockwidget.sennalCableDist.connect(self.rd)
        self.dockwidget.sennalError.connect(self.errorMas)
        self.dockwidget.sennalError2.connect(self.errorMenos)
        self.dockwidget.sennalRevLin.connect(self.revLin)
        self.dockwidget.sennalCableTroncal.connect(self.rt)
        self.dockwidget.sennalModeloCable.connect(self.addModel)
        self.dockwidget.sennalDistancias.connect(self.calculaDistancias)
#Funcion para actualizar conexiones
    def actConex2(self):
        actConex(self)

#Funcion que crea el proyecto nuevo.
    def creaProyecto(self, Nombre_BBDD, Usuario, host, password):

        if not Nombre_BBDD  or not Usuario or not host or not password:
            mensaje = u""
            if not Nombre_BBDD:
                mensaje = mensaje+"Nombre de la BBDD,"
            if not Usuario:
                mensaje = mensaje+"Usuario,"
            if not host:
                mensaje = mensaje+"Host,"
            if not password:
                mensaje = mensaje+"Password,"
            if mensaje[-1:]==',':
                mensaje = mensaje[:-1]
            iface.messageBar().pushMessage(u"Atención. Completa los siguientes campos:",mensaje, QgsMessageBar.WARNING, 10)
        else:
            root = QgsProject.instance().layerTreeRoot()
            #Conexion y creacion de la base de datos
            con = connect(dbname='postgres', user=Usuario, host=host, password=password)
            dbnam = Nombre_BBDD.lower().replace(" ", "_")
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = con.cursor()
            cur.execute('DROP DATABASE  IF EXISTS '+dbnam+';') # Linea a extinguir una vez este todo
            cur.execute('CREATE DATABASE '+dbnam+';' )


            cur.close()
            con.close()

            #Conexion y creacion de las extensiones PostGIS, asi como tambien de la funcion que nos va a limpiar las geometrias.
            con2 = connect(dbname=dbnam,user=Usuario, host=host, password=password)

            cur2 = con2.cursor()

            cur2.execute('CREATE EXTENSION postgis;')
            cur2.execute('CREATE EXTENSION postgis_topology;')
            cur2.execute("""
            /* *************************************************************************************************
            NormalizeGeometry - PL/pgSQL function to remove spikes and simplify geometries with PostGIS
                Author          : Gaspare Sganga
                Version         : 1.1.0
                License         : MIT
                Documentation   : http://gasparesganga.com/labs/postgis-normalize-geometry/
            ************************************************************************************************* */
                CREATE OR REPLACE FUNCTION normalize_geometry(
                PAR_geom                        geometry,
                PAR_area_threshold              double precision,
                PAR_angle_threshold             double precision,
                PAR_point_distance_threshold    double precision,
                PAR_null_area                   double precision,
                PAR_union                       boolean DEFAULT true
            ) RETURNS geometry AS $$
            -- Full documentation at http://gasparesganga.com/labs/postgis-normalize-geometry/
            DECLARE
                REC_linestrings record;
                ARR_output      geometry[];
                ARR_parts       geometry[];
                VAR_linestring  geometry;
                VAR_is_polygon  boolean;
                VAR_is_ring     boolean;
                VAR_tot         integer;
                VAR_old_tot     integer;
                VAR_n           integer;
                VAR_p0          geometry;
                VAR_p1          geometry;
                VAR_p2          geometry;
                VAR_area        double precision;
                VAR_output      geometry;
            BEGIN
                PAR_area_threshold              := abs(PAR_area_threshold);
                PAR_angle_threshold             := radians(PAR_angle_threshold);
                PAR_point_distance_threshold    := abs(PAR_point_distance_threshold);
                PAR_null_area                   := COALESCE(PAR_null_area, 0);

                CASE ST_GeometryType(PAR_geom)
                    WHEN 'ST_LineString', 'ST_MultiLineString' THEN
                        VAR_is_polygon := false;
                    WHEN 'ST_Polygon', 'ST_MultiPolygon' THEN
                        VAR_is_polygon := true;
                    ELSE
                        RETURN PAR_geom;
                END CASE;

                ARR_output := '{}'::geometry[];
                FOR REC_linestrings IN
                    SELECT array_agg((CASE WHEN VAR_is_polygon THEN ST_ExteriorRing((rdump).geom) ELSE (rdump).geom END) ORDER BY (rdump).path[1]) AS geoms
                        FROM (
                            SELECT row_number() OVER () AS r, (CASE WHEN VAR_is_polygon THEN ST_DumpRings(geom) ELSE ST_Dump(geom) END) AS rdump
                                FROM (SELECT (ST_Dump(PAR_geom)).geom) AS p
                        ) AS d
                    GROUP BY r
                LOOP
                    ARR_parts := '{}'::geometry[];
                    FOREACH VAR_linestring IN ARRAY REC_linestrings.geoms LOOP
                        VAR_tot := ST_NPoints(VAR_linestring);
                        SELECT ST_IsClosed(VAR_linestring) INTO VAR_is_ring;
                        IF VAR_is_ring THEN
                            VAR_linestring  := ST_RemovePoint(VAR_linestring, VAR_tot - 1);
                            VAR_tot         := VAR_tot - 1;
                        END IF;
                        LOOP
                            VAR_old_tot := VAR_tot;
                            VAR_n       := 1;
                            WHILE VAR_n <= VAR_tot LOOP
                                LOOP
                                    EXIT WHEN VAR_tot < 3 OR VAR_n > VAR_tot;
                                    VAR_p0   := ST_PointN(VAR_linestring, CASE WHEN VAR_n = 1 THEN VAR_tot ELSE VAR_n - 1 END);
                                    VAR_p1   := ST_PointN(VAR_linestring, VAR_n);
                                    VAR_p2   := ST_PointN(VAR_linestring, CASE WHEN VAR_n = VAR_tot THEN 1 ELSE VAR_n + 1 END);
                                    VAR_area := ST_Area(ST_MakePolygon(ST_MakeLine(ARRAY[VAR_p0, VAR_p1, VAR_p2, VAR_p0])));
                                    IF VAR_area > PAR_null_area THEN
                                        EXIT WHEN VAR_area > PAR_area_threshold;
                                        EXIT WHEN
                                            (abs(pi() - abs(ST_Azimuth(VAR_p0, VAR_p1) - ST_Azimuth(VAR_p1, VAR_p2))) > PAR_angle_threshold)
                                            AND (ST_Distance(VAR_p0, VAR_p1) > PAR_point_distance_threshold OR abs(pi() - abs(ST_Azimuth(VAR_p1, VAR_p2) - ST_Azimuth(VAR_p2, VAR_p0))) > PAR_angle_threshold)
                                            AND (ST_Distance(VAR_p1, VAR_p2) > PAR_point_distance_threshold OR abs(pi() - abs(ST_Azimuth(VAR_p2, VAR_p0) - ST_Azimuth(VAR_p0, VAR_p1))) > PAR_angle_threshold);
                                    END IF;
                                    VAR_linestring  := ST_RemovePoint(VAR_linestring, (CASE WHEN NOT VAR_is_polygon AND VAR_n = 1 AND VAR_area <= GREATEST(PAR_null_area, 0) THEN VAR_n ELSE VAR_n - 1 END));
                                    VAR_tot         := VAR_tot - 1;
                                END LOOP;
                                VAR_n := VAR_n + 1;
                            END LOOP;
                            EXIT WHEN VAR_tot < 3 OR VAR_tot = VAR_old_tot;
                        END LOOP;
                        IF VAR_is_ring THEN
                            IF VAR_tot >= 3 THEN
                                ARR_parts := array_append(ARR_parts, ST_AddPoint(VAR_linestring, ST_PointN(VAR_linestring, 1)));
                            ELSIF NOT VAR_is_polygon THEN
                                ARR_parts := array_append(ARR_parts, VAR_linestring);
                            END IF;
                        ELSE
                            ARR_parts := array_append(ARR_parts, VAR_linestring);
                        END IF;
                    END LOOP;
                    IF VAR_is_polygon THEN
                        ARR_output := array_append(ARR_output, ST_MakePolygon(ARR_parts[1], array_remove(ARR_parts[2:array_upper(ARR_parts, 1)], null)));
                    ELSE
                        ARR_output := array_append(ARR_output, ARR_parts[1]);
                    END IF;
                END LOOP;

                IF PAR_union THEN
                    SELECT ST_Union(ARR_output) INTO VAR_output;
                ELSE
                    SELECT ST_Collect(ARR_output) INTO VAR_output;
                    IF ST_NumGeometries(VAR_output) = 1 THEN
                        SELECT (ST_Dump(VAR_output)).geom INTO VAR_output;
                    END IF;
                END IF;
                RETURN VAR_output;
            END;
            $$ LANGUAGE plpgsql;""")
            cur2.execute(""" CREATE TABLE distribucion (
                id        serial CONSTRAINT dist_key PRIMARY KEY,
                nombre    varchar(40),
                rank      integer,
                fib_cl    integer,
                fib_res   integer,
                fib_total integer,
                marca     varchar(40),
                modelo    varchar(40),
                fib_totales_cable integer,
                tubos     integer,
                fib_tubos integer
            );
            SELECT AddGeometryColumn( 'distribucion', 'geom', 25830, 'LINESTRING', 2 );
            """)
            cur2.execute(""" CREATE TABLE troncal (
                id        serial CONSTRAINT tronc_key PRIMARY KEY,
                nombre    varchar(40),
                rank      integer,
                fib_cl    integer,
                fib_res   integer,
                fib_total integer,
                marca     varchar(40),
                modelo    varchar(40),
                fib_totales_cable integer,
                tubos     integer,
                fib_tubos integer
            );
            SELECT AddGeometryColumn( 'troncal', 'geom', 25830, 'LINESTRING', 2 );
            """)

            cur2.execute(""" CREATE TABLE cajas_empalme_distribucion (
                id        serial CONSTRAINT cedkey PRIMARY KEY,
                nombre    varchar(40),
                rank      integer,
                tipo      varchar (40)
            );
            SELECT AddGeometryColumn( 'cajas_empalme_distribucion', 'geom', 25830, 'POINT', 2 );
            """)

            cur2.execute(""" CREATE TABLE cajas_empalme_troncal (
                id        serial CONSTRAINT cetkey PRIMARY KEY,
                nombre    varchar(40),
                rank      integer,
                tipo      varchar (40)
            );
            SELECT AddGeometryColumn( 'cajas_empalme_troncal', 'geom', 25830, 'POINT', 2 );
            """)

            cur2.execute(""" CREATE TABLE cpd (
                id        serial CONSTRAINT cpdkey PRIMARY KEY,
                n_tarjeta integer,
                n_puertos integer

            );
            SELECT AddGeometryColumn( 'cpd', 'geom', 25830, 'POINT', 2 );
            """)

            cur2.execute(""" CREATE TABLE modelos_cable (
                id        serial CONSTRAINT modelkey PRIMARY KEY,
                marca     varchar(40),
                modelo    varchar(40),
                fib_totales     integer,
                tubos     integer,
                fib_tubos   integer
            );
            """)

            con2.commit()
            cur2.close()
            con2.close()
            listaGrupos = ['ELEMENTOS FIBRA','ANALISIS','CATASTRO','BASE']
            listaSubgr = ['TRONCAL','DISTRIBUCION','VARIOS']


            for gr in listaGrupos:
                removeGroup(gr)
                myGroup1 = root.addGroup(gr)
            #myGroup1 = root.insertGroup(2, "My Group 1")

            for sgr in listaSubgr:
                mygroup = root.findGroup("ELEMENTOS FIBRA")
                myGroupA = mygroup.addGroup(sgr)


            qmlTroncal=home+"""/.qgis2/python/plugins/GeoFibra/estilos/troncal.qml"""
            qmlDist=home+"""/.qgis2/python/plugins/GeoFibra/estilos/distribucion.qml"""
            qmlCEDist=home+"""/.qgis2/python/plugins/GeoFibra/estilos/ce_distribucion.qml"""
            qmlCETron=home+"""/.qgis2/python/plugins/GeoFibra/estilos/ce_troncal.qml"""
            qmlCPD=home+"""/.qgis2/python/plugins/GeoFibra/estilos/cpd.qml"""
            qmlMuni=home+"""/.qgis2/python/plugins/GeoFibra/estilos/municipios.qml"""
            rutaShapeMuni=home+"""/.qgis2/python/plugins/GeoFibra/cartografia/recintos_municipales_inspire_peninbal_etrs89_25830.shp"""
            nombreCapa="""Municipios"""
            urlWMSBase='contextualWMSLegend=0&crs=EPSG:25830&dpiMode=7&featureCount=10&format=image/png&layers=IGNBaseTodo&styles=&url=http://www.ign.es/wms-inspire/ign-base?'
            nombreWMSBase='Mapa Base'
            capas('TRONCAL',host,dbnam,Usuario, password, 'cajas_empalme_troncal', 'Tr-Cajas Empalme', qmlCETron)
            capas('DISTRIBUCION',host,dbnam,Usuario, password, 'cajas_empalme_distribucion', 'Dist-Cajas Empalme', qmlCEDist)
            capas('TRONCAL',host,dbnam, Usuario, password, 'cpd', 'CPD', qmlCPD)

            capas('TRONCAL',host,dbnam, Usuario, password, 'troncal', 'Tr-Cableado', qmlTroncal)
            capas('DISTRIBUCION',host,dbnam, Usuario, password, 'distribucion', 'Dist-Cableado', qmlDist)
            capasNSP('VARIOS',host, dbnam,Usuario, password, 'modelos_cable', 'Modelos Cables')

            cargaShape(rutaShapeMuni,nombreCapa, qmlMuni)
            cargawms(urlWMSBase,nombreWMSBase)

#Funcion para la importacion del archivo CAT de la Direccion General de Catastro.
    def ImportaCAT(self, rutaArchivoCAT, nombreConexion):
        if not rutaArchivoCAT:
            iface.messageBar().pushMessage(u"Atención:",'Selecciona una ruta de archivo.', QgsMessageBar.WARNING, 10)
        else:
            credenciales(nombreConexion)
            contenido = []
            with io.open(rutaArchivoCAT, 'r', encoding='cp1252') as f:
               
                for line in f:
                        if line[0:2] == '15':
                            line.encode("utf-8")
                            contenido.append(line)
            sqlCreate = """drop table  if exists tipo15; create table tipo15( TIPO_REG integer , BLANK_1  varchar(21),COD_DEL_MEH varchar(2),  COD_MUN integer, CLAS_BI  varchar(2),  REF_CAT_PARC  varchar(14),  N_SEC_BI  varchar(4),  "1CC"  varchar(1),  "2CC"  varchar(1),  N_F_BI_GERCAT  varchar(8),  ID_BI_AYTO  varchar(15),  N_FREG  varchar(19),  COD_PROVINE  varchar(2),   NOM_PROV  varchar(25),   DGC  varchar(3),  CODINE  varchar(3),   NOM_MUN  varchar(40),   ENT_MENOR  varchar(30),  COD_VIA  varchar(5),   TIPO_VIA  varchar(5),  NOM_VIA  varchar(25),  "1NOM_POL"  varchar(4),  "1LETRA_DUPL" varchar(1),  "2NUMPOL"  varchar(4),  "2LETRA_DUPL"  varchar(1),  KM  varchar(5),  BLOQUE  varchar(4),  ESCALERA  varchar(2),  PLANTA  varchar(3),  PUERTA  varchar(3),  TXTDIR  varchar(25),   COD_POST  varchar(5),  DISTMUN  varchar(2),   DGC_2  varchar(3),  COD_ZONA_CONC  varchar(2),   COD_POL  varchar(3),  COD_PARC  varchar(5),   COD_PARAJE  varchar(5),  NOMBRE_PARAJE  varchar(30),  BLANK_2  varchar(30),  N_ORDEN_INM  varchar(4),  ANN_ANT  varchar(4),  BLANK_3  varchar(52),  USO  varchar(1),  BLANK  varchar(13),  SUP_EL  varchar(10),  SUP_INM  varchar(10),  COEF_PROP  varchar(9),  BLANK_4  varchar(530));"""
            sqlEncoding = """SET client_encoding to 'UTF8';"""
            ejecutaSQL(nombreBBDD, host, usuario, password, sqlCreate)
            ejecutaSQL(nombreBBDD, host, usuario, password, sqlEncoding)          
            
            contenido2=[]
            for i in contenido:
                contenido2.append(i.replace("'","-"))              
                
            for record in contenido2:
                sqlCon = """insert into tipo15 (TIPO_REG  , BLANK_1  ,COD_DEL_MEH ,  COD_MUN , CLAS_BI  ,  REF_CAT_PARC ,  N_SEC_BI  ,  "1CC" ,  "2CC"  ,  N_F_BI_GERCAT  ,  ID_BI_AYTO ,  N_FREG  ,  COD_PROVINE  ,   NOM_PROV ,   DGC ,  CODINE ,   NOM_MUN  ,   ENT_MENOR  ,  COD_VIA ,   TIPO_VIA ,  NOM_VIA ,  "1NOM_POL" ,  "1LETRA_DUPL",  "2NUMPOL" ,  "2LETRA_DUPL"  ,  KM  ,  BLOQUE ,  ESCALERA ,  PLANTA  ,  PUERTA ,  TXTDIR ,   COD_POST  ,  DISTMUN  ,   DGC_2  ,  COD_ZONA_CONC  ,   COD_POL  ,  COD_PARC ,   COD_PARAJE  ,  NOMBRE_PARAJE  ,  BLANK_2  ,  N_ORDEN_INM  ,  ANN_ANT  ,  BLANK_3  ,  USO  ,  BLANK  ,  SUP_EL  ,SUP_INM  ,  COEF_PROP  ,  BLANK_4  ) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"""%(record[0:2],  record[2:23], record[23:25],    record[25:28],  record[28:30] ,   record[30:44],  record[44:48],  record[48:49] , record[49:50], record[50:58] ,  record[58:73] ,  record[73:92] ,   record[92:94],   record[94:119] ,   record[119:122] ,  record[122:125] ,  record[125:165],  record[165:195] ,  record[195:200] ,  record[200:205] ,   record[205:230],   record[230:234],  record[234:235],  record[235:239] ,  record[239:240] ,  record[240:245] ,  record[245:249], record[249:251] ,  record[251:254] ,   record[254:257],  record[257:282] ,   record[282:287] ,   record[287:289],   record[289:292],  record[292:294] ,  record[294:297] ,  record[297:302] ,  record[302:307] ,   record[307:337],  record[337:367] ,   record[367:371],  record[371:375],  record[375:427],  record[427:428],  record[428:441] ,  record[441:451] , record[451:461] ,   record[461:470] ,   record[470:1000])

                ejecutaSQL(nombreBBDD, host, usuario, password,sqlCon)
            
            capasNSP('CATASTRO',host, nombreBBDD,usuario, password, 'tipo15', 'Tipo 15')
            mensaje = u'La importación ha tenido éxito'
            QMessageBox.information(None, "INFO", mensaje)

#Funcion para la importacion de capas ShapeFile de la Direccion General de Catastro.
    def ImportaCatastro(self, rutaArchivo, nombreConexion):
        if not rutaArchivo:
            iface.messageBar().pushMessage(u"Atención:",'Selecciona una ruta de archivo.', QgsMessageBar.WARNING, 10)
        else:
            credenciales(nombreConexion)
            command = """ogr2ogr -overwrite -lco GEOMETRY_NAME=geom -t_srs epsg:25830 -f "PostgreSQL" PG:"dbname='""" + nombreBBDD + \
              """' host='""" + host + \
              """' user='""" + usuario + \
              """' password='""" + password + \
              """' port='5432' " """ + \
              ''' "'''+ rutaArchivo+'''" '''

            #Averiguar tipo Geometria
            layer=QgsVectorLayer(rutaArchivo, '','ogr')
            tipo=layer.geometryType()

            if tipo==0:
                command = command+' -nlt POINT'
            elif tipo==1:
                command = command+' -nlt LINESTRING'
            elif tipo==2:
                command = command+' -nlt POLYGON'

            os.system(command)

            #Averiguar Nombre Tabla
            rutaFichero=layer.dataProvider().dataSourceUri()
            (directorio,nombreFichero) = os.path.split(rutaFichero)
            nombre_tabla = nombreFichero [:nombreFichero.rfind('|')]
            nombre_tabla = nombre_tabla [:nombre_tabla.rfind('.')].lower()

            #Cambiar nombre comlumna id
            sqlRenoCol=""" ALTER TABLE """+nombre_tabla+""" RENAME COLUMN ogc_fid TO id; """
            ejecutaSQL(nombreBBDD, host, usuario, password, sqlRenoCol)
            home = expanduser("~")
            capas('CATASTRO',host,nombreBBDD,usuario, password, nombre_tabla, nombre_tabla, home+'/.qgis2/python/plugins/GeoFibra/estilos/'+nombre_tabla+'.qml')
            mensaje = u'La importación ha tenido éxito'
            iface.messageBar().pushMessage(u"INFO:",mensaje, QgsMessageBar.INFO, 10)

#Funcion para la carga de capas ShapeFile de CartoCiudad.
    def ImportaCartoCiudad(self, conexion, municipios,cartociudad):
        if not cartociudad:
            iface.messageBar().pushMessage(u"Atención:",'Selecciona una ruta de archivo.', QgsMessageBar.WARNING, 10)
        else:
            credenciales(conexion)
            for layer in QgsMapLayerRegistry.instance().mapLayers().values():
                if layer.name() == municipios:

                    features = layer.selectedFeatures()
                    nombrePostGIS = None

                    osyst= platform.system()
                    if osyst=='Windows':
                        head,tail = ntpath.split(cartociudad)
                        nombrePostGIS=tail.rsplit('.',1)[0]

                    elif osyst=='Linux':
                        corte1=cartociudad.rsplit("/",1)[1]
                        nombrePostGIS=corte1.rsplit('.',1)[0]
                    nombrePostGISlow =  nombrePostGIS.lower()

                    if features:
                        cambioSRC =processing.runalg("qgis:reprojectlayer", cartociudad, "epsg:25830", None)
                        clip=processing.runalg("qgis:clip",cambioSRC['OUTPUT'], layer.source(),None)
                        single=processing.runalg("qgis:multiparttosingleparts",clip['OUTPUT'], None)
                        command = """ogr2ogr -overwrite -lco GEOMETRY_NAME=geom -t_srs epsg:25830 -f "PostgreSQL" PG:"dbname='""" + nombreBBDD + \
                          """' host='""" + host + \
                          """' user='""" + usuario + \
                          """' password='""" + password + \
                          """' port='""" + """5432""" + """' " """ + \
                          ''' "'''+  single['OUTPUT']+'''" -nln '''+nombrePostGISlow
                        os.system(command)
                        capas('BASE',host,nombreBBDD,usuario, password, nombrePostGISlow, nombrePostGIS, home+'/.qgis2/python/plugins/GeoFibra/estilos/'+nombrePostGISlow+'.qml')

                    else:

                        iface.messageBar().pushMessage("AVISO", 'Selecciona un Municipio.', level=QgsMessageBar.WARNING)

#Funcion que calculara las UUII según los datos de Catastro.
    def analizaUUII(self, almacen,residencial,industrial,oficinas,comercial,deportivo,espectaculos,ocio_hosteleria, sanidad_beneficiencia,cultural,religioso,suelo_sin_edificar,edificio_singular,almacen_agrario, industrial_agrario, agrario, numeroUUII,nombreConexion):
        credenciales(nombreConexion)

        sqlCond =' '
        listaTablas=['uuii', 'uuii_masa', 'masa_uuii_ict','masa_uuii_no_ict']
        for i in listaTablas:
            print nombreBBDD,host, usuario,password
            ejecutaSQL(nombreBBDD,host, usuario,password,'drop table if exists '+i+';')

        sql_uuii_parc= """ CREATE TABLE uuii as (SELECT row_number() over() as id,"parcela"."refcat",Count("tipo15"."uso") as uuii ,"tipo15"."ann_ant"::integer as year,"parcela"."geom" FROM "parcela", "tipo15" WHERE "tipo15"."ref_cat_parc"="parcela"."refcat" and %s GROUP BY "parcela"."refcat", "parcela"."geom" ,"parcela"."id","tipo15"."ann_ant"); ALTER TABLE uuii ADD PRIMARY KEY (id);"""

        #Condicionales que annaden a la consulta
        if almacen:
            sqlCond=sqlCond+''' "tipo15"."uso"!='A' and'''
        if residencial:
            sqlCond=sqlCond+''' "tipo15"."uso"!='V' and'''
        if industrial:
            sqlCond=sqlCond+''' "tipo15"."uso"!='I' and'''
        if oficinas:
            sqlCond=sqlCond+''' "tipo15"."uso"!='O' and'''
        if comercial:
            sqlCond=sqlCond+''' "tipo15"."uso"!='C' and'''
        if deportivo:
            sqlCond=sqlCond+''' "tipo15"."uso"!='K' and'''
        if espectaculos:
            sqlCond=sqlCond+''' "tipo15"."uso"!='T' and'''
        if ocio_hosteleria:
            sqlCond=sqlCond+''' "tipo15"."uso"!='G' and'''
        if sanidad_beneficiencia:
            sqlCond=sqlCond+''' "tipo15"."uso"!='Y' and'''
        if cultural:
            sqlCond=sqlCond+''' "tipo15"."uso"!='E' and'''
        if religioso:
            sqlCond=sqlCond+''' "tipo15"."uso"!='R' and'''
        if suelo_sin_edificar:
            sqlCond=sqlCond+''' "tipo15"."uso"!='M' and'''
        if edificio_singular:
            sqlCond=sqlCond+''' "tipo15"."uso"!='P' and'''
        if almacen_agrario:
            sqlCond=sqlCond+''' "tipo15"."uso"!='B' and'''
        if industrial_agrario:
            sqlCond=sqlCond+''' "tipo15"."uso"!='J' and'''
        if agrario:
            sqlCond=sqlCond+''' "tipo15"."uso"!='Z' and'''

        if sqlCond[-3:]=='and':
            sqlCond = sqlCond[:-3]
        sql_completa_uuii= sql_uuii_parc % (sqlCond)

        #sql Masa
        sql_uuii_masa = """CREATE TABLE uuii_masa as (select sum(uuii) as uuii, "masa"."id", "masa"."masa", 0::integer as cluster, normalize_geometry("masa"."geom", 0.5, 0.5, 0.005, 0.0001) geom from (SELECT ST_Centroid("uuii"."geom") as geomct,"uuii"."uuii" FROM "public"."uuii") as centroid , "public"."masa"  where st_within ("centroid"."geomct", "masa"."geom") group by "masa"."id");ALTER TABLE uuii_masa ADD PRIMARY KEY (id);ALTER TABLE uuii_masa ALTER COLUMN geom type geometry(Polygon, 25830);
        create table masa_uuii_ict as select sum(uuii) uuii, "masa_ict".masa, normalize_geometry(masa_ict.geom, 0.5, 0.5, 0.005, 0.0001) geom from (SELECT (ST_Dump(ST_union("uuii"."geom") )).geom geom, substring(refcat, 1,5) masa FROM "public"."uuii" WHERE "uuii"."year">1998 and "uuii"."uuii">="""+str(numeroUUII)+""" group by masa) masa_ict , (SELECT ST_Centroid("uuii"."geom") geom,"uuii"."uuii" FROM "public"."uuii") as centroide where st_within ("centroide"."geom", "masa_ict"."geom") group by "masa_ict".masa, masa_ict.geom;
        ALTER TABLE masa_uuii_ict ALTER COLUMN geom type geometry(Polygon, 25830);
        ALTER TABLE masa_uuii_ict ADD column id bigserial;
        ALTER TABLE masa_uuii_ict ADD PRIMARY KEY (id);
        create table masa_uuii_no_ict as select sum(uuii) uuii, "masa_no_ict".masa, normalize_geometry(masa_no_ict.geom, 0.5, 0.5, 0.005, 0.0001) geom from ( SELECT a.masa, (ST_Dump(COALESCE(ST_Difference(geom, (SELECT ST_Union(b.geom) FROM masa_uuii_ict b WHERE  ST_Intersects(a.geom, b.geom) AND a.id != b.id)), a.geom))).geom geom FROM masa a ) masa_no_ict, (SELECT ST_Centroid("uuii"."geom") geom,"uuii"."uuii" FROM "public"."uuii") as centroide where st_within ("centroide"."geom", "masa_no_ict"."geom") group by "masa_no_ict".masa, masa_no_ict.geom;
        ALTER TABLE masa_uuii_no_ict ALTER COLUMN geom type geometry(Polygon, 25830);
        ALTER TABLE masa_uuii_no_ict ADD column id bigserial;
        ALTER TABLE masa_uuii_no_ict ADD PRIMARY KEY (id);"""

        ejecutaSQL(nombreBBDD,host, usuario,password,sql_completa_uuii)
        ejecutaSQL(nombreBBDD,host, usuario,password,sql_uuii_masa)



        qmlUUII_masa=home+"""/.qgis2/python/plugins/GeoFibra/estilos/uuii_masa.qml"""
        qmlUUII_masa_ict=home+"""/.qgis2/python/plugins/GeoFibra/estilos/uuii_masa_ict.qml"""
        qmlUUII_masa_no_ict=home+"""/.qgis2/python/plugins/GeoFibra/estilos/uuii_masa_no_ict.qml"""
        qmlUUII=home+"""/.qgis2/python/plugins/GeoFibra/estilos/uuii.qml"""

        capas('ANALISIS',host,nombreBBDD, usuario, password, 'uuii_masa', 'UUII-Masa', qmlUUII_masa)
        capas('ANALISIS',host,nombreBBDD, usuario, password, 'masa_uuii_ict', 'UUII-ICT', qmlUUII_masa_ict)
        capas('ANALISIS',host,nombreBBDD, usuario, password, 'masa_uuii_no_ict', 'UUII-No ICT', qmlUUII_masa_no_ict)
        capas('ANALISIS',host,nombreBBDD, usuario, password, 'uuii', 'UUII-Parcela', qmlUUII)

#Funcion para el cambio de estilo de la capa masa_uuii
    def cambiaEstilo(self):
        lyr = QgsMapLayerRegistry.instance().mapLayersByName("UUII-Masa")[0]
        home = expanduser("~")
        qmlUUII_masa=home+"""/.qgis2/python/plugins/GeoFibra/estilos/uuii_masa_cluster_edicion.qml"""
        lyr.triggerRepaint()
        lyr.loadNamedStyle(qmlUUII_masa)
        iface.mapCanvas().refresh()

#Funcion para el cambio de estilo por defecto de la capa masa_uuii
    def cambiaEstiloNormal(self):
        lyr = QgsMapLayerRegistry.instance().mapLayersByName("UUII-Masa")[0]
        home = expanduser("~")
        qmlUUII_masa=home+"""/.qgis2/python/plugins/GeoFibra/estilos/uuii_masa.qml"""
        lyr.triggerRepaint()
        lyr.loadNamedStyle(qmlUUII_masa)
        iface.mapCanvas().refresh()

#Boton que asigna Clusters
    def asignaCluster(self,cluster, nombreConexion):
        credenciales(nombreConexion)
        lyr = QgsMapLayerRegistry.instance().mapLayersByName("UUII-Masa")[0]
        features = lyr.selectedFeatures()
        idx = lyr.fieldNameIndex('cluster')
        lyr.startEditing()
        lyr.updateFields()
        for f in features:
            fid= f.id()
            lyr.changeAttributeValue(fid,idx,cluster)
        lyr.commitChanges()
        lyr.removeSelection()
        self.dockwidget.numeroCluster.setValue(int(cluster+1))

#Funcion que crea las cajas de Troncal. 
    def creaSp1n (self,nombreConexion,Acronimo_Poblacion, ratioSpliteo):
        credenciales(nombreConexion)

        sqlDrop = """Drop table if exists peanas; """
        sqlPeanas = """create table peanas as
        select * ,ST_SetSRID(st_makepoint(coordenadas.cx,coordenadas.cy),25830) geom, 0 as splitters, 'N'::varchar as derivador from (
        select puntos.cluster, sum(st_x(puntos.geom)*uuii)/sum(uuii) as cx, sum(st_y(puntos.geom)*uuii)/sum(uuii) as cy from (select st_centroid(uuii_masa.geom) geom, uuii, cluster from uuii_masa) puntos group by  cluster) coordenadas order by cluster::integer;
        ALTER TABLE peanas ADD column id bigserial;
        ALTER TABLE peanas ADD column ratio_spliteo integer;
        UPDATE peanas SET ratio_spliteo="""+ratioSpliteo+""";
        ALTER TABLE peanas ADD PRIMARY KEY (id);
        ALTER TABLE peanas ALTER COLUMN geom type geometry(Point, 25830);"""
        sqlNombre = """ALTER TABLE peanas ADD COLUMN nombre varchar(100);  update peanas set nombre=a.nombre from (select p.id, 'P-"""+Acronimo_Poblacion+"""-'||p.cluster nombre from peanas p ) a where a.id=peanas.id ; """
        sqlDropClust = """Drop table if exists clusteres; """
        sqlClust = """ create table clusteres as select distinct cluster, (st_dump(st_buffer( (st_dump(st_union(st_buffer(geom,30,'endcap=flat join=mitre mitre_limit=10.0')))).geom,-28.5,'endcap=flat join=round'))).geom as geom from uuii_masa  where cluster!= 0 group by cluster;
        ALTER TABLE clusteres ADD column id bigserial;
        ALTER TABLE clusteres ADD PRIMARY KEY (id);
        ALTER TABLE peanas ALTER COLUMN geom type geometry(Point, 25830);
        ALTER TABLE clusteres ALTER COLUMN geom type geometry(Polygon, 25830);"""

        ejecutaSQL(nombreBBDD,host, usuario,password,sqlDrop)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlPeanas)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlNombre)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlDropClust)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlClust)

        qmlPeanas=home+"""/.qgis2/python/plugins/GeoFibra/estilos/peanas.qml"""
        qmlClusteres = home+"""/.qgis2/python/plugins/GeoFibra/estilos/clusteres.qml"""
        capas('TRONCAL',host,nombreBBDD, usuario, password, 'peanas', 'SP 1er Nivel', qmlPeanas)
        capas('ANALISIS',host,nombreBBDD, usuario, password, 'clusteres', 'Clusteres', qmlClusteres)

#Funcion para la creacion de las CTO's. Abierto
    def creaCTO (self, tasaPen,ratioSp, spCTO,num_min_man,num_min_ict,caja_ext,caja_int,acron_pob,nombreConexion):
        credenciales(nombreConexion)
        #Variable para el calculo del prcentaje de penetracion
        tsPorc=str(float(tasaPen)/100)

        #elimino cto viejas
        ejecutaSQL(nombreBBDD,host, usuario,password,'drop table if exists cto')
        ejecutaSQL(nombreBBDD,host, usuario,password,'drop table if exists splitters')
        #Consulta que crea las cajas
        sql_cajas = """CREATE TABLE cto as
            (select
            st_closestpoint(ST_ExteriorRing(masa.geom),cajas.geom) as geom,
            """+str(spCTO)+""" as splitters,
            '"""+caja_ext+"""' as tipo_caja,
            'Exterior/Fachada' as situacion,
            '"""+str(ratioSp)+"""' as ratio_spliteo,
            'N' as derivadora
        from
            masa,
            (select
                (st_dump(ST_GeneratePoints(masa.geom, cajasMasa.cajas::int) )).geom as geom,
                cajasMasa.masa,
                cajasMasa.id
            from
                public.masa,
                (select
                    masa.id,
                    masa.masa,
                    sum(cent_masa_no_ict.uuii) uuii,
                    ceil((sum(cent_masa_no_ict.uuii)::float*"""+tsPorc+"""/"""+str(ratioSp)+""")/"""+str(spCTO)+""" )as cajas,
                    masa.geom
                from
                    masa,
                    (select
                        masa_uuii_no_ict.id,
                        masa_uuii_no_ict.masa,
                        masa_uuii_no_ict.uuii,
                        ST_PointOnSurface(masa_uuii_no_ict.geom) geom
                    from
                        masa_uuii_no_ict
                    where
                        uuii>="""+str(num_min_man)+"""
                    ) cent_masa_no_ict
                where
                    st_intersects(masa.geom,cent_masa_no_ict.geom)
                group by
                    masa.id) cajasMasa
            where
                cajasMasa.masa= masa.masa and
                cajasMasa.id=masa.id ) cajas
        where
            cajas.masa= masa.masa and
            cajas.id= masa.id

        union

        SELECT
            ST_PointOnSurface("uuii"."geom") geom,
            ceiling("uuii"."uuii"::numeric*"""+tsPorc+"""/"""+str(ratioSp)+""" )::float splitters,
            '"""+caja_int+"""' as tipo_caja,
            'Interior/RITI' as situacion,
            """+str(ratioSp)+"""  as ratio_spliteo,
            'N' as derivadora
        FROM
            "public"."uuii"
        WHERE
            "uuii"."year">1998 and
            "uuii"."uuii">="""+str(num_min_ict)+"""
            );

        ALTER TABLE cto ADD column id bigserial;
        ALTER TABLE cto ADD PRIMARY KEY (id);
        ALTER TABLE cto ALTER COLUMN geom type geometry(Point, 25830);
        ALTER TABLE cto ADD column nombre varchar(100);
        ALTER TABLE cto ADD column nombre_alternativo varchar(100);"""
        ejecutaSQL(nombreBBDD,host, usuario,password,sql_cajas)

        sqlNombre = """ update cto set nombre=a.nombre from (select cto.id, '"""+acron_pob+"""-'||cto.id nombre from cto ) a where a.id=cto.id ;"""
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlNombre)



        qmlCto=home+"""/.qgis2/python/plugins/GeoFibra/estilos/cto.qml"""
        capas('DISTRIBUCION',host,nombreBBDD, usuario, password, 'cto', 'CTO', qmlCto)

        sql_splitters = """ CREATE TABLE splitters (id serial CONSTRAINT firstkey PRIMARY KEY,nombre_cto  varchar(40) NOT NULL, n_splitter  integer NOT NULL, pat_splitter integer, cliente varchar(50));"""
        ejecutaSQL(nombreBBDD,host, usuario,password,sql_splitters)

        creSplittersDist('nombre','splitters', 'ratio_spliteo')
        capasNSP('VARIOS',host, nombreBBDD,usuario, password, 'splitters', 'Splitters')
        relaciones('Splitters', 'CTO', 'nombre_cto', 'nombre','relacion_splitters',  'Splitter CTO')
        mensaje = u"Creación de CTO's y Tabla de splitters realizada"
        QMessageBox.information(None, "INFO", mensaje)
#Funcion para la creacion de las CTO's. Cerrado
    def creaCTO2 (self, tasaPen,ratioSp, spCTO, acron_pob,nombreConexion):
        credenciales(nombreConexion)
        tasPrc = str(float(tasaPen)/100)
        ratSp = str(ratioSp)


        sqlDrop1 = """Drop table if exists cto;"""
        sqlCTOCerr = """Create table cto as
                    select
                        geomcajas.geom,
                        '"""+spCTO+"""'::integer as splitters,
                        'Por Determinar'::varchar as tipo_caja,
                        'Por Determinar'::varchar as situacion,
                        '"""+ratSp+"""'::integer as ratio_spliteo,
                        'N'::varchar as derivadora

                    from
                        (SELECT
                            ST_LineInterpolatePoint(
                            a.geom,
                            generate_series(1, cajas.cajas):: double precision / cajas.cajas
                            )  geom
                        from
                            (select
                                    st_boundary(st_buffer(st_pointonsurface(clu.geom),30 )) geom,
                                    cluster
                                from
                                    clusteres clu
                            ) a,
                            (
                            select
                                clu.cluster,
                                ceil((sum(ui.uuii)*"""+str(tasPrc)+""")/"""+str(ratSp)+"""::numeric/"""+spCTO+""") cajas
                            from
                                clusteres  clu,
                                uuii ui
                            where
                                st_intersects(clu.geom, st_pointonsurface(ui.geom))
                            group by
                                clu.cluster
                            ) cajas
                        where
                            cajas.cluster=a.cluster) geomcajas;

                        ALTER TABLE cto ADD column id bigserial;
                        ALTER TABLE cto ADD PRIMARY KEY (id);
                        ALTER TABLE cto ALTER COLUMN geom type geometry(Point, 25830);
                        ALTER TABLE cto ADD column nombre varchar(100);
                        ALTER TABLE cto ADD column nombre_alternativo varchar(100);"""

        sqlNombre = """ update cto set nombre=a.nombre from (select cto.id, '"""+acron_pob+"""-'||cto.id nombre from cto ) a where a.id=cto.id ;"""
        sqlDrop = """Drop table if exists  splitters; """
        sql_splitters = """ CREATE TABLE splitters (
                                id serial CONSTRAINT firstkey PRIMARY KEY,
                                nombre_cto  varchar(40) NOT NULL,
                                n_splitter  integer NOT NULL,
                                pat_splitter integer,
                                cliente varchar(50));"""

        ejecutaSQL(nombreBBDD,host, usuario,password,sqlDrop1)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlCTOCerr)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlNombre)
        ejecutaSQL(nombreBBDD,host, usuario,password,sqlDrop)
        ejecutaSQL(nombreBBDD,host, usuario,password,sql_splitters)

        qmlCto=home+"""/.qgis2/python/plugins/GeoFibra/estilos/cto.qml"""
        creSplittersDist('nombre','splitters', 'ratio_spliteo')

        capasNSP('VARIOS',host, nombreBBDD,usuario, password, 'splitters', 'Splitters')
        capas('DISTRIBUCION',host,nombreBBDD, usuario, password, 'cto', 'CTO', qmlCto)
        relaciones('Splitters', 'CTO', 'nombre_cto', 'nombre','relacion_splitters',  'Splitter CTO')

        mensaje = u"Creación de CTO's y Tabla de splitters realizada"
        QMessageBox.information(None, "INFO", mensaje)

#Funcion que servira para el trabajo con la RD
    def rd(self, checkNombrado, checkConteo, checkCE,checkSD,checkCartaEmp,checkEtiq, porceRes, acron_pob,conexion):
        credenciales(conexion)
        layerModelos = QgsMapLayerRegistry.instance().mapLayersByName( 'Modelos Cables' )[0]
        if layerModelos.featureCount()==0:
            widget = iface.messageBar().createMessage(u"AVISO", u"No hay ningún modelo en la Base de Datos. Por favor, introduzca algunos en la pestaña Modelos Cables.")
            iface.messageBar().pushWidget(widget, QgsMessageBar.WARNING)
        else:
            global nombreRed
            nombreRed = 'distribucion'
            global nombreCapaCableado
            nombreCapaCableado = 'Dist-Cableado'

            credenciales(conexion)
            abrevRed = nombreRed
            cajasSplitters = 'cto'
            cableado = nombreRed
            peanas_cpd_origen = 'peanas'
            nombreCapaTOC = 'Errores Distribucion'
            campoDerivador = 'derivadora'
            errores_topologia(self, abrevRed,cajasSplitters, cableado, peanas_cpd_origen,nombreCapaTOC, checkCE, checkSD, campoDerivador)
            sql=''
            uri = QgsDataSourceURI()
            uri.setConnection(host, '5432', nombreBBDD, usuario, password)
            uri.setDataSource("public",'errores_'+abrevRed, "geom",sql)
            mensaje = u'Comprobación de topología'
            vlayer = QgsVectorLayer(uri.uri(), nombreCapaTOC, "postgres")

            sqlSelectNombreDist = """   case when
                                            c.cluster::integer < 10 then ('D-"""+acron_pob+"""-00'||c.cluster||'-00'||ROW_NUMBER () OVER (partition by c.cluster order by t.id))
                                        else
                                            ('D-"""+acron_pob+"""-0'||c.cluster||'-00'||ROW_NUMBER () OVER (partition by c.cluster order by t.id))
                                        end nombre, """
            if vlayer.featureCount()==0:
                if checkNombrado:
                    iface.messageBar().pushMessage(u"INFO",'Comienza el Nombrado.', QgsMessageBar.INFO, 10)
                    mensaje = mensaje+', nombrado'
                    if checkSD:
                        nombresPrimarios(nombreBBDD,host, usuario,password, 'distribucion', sqlSelectNombreDist, 'peanas', 'D')
                    else:
                        nombresPrimarios(nombreBBDD,host, usuario,password, 'distribucion', sqlSelectNombreDist, 'peanas', 'D')
                        layerDist = QgsMapLayerRegistry.instance().mapLayersByName( 'Dist-Cableado' )[0]
                        nombresSecundarios(nombreBBDD,host, usuario,password, layerDist, 'distribucion')

                        nomCapaCe = 'cajas_empalme_distribucion'
                        nombreTablaCables='distribucion'
                        cajas_empalme(nomCapaCe, nombreTablaCables, acron_pob)

                if checkConteo:
                    iface.messageBar().pushMessage(u"INFO",'Comienza el conteo de fibras.', QgsMessageBar.INFO, 10)
                    mensaje = mensaje+', conteo'
                    conteoCables(nombreBBDD,host, usuario,password, 'distribucion', 'Dist-Cableado', 13, 'cto', str(porceRes))


                if checkCartaEmp:
                    iface.messageBar().pushMessage(u"INFO",u'Comienza la creación de la Carta de Empalmes.', QgsMessageBar.INFO, 10)
                    mensaje = mensaje+', carta de empalmes'
                    carta_empalmes(nombreBBDD, host, usuario,password,'Dist-Cableado', 'distribucion', 'cto_cables', 'cto', 'derivadora', 'Dist')
                    relaciones('Dist-Carta de Empalmes', 'Dist-Cableado', 'nombre', 'nombre', 'id_dist_ce','dist_ce')

                if checkEtiq:
                    iface.messageBar().pushMessage(u"INFO",'Comienza el etiquetado.', QgsMessageBar.INFO, 10)
                    mensaje = mensaje+', etiquetado'
                    letra='D'
                    nombreCampo='derivadora'
                    etiquetado(letra, nombreCampo,'distribucion','cto','carta_empalmes_distribucion','cajas_empalme_distribucion','Dist')
                widget = iface.messageBar().createMessage(u"Tareas Realizadas:",mensaje)
                iface.messageBar().pushWidget(widget, QgsMessageBar.INFO)
            else:
                widget = iface.messageBar().createMessage(u"INFO", u"Hay errores.")
                iface.messageBar().pushWidget(widget, QgsMessageBar.WARNING)


#Funcion para viajar entre los errores del modulo de Errores.
    def errorMas(self,valorEtiqueta):
        nuevoId = str(int(valorEtiqueta)+1)
        expresion = '"id"='+nuevoId
        sql=''
        uri = QgsDataSourceURI()
        uri.setConnection(host, '5432', nombreBBDD, usuario, password)
        uri.setDataSource("public",'errores_'+nombreRed, "geom",sql)
        vlayer = QgsVectorLayer(uri.uri(), 'Errores '+nombreRed, "postgres")
        if int(nuevoId) == vlayer.featureCount():
            self.dockwidget.pushButton_errorAd.setEnabled(False)
            self.dockwidget.pushButton_errorAt.setEnabled(True)
        elif int(nuevoId) > 1:
            self.dockwidget.pushButton_errorAt.setEnabled(True)
        seleccionZoomError(self, expresion, vlayer )

#Funcion para viajar entre los errores del modulo de Errores.
    def errorMenos(self,valorEtiqueta):
        nuevoId = str(int(valorEtiqueta)-1)
        expresion = '"id"='+nuevoId
        sql=''
        uri = QgsDataSourceURI()
        uri.setConnection(host, '5432', nombreBBDD, usuario, password)
        uri.setDataSource("public",'errores_'+nombreRed, "geom",sql)
        vlayer = QgsVectorLayer(uri.uri(), 'Errores '+nombreRed, "postgres")

        if int(nuevoId) < vlayer.featureCount():
            self.dockwidget.pushButton_errorAd.setEnabled(True)

        if int(nuevoId) == 1:
            self.dockwidget.pushButton_errorAt.setEnabled(False)
        seleccionZoomError(self, expresion, vlayer )

#Funcion que revierte lós cables de sentido.
    def revLin(self,conexion):
        credenciales(conexion)
        name = nombreCapaCableado
        layer = QgsMapLayerRegistry.instance().mapLayersByName( name )[0]
        features = layer.selectedFeatures()
        listaId=[]

        sqlReverse=""" UPDATE """+nombreRed+""" SET geom=a.geom FROM
            (select
                id,
                st_reverse(d.geom) geom
            from
                """+nombreRed+""" d
            where
                %s ) a
        where a.id="""+nombreRed+""".id; """

        sqlId=""

        if features:
            for f in features:
                listaId.append( f['id'])
            for id in listaId:
                sqlId=sqlId+"id="+str(id)+" or "

            if sqlId[-3:]=='or ':
                sqlId = sqlId[:-3]

            sqlFinal=sqlReverse%(sqlId)

            ejecutaSQL(nombreBBDD,host, usuario,password,sqlFinal)
            layer.removeSelection()
            iface.mapCanvas().refresh()
        else:
            iface.messageBar().pushMessage("AVISO", 'Selecciona cables invertidos.', level=QgsMessageBar.WARNING)

#Funcion que servira para el trabajo con la RT
    def rt(self,checkNombrado, checkConteo, checkCE,checkSD,checkCartaEmp,checkEtiq, porceRes, acron_pob,conexion):
        global nombreRed
        nombreRed = 'troncal'
        global nombreCapaCableado
        nombreCapaCableado = 'Tr-Cableado'
        mensaje = u'Comprobación de topología'
        credenciales(conexion)
        abrevRed = nombreRed
        cajasSplitters = 'peanas'
        cableado = nombreRed
        peanas_cpd_origen = 'cpd'
        nombreCapaTOC = 'Errores Troncal'
        campoDerivador = 'derivador'
        errores_topologia(self, abrevRed,cajasSplitters, cableado, peanas_cpd_origen,nombreCapaTOC, checkCE, checkSD, campoDerivador)
        sql=''
        uri = QgsDataSourceURI()
        uri.setConnection(host, '5432', nombreBBDD, usuario, password)
        uri.setDataSource("public",'errores_'+abrevRed, "geom",sql)
        vlayer = QgsVectorLayer(uri.uri(), nombreCapaTOC, "postgres")

        sqlSelectNombreTr = """('T-"""+acron_pob+"""-00'||ROW_NUMBER () OVER (order by t.id)) nombre, """
        if vlayer.featureCount()==0:

            if checkNombrado:
                mensaje = mensaje+', nombrado'
                if checkSD:
                    nombresPrimarios(nombreBBDD,host, usuario,password, 'troncal', sqlSelectNombreTr, 'cpd', 'T')
                else:
                    nombresPrimarios(nombreBBDD,host, usuario,password, 'troncal', sqlSelectNombreTr, 'cpd', 'T')
                    vlayer = QgsMapLayerRegistry.instance().mapLayersByName( 'Tr-Cableado' )[0]
                    nombresSecundarios(nombreBBDD,host, usuario,password, vlayer, 'troncal')

                    nomCapaCe = 'cajas_empalme_troncal'
                    nombreTablaCables='troncal'
                    cajas_empalme(nomCapaCe, nombreTablaCables, acron_pob)

            if checkConteo:
                mensaje = mensaje+', conteo'
                #Asignacion de splitters a peanas
                sqlSplitters="""
                            UPDATE peanas p
                            SET splitters=a.splitters
                                FROM (
                            select
                                ceil(sum(d.fib_cl)::numeric/p.ratio_spliteo )::integer  splitters,
                                p.id
                            from
                                distribucion d,
                                peanas p
                            where
                                st_dwithin(p.geom,  d.geom, 1)
                            group by
                                p.id ) a
                                where p.id=a.id;"""

                ejecutaSQL(nombreBBDD,host, usuario,password,sqlSplitters)
                conteoCables(nombreBBDD,host, usuario,password, 'troncal', 'Tr-Cableado', 9, 'peanas', str(porceRes))

            if checkCartaEmp:
                mensaje = mensaje+', carta de empalmes'
                carta_empalmes(nombreBBDD, host, usuario,password,'Tr-Cableado', 'troncal', 'peanas_cables', 'peanas', 'derivador', 'Tr')

                sqlDrop = """ DROP TABLE IF EXISTS splitters_peanas ;"""
                sqlSplittersPeanas = """ CREATE TABLE splitters_peanas (id serial, nombre_peana varchar(40) NOT NULL, fibra_troncal varchar(40), splitter_num integer NOT NULL, patilla_splitter integer NOT NULL, nom_fib_dist varchar (100), cto varchar(40), PRIMARY KEY(id));"""

                ejecutaSQL(nombreBBDD,host, usuario,password,sqlDrop)
                ejecutaSQL(nombreBBDD,host, usuario,password,sqlSplittersPeanas)

                layerP=QgsMapLayerRegistry.instance().mapLayersByName( 'SP 1er Nivel' )[0]
                ratioSpliteoList = []
                for f in layerP.getFeatures():
                    ratioSpliteoList.append(f["ratio_spliteo"])

                ratioSpliteo=''
                for i in set(ratioSpliteoList):
                    ratioSpliteo = i

                rSP=int(ratioSpliteo)+1
                for feature in layerP.getFeatures():
                    idx = layerP.fieldNameIndex('nombre')
                    idSp = layerP.fieldNameIndex('splitters')
                    for counter1 in range (1,int(feature.attributes()[idSp]+1.0)):
                        for counter in range (1,rSP):
                                sqlF = """insert into splitters_peanas(nombre_peana, splitter_num,patilla_splitter,nom_fib_dist, cto) values ('%s',%s,%s, 'LIBRE', 'NO_CTO');"""% (feature.attributes()[idx],str(counter1),str(counter))

                                ejecutaSQL(nombreBBDD, host, usuario,password,sqlF)

                sql1= """ update    splitters_peanas set fibra_troncal=aa.fib_tronc, nom_fib_dist=aa.fibra_dist      from(
                                select fib_tronc, peana, splitter, patilla_splitter, fibra_dist from
                                -------select de la relacion fibras troncal con peanas  y splitters
                                    (select
                                        concat(carta_empalmes_troncal.nombre,', T', carta_empalmes_troncal.tubo_numero,', F', carta_empalmes_troncal.fibra) fib_tronc,
                                        coalesce(split_part(destino, '.', 1)) peana,
                                        coalesce(split_part(destino, '.', 2)) AS splitter
                                    from
                                        carta_empalmes_troncal
                                    where
                                        substring(destino, 1, 1)='P') tronc_peana,
                                -------select de la relacion fibras distribucion con peanas y splitters
                                        (select
                                            nombre_peana,
                                            splitter_num,
                                            patilla_splitter,
                                            fibra_dist
                                        from
                                            (select
                                                *,
                                                ROW_NUMBER () OVER (PARTITION BY splitters_peanas.nombre_peana order by splitters_peanas.splitter_num, splitters_peanas.patilla_splitter ) id_patilla
                                            from
                                                splitters_peanas) split_pat ,

                                            (select
                                                ROW_NUMBER () OVER (PARTITION BY pean_nom order by carta_empalmes_distribucion.nombre, carta_empalmes_distribucion.id_fibra ) id_patilla,
                                                concat(carta_empalmes_distribucion.nombre, ', T',carta_empalmes_distribucion.tubo_numero, ', F',carta_empalmes_distribucion.fibra ) fibra_dist,
                                                dist_peana.pean_nom

                                            from
                                                carta_empalmes_distribucion,
                                                (select
                                                    dist.nombre nom_dist,
                                                    peanas.nombre pean_nom
                                                from
                                                    distribucion dist,
                                                    peanas peanas
                                                where
                                                    st_dwithin(dist.geom,peanas.geom,0.01 )
                                                order by
                                                    dist.nombre) dist_peana
                                            Where
                                                destino !='RESERVA' and
                                                destino !='LIBRE' and
                                                dist_peana.nom_dist=carta_empalmes_distribucion.nombre
                                            order by
                                                nombre, tubo_numero, id_fibra) dist_pat
                                        where
                                            dist_pat.pean_nom::varchar=split_pat.nombre_peana::varchar and
                                            dist_pat.id_patilla=split_pat.id_patilla) dist_peana
                                ---Condicionales-------------------
                                where
                                    dist_peana.nombre_peana=tronc_peana.peana and
                                    dist_peana.splitter_num::varchar=tronc_peana.splitter) aa
                            where
                                splitters_peanas.nombre_peana=aa.peana and
                                splitters_peanas.splitter_num=aa.splitter::integer and
                                splitters_peanas.patilla_splitter=aa.patilla_splitter::integer """

                ejecutaSQL(nombreBBDD, host, usuario,password,sql1)
                distribucion=QgsMapLayerRegistry.instance().mapLayersByName( 'Dist-Cableado' )[0]
                listaRanks=[]
                for f in distribucion.getFeatures():
                    listaRanks.append(f["rank"])
                rankMax= max(listaRanks)

                for i in range(1,rankMax+1):
                    sqlCore="""select
                                a.destino,
                                a.nombre,
                                a.tubo_numero,
                                a.fibra
                            from
                                carta_empalmes_distribucion a
                            where
                                a.destino not like 'LIBRE' and
                                a.destino not like 'RESERVA' and
                                a.destino not like 'D-%' and
                                length(a.nombre)="""+str(10+(i*3))

                    sqlUpdate="""UPDATE splitters_peanas
                                SET cto=aa.destino
                                FROM (%s) aa
                                WHERE nom_fib_dist=concat(aa.nombre,', T',aa.tubo_numero,', F',aa.fibra);"""

                    if i == 1:

                        ejecutaSQL(nombreBBDD, host, usuario,password,sqlUpdate %sqlCore)

                    else:
                        for ii in range(1,i):
                            aliasSQL=string.ascii_lowercase[ii]
                            sqlAnid=""" select
                                    origen"""+str(ii)+""".destino,
                                    """+aliasSQL+""".nombre,
                                    """+aliasSQL+""".tubo_numero,
                                    """+aliasSQL+""".fibra
                                from
                                    (%s) origen"""+str(ii)+""",
                                    carta_empalmes_distribucion """+aliasSQL+"""
                                where
                                    concat(origen"""+str(ii)+""".nombre,', T',origen"""+str(ii)+""".tubo_numero,', F',origen"""+str(ii)+""".fibra)="""+aliasSQL+""".destino """
                            sql1=sqlAnid % sqlCore
                            for iii in range(1,ii):
                                aliasSQL2=string.ascii_lowercase[ii+1]
                                sqlAnid2=""" select
                                    origen"""+str(iii)+""".destino,
                                    """+aliasSQL2+""".nombre,
                                    """+aliasSQL2+""".tubo_numero,
                                    """+aliasSQL2+""".fibra
                                from
                                    (%s) origen"""+str(iii)+""",
                                    carta_empalmes_distribucion """+aliasSQL2+"""
                                where
                                    concat(origen"""+str(iii)+""".nombre,', T',origen"""+str(iii)+""".tubo_numero,', F',origen"""+str(iii)+""".fibra)="""+aliasSQL2+""".destino"""
                                sql1=sqlAnid2 % sql1

                        ejecutaSQL(nombreBBDD, host, usuario,password, sqlUpdate % sql1)
                sqlOrd = """create table splitters_peanas2 as select * from splitters_peanas order by nombre_peana, splitter_num, patilla_splitter asc;
                ALTER TABLE splitters_peanas RENAME TO "table_old";
                ALTER TABLE splitters_peanas2 RENAME TO splitters_peanas;
                DROP TABLE "table_old";"""
                ejecutaSQL(nombreBBDD, host, usuario,password, sqlOrd)
                capasNSP('VARIOS',host, nombreBBDD,usuario, password, 'splitters_peanas', 'Tr-Fusiones Splitters Troncal')
                relaciones('Tr-Fusiones Splitters Troncal', 'SP 1er Nivel', 'nombre_peana', 'nombre', 'id_pe_fus','pe_fus')
                actualizaNombre(nombreBBDD, host, usuario,password)
                relaciones('Tr-Carta de Empalmes', 'Tr-Cableado', 'nombre', 'nombre', 'id_tr_ce','tr_ce')
                capasNSP('VARIOS',host, nombreBBDD,usuario, password, 'cpd_gpon', 'Tarjetas GPON')
                relaciones('Tarjetas GPON', 'CPD', 'gid', 'id', 'id_cpd_gpon','cpd_gpon')

            if checkEtiq:
                mensaje = mensaje+', etiquetado'
                letra='T'
                nombreCampo='derivador'
                etiquetado(letra, nombreCampo,'troncal','peanas','carta_empalmes_troncal','cajas_empalme_troncal', 'Tr')

                sqlDrop = """drop table if exists etiquetado_peanas; """

                sqlEtiquetadoFus = """ create table etiquetado_peanas as select distinct
                                                row_number() over() id,
                                                a.nombre_peana,
                                                string_agg( fus_pat,'/' order by fus_pat asc nulls last) etiqueta,
                                                st_x(a.geom) x,
                                                st_y(a.geom) y,
                                                a.geom
                                            from
                                                (select
                                                    p.geom,
                                                    sp.nombre_peana,
                                                        concat(
                                                            'FT:',sp.fibra_troncal,
                                                            '||SP:',sp.splitter_num,
                                                            '||PAT:',patilla_splitter,
                                                            '||FIB_DIST:',sp.nom_fib_dist
                                                        ) fus_pat

                                                from
                                                    splitters_peanas sp,
                                                    peanas p
                                                where
                                                    p.nombre=sp.nombre_peana and not
                                                    sp.nom_fib_dist = 'LIBRE'
                                                order by
                                                    split_part(sp.nombre_peana::text, '-',3)::integer,
                                                    sp.splitter_num,
                                                    sp.patilla_splitter) a
                                            group by
                                                a.nombre_peana,
                                                a.geom;"""
                ejecutaSQL(nombreBBDD, host, usuario,password, sqlDrop)
                ejecutaSQL(nombreBBDD, host, usuario,password, sqlEtiquetadoFus)
                qmlEtiquetas = home+"""/.qgis2/python/plugins/GeoFibra/estilos/etiquetado_peanas.qml"""
                capas('TRONCAL',host,nombreBBDD, usuario, password, 'etiquetado_peanas', 'Tr-Etiquetado Splitters', qmlEtiquetas)


            widget = iface.messageBar().createMessage(u"Tareas Realizadas: ", mensaje)
            iface.messageBar().pushWidget(widget, QgsMessageBar.INFO)
        else:
            widget = iface.messageBar().createMessage(u"INFO", 'Hay errores.')
            iface.messageBar().pushWidget(widget, QgsMessageBar.WARNING)

#Funcion para añadir modelos de cables
    def addModel(self,marca, modelo, fibrasTot, tubos, fibTubos, conexion):
        if not marca  or not modelo or not fibrasTot or not tubos or not fibTubos:
            mensaje = u""
            if not marca:
                mensaje = mensaje+"Marca,"
            if not modelo:
                mensaje = mensaje+"Modelo,"
            if not fibrasTot:
                mensaje = mensaje+"Fibras Totales,"
            if not tubos:
                mensaje = mensaje+"Tubos,"
            if not fibTubos:
                mensaje = mensaje+"Fibras por Tubo,"
            if mensaje[-1:]==',':
                mensaje = mensaje[:-1]
                mensaje =mensaje+"."
            iface.messageBar().pushMessage(u"Atención. Completa los siguientes campos:",mensaje, QgsMessageBar.WARNING, 10)

        else:
            credenciales(conexion)
            sqlInsert = """insert into modelos_cable (marca, modelo, fib_totales, tubos, fib_tubos) values ('"""+marca+"""','"""+modelo+"""','"""+fibrasTot+"""','"""+tubos+"""','"""+fibTubos+"""') """ #% marca, modelo, fibrasTot, tubos, fibTubos
            ejecutaSQL(nombreBBDD,host,usuario,password,sqlInsert)
            capasNSP('VARIOS',host, nombreBBDD,usuario, password, 'modelos_cable', 'Modelos Cables')
            iface.messageBar().pushMessage("INFO", u'Modelo añadido.', level=QgsMessageBar.INFO)

#Funcion que ejecuta las funciones de longitudes de cable.
    def calculaDistancias(self,checkTr, checkDs, conexion):
        credenciales(conexion)

        nombreCapaS = """ """
        nombreTablaCables = """ """
        nombreTablaCajas = """ """
        nombreTOC = """ """

        if checkTr:
            nombreCapaS="""troncal_peana"""
            nombreTablaCables = """troncal"""
            nombreTablaCajas = """peanas"""
            nombreTOC = """Tr-Distancias"""
            cortaCables(nombreCapaS,nombreTablaCables,nombreTablaCajas, nombreTOC)

        if checkDs:
            nombreCapaS="""dist_cto"""
            nombreTablaCables = """distribucion"""
            nombreTablaCajas = """cto"""
            nombreTOC = """Dist-Distancias"""
            cortaCables(nombreCapaS,nombreTablaCables,nombreTablaCajas, nombreTOC)

        if  not checkTr and not checkDs:
            iface.messageBar().pushMessage(u"Atención",u"Elige una opción.", QgsMessageBar.INFO, 10)

#Funcion que determina la existencia o no de elementos en las cajas de empalmes de distribucion
    def compruebaCajasEmpalmeD(self,fid):

        layer = QgsMapLayerRegistry.instance().mapLayersByName( 'Dist-Cajas Empalme' )[0]
        if layer.featureCount() == 0:
            self.dockwidget.checkBox_sinCE.setEnabled(True)
        else:
            self.dockwidget.checkBox_sinCE.setEnabled(False)

#Funcion que determina la existencia o no de elementos en las cajas de empalmes de troncal
    def compruebaCajasEmpalmeTr(self,fid):
        layer = QgsMapLayerRegistry.instance().mapLayersByName( 'Tr-Cajas Empalme' )[0]

        if layer.featureCount() == 0:
            self.dockwidget.checkBox_sinCE_tr.setEnabled(True)
        else:
            self.dockwidget.checkBox_sinCE_tr.setEnabled(False)









    #--------------------------------------------------------------------------

    def onClosePlugin(self):
        """Cleanup necessary items here when plugin dockwidget is closed"""

        #print "** CLOSING GeoFibra"

        # disconnects
        self.dockwidget.closingPlugin.disconnect(self.onClosePlugin)

        # remove this statement if dockwidget is to remain
        # for reuse if plugin is reopened
        # Commented next statement since it causes QGIS crashe
        # when closing the docked window:
        # self.dockwidget = None

        self.pluginIsActive = False


    def unload(self):
        """Removes the plugin menu item and icon from QGIS GUI."""

        #print "** UNLOAD GeoFibra"

        for action in self.actions:
            self.iface.removePluginMenu(
                self.tr(u'&GeoFibra'),
                action)
            self.iface.removeToolBarIcon(action)
        # remove the toolbar
        del self.toolbar

    #--------------------------------------------------------------------------

    def run(self):
        """Run method that loads and starts the plugin"""
        actConex(self)
        self.dockwidget.actualizaLista.clicked.connect(self.actConex2)
        self.dockwidget.botonCambiaEstilo.clicked.connect(self.cambiaEstilo)
        self.dockwidget.botonCambiaEstiloNormal.clicked.connect(self.cambiaEstiloNormal)


        self.dockwidget.pushButton_errorAd.setEnabled(False)
        self.dockwidget.pushButton_errorAt.setEnabled(False)
        self.dockwidget.pushButton_RevLin.setEnabled(False)

        try:
            cajasEmpDis = QgsMapLayerRegistry.instance().mapLayersByName( 'Dist-Cajas Empalme' )[0]
            cajasEmpTr = QgsMapLayerRegistry.instance().mapLayersByName( 'Tr-Cajas Empalme' )[0]
            cajasEmpDis.featureAdded.connect(self.compruebaCajasEmpalmeD)
            cajasEmpDis.featureDeleted.connect(self.compruebaCajasEmpalmeD)
            cajasEmpTr.featureAdded.connect(self.compruebaCajasEmpalmeTr)
            cajasEmpTr.featureDeleted.connect(self.compruebaCajasEmpalmeTr)
            if cajasEmpDis.featureCount() == 0:
                self.dockwidget.checkBox_sinCE.setEnabled(True)
            else:
                self.dockwidget.checkBox_sinCE.setEnabled(False)
            
            if cajasEmpTr.featureCount() == 0:
                self.dockwidget.checkBox_sinCE_tr.setEnabled(True)
            else:
                self.dockwidget.checkBox_sinCE_tr.setEnabled(False)
        except:
            pass

        if not self.pluginIsActive:
            self.pluginIsActive = True

            #print "** STARTING GeoFibra"

            # dockwidget may not exist if:
            #    first run of plugin
            #    removed on close (see self.onClosePlugin method)
            if self.dockwidget == None:
                # Create the dockwidget (after translation) and keep reference
                self.dockwidget = GeoFibraDockWidget()

            # connect to provide cleanup on closing of dockwidget
            self.dockwidget.closingPlugin.connect(self.onClosePlugin)

            # show the dockwidget
            # TODO: fix to allow choice of dock location
            self.iface.addDockWidget(Qt.RightDockWidgetArea, self.dockwidget)
            self.dockwidget.show()
