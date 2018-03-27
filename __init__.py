# -*- coding: utf-8 -*-
"""
/***************************************************************************
 GeoFibra
                                 A QGIS plugin
 Asistente de Diseño de Redes FTTH.
                             -------------------
        begin                : 2017-12-27
        copyright            : (C) 2017 by Luis Miguel Royo Perez
        email                : admin@inisig.com; luis.miguel.royo@gmail.com
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load GeoFibra class from file GeoFibra.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    #
    from .geofibra import GeoFibra
    return GeoFibra(iface)
