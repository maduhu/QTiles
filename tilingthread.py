# -*- coding: utf-8 -*-
"""Tiling module that actually renders the tiles."""
#******************************************************************************
#
# QTiles
# ---------------------------------------------------------
# Generates tiles from QGIS project
#
# Copyright (C) 2012-2013 Alexander Bruy (alexander.bruy@gmail.com)
#
# This source is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 2 of the License, or (at your option)
# any later version.
#
# This code is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# A copy of the GNU General Public License is available on the World Wide Web
# at <http://www.gnu.org/licenses/>. You can also obtain it by writing
# to the Free Software Foundation, 51 Franklin Street, Suite 500 Boston,
# MA 02110-1335 USA.
#
#******************************************************************************

import os
import time
import zipfile
from string import Template

from PyQt4.QtCore import *
from PyQt4.QtGui import *

from qgis.core import *

from tile import Tile

from mbutils import (
    optimize_connection,
    mbtiles_setup,
    json,
    mbtiles_connect,
    optimize_database)
import sqlite3

import resources_rc
from pydev.pydevd import settrace


class TilingThread(QThread):
    """Threaded tiler implementation."""
    rangeChanged = pyqtSignal(str, int)
    updateProgress = pyqtSignal()
    processFinished = pyqtSignal()
    processInterrupted = pyqtSignal()

    def __init__(self, layers, extent, minZoom, maxZoom, width, height,
                 outputPath, rootDir, antialiasing, tmsConvention, mapUrl,
                 viewer):
        QThread.__init__(self, QThread.currentThread())
        self.mutex = QMutex()
        self.stopMe = 0
        self.interrupted = False

        self.layers = layers
        self.extent = extent
        self.minZoom = minZoom
        self.maxZoom = maxZoom
        self.output = outputPath
        self.width = width
        if rootDir:
            self.rootDir = rootDir
        else:
            "tileset_%s" % unicode(
                time.time()).split(".")[0]

        self.antialias = antialiasing
        self.tmsConvention = tmsConvention

        self.mapurl = mapUrl
        self.viewer = viewer

        self.interrupted = False
        self.tiles = []

        myRed = QgsProject.instance().readNumEntry(
            "Gui", "/CanvasColorRedPart", 255)[0]
        myGreen = QgsProject.instance().readNumEntry(
            "Gui", "/CanvasColorGreenPart", 255)[0]
        myBlue = QgsProject.instance().readNumEntry(
            "Gui", "/CanvasColorBluePart", 255)[0]

        if int(QT_VERSION_STR[2]) >= 8:
            self.color = QColor(myRed, myGreen, myBlue)
        else:
            self.color = qRgb(myRed, myGreen, myBlue)

        self.image = QImage(width, height, QImage.Format_ARGB32_Premultiplied)

        self.projector = QgsCoordinateTransform(
            QgsCoordinateReferenceSystem("EPSG:4326"),
            QgsCoordinateReferenceSystem("EPSG:3395")
        )

        self.scaleCalc = QgsScaleCalculator()
        self.scaleCalc.setDpi(self.image.logicalDpiX())
        self.scaleCalc.setMapUnits(
            QgsCoordinateReferenceSystem("EPSG:3395").mapUnits())

        self.labeling = QgsPalLabeling()
        self.renderer = QgsMapRenderer()
        self.renderer.setOutputSize(
            self.image.size(), self.image.logicalDpiX())
        self.renderer.setDestinationCrs(
            QgsCoordinateReferenceSystem("EPSG:3395"))
        self.renderer.setProjectionsEnabled(True)
        self.renderer.setLabelingEngine(self.labeling)
        self.renderer.setLayerSet(self.layers)
        self.con = None  # for mbtiles
        self.cur = None  # for mbtiles

    def mode(self):
        """Find out if we are creating tiledir, zip or mbtiles."""
        if self.output.isDir():
            return "TILE_DIR"
        file_extension = os.path.splitext(str(self.output))
        if file_extension == ".ZIP":
            return "ZIP"
        else:
            return "MBTILES"

    def setup_mbtiles(self):
        """Prepare for rendering to mbtiles."""
        path = str(self.output.absoluteFilePath())
        self.con = mbtiles_connect(path)
        directory_path = os.path.dirname(str(self.output))
        self.cur = self.con.cursor()
        optimize_connection(self.cur)
        mbtiles_setup(self.cur)
        try:
            metadata = json.load(
                open(os.path.join(directory_path, 'metadata.json'), 'r'))
            for name, value in metadata.items():
                self.cur.execute(
                    'insert into metadata (name, value) values (?, ?)',
                    (name, value))
                #logger.info('metadata from metadata.json restored')
        except IOError:
            #logger.warning('metadata.json not found')
            pass

    def run(self):
        """Thread start implementation."""
        settrace('localhost', port=6789, stdoutToServer=True,
            stderrToServer=True)
        self.mutex.lock()
        self.stopMe = 0
        self.mutex.unlock()
        mode = self.mode()
        self.zip = None
        # prepare output
        if mode == "TILE_DIR":
            self.tmp = None
            if self.mapurl:
                self.writeMapurlFile()

            if self.viewer:
                self.writeLeafletViewer()
        elif mode == "ZIP":
            self.zip = zipfile.ZipFile(
                unicode(self.output.absoluteFilePath()), "w")
            self.tmp = QTemporaryFile()
            self.tmp.setAutoRemove(False)
            self.tmp.open(QIODevice.WriteOnly)
            self.tempFileName = self.tmp.fileName()
        else:  # mode == "MBTILES":
            self.setup_mbtiles()

        self.rangeChanged.emit(self.tr("Searching tiles..."), 0)

        useTMS = 1
        if self.tmsConvention:
            useTMS = -1

        self.countTiles(Tile(0, 0, 0, useTMS))

        if self.interrupted:
            #del self.tiles[:]
            #self.tiles = None

            if self.zip is not None and mode == "ZIP":
                self.zip.close()
                self.zip = None

                self.tmp.close()
                self.tmp.remove()
                self.tmp = None
            if self.con is not None and mode == "MBTILES":
                optimize_database(self.con)

            self.processInterrupted.emit()

        self.rangeChanged.emit(self.tr("Rendering: %v from %m (%p%)"), len(self.tiles))

        self.painter = QPainter()
        if self.antialias:
            self.painter.setRenderHint(QPainter.Antialiasing)

        for t in self.tiles:
            self.render(t)

            self.updateProgress.emit()

            self.mutex.lock()
            s = self.stopMe
            self.mutex.unlock()
            if s == 1:
                self.interrupted = True
                break

        if self.zip is not None and mode == "ZIP":
            self.zip.close()
            self.zip = None

        if self.con is not None and mode == "MBTILES":
            optimize_database(self.con)

        if not self.interrupted:
            self.processFinished.emit()
        else:
            self.processInterrupted.emit()

    def stop(self):
        """Runs when the process completes."""
        self.cur = None
        self.mutex.lock()
        self.stopMe = 1
        self.mutex.unlock()

        QThread.wait(self)

    def writeMapurlFile(self):
        """Write the map url file."""
        filePath = "%s/%s.mapurl" % (
            self.output.absoluteFilePath(), self.rootDir)
        tileServer = "tms" if self.tmsConvention else "google"
        with open(filePath, "w") as mapurl:
            mapurl.write("%s=%s\n" % ("url", self.rootDir + "/ZZZ/XXX/YYY.png"))
            mapurl.write("%s=%s\n" % ("minzoom", self.minZoom))
            mapurl.write("%s=%s\n" % ("maxzoom", self.maxZoom))
            mapurl.write("%s=%f %f\n" % ("center", self.extent.center().x(),
                                         self.extent.center().y()))
            mapurl.write("%s=%s\n" % ("type", tileServer))

    def writeLeafletViewer(self):
        """Create the leaflet viewer."""
        templateFile = QFile(":/resources/viewer.html")
        if templateFile.open(QIODevice.ReadOnly | QIODevice.Text):
            viewer = MyTemplate(unicode(templateFile.readAll()))
            tilesDir = "%s/%s" % (self.output.absoluteFilePath(), self.rootDir)
            useTMS = "true" if self.tmsConvention else "false"
            substitutions = {"tilesdir": tilesDir,
                             "tilesetname": self.rootDir,
                             "tms": useTMS,
                             "centerx": self.extent.center().x(),
                             "centery": self.extent.center().y(),
                             "avgzoom": (self.maxZoom + self.minZoom) / 2,
                             "maxzoom": self.maxZoom}

            filePath = "%s/%s.html" % (
                self.output.absoluteFilePath(), self.rootDir)
            with open(filePath, "w") as fOut:
                fOut.write(viewer.substitute(substitutions))

            templateFile.close()

    def countTiles(self, tile):
        """Count how many tiles have been created geven a tile.

        :param tile: A tile object.
        """
        if self.interrupted or not self.extent.intersects(tile.toRectangle()):
            return

        if self.minZoom <= tile.z and tile.z <= self.maxZoom:
            self.tiles.append(tile)

        if tile.z < self.maxZoom:
            for x in xrange(2 * tile.x, 2 * tile.x + 2, 1):
                for y in xrange(2 * tile.y, 2 * tile.y + 2, 1):
                    self.mutex.lock()
                    s = self.stopMe
                    self.mutex.unlock()
                    if s == 1:
                        self.interrupted = True
                        return

                    subTile = Tile(x, y, tile.z + 1, tile.tms)
                    self.countTiles(subTile)

    def render(self, tile):
        """Render a tile to an image.

        :param tile: A valid tile instance.
        """
        self.renderer.setExtent(self.projector.transform(tile.toRectangle()))
        scale = self.scaleCalc.calculate(self.renderer.extent(), self.width)
        self.renderer.setScale(scale)
        self.image.fill(self.color)
        self.painter.begin(self.image)
        self.renderer.render(self.painter)
        self.painter.end()

        # save image
        path = "%s/%s/%s" % (self.rootDir, tile.z, tile.x)
        if self.mode() == "TILE_DIR":
            dirPath = "%s/%s" % (self.output.absoluteFilePath(), path)
            QDir().mkpath(dirPath)
            self.image.save("%s/%s.png" % (dirPath, tile.y), "PNG")
        elif self.mode() == "ZIP":
            self.image.save(self.tempFileName, "PNG")
            self.tmp.close()

            tilePath = "%s/%s.png" % (path, tile.y)
            self.zip.write(unicode(
                self.tempFileName), unicode(tilePath).encode("utf8"))
        else:  # MBTILES
            byte_array = QByteArray()
            buffer = QBuffer(byte_array)
            self.image.save(buffer, "PNG")

            try:
                self.cur.execute("""insert into tiles (zoom_level,
                    tile_column, tile_row, tile_data) values
                    (?, ?, ?, ?);""", (
                    tile.z, tile.x, tile.y, sqlite3.Binary(buffer.data())))
            except Exception, e:
                print e.message
            buffer.close()


class MyTemplate(Template):
    """Template class."""
    delimiter = "@"

    def __init__(self, templateString):
        """Constructor."""
        Template.__init__(self, templateString)
