import PySide2 
from PySide2.QtWidgets import QApplication, QDialog, QLineEdit, QPushButton, QVBoxLayout, QAction
from PySide2.QtWidgets import *
import sqlalchemy as _sqla

from PySide2.QtCore import Qt, QAbstractTableModel, QModelIndex
from PySide2.QtGui import QColor

_DATOS_EDIT={}
def _DATOS_EDITADOS():
    return _DATOS_EDIT

class Modelo_Tabla_Suj_Pred_Obj(QAbstractTableModel):
    def __init__(self, data=None):
        QAbstractTableModel.__init__(self)
        self.input_dates=[]
        self.load_data(data)
    def load_data(self, data):
        self.column_count = 3
        for u,v, k in data:
            self.input_dates.append((u , v.split('#')[1], k ))
        self.row_count = len(self.input_dates)
    def headerData(self, section, orientation, role):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return ("Sujeto", "Predicado", "Objeto")[section]
        else:
            return "{}".format(section)   
    def data(self, index, role=Qt.DisplayRole):
        column = index.column()
        row = index.row()

        if role == Qt.DisplayRole or role == Qt.EditRole:
            if column == 0:
                date = "{}".format(self.input_dates[row][0])
                return date
            elif column == 1:
                return "{}".format(self.input_dates[row][1])
            elif column == 2:
                return "{}".format(self.input_dates[row][2])
        elif role == Qt.BackgroundRole:
            return QColor(Qt.white)
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignRight
        return None
    def rowCount(self, parent=QModelIndex()):
        return self.row_count

    def columnCount(self, parent=QModelIndex()):
        return self.column_count
class Modelo_Tabla_Personalizado(QAbstractTableModel):
    def __init__(self, descripcion= None, data=None):
        QAbstractTableModel.__init__(self)
        self.input_magnitudes=[]
        self.input_dates=[]
        self.tab2Rel=[]
        self.load_data(descripcion, data)

    def load_data(self, descripcion, data):
        if(descripcion=='Tablas'):
            self.input_dates = data
            self.column_count = 1
            self.row_count = len(self.input_dates)
        elif(descripcion== "Propiedades"):
            for froz_set in data:
                for tab, prop in froz_set:
                    self.input_dates.append(tab)
                    self.input_magnitudes.append(prop)
            self.column_count = 2
            self.row_count = len(self.input_dates)
        elif(descripcion=="Relaciones"):
            for froz_set in data:
                for tab, rel, tab2 in froz_set:
                    self.input_dates.append(tab)
                    self.input_magnitudes.append(rel)
                    self.tab2Rel.append(tab2)
            self.column_count = 3
            self.row_count = len(self.input_dates)

    def headerData(self, section, orientation, role):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return ("Tabla", "Propiedad", "Tabla")[section]
        else:
            return "{}".format(section)

    def data(self, index, role=Qt.DisplayRole):
        column = index.column()
        row = index.row()

        if role == Qt.DisplayRole or role == Qt.EditRole:
            if column == 0:
                date = "{}".format(self.input_dates[row])
                return date
            elif column == 1:
                return "{}".format(self.input_magnitudes[row])
            elif column == 2:
                return "{}".format(self.tab2Rel[row])
        elif role == Qt.BackgroundRole:
            return QColor(Qt.white)
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignRight

        return None

    def rowCount(self, parent=QModelIndex()):
        return self.row_count

    def columnCount(self, parent=QModelIndex()):
        return self.column_count
    
    def flags(self, index):
        flags = super(self.__class__,self).flags(index)

        flags |= Qt.ItemIsEditable
        flags |= Qt.ItemIsSelectable
        flags |= Qt.ItemIsEnabled
        flags |= Qt.ItemIsDragEnabled
        flags |= Qt.ItemIsDropEnabled

        return flags
    
    def setData(self, index, value, role=Qt.EditRole):
        row = index.row()
        col = index.column()
        to_change=None
        #if col == 0: 
            #to_change = self.input_dates[row]
            #self.seter(to_change, value)
            #self.input_dates[row]=value
        if col == 1:
            to_change=self.input_magnitudes[row]
            self.seter(to_change,value)
            self.input_magnitudes[row]=value
        #elif col == 2:
            #to_change = self.tab2Rel[row]
            #self.seter(to_change, value)
            #self.tab2Rel[row]=value
        return True
    def seter(self, to_change, value):
        if not to_change in _DATOS_EDIT.values():
            _DATOS_EDIT[to_change]= value
        else:
            for k in _DATOS_EDIT:
                if _DATOS_EDIT[k]==to_change:
                    _DATOS_EDIT[k]=value