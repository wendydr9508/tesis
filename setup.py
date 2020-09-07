import Controladora as _Class
import rdflib as _rdf
from rdflib.store import Store
import sys
import os as oss
import PySide2 
from PySide2.QtWidgets import QApplication, QDialog, QLineEdit, QPushButton, QVBoxLayout, QAction
from PySide2.QtWidgets import *
import sqlalchemy as _sqla

from PySide2.QtCore import Qt, QAbstractTableModel, QModelIndex
from PySide2.QtGui import QColor

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.pyplot                  import Figure
import networkx as nx
import matplotlib.pyplot as plt
from urllib.parse import quote as _pct_encoded    
from utils import *
import utils as util

class Form_Ver_ONTO(QDialog):
    def __init__(self, parent=None):
        super(Form_Ver_ONTO, self).__init__(parent)
        self.setGeometry(150, 50, 1000, 700)
        
        self.textbox = QTextBrowser()
        
        self.edit = QLineEdit("SELECT * WHERE { ?s ?p ?o} ")
        
        self.g = _Class.run_code(util._DATOS_EDITADOS())
        cant_triples = self.g.__len__()
        self.setWindowTitle("Datos Obtenidos: " + str(cant_triples) + " tripletas")

        self.menuBar= QMenuBar()
        menubar = self.menuBar
        tablas_menu = menubar.addAction('Ver gráfico')
        tablas_menu.triggered.connect(self.plot)
        save_rdf= menubar.addAction('Salvar archivo')
        ejecutar_query= menubar.addAction('Realizar consulta')
        ejecutar_query.triggered.connect(self.consultar)

        self.rdf_text=self.g.serialize(destination= None, format="turtle")
        self.rdf_TB= QTextBrowser()
        string= self.rdf_text.decode('utf-8')
        self.rdf_TB.setText(string)

        save_rdf.triggered.connect(self.save)


        self.table_view = QTableView()
        modelo=Modelo_Tabla_Suj_Pred_Obj(self.g)
        self.table_view.setModel(modelo)
        self.table_view.model = self.g.subject_predicates(object=None)
        self.layout1 = QVBoxLayout(self)  # Top master layout
        self.layout2 = QHBoxLayout()

        self.layout2.addWidget(self.table_view)

        self.layout1.addWidget(self.menuBar)
        self.layout1.addWidget(self.edit)
       
        self.layout2.addWidget(self.rdf_TB)
        self.layout1.addLayout(self.layout2)
        self.setLayout(self.layout1)

    def consultar(self):
        query=self.edit.text()
        result=_Class.Consulta(grafo= self.g, query = query)
        string=''
        for a in result:
            #a[_rdf.Variable]
            string= string + str(a) + '\n'
            print(a)
        self.textbox.setText(string)
        self.layout1.addWidget(self.textbox)
    def save(self):
        save_file_dialog = QFileDialog()
        name= save_file_dialog.getSaveFileName(self, 'Guardar archivo',filter='*.ttl')[0]
        #save_file_dialog.setFilter('*.ttl')
        try:
            file=open( name, 'w')
            file.write(self.rdf_text.decode('utf-8'))
            file.close()
        except: 
            pass
    def plot(self):
        G = nx.DiGraph()
        for u,v, k in self.g:
            G.add_edge(u , k ,title= v.split('#')[1])
        self.g.close()
        # Plot Networkx instance of RDF Graph
        pos = nx.spring_layout(G , scale=2)
        edge_labels = nx.get_edge_attributes(G, 'title')
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color= 'b')
        options= { 'node_color': 'blue',    'node_size': 200,   'width': 3, 'scale':2 , 'edge_labels' : edge_labels,
        'label_pos': 0.5, 'font_size' : 10, 'font_color': 'k', 'font_family' : 'sans-serif' ,  'font_weight': 'normal',
        'alpha': 1.0, 'bbox': None, 'ax' : None, 'rotate' : True,    }

        nx.draw(G, pos, with_labels=True, **options)
        plt.show()

class Form_Preprocesamiento(QDialog):

    def __init__(self, parent=None):
        super(Form_Preprocesamiento, self).__init__(parent)
        self.data =  _Class.mostrar_identificadores()
        self.setGeometry(400, 200, 500, 500)
        self.setWindowTitle("Preprocesamiento de la Base de Datos")
        self.edit = QLineEdit("Para escribir algo aqui")
        #self.button = QPushButton("Mostrar Gráfico RDF" )
        self.table_view=None
        self.btn=None

        exitAction = QAction('Exit', self)
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit application')
        exitAction.triggered.connect(self.close)
        selectAction= QAction('Select', self)
        #selectAction.triggered.connect(self._action_select_DB)

        
        self.menuBar= QMenuBar()
        menubar = self.menuBar
        tablas_menu = menubar.addAction('Tablas')
        tablas_menu.triggered.connect(self._action_show_tables_names)
        prop_menu=menubar.addAction('&Propiedades')
        prop_menu.triggered.connect(self._action_show_prop)
        rel_menu = menubar.addAction('&Relaciones')
        rel_menu.triggered.connect(self._action_show_rel)
        #mostrar_menu=menubar.addAction("Mostrar Gráfico RDF")
        #mostrar_menu.triggered.connect(self.greetings)
        #Relac_Menu.addAction(selectAction)
        #Prop_Menu.addAction(exitAction)
        
        
        #self.statusBar= QStatusBar()
        #self.statusBar.showMessage('Ready')
        # Create layout and add widgets
        self.layout = QVBoxLayout()
        self.layout.addWidget(self.menuBar)
        #self.layout.addWidget(toolbar)
        #self.layout.addWidget(self.button)
        #self.layout.addWidget(self.edit)
        #self.layout.addWidget(self.statusBar)
        
        
        # Set dialog layout
        self.setLayout(self.layout)
        # Add button signal to greetings slot
        #self.button.clicked.connect(self.greetings)
        
    # Greets the user
    def greetings(self):
        self._mostrar_onto()
        #initial_form.OCULTAR
        

    def _action_show_tables_names(self):
        self.layout.removeWidget(self.table_view)
        self.layout.removeWidget(self.btn)
        self.btn=QPushButton("Guardar Cambios")
        self.btn.clicked.connect(self.greetings)
        
        self.model = Modelo_Tabla_Personalizado('Tablas' , self.data[0])
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.layout.addWidget(self.table_view)
        self.layout.addWidget(self.btn)

    def _action_show_prop(self):
        self.layout.removeWidget(self.table_view)
        self.layout.removeWidget(self.btn)
        self.btn=QPushButton("Guardar Cambios")
        self.btn.clicked.connect(self.greetings)
        
        self.model = Modelo_Tabla_Personalizado('Propiedades' , self.data[1])
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        
        self.layout.addWidget(self.table_view)
        self.layout.addWidget(self.btn)

    def _action_show_rel(self):
        self.layout.removeWidget(self.table_view)
        self.layout.removeWidget(self.btn)
        self.btn=QPushButton("Guardar Cambios")
        self.btn.clicked.connect(self.greetings)

        self.model = Modelo_Tabla_Personalizado('Relaciones' , self.data[2]) 
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.layout.addWidget(self.table_view)
        self.layout.addWidget(self.btn)

    def _mostrar_onto(self):
        second_form= Form_Ver_ONTO(self)
        second_form.setModal(True)
        second_form.show()
        #_Class.run_code(_DATOS_EDITADOS)
       
if __name__ == '__main__':
   

    # Create the Qt Application
    
    app = QApplication(sys.argv)
    app.setStyle("plastique")
   
    # Create and show the form
    initial_form = Form_Preprocesamiento()
    initial_form.show()
    # Run the main Qt loop
    sys.exit(app.exec_())
    # Create widgets

