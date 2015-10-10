import sys
from PyQt4 import QtGui, QtCore

from pySecMaster import maintenance, data_download


class MainApp(QtGui.QMainWindow):

    def __init__(self):
        super(MainApp, self).__init__()

        self.initUI()

    def initUI(self):

        self.options = Options(self)
        self.setCentralWidget(self.options)

        # Enable the bottom status bar
        self.statusBar()

        # Build the File menu exit functionality
        exit_act = QtGui.QAction(QtGui.QIcon('images/Solar System Black.png'),
                                 '&Exit', self)
        exit_act.setShortcut('Ctrl+Q')
        exit_act.setStatusTip('Exit Application')
        exit_act.triggered.connect(QtGui.qApp.quit)

        # Create the menu
        menubar = self.menuBar()
        file_menu = menubar.addMenu('&File')
        file_menu.addAction(exit_act)

        # # Create a Toolbar with an application exit
        # toolbar = self.addToolBar('Exit')
        # toolbar.addAction(exit_act)

        self.resize(300, 150)
        self.center()
        self.setWindowTitle('pySecMaster')
        self.setWindowIcon(QtGui.QIcon('images/Molecule 5 Black.png'))
        self.show()

    def center(self):
        # Reposition the widget frame to the center of the main screen

        center = QtGui.QDesktopWidget().availableGeometry().center()
        frame = self.frameGeometry()
        frame.moveCenter(center)
        self.move(frame.topLeft())

    def closeEvent(self, event):
        # Message box requiring user consent to close program

        reply = QtGui.QMessageBox.question(self, 'Confirm Exit',
                                           'Are you sure you want to exit?',
                                           QtGui.QMessageBox.Yes |
                                           QtGui.QMessageBox.No,
                                           QtGui.QMessageBox.No)
        if reply == QtGui.QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()


class Options(QtGui.QWidget):

    def __init__(self, parent):
        super(Options, self).__init__(parent)

        self.initOptions()

    def initOptions(self):

        form_layout = QtGui.QFormLayout()

        self.txt_ed_db_name = QtGui.QLineEdit(self)
        self.txt_ed_db_name.setText('pySecMaster.db')   # setPlaceholderText
        self.txt_ed_db_local = QtGui.QLineEdit(self)
        self.txt_ed_db_local.setText('C:/Users/Josh/Desktop/')

        self.cmb_dwnld_source = QtGui.QComboBox(self)
        self.cmb_dwnld_source.addItems(['google_fin', 'quandl', 'all'])

        self.cmb_qndl_ticker_source = QtGui.QComboBox(self)
        self.cmb_qndl_ticker_source.addItems(['csidata', 'quandl'])
        self.cmb_googfin_ticker_source = QtGui.QComboBox(self)
        self.cmb_googfin_ticker_source.addItems(['csidata', 'quandl'])

        qndl_sel = ['all', 'wiki', 'us_only', 'us_main',
                    'wiki_and_us_main_goog']
        self.cmb_qndl_selection = QtGui.QComboBox(self)
        self.cmb_qndl_selection.addItems(qndl_sel)
        self.cmb_qndl_selection.setCurrentIndex(qndl_sel.index('us_main'))
        googfin_sel = ['all', 'us_only', 'us_main', 'us_main_goog']
        self.cmb_googfin_selection = QtGui.QComboBox(self)
        self.cmb_googfin_selection.addItems(googfin_sel)
        self.cmb_googfin_selection.setCurrentIndex(googfin_sel.index('us_main'))

        form_layout.addRow('Database Name', self.txt_ed_db_name)
        form_layout.addRow('Database Location', self.txt_ed_db_local)
        form_layout.addRow('Download Source', self.cmb_dwnld_source)
        form_layout.addRow('Quandl Ticker Source', self.cmb_qndl_ticker_source)
        form_layout.addRow('Google Fin Ticker Source',
                           self.cmb_googfin_ticker_source)
        form_layout.addRow('Quandl Data Selection', self.cmb_qndl_selection)
        form_layout.addRow('Google Fin Data Selection',
                           self.cmb_googfin_selection)

        # Button: OK
        btn_ok = QtGui.QPushButton('Run', self)
        btn_ok.setToolTip('Run the program')
        btn_ok.resize(btn_ok.sizeHint())
        btn_ok.setDefault(True)
        btn_ok.clicked.connect(self.run_pysecmaster)
        btn_ok.clicked.connect(QtCore.QCoreApplication.instance().quit)

        # Button: Quit
        btn_quit = QtGui.QPushButton('Quit', self)
        btn_quit.setToolTip('Quit the application')
        btn_quit.clicked.connect(QtCore.QCoreApplication.instance().quit)
        btn_quit.resize(btn_quit.sizeHint())

        # Sub Layout
        hbox = QtGui.QHBoxLayout()
        hbox.addStretch(1)
        hbox.addWidget(btn_ok)
        hbox.addWidget(btn_quit)

        # Main Layout
        form_layout.addRow(hbox)

        self.setLayout(form_layout)

    def run_pysecmaster(self):

        database_link = self.txt_ed_db_local.text() + self.txt_ed_db_name.text()

        maintenance(database_link,
                    self.cmb_qndl_ticker_source.currentText(),
                    self.cmb_googfin_ticker_source.currentText())
        data_download(database_link,
                      self.cmb_qndl_ticker_source.currentText(),
                      self.cmb_googfin_ticker_source.currentText(),
                      self.cmb_dwnld_source.currentText(),
                      self.cmb_qndl_selection.currentText(),
                      self.cmb_googfin_selection.currentText())


def main():

    app = QtGui.QApplication(sys.argv)
    ma = MainApp()
    sys.exit(app.exec_())


if __name__ == '__main__':

    main()
