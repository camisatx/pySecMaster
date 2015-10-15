import sys
from queue import Queue
from PyQt4 import QtGui, QtCore
import time

from pySecMaster import maintenance, data_download

__author__ = 'Josh Schertz'
__copyright__ = 'Copyright (C) 2015 Josh Schertz'
__description__ = 'An automated system to store and maintain financial data.'
__email__ = 'josh[AT]joshschertz[DOT]com'
__license__ = 'GNU AGPLv3'
__maintainer__ = 'Josh Schertz'
__status__ = 'Development'
__url__ = 'https://joshschertz.com/'
__version__ = '1.2'

'''
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''


class MainApp(QtGui.QMainWindow):

    def __init__(self):
        super(MainApp, self).__init__()

        # self.app_ui()

    def app_ui(self):

        # Enable the bottom status bar
        self.statusBar()

        # Build the File menu exit functionality
        exit_act = QtGui.QAction(QtGui.QIcon('images/Solar System Black.png'),
                                 '&Exit', self)
        exit_act.setShortcut('Ctrl+Q')
        exit_act.setStatusTip('Exit Application')
        exit_act.triggered.connect(self.confirm_close)

        # Create the menu
        menubar = self.menuBar()
        file_menu = menubar.addMenu('&File')
        file_menu.addAction(exit_act)

        # # Create a Toolbar with an application exit
        # toolbar = self.addToolBar('Exit')
        # toolbar.addAction(exit_act)

        # self.resize(300, 150)
        self.center()
        self.setWindowTitle('pySecMaster')
        self.setWindowIcon(QtGui.QIcon('images/Molecule 5 Black.png'))

        # Run the Options class, which will build all app buttons/features
        builder = AppBuilder(self)
        # central_widget = QtGui.QWidget()
        # central_widget.setLayout(builder.app_builder())
        self.setCentralWidget(builder)
        # self.show()

    def center(self):
        # Reposition the widget frame to the center of the main screen

        center = QtGui.QDesktopWidget().availableGeometry().center()
        frame = self.frameGeometry()
        frame.moveCenter(center)
        self.move(frame.topLeft())

    def closeEvent(self, event):
        # closeEvent method is called when the user clicks window close button

        self.confirm_close(event)

    def confirm_close(self, event=None):
        """ Popup message box requiring user consent to close program

        :param event: A Qt object that is only used via the closeEvent method
        """

        reply = QtGui.QMessageBox.question(self, 'Confirm Exit',
                                           'Are you sure you want to exit?',
                                           QtGui.QMessageBox.Yes |
                                           QtGui.QMessageBox.No,
                                           QtGui.QMessageBox.No)

        if event:
            # Request originated from the closeEvent method
            if reply == QtGui.QMessageBox.Yes:
                event.accept()
            else:
                event.ignore()
        else:
            # Request originated from a specific exit feature
            if reply == QtGui.QMessageBox.Yes:
                sys.exit()
            else:
                pass


class AppBuilder(QtGui.QWidget):

    def __init__(self, parent):
        super(AppBuilder, self).__init__(parent)

        self.app_builder()

    def app_builder(self):

        # Button and Combo Box creation ----------------------------------------
        # Set the database name
        self.txt_ed_db_name = QtGui.QLineEdit(self)
        self.txt_ed_db_name.setText('pySecMaster.db')   # setPlaceholderText

        # Set the database directory
        self.txt_ed_db_dir = QtGui.QLineEdit(self)
        # Button to open a search window to select the database directory
        btn_db_dir = QtGui.QPushButton(QtGui.QIcon('images/Open Folder.png'),
                                       '')
        btn_db_dir.setToolTip('Select directory')
        btn_db_dir.resize(btn_db_dir.sizeHint())
        btn_db_dir.clicked.connect(self.select_dir)
        # Build HBox for db_dir text box and directory search button
        hbox_db_dir = QtGui.QHBoxLayout()
        hbox_db_dir.addWidget(self.txt_ed_db_dir)
        hbox_db_dir.addWidget(btn_db_dir)

        # Combobox for source of downloaded data
        self.cmb_dwnld_source = QtGui.QComboBox(self)
        self.cmb_dwnld_source.addItems(['google_fin', 'quandl', 'all'])

        # Combobox for quandl and google fin data download selections (queries)
        qndl_sel = ['all', 'wiki', 'us_only', 'us_main',
                    'wiki_and_us_main_goog']
        self.cmb_qndl_selection = QtGui.QComboBox(self)
        self.cmb_qndl_selection.addItems(qndl_sel)
        self.cmb_qndl_selection.setCurrentIndex(qndl_sel.index('us_main'))
        googfin_sel = ['all', 'us_only', 'us_main', 'us_main_goog']
        self.cmb_googfin_selection = QtGui.QComboBox(self)
        self.cmb_googfin_selection.addItems(googfin_sel)
        self.cmb_googfin_selection.setCurrentIndex(googfin_sel.index('us_main'))

        # Combobox for quandl and google fin ticker source
        self.cmb_qndl_ticker_source = QtGui.QComboBox(self)
        self.cmb_qndl_ticker_source.addItems(['csidata', 'quandl'])
        self.cmb_googfin_ticker_source = QtGui.QComboBox(self)
        self.cmb_googfin_ticker_source.addItems(['csidata', 'quandl'])

        # Tab creation ---------------------------------------------------------
        tab_data = QtGui.QWidget()
        form_layout_tab_data = QtGui.QFormLayout(tab_data)
        form_layout_tab_data.addRow('Download Source',
                                    self.cmb_dwnld_source)
        form_layout_tab_data.addRow('Quandl Data Selection',
                                    self.cmb_qndl_selection)
        form_layout_tab_data.addRow('Google Fin Data Selection',
                                    self.cmb_googfin_selection)

        tab_tickers = QtGui.QWidget()
        form_layout_tab_tickers = QtGui.QFormLayout(tab_tickers)
        form_layout_tab_tickers.addRow('Quandl Ticker Source',
                                       self.cmb_qndl_ticker_source)
        form_layout_tab_tickers.addRow('Google Fin Ticker Source',
                                       self.cmb_googfin_ticker_source)

        tabs_options = QtGui.QTabWidget(self)
        tabs_options.addTab(tab_data, 'Data')
        tabs_options.addTab(tab_tickers, 'Tickers')

        # Create the text editor that will show the code output ----------------
        self.output = QtGui.QTextEdit()

        # Main Form creation ---------------------------------------------------
        form_layout = QtGui.QFormLayout()
        form_layout.addRow('Database Name', self.txt_ed_db_name)
        form_layout.addRow('Database Location', hbox_db_dir)
        form_layout.addRow(tabs_options)
        form_layout.addRow(self.output)

        # Bottom - Call to Action ----------------------------------------------
        # Button: Run
        self.btn_run = QtGui.QPushButton('Run', self)
        self.btn_run.setToolTip('Run the program')
        self.btn_run.resize(self.btn_run.sizeHint())
        self.btn_run.setDefault(True)
        # self.btn_run.clicked.connect(self.run_pysecmaster)
        self.btn_run.clicked.connect(self.process)
        # self.btn_run.clicked.connect(QtCore.QCoreApplication.instance().quit)

        # Button: Quit
        btn_quit = QtGui.QPushButton('Quit', self)
        btn_quit.setToolTip('Quit the application')
        btn_quit.clicked.connect(self.confirm_close)
        btn_quit.resize(btn_quit.sizeHint())

        # Build Sub Layout
        hbox = QtGui.QHBoxLayout()
        hbox.addStretch(1)
        hbox.addWidget(self.btn_run)
        hbox.addWidget(btn_quit)
        # Add Sub Layout to Main Layout
        form_layout.addRow(hbox)

        # Enable Main Layout ---------------------------------------------------
        self.setLayout(form_layout)

    def confirm_close(self):
        """ Popup message box requiring user consent to close program. """

        reply = QtGui.QMessageBox.question(self, 'Confirm Exit',
                                           'Are you sure you want to exit?',
                                           QtGui.QMessageBox.Yes |
                                           QtGui.QMessageBox.No,
                                           QtGui.QMessageBox.No)

        if reply == QtGui.QMessageBox.Yes:
            sys.exit()
        else:
            pass

    def onDataReady(self, string):

        # Write code output the the text editor 'output'
        cursor = self.output.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(str(string))
        self.output.ensureCursorVisible()

    def process(self):

        self.thread_worker = QtCore.QThread()
        self.worker = Worker()
        self.worker.dataReady.connect(self.onDataReady)

        self.worker.moveToThread(self.thread_worker)

        # Stops the thread after the worker is done. If you need to start it
        #   again, call thread.start()
        self.worker.finished.connect(self.thread_worker.quit)

        # # Calls the Worker process directly, but it's difficult to send data
        # #     to the worker object from the main gui thread.
        # self.thread_worker.started.connect(self.worker.processA)
        # self.thread_worker.finished.connect(main().app.exit)

        # Tell the thread to start working
        self.thread_worker.start()

        # Invoke the Worker process with the ability of safely communicating
        #   with the worker through signals and slots. Worker must already be
        #   running in order for the process to be invoked. If you need to use
        #   arguments for the worker process, add a "QtCore.Q_ARG(str, 'arg')"
        #   variable for each argument in the invokeMethod statement after
        #   the QueuedConnection item.
        # QtCore.Q_ARG(str, 'Hello'),
        # QtCore.Q_ARG(list, ['Hello', 0, 1]))
        QtCore.QMetaObject.invokeMethod(self.worker, 'pysecmaster',
                                        QtCore.Qt.QueuedConnection,
                                        QtCore.Q_ARG(str, self.txt_ed_db_dir.text()),
                                        QtCore.Q_ARG(str, self.txt_ed_db_name.text()),
                                        QtCore.Q_ARG(str, self.cmb_qndl_ticker_source.currentText()),
                                        QtCore.Q_ARG(str, self.cmb_googfin_ticker_source.currentText()),
                                        QtCore.Q_ARG(str, self.cmb_dwnld_source.currentText()),
                                        QtCore.Q_ARG(str, self.cmb_qndl_selection.currentText()),
                                        QtCore.Q_ARG(str, self.cmb_googfin_selection.currentText()))

        # if self.thread_worker.isRunning():
        #     self.btn_run.setEnabled(False)
        # else:
        #     self.btn_run.setEnabled(True)

    def select_dir(self):
        """ Opens a PyQt folder search. If a folder is selected, it will
        populate the db_dir text editor box.
        """

        db_dir = QtGui.QFileDialog.getExistingDirectory(self,
                                                        'Select Directory')
        if db_dir:
            self.txt_ed_db_dir.setText(db_dir)


class Worker(QtCore.QObject):
    finished = QtCore.pyqtSignal()
    dataReady = QtCore.pyqtSignal(str)

    @QtCore.pyqtSlot()
    def processA(self):
        print('Worker.processA()')
        self.finished.emit()

    @QtCore.pyqtSlot(str, list, list)
    def processB(self, foo, bar=None):
        print('Worker.processB()')
        for thing in bar:
            # lots of processing...
            self.dataReady.emit(['dummy', 'data'], {'dummy': ['data']})
        self.finished.emit()

    @QtCore.pyqtSlot(str, str, str, str, str, str, str)
    def pysecmaster(self, txt_ed_db_dir, txt_ed_db_name, cmb_qndl_ticker_source,
                    cmb_googfin_ticker_source, cmb_dwnld_source,
                    cmb_qndl_selection, cmb_googfin_selection):
        """
        Prepares the db link, and then calls the actual functions that
        operate the pySecMaster. Emits signals back to the main gui for
        further processing, using the dataReady process.
        """

        # # Create the QProcess object to be able to run the app externally
        # self.process = QtCore.QProcess(self)
        #
        # self.process.start(temp())
        #
        # # QProcess emits 'readyRead' when there is data to be read
        # self.process.readyRead.connect(self.dataReady)
        #
        # # Prevent running multiple instances; disable btn_run while running
        # self.process.started.connect(lambda: self.options.
        #                              btn_run.setEnabled(False))
        # self.process.finished.connect(lambda: self.options.
        #                               btn_run.setEnabled(True))

        # Handles db_dir's with and without the end backslash
        if txt_ed_db_dir[-1] == '\\':
            db_link = txt_ed_db_dir + txt_ed_db_name
        else:
            db_link = (txt_ed_db_dir + '\\' + txt_ed_db_name)

        self.dataReady.emit('Starting pySecMaster using the database %s '
                            'located at %s\n' % (txt_ed_db_name, txt_ed_db_dir))

        maintenance(db_link,
                    cmb_qndl_ticker_source,
                    cmb_googfin_ticker_source)
        data_download(db_link,
                      cmb_qndl_ticker_source,
                      cmb_googfin_ticker_source,
                      cmb_dwnld_source,
                      cmb_qndl_selection,
                      cmb_googfin_selection)

        self.dataReady.emit('Finished running the pySecMaster process\n')
        self.finished.emit()


class StdoutQueue(object):
    """
    This is a queue that acts like the default system standard output (stdout).
    """

    def __init__(self, queue):
        self.queue = queue

    def write(self, string):
        self.queue.put(string)

    def flush(self):
        sys.__stdout__.flush()


class Receiver(QtCore.QObject):
    """
    A QObject (to be run in a QThread) that sits waiting for data to come
    through a Queue.Queue(). It blocks until data is available, and once it's
    received something from the queue, it sends it to the "MainThread" by
    emitting a Qt Signal.
    """

    signal = QtCore.pyqtSignal(str)

    def __init__(self, queue, *args, **kwargs):
        QtCore.QObject.__init__(self, *args, **kwargs)
        self.queue = queue

    @QtCore.pyqtSlot()
    def run(self):
        while True:
            text = self.queue.get()
            self.signal.emit(text)


def main():

    # # Create Queue and redirect sys.stdout to this queue
    # queue = Queue()
    # sys.stdout = StdoutQueue(queue)

    app = QtGui.QApplication(sys.argv)
    ma = MainApp()
    ma.app_ui()
    ma.show()

    # # Create thread that will listen for new strings in the queue. Upon new
    # #   items, it will send the text to the onDataReady method in the
    # #   AppBuilder class, which will add the string to the text editor.
    # # ab = AppBuilder(QtCore.QThread)
    # thread = QtCore.QThread()
    # receiver = Receiver(queue)
    # # receiver.signal.connect(ab.onDataReady)
    # receiver.moveToThread(thread)
    # thread.started.connect(receiver.run)
    # thread.start()

    sys.exit(app.exec_())


if __name__ == '__main__':

    main()
