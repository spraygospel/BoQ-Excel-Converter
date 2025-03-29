import sys
import os
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QFileDialog, 
                            QVBoxLayout, QHBoxLayout, QWidget, QLabel, QComboBox,
                            QTableView, QCheckBox, QGroupBox, QLineEdit, QMessageBox,
                            QProgressBar, QSplitter, QHeaderView)
from PyQt5.QtCore import Qt, QAbstractTableModel, QVariant

class PandasModel(QAbstractTableModel):
    """Model untuk menampilkan DataFrame di QTableView"""
    
    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return str(self._data.columns[section])
        if orientation == Qt.Vertical and role == Qt.DisplayRole:
            return str(self._data.index[section])
        return QVariant()

class ExcelCleanerApp(QMainWindow):
    """Aplikasi untuk membersihkan file Excel"""
    
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("Excel Cleaner")
        self.setGeometry(100, 100, 1200, 800)
        
        # Data
        self.input_file = None
        self.df_original = None
        self.df_cleaned = None
        self.selected_sheet = None
        self.sheets = []
        
        # Setup UI
        self.setup_ui()
    
    def setup_ui(self):
        # Main layout
        main_widget = QWidget()
        main_layout = QVBoxLayout()
        
        # Top section - File selection
        file_group = QGroupBox("File Selection")
        file_layout = QHBoxLayout()
        
        self.file_path_label = QLabel("No file selected")
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_file)
        
        file_layout.addWidget(self.file_path_label)
        file_layout.addWidget(self.browse_button)
        file_group.setLayout(file_layout)
        
        # Spreadsheet selection
        sheet_group = QGroupBox("Sheet Selection")
        sheet_layout = QHBoxLayout()
        
        self.sheet_combo = QComboBox()
        self.sheet_combo.currentIndexChanged.connect(self.sheet_selected)
        self.load_button = QPushButton("Load Sheet")
        self.load_button.clicked.connect(self.load_sheet)
        self.load_button.setEnabled(False)
        
        sheet_layout.addWidget(QLabel("Select Sheet:"))
        sheet_layout.addWidget(self.sheet_combo)
        sheet_layout.addWidget(self.load_button)
        sheet_group.setLayout(sheet_layout)
        
        # Cleaning options
        cleaning_group = QGroupBox("Cleaning Options")
        cleaning_layout = QVBoxLayout()
        
        # Header row finder
        header_layout = QHBoxLayout()
        self.header_auto_checkbox = QCheckBox("Auto-detect header row")
        self.header_auto_checkbox.setChecked(True)
        self.header_row_input = QLineEdit()
        self.header_row_input.setPlaceholderText("Header row number")
        self.header_row_input.setEnabled(False)
        self.header_auto_checkbox.clicked.connect(
            lambda: self.header_row_input.setEnabled(not self.header_auto_checkbox.isChecked())
        )
        
        header_layout.addWidget(self.header_auto_checkbox)
        header_layout.addWidget(self.header_row_input)
        
        # Section detection
        section_layout = QHBoxLayout()
        self.section_checkbox = QCheckBox("Detect section headers")
        self.section_checkbox.setChecked(True)
        self.section_column = QLineEdit()
        self.section_column.setPlaceholderText("Section column index (default: 6)")
        self.section_column.setText("6")
        
        section_layout.addWidget(self.section_checkbox)
        section_layout.addWidget(self.section_column)
        
        # Action buttons
        button_layout = QHBoxLayout()
        self.preview_button = QPushButton("Preview Cleaning")
        self.preview_button.clicked.connect(self.preview_cleaning)
        self.preview_button.setEnabled(False)
        
        self.save_button = QPushButton("Clean and Save")
        self.save_button.clicked.connect(self.clean_and_save)
        self.save_button.setEnabled(False)
        
        button_layout.addWidget(self.preview_button)
        button_layout.addWidget(self.save_button)
        
        # Add all layouts to cleaning group
        cleaning_layout.addLayout(header_layout)
        cleaning_layout.addLayout(section_layout)
        cleaning_layout.addLayout(button_layout)
        cleaning_group.setLayout(cleaning_layout)
        
        # Table views
        splitter = QSplitter(Qt.Vertical)
        
        # Original data table
        original_group = QGroupBox("Original Data")
        original_layout = QVBoxLayout()
        self.original_table = QTableView()
        original_layout.addWidget(self.original_table)
        original_widget = QWidget()
        original_widget.setLayout(original_layout)
        
        # Cleaned data table
        cleaned_group = QGroupBox("Cleaned Data")
        cleaned_layout = QVBoxLayout()
        self.cleaned_table = QTableView()
        cleaned_layout.addWidget(self.cleaned_table)
        cleaned_widget = QWidget()
        cleaned_widget.setLayout(cleaned_layout)
        
        # Add tables to splitter
        splitter.addWidget(original_group)
        splitter.addWidget(cleaned_group)
        
        # Status bar
        self.status_bar = QProgressBar()
        self.status_bar.setValue(0)
        
        # Add all components to main layout
        main_layout.addWidget(file_group)
        main_layout.addWidget(sheet_group)
        main_layout.addWidget(cleaning_group)
        main_layout.addWidget(splitter)
        main_layout.addWidget(self.status_bar)
        
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)
    
    def browse_file(self):
        file_dialog = QFileDialog()
        file_dialog.setNameFilter("Excel files (*.xlsx *.xls)")
        file_dialog.setViewMode(QFileDialog.Detail)
        
        if file_dialog.exec_():
            file_paths = file_dialog.selectedFiles()
            if file_paths:
                self.input_file = file_paths[0]
                self.file_path_label.setText(os.path.basename(self.input_file))
                self.load_sheet_names()
    
    def load_sheet_names(self):
        try:
            self.sheets = pd.ExcelFile(self.input_file).sheet_names
            self.sheet_combo.clear()
            self.sheet_combo.addItems(self.sheets)
            self.load_button.setEnabled(True)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not load sheets: {str(e)}")
    
    def sheet_selected(self):
        self.selected_sheet = self.sheet_combo.currentText()
    
    def load_sheet(self):
        if not self.input_file or not self.selected_sheet:
            return
        
        try:
            # Load without headers since we'll determine headers later
            self.df_original = pd.read_excel(self.input_file, sheet_name=self.selected_sheet, header=None)
            
            # Display in table
            model = PandasModel(self.df_original)
            self.original_table.setModel(model)
            
            # Enable preview
            self.preview_button.setEnabled(True)
            
            # Auto-resize columns
            self.original_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
            
            QMessageBox.information(self, "Success", f"Loaded sheet '{self.selected_sheet}' with "
                                   f"{self.df_original.shape[0]} rows and {self.df_original.shape[1]} columns")
        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not load sheet: {str(e)}")
    
    def find_header_row(self):
        """Find the row containing column headers (usually has 'No.' in column 1)"""
        if not self.header_auto_checkbox.isChecked():
            try:
                return int(self.header_row_input.text())
            except ValueError:
                QMessageBox.warning(self, "Warning", "Invalid header row number. Using auto-detection.")
        
        # Auto-detect
        for i in range(self.df_original.shape[0]):
            if self.df_original.iloc[i, 1] == "No.":
                return i
        
        # If not found, return 0
        return 0
    
    def preview_cleaning(self):
        if self.df_original is None:
            return
        
        try:
            self.status_bar.setValue(10)
            
            # Find header row
            header_row = self.find_header_row()
            self.status_bar.setValue(20)
            
            # Get headers
            headers = self.df_original.iloc[header_row].tolist()
            
            # Kolom yang ingin dipertahankan (sesuaikan sesuai kebutuhan)
            keep_columns = [1, 4, 6, 7, 8, 9, 10, 18]  # No., Internal Ref, Description, Qty, Unit, Unit Price, Total, Supplier
            
            # Filter headers
            headers_filtered = [headers[i] for i in keep_columns if i < len(headers)]
            
            # Add section column if needed
            if self.section_checkbox.isChecked():
                headers_filtered.append("Section")
            
            self.status_bar.setValue(30)
            
            # Process data
            clean_data = []
            section_col = 6  # Default section column
            
            # Get section column if specified
            if self.section_checkbox.isChecked():
                try:
                    if self.section_column.text():
                        section_col = int(self.section_column.text())
                except ValueError:
                    pass
            
            self.status_bar.setValue(40)
            
            # Clean data
            current_section = None
            
            # Start from row after header
            for i in range(header_row + 1, self.df_original.shape[0]):
                row = self.df_original.iloc[i]
                
                # Section header detection
                if self.section_checkbox.isChecked():
                    if isinstance(row[section_col], str) and row[section_col].startswith("Material") and pd.isna(row[section_col+1]):
                        current_section = row[section_col]
                        continue
                
                # Skip empty rows
                if pd.isna(row[1]) or not (isinstance(row[1], int) or isinstance(row[1], float)):
                    continue
                
                # Get values from kept columns
                row_data = [row[i] if i < len(row) else None for i in keep_columns]
                
                # Add section if needed
                if self.section_checkbox.isChecked():
                    row_data.append(current_section)
                
                # Add to clean data
                clean_data.append(row_data)
            
            self.status_bar.setValue(70)
            
            # Create DataFrame
            self.df_cleaned = pd.DataFrame(clean_data, columns=headers_filtered)
            
            # Clean NA values
            self.df_cleaned = self.df_cleaned.fillna("")
            
            self.status_bar.setValue(90)
            
            # Display in cleaned table
            model = PandasModel(self.df_cleaned)
            self.cleaned_table.setModel(model)
            
            # Auto-resize columns
            self.cleaned_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
            
            # Enable save button
            self.save_button.setEnabled(True)
            
            self.status_bar.setValue(100)
            
            QMessageBox.information(self, "Preview", f"Preview successful! Cleaned data has "
                                    f"{self.df_cleaned.shape[0]} rows and {self.df_cleaned.shape[1]} columns")
            
        except Exception as e:
            self.status_bar.setValue(0)
            QMessageBox.critical(self, "Error", f"Preview failed: {str(e)}")
    
    def clean_and_save(self):
        if self.df_cleaned is None:
            QMessageBox.warning(self, "Warning", "Please preview cleaning first")
            return
        
        file_dialog = QFileDialog()
        file_dialog.setAcceptMode(QFileDialog.AcceptSave)
        file_dialog.setNameFilter("Excel files (*.xlsx)")
        file_dialog.setDefaultSuffix("xlsx")
        
        if file_dialog.exec_():
            file_paths = file_dialog.selectedFiles()
            if file_paths:
                output_file = file_paths[0]
                try:
                    self.df_cleaned.to_excel(output_file, index=False)
                    QMessageBox.information(self, "Success", f"Data saved to {output_file}")
                except Exception as e:
                    QMessageBox.critical(self, "Error", f"Failed to save file: {str(e)}")

# Run the application
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ExcelCleanerApp()
    window.show()
    sys.exit(app.exec_())
