from flask import Flask, request, render_template, send_file, redirect, url_for
import pandas as pd
from werkzeug.utils import secure_filename
import os
import glob
from excel_modifications import FileUpdate
from utils import column_mappings, payor_mappings

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def find_csv_files(directory):
    csv_files = glob.glob(os.path.join(directory, '**/*.csv'), recursive=True)
    return csv_files


@app.route('/')
def upload_file():
    return render_template('upload.html')


@app.route('/upload', methods=['POST'])
def upload_file_post():
    folder_path = request.form.get('folder_path')
    facility_name = request.form.get('facility_name')
    files = request.files.getlist('file')
    if folder_path:
        csv_files = find_csv_files(folder_path)
        if not csv_files:
            return "No CSV files found in the specified directory."
        dfs = [pd.read_csv(csv_file) for csv_file in csv_files]
        df = pd.concat(dfs, ignore_index=True)
        file_update.upload_type = 'folder'
    elif files and all(allowed_file(file.filename) for file in files):
        if len(files) == 1 and files[0].filename.endswith('.xlsx'):
            file = files[0]
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            excel_file = pd.ExcelFile(filepath)
            sheet_names = excel_file.sheet_names
            if len(sheet_names) > 1:
                df = file_update.process_single_file(filepath)
                df['Facility'] = facility_name
                file_update.upload_type = 'single_multi_tab'
            else:
                df = pd.read_excel(filepath, sheet_name=sheet_names[0])
                file_update.upload_type = 'single_single_tab'
        else:
            dfs = []
            for file in files:
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)
                dfs.append(pd.read_csv(filepath))
            df = pd.concat(dfs, ignore_index=True).fillna('')
            df = df[df['Facility'] != '']
    else:
        return "Invalid input. Please upload files or provide a directory path."
    df = df.rename(columns=column_mappings)
    df['Payors'] = df['Payors'].replace(payor_mappings).fillna('')
    df = df[(df['Facility'] != '(blank)') & (df['Facility'] != 'Facility') & (df['Payors'] != '')].fillna('')
    df.insert(0, 'Facility', df.pop('Facility'))
    df.to_pickle('data.pkl')  # Save DataFrame to a pickle file

    return render_template('columns.html', columns=df.columns, table=df.to_html(classes='dataframe', header="true", index=False))


@app.route('/rename_columns', methods=['POST'])
def rename_columns():
    df = pd.read_pickle('data.pkl')

    new_column_names = request.form.getlist('column_name')
    df.columns = new_column_names
    df.to_pickle('data.pkl')  # Save DataFrame with new column names

    unique_payors = df['Payors'].unique().tolist()
    unique_facilities = df['Facility'].unique().tolist()

    return render_template('modify.html', unique_payors=unique_payors, unique_facilities=unique_facilities)


@app.route('/modify', methods=['POST'])
def modify_file_post():
    df = pd.read_pickle('data.pkl')

    payor_changes = request.form.getlist('payor_change')
    facility_changes = request.form.getlist('facility_change')

    payors = df['Payors'].unique()
    facilities = df['Facility'].unique()
    for old_payor, new_payor in zip(payors, payor_changes):
        if new_payor:
            df['Payors'] = df['Payors'].replace(old_payor, new_payor)
            if file_update.upload_type == 'single_multi_tab':
                file_update.multi_tab_df.rename(index={old_payor: new_payor}, inplace=True)

    for old_facility, new_facility in zip(facilities, facility_changes):
        if new_facility:
            df['Facility'] = df['Facility'].replace(old_facility, new_facility)
    cdf = pd.DataFrame()
    if file_update.upload_type == 'single_single_tab':
        cdf = file_update.process_single_tab_file(df)
        cdf = cdf[['Date', '30', '60', '90', '120', '150', '180', 'Over 180']]
        file_update.workbook.save("modified_data.xlsx")
    elif file_update.upload_type == 'single_multi_tab':
        cdf = file_update.update_single_facility(df)
        file_update.workbook.save("modified_data.xlsx")
    elif file_update.upload_type == 'folder':
        unique_facilities = df['Facility'].unique()
        mdf_dict = {fac: df[df['Facility'] == fac] for fac in unique_facilities}
        cdf = file_update.update_facilities(mdf_dict)
        file_update.workbook.save("modified_data.xlsx")
    cdf.to_pickle('modified_cdf.pkl')

    return redirect(url_for('show_modified_data'))
    # return send_file('modified_data.xlsx', as_attachment=True, download_name='modified_data.xlsx')

@app.route('/show_modified_data')
def show_modified_data():
    cdf = pd.read_pickle('modified_cdf.pkl')
    return render_template('show_modified_data.html', table=cdf.to_html(classes='dataframe', header=True, index=False))

@app.route('/download')
def download_file():
    return send_file('modified_data.xlsx', as_attachment=True, download_name='modified_data.xlsx')


if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    file_update = FileUpdate()
    app.run(debug=True)
