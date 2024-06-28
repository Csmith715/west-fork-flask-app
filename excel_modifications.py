import pandas as pd
import openpyxl
from datetime import datetime
# from io import BytesIO
# from base64 import b64encode


class FileUpdate:
    def __init__(self):
        self.workbook = openpyxl.load_workbook('data/FE Template - WFAS - CLIENT - BORROWER - Field Exam Report - Automation.xlsx')
        self.cols = ['Payors', 'Future Cash', 'Current', '30', '60', '90', '120', '150', '180', '210', 'As Of Date']
        self.num_payers = 1
        self.facility_cols = {
            1: ['N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U'],
            2: ['Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF'],
            3: ['AJ', 'AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ'],
            4: ['AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'BA', 'BB'],
            5: ['BF', 'BG', 'BH', 'BI', 'BJ', 'BK', 'BL', 'BM']
        }
        self.facility_name_cells = {1: 'M14', 2: 'X14', 3: 'AI14', 4: 'AT14', 5: 'BE14', 6: 'BP14', 7: 'CA14', 8: 'CL14'}
        self.facility_cell = ''
        self.facility_cols_single_file = ['O', 'P', 'Q', 'R', 'S', 'T', 'U']
        self.sheet_cols = []
        self.ar_bucket_labels = ['30', '60', '90', '120', '150', '180']
        self.ar_bucket_ranges = [
            range(1, 31),
            range(31, 61),
            range(61, 91),
            range(91, 121),
            range(121, 151),
            range(151, 181)
        ]
        self.date_start_row = 81
        self.unique_payers = []
        self.payer_mappings = {'Medicare A': 'Medicare', 'Medicare B': 'Medicare'}
        self.max_date = None
        self.encoding_options = ['utf-8', 'latin1', 'ISO-8859-1']
        self.upload_type = None
        self.multi_tab_df = pd.DataFrame()
        self.single_facility_name = ''

    def update_facilities(self, dataframe_dict: dict):
        final_dfs = []
        for i, (sub, facility_df) in enumerate(dataframe_dict.items(), start=1):
            self.sheet_cols = self.facility_cols[i]
            self.facility_cell = self.facility_name_cells[i]
            # Concatenate dataframes
            # temp_df = pd.concat(facility_dfs)
            temp_df = facility_df[self.cols].fillna('')
            temp_df['30'] = temp_df['Current'] + temp_df['30']
            temp_df = temp_df.drop('Current', axis=1)
            temp_df = temp_df[temp_df['Payors'] != ''].rename(columns={'As Of Date': 'Date'})
            temp_df = temp_df.reset_index(drop=True)
            temp_df['Date'] = [datetime.strptime(w, '%m/%d/%Y') for w in temp_df.Date]
            temp_df['Payors'] = temp_df['Payors'].replace(self.payer_mappings)
            self.date_start_row = (24 - len(pd.unique(temp_df.Date))) + 81
            self.unique_payers = temp_df['Payors'].unique()
            self.num_payers = len(self.unique_payers)
            self.update_facility(temp_df, sub, False)
            final_dfs.append(temp_df)
        # self.save_file()
        final_df = pd.concat(final_dfs)
        # final_df = final_df.groupby(['Payer Type', 'Date']).sum()
        final_df = final_df.drop(['Payors'], axis=1)
        final_df = final_df.groupby(['Date']).sum()
        return final_df.reset_index()

    def update_facility(self, df, facility_name, single=False):
        # if single facility instance
        if single:
            df_grouped = self.multi_tab_df
            self.sheet_cols = self.facility_cols_single_file
            self.facility_cell = 'M14'
        else:
            df_grouped = df.groupby(['Payors', 'Date']).sum()
            self.max_date = max(df['Date'])
            self.unique_payers.sort()
        ar6 = self.workbook['AR6  AR Aging Trend']
        ar6['A39'] = self.max_date
        ar6[self.facility_cell] = facility_name
        ranges = [self.date_start_row + (x * 33) for x in range(self.num_payers)]
        payer_name_start_cell = [f'A{(r * 33) + 79}' for r in range(self.num_payers)]
        for w, start_row_val, pcell in zip(self.unique_payers, ranges, payer_name_start_cell):
            payor_df = df_grouped[df_grouped.index.get_level_values('Payors') == w]
            ar6[pcell] = w
            row_sums = [row for row in payor_df.values]
            for r, row_add in zip(row_sums, range(len(row_sums))):
                for row_val, col in zip(r, self.sheet_cols):
                    ar6[f'{col}{start_row_val + row_add}'] = row_val

    def update_single_facility(self, single_df):
        self.unique_payers = single_df['Payors'].unique()
        self.num_payers = len(self.unique_payers)
        self.single_facility_name = single_df['Facility'][0]
        self.update_facility(single_df, single_df['Facility'][0], single=True)

        return single_df

    def label_ar_bucket(self, value: int):
        label = ''
        if value > 180:
            label = 'Over 180'
        else:
            for ar_range, lab in zip(self.ar_bucket_ranges, self.ar_bucket_labels):
                if value in ar_range:
                    label = lab
        return label

    def process_single_tab_file(self, single_tab_file: pd.DataFrame) -> pd.DataFrame:
        self.max_date = max(single_tab_file['Date'])
        self.date_start_row = (24 - len(single_tab_file['Date'])) + 81
        fac_names = pd.unique(single_tab_file['Facility'])
        fac_names = [f for f in fac_names if f != 'Grand Total' and f != '(blank)' and f != 'Facility']
        fac_df = single_tab_file.fillna('')
        fac_df = filter_out_string_rows(
            fac_df,
            ['30', '60', '90', '120']
        ).fillna(0).reset_index(drop=True)
        fac_df['150'] = 0
        fac_df['180'] = 0
        fac_df['Over 180'] = 0
        fac_df = fac_df.drop(['Current Payor', 'Sum of atb_balance', 'Sum of atb_current', 'Sum of atb_30'], axis=1)
        for i, facility in enumerate(fac_names, start=1):
            self.facility_cell = self.facility_name_cells[i]
            self.sheet_cols = self.facility_cols[i]
            single_fac_df = fac_df[fac_df['Facility'] == facility]
            single_fac_df = single_fac_df.drop(['Facility'], axis=1)
            self.date_start_row = (24 - len(pd.unique(single_fac_df.Date))) + 81
            self.unique_payers = single_fac_df['Payors'].unique()
            self.num_payers = len(self.unique_payers)
            self.update_facility(single_fac_df, facility, False)
        final_df = fac_df.drop(['Facility', 'Payors'], axis=1)
        final_df = final_df.groupby(['Date']).sum()
        # final_df = fac_df.drop(['Payors'], axis=1)
        # final_df = final_df.groupby(['Facility', 'Date']).sum()
        return final_df.reset_index()

    def process_single_file(self, single_file) -> pd.DataFrame:
        sdf = pd.ExcelFile(single_file)
        sheet_names = sdf.sheet_names
        sheet_dates = [s.strip('ATB ').replace('  ', ' ') for s in sheet_names]
        sheet_dates = [datetime.strptime(d, '%m %d %Y') for d in sheet_dates]
        self.max_date = max(sheet_dates)
        self.date_start_row = (24 - len(sheet_dates)) + 81
        all_dataframes = []
        # Tabs represent dates
        for tab_name in sheet_names:
            tab_df = pd.read_excel(single_file, sheet_name=tab_name)
            tab_df = tab_df.rename(columns={
                'FIN CLASS': 'Financial Class',
                'FC': 'Financial Class',
                'DISCH DATE': 'DISCH DT',
                ' ACHGS ': 'ACHGS'
            })
            sdate = datetime.strptime(tab_name.strip('ATB ').replace('  ', ' '), '%m %d %Y')
            days_since_discharge = [(sdate - d).days for d in tab_df['DISCH DT']]
            ar_buckets = [self.label_ar_bucket(max(1, d)) for d in days_since_discharge]
            temp_df = pd.DataFrame({
                'Payors': tab_df['Financial Class'],
                'Charges': tab_df['ACHGS'],
                'AR Bucket': ar_buckets,
                'Date': sdate
            })
            all_dataframes.append(temp_df)
        all_tab_df = pd.concat(all_dataframes).reset_index(drop=True)
        pivot_df = all_tab_df.pivot_table(values='Charges', index=['Payors', 'Date'], columns='AR Bucket', aggfunc='sum')
        pivot_df = pivot_df[['30', '60', '90', '120', '150', '180', 'Over 180']].sort_index()
        self.multi_tab_df = pivot_df

        return pivot_df.reset_index()


def filter_out_string_rows(df, columns):
    # Define a helper function to check if a value is a float
    def is_float_or_nan(value):
        if value == '':
            return True
        elif isinstance(value, str):
            return False
        else:
            return True

    # Apply the helper function to filter out rows with strings in the specified columns
    for column in columns:
        df = df[df[column].apply(is_float_or_nan)]
    # Convert the filtered columns to float type
    df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')
    return df

class PayorUpdate:
    def __init__(self, payor_mapping_df):
        self.payor_mapping_df = payor_mapping_df

    def map_payors(self, loaded_file, file_type):
        unique_payors = []
        if file_type == 'single multi-tab':
            # Multi-tab Excel File
            file_update = FileUpdate()
            smt_df = file_update.process_single_file(loaded_file)
            unique_payors = [p for p in pd.unique(smt_df.index.get_level_values('Payer Type')) if p]
        elif file_type == 'single single-tab':
            # Dataframe
            unique_payors = [lf for lf in loaded_file['BBC Mapping'].unique() if lf]
        elif file_type == 'multiple':
            # Dictionary of Dataframes
            fdfs = []
            for _, facility_dfs in loaded_file.items():
                # Concatenate dataframes
                tdf = pd.concat(facility_dfs)
                tdf = tdf[tdf['Payer Type'] != ''].fillna('')
                tdf = tdf.reset_index(drop=True)
                fdfs.append(tdf)
            complete_df = pd.concat(fdfs)
            unique_payors = [c for c in complete_df['Payer Type'].unique() if c]
        if unique_payors:
            mapped_payors = map_names(unique_payors, self.payor_mapping_df)
            mapped_df = pd.DataFrame({'Text': unique_payors, 'Mapping': mapped_payors})
        else:
            mapped_df = pd.DataFrame({'Text': [], 'Mapping': []})
        return mapped_df

def map_names(names_list, mapping_df):
    # Convert mapping DataFrame to a dictionary for faster lookup
    mapping_dict = mapping_df.set_index('Text')['Mapping'].to_dict()
    # Map names_list using the dictionary
    mapped_names = [mapping_dict.get(name, name) for name in names_list]

    return mapped_names
