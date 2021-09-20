import os
import msoffcrypto
import io
import glob
import pandas as pd
from numba import njit,prange
import warnings
#from openpyxl import Workbook
#from openpyxl.worksheet.datavalidation import DataValidation
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module='openpyxl'
)


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)

def find_salary (df,eid):
    temp_salary_data = ''
    temp_location_data = ''
    temp_level_data = ''
    temp_df = trim_all_columns(df)
    if temp_df.loc[temp_df['Employee ID'] == float(eid)]['Salary'].to_list()[0]:
        temp_salary_data = temp_df.loc[temp_df['Employee ID'] == float(eid)]['Salary'].to_list()[0]
    else:
        temp_salary_data = ''
    if temp_df.loc[temp_df['Employee ID'] == float(eid)]['Location'].to_list()[0]:
        temp_location_data = temp_df.loc[temp_df['Employee ID'] == float(eid)]['Location'].to_list()[0]
    else:
        temp_location_data = ''
    if temp_df.loc[temp_df['Employee ID'] == float(eid)]['Level'].to_list()[0]:
        temp_level_data = temp_df.loc[temp_df['Employee ID'] == float(eid)]['Level'].to_list()[0]
    else:
        temp_level_data = ''
    #temp_salary_data = lambda x:df['Base Salary']  if df.loc[df['Number'] == float(eid)] else 0
    return temp_salary_data,temp_location_data,temp_level_data

def prepare_survey_data (source_file):
    df_survey = {}
    df_clean_survey = {}
    empl_id =''
    df_survey = source_file['Survey'][['Unnamed: 1','Unnamed: 2','Unnamed: 3','Unnamed: 4','Unnamed: 5','Unnamed: 6','Unnamed: 7','Unnamed: 8','Unnamed: 9','Unnamed: 10','Unnamed: 11']].copy()
    df_survey = trim_all_columns(df_survey)
    df_clean_survey = df_survey.drop (df_survey[df_survey['Unnamed: 1'].isna() & df_survey['Unnamed: 2'].isna() & df_survey['Unnamed: 3'].isna()].index)
    df_clean_survey = df_clean_survey.drop (df_clean_survey[df_clean_survey['Unnamed: 1'].astype(str).str.isnumeric() & df_clean_survey['Unnamed: 2'].isna() & df_clean_survey['Unnamed: 3'].isna()].index)
    df_clean_survey = df_clean_survey.drop (df_clean_survey[df_clean_survey['Unnamed: 2'] == 'Review the Competency Development Dictionary and select the most relevant skill set to succeed in your role'].index)
    df_clean_survey = df_clean_survey.drop (df_clean_survey[df_clean_survey['Unnamed: 2'] == 'Note: If you need more information regarding skill area, please refer to "skills development dictionary" to read the definition'].index)
    df_clean_survey = df_clean_survey.drop (df_clean_survey[df_clean_survey['Unnamed: 2'] == 'PLEASE NOT ANY SIGNIFICANT TASKS THAT WERE NOT MENTIONED ABOVE'].index)
    df_clean_survey.reset_index(drop=True, inplace=True)
    #namep = ((df_clean_survey[df_clean_survey['Unnamed: 1'] == 'Name']['Unnamed: 3'].values[0].replace(' ','')) + (df_clean_survey[df_clean_survey['Unnamed: 1'] == 'Position']['Unnamed: 3'].values[0].replace(' ',''))).lower()
    empl_id = df_clean_survey[df_clean_survey['Unnamed: 1'] == 'Employee ID']['Unnamed: 3'].values[0]
    return df_clean_survey,empl_id

def prepare_salary_data (temp_salary_file):
    temp_df_salary = pd.DataFrame()
    decrypted_salary = io.BytesIO()
    encrypted_salary = open(temp_salary_file, "rb")
    file_salary = msoffcrypto.OfficeFile(encrypted_salary)
    file_salary.load_key(password="tpgmar21")
    file_salary.decrypt(decrypted_salary)
    temp_df_salary = pd.read_excel(decrypted_salary, sheet_name='OCW8A9A', header=0, engine='openpyxl')
    encrypted_salary.close()
    return temp_df_salary

def get_user_profile (df_clean_survey_tem,file_num,eid,sal,loca,levl):
    df_profile = pd.DataFrame()
    df_profile.loc[file_num,'Name'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 1'] == 'Name']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Position'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 1'] == 'Position']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Supervisor\'s Name'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 1'] == 'Supervisor\'s Name']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Department'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 1'] == 'Department']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Avg # of hours you work per day'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Avg # of hours you work per day:']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'# of days you work per week'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == '# of days you work per week:']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Avg actual # of hours you work per week'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Avg actual # of hours you work per week:']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Avg actual # of on-call hours you work per week (if applicable)'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Avg actual # of on-call hours you work per week (if applicable)']['Unnamed: 3'].values[0]
    df_profile.loc[file_num,'Total Avg # of hours worked per week (incl\'d on-call)'] = df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Total Avg # of hours worked per week (incl\'d on-call)']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Communication'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Communication']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Leadership'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Leadership']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Logical Reasoning'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Logical Reasoning']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'People Management'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'People Management']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Personal Development'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Personal Development']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Transferable Competencies'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Transferable Competencies']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Technical Competencies'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Technical Competencies']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Other 1'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Other 1']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Other 2'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Other 2']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Other 3'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Other 3']['Unnamed: 3'].values[0]
    df_profile.loc[file_num, 'Emp_ID'] = eid
    df_profile.loc[file_num, 'Emp_Salary'] = sal
    df_profile.loc[file_num, 'Location'] = loca
    df_profile.loc[file_num, 'Level'] = levl
    #df_profile.loc[file_num,'Name and Position']=np
    return df_profile

# def get_user_competency (df_clean_survey_tem,file_num,np):
#     df_competency = pd.DataFrame()
#     df_competency.loc[file_num, 'Communication'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Communication']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'Leadership'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Leadership']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'Logical Reasoning'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Logical Reasoning']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'People Management'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'People Management']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'Personal Development'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Personal Development']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'Transferable Competencies'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Transferable Competencies']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'Technical Competencies'] =  df_clean_survey_tem[df_clean_survey_tem['Unnamed: 2'] == 'Technical Competencies']['Unnamed: 3'].values[0]
#     df_competency.loc[file_num, 'Name and Position'] = np
#     return df_competency

def get_user_actpro (df_clean_survey_tem,eid):
    actpro_index = []
    actpro_column = []
    temp_actpro = pd.DataFrame()
    actpro_index = df_clean_survey_tem.loc[(df_clean_survey_tem['Unnamed: 1' ] == '#') & (df_clean_survey_tem['Unnamed: 2'] == 'Process') ].index
    actpro_index.tolist()
    actpro_column = df_clean_survey_tem.loc[actpro_index].values
    df_actpro = pd.DataFrame (columns=actpro_column[0])
    temp_actpro = df_clean_survey_tem [(actpro_index.to_list()[0]+1):]
    temp_actpro.columns = actpro_column[0]
    temp_actpro = temp_actpro.drop (temp_actpro.loc[temp_actpro['Activity'].astype(str) == ''].index)
    df_actpro = df_actpro.append(temp_actpro,ignore_index=True)
    #df_actpro['Name and Position'] = np
    df_actpro['Emp_ID'] = eid
    #df_actpro.index.rename ('index_key',inplace=True)
    return df_actpro

dataframe_profile = pd.DataFrame(columns=['Name' 
                                , 'Position'
                                , 'Supervisor\'s Name'
                                , 'Department'
                                , 'Avg # of hours you work per day'
                                , '# of days you work per week'
                                , 'Avg actual # of hours you work per week'
                                , 'Avg actual # of on-call hours you work per week (if applicable)'
                                , 'Total Avg # of hours worked per week (incl\'d on-call)'
                                #, 'Name and Position'
                                , 'Communication'
                                , 'Leadership'
                                , 'Logical Reasoning'
                                , 'People Management'
                                , 'Personal Development'
                                , 'Transferable Competencies'
                                , 'Technical Competencies'
                                , 'Other 1'
                                , 'Other 2'
                                , 'Other 3'
                                , 'Emp_ID'
                                , 'Emp_Salary'
                                ])


# dataframe_competency = pd.DataFrame(columns=['Communication'
#                                     ,'Leadership'
#                                     ,'Logical Reasoning'
#                                     ,'People Management'
#                                     ,'Personal Development'
#                                     ,'Transferable Competencies'
#                                     ,'Technical Competencies'
#                                     ,'Others'
#                                     ,'Name and Position'
#                                     ])



if __name__ == "__main__":
    all_files = []
    file_path = []
    #namepos = ''
    dataframe_profile = pd.DataFrame()
    dataframe_competency = pd.DataFrame()
    dataframe_actpro = pd.DataFrame()
    dataframe_salary = pd.DataFrame()

    file_path = os.path.abspath('')
    all_files = glob.glob(os.path.join(file_path,'Surveyfiles', '*.xlsm'))
    all_files.extend(glob.glob(os.path.join(file_path,'Surveyfiles', '*.xlsx')))

    salary_file = os.path.join(file_path,r'Peavey Org Chart.XLSX')

    dataframe_salary = prepare_salary_data (salary_file)

    file_number=0
    for file in all_files:
        try:
            file_name = ''
            emp_id= ''
            emp_salary = ''
            emp_location = ''
            emp_level = ''
            source_file = {}
            file_name = file.split('\\')[-1]
            source_file = pd.read_excel(file, sheet_name=None, header=0, engine='openpyxl')

            #print(source_file)
            clean_survey_data,emp_id = prepare_survey_data (source_file)

            
            emp_salary,emp_location,emp_level = find_salary (dataframe_salary,emp_id)

            dataframe_profile = dataframe_profile.append(get_user_profile(clean_survey_data,file_number,emp_id,emp_salary,emp_location,emp_level))

            #dataframe_competency = dataframe_competency.append(get_user_competency(clean_survey_data,file_number,namepos))

            dataframe_actpro = dataframe_actpro.append(get_user_actpro(clean_survey_data,emp_id))

            dataframe_profile.index.rename ('index_key',inplace=True)
            dataframe_competency.index.rename ('index_key',inplace=True)
            dataframe_actpro.reset_index(drop=True, inplace=True)
            dataframe_actpro.index.rename ('index_key',inplace=True)

        except (IndexError) as e:
                print('############################')
                print('File Error at'+ file_name)
                print (e)
                print('############################')
                continue
        else:
                file_number+=1
                print ('Captured data from '+file_name)

    print('Saving data into .csv files...')
    dataframe_profile.to_csv (r'profile.csv')
    #dataframe_competency.to_csv(r'competence.csv')
    dataframe_actpro.to_csv(r'proact.csv')
    print('Completed.')