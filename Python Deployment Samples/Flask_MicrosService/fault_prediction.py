
# coding: utf-8

import pandas as pd
import numpy as np
import pickle
import xgboost as xgb
from scipy.signal import argrelextrema

    
def create_features_obs(df_inst):
    
    df_inst = df_inst   
    all_current_gradient = [] ## current gradient features 
    tot_points = []  ## Total Points captured
    all_duration = [] ## Duration of the operation cycle
    
    current_gradient = np.corrcoef(df_inst.Std_Value,df_inst.index.values)[0][1] 
    all_current_gradient.append(current_gradient) ## get current gradient
    
    tot_points.append(df_inst.shape[0]) ## Get total points captured
    
    duration = df_inst.TIMESTAMP.max() - df_inst.TIMESTAMP.min()
    all_duration.append(duration) ## Get duration of the operation cycle
        
    All_Features = pd.DataFrame()
    All_Features['points_captured'] = tot_points
    All_Features['current_gradient'] = all_current_gradient
    All_Features['Duration'] = all_duration
    return All_Features
    
    
    
def create_features_cb(df_inst):
    
    df_inst = df_inst
    tot_max = []
    tot_min = []
    all_current_gradient = []
    
    All_Features_inst = df_inst
    get_max = np.array(argrelextrema(np.array(All_Features_inst.VALUE), np.greater)).shape[1]
    tot_max.append(get_max)
    get_min = np.array(argrelextrema(np.array(All_Features_inst.VALUE), np.less)).shape[1]
    tot_min.append(get_min)
    current_gradient = np.corrcoef(All_Features_inst.VALUE,All_Features_inst.index.values)[0][1] 
    all_current_gradient.append(current_gradient) ## get current gradient
    all_features_cb = pd.DataFrame()
    all_features_cb['tot_max'] = tot_max
    all_features_cb['tot_min'] = tot_min
    all_features_cb['slope'] = all_current_gradient
    return all_features_cb


def create_newstring(TIMESTAMP,VALUE,PM_Code):
        TIMESTAMP = TIMESTAMP
        VALUE = VALUE
        PM_Code = PM_Code
        operation_2 = pd.DataFrame()
        operation_2['TIMESTAMP'] = TIMESTAMP
        operation_2['VALUE'] = VALUE
        operation_2['Std_Value'] = VALUE
        operation_2['PM_Code'] = PM_Code
        return operation_2

    
def create_features_pc(TIMESTAMP,VALUE,PM_Code,data):
    TIMESTAMP = TIMESTAMP
    VALUE = VALUE
    PM_Code = PM_Code
    cutoff=data[data['PointMachineCode']== PM_Code]['Cutoff'].unique()[0]
    max_current = max(VALUE)
    if (max_current >cutoff):
             return 1
    else:
        return 0



def main_process(data_input):
    
    TIMESTAMP = data_input['time_stamp'].values
    VALUE = data_input['current'].values
    PM_Code = data_input['pointMachineCode'].unique()[0]
    model_obs = pickle.load(open("model_Obstruction", "rb"))
    model_CB = pickle.load(open("model_CB", "rb"))
    data=pd.read_csv("Threshold_limits_pointmachine.csv")
    operation_1 = create_newstring(TIMESTAMP,VALUE,PM_Code)
    all_features_obs = create_features_obs(operation_1)
    all_features_cb = create_features_cb(operation_1)
    d_test_obs = xgb.DMatrix(all_features_obs)
    d_test_cb = xgb.DMatrix(all_features_cb)
    prediction_obs = ((model_obs.predict(d_test_obs))[0]).astype('int')     
    #prediction_cb = ((model_CB.predict(d_test_cb))).item()
    prediction_cb =((model_CB.predict(d_test_cb))[0]).astype('int')   
    prediction_pk = create_features_pc(TIMESTAMP,VALUE,PM_Code,data)
 
    
    if( (prediction_obs==0) and (prediction_cb == 0 )and (prediction_pk==0)):
        prediction = 'Normal Operation'
    
    elif((prediction_obs==1) and (prediction_cb == 0 ) and (prediction_pk==0)) :
        prediction =  'Obstruction Present'
    
    elif((prediction_obs==0) and (prediction_cb >=0.5 )and (prediction_pk==0)):
        prediction =  'Carbon Brush Problem'
    
    elif((prediction_obs==0) and (prediction_cb == 0 )and (prediction_pk==1)) :
        prediction =  'Peak Current Present'
    else:
        prediction='More than one fault'
    
    return prediction
    


