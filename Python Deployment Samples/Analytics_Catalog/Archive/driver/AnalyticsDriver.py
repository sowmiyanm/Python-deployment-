# -*- coding: utf-8 -*-
"""
Created on Aug 2017

@author: 212589696
"""

import pandas as pd
from analytics.fault_prediction import fault_prediction


def mapper(*args, **kwargs):
    # decode args and kwargs
    try:
        data = kwargs.pop('data')
        df = pd.DataFrame(data['time_series'])
        
        model = fault_prediction()
        out =  model.main_process(df)
        if (out == ' ') :
            out = 'Error in processing, check input'
        return out 
        
    except Exception as e:
        return "error "+ e.message
    return out

def resolve(scores):
    return scores    

