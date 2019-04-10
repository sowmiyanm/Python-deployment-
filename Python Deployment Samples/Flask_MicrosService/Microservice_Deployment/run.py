from flask import Flask,request
import os,json
import fault_prediction as analytic
app = Flask(__name__)

port = int(os.getenv("PORT",3000))

@app.route('/',methods=['POST'])
# def mapper(*args, **kwargs):
#     # decode args and kwargs
#     try:
#         data = kwargs.pop('data')
#         df = pd.DataFrame(data['time_series'])
        
#         model = fault_prediction()
#         out =  model.main_process(df)
#         if (out == ' ') :
#             out = 'Error in processing, check input'
#         return out 
        
#     except Exception as e:
#         return "error "+ e.message
#     return out

# def resolve(scores):
#     return scores 

def run_main():
    data_dict = json.loads(request.data)
    print data_dict
    return json.dumps({'value':analytic.init_func(data_dict['data']['time_series']['time_stamp'],data_dict['data']['time_series']['current'],data_dict['data']['time_series']['pointMachineCode'])})
    #return json.dumps({'fault_type':analytic.init_func(data_dict['data']['time_series']['time_stamp'],data_dict['data']['time_series']['current'],data_dict['data']['time_series']['pointMachineCode']),'operationId':data_dict['data']['time_series']['operationId']})
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)