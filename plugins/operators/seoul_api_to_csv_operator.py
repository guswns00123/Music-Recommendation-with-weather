from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'apihub.kma.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.openApi}}'
        self.base_dt = base_dt
        # https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstMsgService/getLandFcst?pageNo=1&numOfRows=10&dataType=json&regId=11B10101&authKey={{var.value.apikey_openapi_seoul_go_kr}}
    def execute(self, context):
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}/api/typ02/openApi/VilageFcstMsgService/getLandFcst?pageNo=1&numOfRows=10&dataType=json&regId=11B10101&authKey={self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 10

        total_row_df = self._call_api(self.base_url, start_row, end_row)
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json 

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}'
        self.log.info(request_url)
        if self.base_dt is not None:
            request_url = f'{base_url}'
        response = requests.get(request_url)
        contents = json.loads(response.text)
        self.log.info(contents)
        items = contents['response']['body']['items']['item']

# Create a DataFrame
        df = pd.DataFrame(items)
        return df