import os
import requests
import time
from typing import Optional
from modelcontextprotocol.sdk.types import McpError, ErrorCode

class QueryExecutor:
    def __init__(self, redash_url: str, api_key: str):
        self.redash_url = redash_url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({'Authorization': f'Key {api_key}'})

    async def poll_job(self, job_id: str, timeout: int = 30) -> Optional[int]:
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = self.session.get(f'{self.redash_url}/api/jobs/{job_id}')
            if response.status_code != 200:
                raise McpError(ErrorCode.InternalError, 
                             f'Job status check failed: {response.text}')
            
            job = response.json()['job']
            if job['status'] == 3:  # Completed
                return job['query_result_id']
            if job['status'] == 4:  # Failed
                raise McpError(ErrorCode.InternalError, 'Query execution failed')
            
            time.sleep(1)
        
        raise McpError(ErrorCode.Timeout, 'Query execution timeout')

    async def execute(self, query_id: int, params: dict = None) -> dict:
        try:
            payload = {
                'max_age': 0,
                'parameters': params or {}
            }
            
            response = self.session.post(
                f'{self.redash_url}/api/queries/{query_id}/results',
                json=payload
            )
            
            if response.status_code != 200:
                raise McpError(ErrorCode.InternalError,
                             f'Query execution failed: {response.text}')
            
            job_id = response.json()['job']['id']
            result_id = await self.poll_job(job_id)
            
            if not result_id:
                raise McpError(ErrorCode.InternalError, 'No result ID returned')
                
            return await self.get_results(query_id, result_id)
            
        except requests.exceptions.RequestException as e:
            raise McpError(ErrorCode.NetworkError, str(e))

    async def get_results(self, query_id: int, result_id: int) -> dict:
        response = self.session.get(
            f'{self.redash_url}/api/queries/{query_id}/results/{result_id}.json'
        )
        
        if response.status_code != 200:
            raise McpError(ErrorCode.InternalError,
                         f'Failed to get results: {response.text}')
            
        return response.json()['query_result']['data']
