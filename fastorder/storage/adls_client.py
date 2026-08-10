import os
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

def get_adls_service_client():
    """
    Trả về DataLakeServiceClient đã authenticate.
    Sử dụng DefaultAzureCredential để tự động giải quyết thông tin xác thực từ môi trường.
    """
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    
    if not account_name:
        raise ValueError(
            "Biến môi trường 'AZURE_STORAGE_ACCOUNT_NAME' bị thiếu hoặc trống. "
        )
        
    credential = DefaultAzureCredential()
    account_url = f"https://{account_name}.dfs.core.windows.net"
    
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    return service_client

def get_bronze_file_system_client():
    """
    Trả về FileSystemClient của filesystem (container) được định nghĩa từ môi trường.
    """
    file_system_name = os.getenv("AZURE_STORAGE_FILE_SYSTEM")
    
    if not file_system_name:
        raise ValueError(
            "Biến môi trường 'AZURE_STORAGE_FILE_SYSTEM' bị thiếu hoặc trống. "
        )
        
    service_client = get_adls_service_client()
    filesystem_client = service_client.get_file_system_client(file_system=file_system_name)
    return filesystem_client