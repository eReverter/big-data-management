import paramiko
from sshtunnel import SSHTunnelForwarder

def activate_mongo(mongo_host, server_user, server_pwd):
    """
    Activates MongoDB on a remote server using SSH.

    Args:
        mongo_host (str): The hostname of the MongoDB server.
        server_user (str): The username for the server.
        server_pwd (str): The password for the server.

    Returns:
        paramiko.client.SSHClient: The SSH client connected to the remote server.
    """
    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(mongo_host, username=server_user, password=server_pwd)
    
    command = '~/BDM_Software/mongodb/bin/mongod --bind_ip_all --dbpath /home/bdm/BDM_Software/data/mongodb_data/'
    (stdin, stdout, stderr) = client.exec_command(command)
    
    return client

def get_server(mongo_host, server_user, server_pwd):
    """
    Creates an SSH tunnel to the MongoDB server.

    Args:
        mongo_host (str): The hostname of the MongoDB server.
        server_user (str): The username for the server.
        server_pwd (str): The password for the server.

    Returns:
        SSHTunnelForwarder: The SSH tunnel server object.
    """
    server = SSHTunnelForwarder(
        mongo_host,
        ssh_username=server_user,
        ssh_password=server_pwd,
        remote_bind_address=('127.0.0.1', 27017)
    )
    
    return server