import logging
import os
from paramiko import SSHClient, AutoAddPolicy

class SftpHandler:
    def __init__(self, host, port, user, password, path):
        self.path = path
        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())

        try:
            self.ssh_client.connect(
                hostname=host,
                port=port,
                username=user,
                password=password
            )
            self.sftp = self.ssh_client.open_sftp()
            logging.info(f">>> Connection to sftp server successful on {host}:{port}")
        except Exception as e:
            logging.error(f">>> Failed to connect to SFTP server: {e}")
            raise


    def get_meta(self):
        """
        Get files meta from sftp server
        """
        meta = {}
        stack = [self.path]

        while stack:
            current_path = stack.pop()
            try:
                for entry in self.sftp.listdir_attr(current_path):
                    entry_path = f"{current_path}/{entry.filename}"
                    
                    try:
                        self.sftp.listdir(entry_path)
                        stack.append(entry_path)
                    except IOError:
                        meta[entry_path] = {
                            "size": entry.st_size,
                            "last_mod": entry.st_mtime
                        }
            except Exception as e:
                logging.error(f">>> Error while retrieving metadata for {current_path}: {e}")

        logging.info(f">>> Retrieved metadata for {len(meta)} files from SFTP")
        return meta
    

    def sftp_to_local(self, remote_path, local_path):
        """
        Get file from sftp file and load it to local dir
        """
        try:
            if not os.path.exists(os.path.dirname(local_path)):
                os.makedirs(os.path.dirname(local_path))
            
            logging.info(f">>> (TO LOCAL) Starting download from {remote_path} to {local_path}")
            self.sftp.get(remote_path, local_path)
            logging.info(f">>> (TO LOCAL) Downloaded {remote_path} to {local_path}")
        except Exception as e:
            logging.error(f">>> (TO LOCAL) Failed to download {remote_path}: {e}")
            raise
    
    def close_conn(self):
        """
        Close connection to sftp 
        """
        try: 
            self.sftp.close()
            self.ssh_client.close()
            logging.info(">>> Closed SFTP connection.")
        except Exception as e:
            logging.error(f">>> Failed to close SFTP connection: {e}")
            raise